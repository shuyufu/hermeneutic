#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "aggregator_service.hpp"
#include "bobby/hermeneutic/symbol_sync.hpp"
#include "http_client.hpp"
#include "net_traits.hpp"
#include "websocket_connection.hpp"

namespace bobby::hermeneutic::ingestion {

using bobby::hermeneutic::Side;
using bobby::hermeneutic::VenueId;
using bobby::hermeneutic::aggregator::SymbolBook;

// symbol -> SymbolBook*, built once at startup (e.g. from
// AggregatorService::book(symbol) for every symbol a VenueSession covers)
// and never modified afterward - no dynamic add/remove.
class SymbolRegistry {
  public:
    void add(SymbolId symbol, SymbolBook* book) { books_.emplace(std::move(symbol), book); }

    SymbolBook* book(const SymbolId& symbol) const {
        auto it = books_.find(symbol);
        return it != books_.end() ? it->second : nullptr;
    }

  private:
    std::unordered_map<SymbolId, SymbolBook*> books_;
};

// The I/O driver for one venue connection: owns a SymbolSync<Policy> per
// symbol it covers, drives them by feeding parsed WebSocket messages in,
// and executes whatever SyncAction(s) come back by calling the matching
// SymbolBook (via `registry`) or, for RequestSnapshot, performing the
// actual REST fetch. Reconnects with backoff on any drop, invalidating
// every covered symbol first.
//
// Templated the same way WebSocketConnection/http_get are: `NextLayer`
// picks plain TCP (tests, against a local server) or an SSL stream
// (production, wss://). `Feed`/`Policy` are per-exchange - see
// docs/ingestion_design.md.
template <typename Feed, typename Policy, typename NextLayer>
class VenueSession {
  public:
    VenueSession(Feed feed, VenueId venue, std::vector<SymbolId> symbols, SymbolRegistry registry,
                 net::ssl::context* ssl_ctx = nullptr)
        : feed_(std::move(feed)),
          venue_(std::move(venue)),
          symbols_(std::move(symbols)),
          registry_(std::move(registry)),
          ssl_ctx_(ssl_ctx) {
        for (const auto& symbol : symbols_) symbol_syncs_.try_emplace(symbol);
    }

    // Runs until cancelled: connect, subscribe, read/dispatch loop; on any
    // failure (including a failed first connect), invalidate every symbol
    // this session covers and reconnect after a backoff.
    net::awaitable<void> run() {
        int attempt = 0;
        while (true) {
            bool disconnected = false;
            try {
                auto executor = co_await net::this_coro::executor;
                auto connection = make_connection(executor);
                co_await connection.connect(feed_.ws_host(), feed_.ws_port(), feed_.ws_target());
                co_await connection.send(feed_.subscribe_message(symbols_));
                attempt = 0;

                for (auto& [symbol, sync] : symbol_syncs_) {
                    co_await execute_actions(symbol, sync.on_connected());
                }

                while (true) {
                    std::string text = co_await connection.read();
                    auto parsed = feed_.parse_message(text);
                    if (!parsed || !parsed->has_value()) continue;  // malformed or a control message

                    auto& message = **parsed;
                    if (auto* snapshot = std::get_if<SnapshotMessage>(&message)) {
                        co_await dispatch_snapshot(std::move(*snapshot));
                    } else {
                        co_await dispatch_depth_update(std::move(std::get<DepthUpdate>(message)));
                    }
                }
            } catch (const std::exception&) {
                // Connect failed, the read loop threw, or a coroutine we
                // co_await'ed inside it did - either way the connection is
                // gone and every symbol it carried is now untrustworthy.
                // co_await isn't allowed inside a catch handler itself
                // (coroutine suspension can't interleave with unwinding),
                // so just note it happened and act once outside the catch.
                disconnected = true;
            }

            if (disconnected) {
                for (auto& [symbol, sync] : symbol_syncs_) {
                    co_await execute_actions(symbol, sync.on_disconnected());
                }
            }

            ++attempt;
            co_await backoff(attempt);
        }
    }

  private:
    WebSocketConnection<NextLayer> make_connection(net::any_io_executor executor) {
        if constexpr (detail::is_ssl_stream_v<NextLayer>) {
            return WebSocketConnection<NextLayer>(executor, *ssl_ctx_);
        } else {
            return WebSocketConnection<NextLayer>(executor);
        }
    }

    net::awaitable<std::expected<std::string, std::errc>> fetch(std::string_view host,
                                                                  std::string_view port,
                                                                  std::string_view target) {
        auto executor = co_await net::this_coro::executor;
        if constexpr (detail::is_ssl_stream_v<NextLayer>) {
            co_return co_await http_get<NextLayer>(host, port, target, executor, *ssl_ctx_);
        } else {
            co_return co_await http_get<NextLayer>(host, port, target, executor);
        }
    }

    net::awaitable<void> execute_actions(const SymbolId& symbol, std::vector<SyncAction> actions) {
        for (auto& action : actions) co_await execute_action(symbol, std::move(action));
    }

    // Named coroutines for the two ParsedMessage alternatives, rather than
    // one generic lambda passed to std::visit - easier to read/step
    // through, and keeps std::get_if's dispatch (below) simple.
    net::awaitable<void> dispatch_snapshot(SnapshotMessage message) {
        SymbolId symbol = message.symbol;
        auto it = symbol_syncs_.find(symbol);
        if (it == symbol_syncs_.end()) co_return;  // not ours
        co_await execute_actions(symbol, it->second.on_snapshot(std::move(message)));
    }

    net::awaitable<void> dispatch_depth_update(DepthUpdate message) {
        SymbolId symbol = message.symbol;
        auto it = symbol_syncs_.find(symbol);
        if (it == symbol_syncs_.end()) co_return;  // not ours
        co_await execute_actions(symbol, it->second.on_depth_update(std::move(message)));
    }

    net::awaitable<void> execute_action(const SymbolId& symbol, SyncAction action) {
        if (std::holds_alternative<RequestSnapshot>(action)) {
            // Spawned rather than co_await'ed: the snapshot fetch can take
            // a while, and the whole point of buffering is that live
            // events keep being read and buffered *while it's in flight*,
            // not only after it returns. Awaiting it inline here would
            // starve the read loop below until the fetch completed, so
            // on_snapshot() would always find an empty buffer and nothing
            // to bridge with - see docs/ingestion_design.md.
            auto executor = co_await net::this_coro::executor;
            net::co_spawn(executor, handle_request_snapshot(symbol), [](std::exception_ptr e) {
                if (!e) return;
                try {
                    std::rethrow_exception(e);
                } catch (const std::exception& ex) {
                    std::cerr << "[venue_session] snapshot fetch failed: " << ex.what() << '\n';
                }
            });
            co_return;
        }

        // RequestSnapshot returned above before reaching here. Everything
        // below is synchronous (SymbolBook's own methods never suspend),
        // so this visitor is a plain function, not a coroutine - it must
        // not be declared to return net::awaitable<void>, since with no
        // co_await/co_return in its body it would fall off the end
        // without ever actually producing one (this was the real cause of
        // a SIGTRAP: Clang traps on falling off the end of a non-void,
        // non-coroutine function instead of silently returning garbage).
        std::visit(
            [this, &symbol](auto&& a) {
                using T = std::decay_t<decltype(a)>;
                if constexpr (std::is_same_v<T, ApplySnapshot>) {
                    if (auto* book = registry_.book(symbol)) {
                        book->apply_snapshot(venue_, Side::Bid, a.bids);
                        book->apply_snapshot(venue_, Side::Ask, a.asks);
                    }
                } else if constexpr (std::is_same_v<T, ApplyDelta>) {
                    // One call, not one apply_delta() per level: a.bids/
                    // a.asks together are everything one upstream exchange
                    // message carried, and apply_batch() is what keeps that
                    // one atomic update from becoming several separate seq
                    // bumps/broadcasts on our own wire protocol.
                    if (auto* book = registry_.book(symbol)) {
                        book->apply_batch(venue_, a.bids, a.asks);
                    }
                } else if constexpr (std::is_same_v<T, InvalidateVenue>) {
                    if (auto* book = registry_.book(symbol)) book->invalidate_venue(venue_);
                }
            },
            std::move(action));
    }

    // RequestSnapshot's fulfillment: for a Feed whose snapshot arrives over
    // REST (kSnapshotViaRest), fetch it and feed the result back into that
    // symbol's SymbolSync, executing whatever actions that in turn
    // produces. For a Feed where the exchange pushes its own snapshot over
    // the WebSocket instead, this is a no-op - nothing to fetch.
    //
    // `symbol` is taken *by value*, not by reference: this coroutine is
    // handed to co_spawn() (see execute_action()) rather than co_await'ed
    // directly, so it outlives its caller's stack/coroutine frame - a
    // reference parameter would dangle the moment that caller's frame is
    // destroyed, which happens well before this resumes from the
    // `co_await fetch(...)` below (the fetch takes real time; the caller
    // returns almost immediately after spawning it). Caught the hard way:
    // this was a genuine use-after-free that surfaced as a SIGTRAP crash.
    net::awaitable<void> handle_request_snapshot(SymbolId symbol) {
        if constexpr (!Feed::kSnapshotViaRest) {
            co_return;
        } else {
            auto spec = feed_.snapshot_request(symbol);
            auto body = co_await fetch(spec.host, spec.port, spec.target);
            if (!body) co_return;  // next reconnect or steady-state gap retries

            auto snapshot = feed_.parse_snapshot_response(symbol, *body);
            if (!snapshot) co_return;

            auto it = symbol_syncs_.find(symbol);
            if (it == symbol_syncs_.end()) co_return;
            co_await execute_actions(symbol, it->second.on_snapshot(std::move(*snapshot)));
        }
    }

    // Exponential-ish backoff with jitter before a reconnect attempt.
    // Shape (per-connection, jittered) is settled; the exact numbers below
    // are provisional - see docs/ingestion_design.md's open items.
    net::awaitable<void> backoff(int attempt) {
        auto base_ms = std::min(30'000, 500 * (1 << std::min(attempt, 6)));
        thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> jitter(0, base_ms / 5);
        auto delay = std::chrono::milliseconds(base_ms + jitter(rng));

        auto executor = co_await net::this_coro::executor;
        net::steady_timer timer(executor, delay);
        co_await timer.async_wait(net::use_awaitable);
    }

    Feed feed_;
    VenueId venue_;
    std::vector<SymbolId> symbols_;
    SymbolRegistry registry_;
    net::ssl::context* ssl_ctx_;
    std::unordered_map<SymbolId, SymbolSync<Policy>> symbol_syncs_;
};

}  // namespace bobby::hermeneutic::ingestion

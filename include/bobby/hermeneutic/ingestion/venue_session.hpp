#pragma once

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/cancellation_type.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iostream>
#include <list>
#include <optional>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/book/aggregate_order_book.hpp"
#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/ingestion/idle_timeout.hpp"
#include "bobby/hermeneutic/net/http_client.hpp"
#include "bobby/hermeneutic/net/net_traits.hpp"
#include "bobby/hermeneutic/net/websocket_connection.hpp"

namespace bobby::hermeneutic::ingestion {

using bobby::hermeneutic::Side;
using bobby::hermeneutic::VenueId;

// Logs `e` (if non-null) as "[component] action: <what>" - the shape a
// background coroutine's completion handler needs when an exception there
// should be reported but must never propagate further (a stopped session
// or a single failed snapshot fetch isn't a reason to crash the process).
// Shared by VenueSession's own snapshot-fetch handler and
// VenueSessionAdapter's start() handler (ingestion_runner.hpp) rather than
// each repeating the same rethrow/catch.
// Builds the whole line before the one std::cerr call below: with
// io_context::run() driven by more than one thread (see server_main.cpp's
// io_threads config), two concurrent calls here would otherwise interleave
// their chained << operators into a garbled line.
inline void log_exception(std::string_view component, std::string_view action, std::exception_ptr e) {
    if (!e) return;
    try {
        std::rethrow_exception(e);
    } catch (const std::exception& ex) {
        std::ostringstream line;
        line << "[" << component << "] " << action << ": " << ex.what() << '\n';
        std::cerr << line.str();
    }
}

// symbol -> Book*, built once at startup and never modified afterward - no
// dynamic add/remove.
//
// Templated on Book (duck-typed, like Feed/Policy below), not hardcoded to
// aggregator::SymbolBook: VenueSession only ever calls apply_snapshot/
// apply_batch/invalidate_venue on it (see execute_action() below), all
// three of which AggregateOrderBook (zero-dependency) already provides.
// Hardcoding SymbolBook here would force this header, and every
// ingestion-tier consumer, to pull in aggregator_service.hpp's gRPC/
// protobuf dependency just to route a parsed delta into a book. A
// production binary still supplies SymbolBook (see server_main.cpp) for
// its gRPC fan-out; a test that only cares about VenueSession's own
// mechanics can supply plain AggregateOrderBook instead.
template <typename Book>
class SymbolRegistry {
  public:
    void add(NativeSymbol symbol, Book* book) { books_.emplace(std::move(symbol), book); }

    Book* book(const NativeSymbol& symbol) const {
        auto it = books_.find(symbol);
        return it != books_.end() ? it->second : nullptr;
    }

  private:
    std::unordered_map<NativeSymbol, Book*> books_;
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
// (production, wss://). `Feed`/`Policy` are per-exchange. `Book` is the
// SymbolRegistry element type - see SymbolRegistry's own comment for why
// this isn't hardcoded to aggregator::SymbolBook.
template <typename Feed, typename Policy, typename NextLayer, typename Book>
class VenueSession {
  public:
    // Not tuned to any particular venue - see WebSocketConnection::
    // connect()'s own comment for what this guards against (passed
    // straight through to it below). A caller (e.g. server_main.cpp, from
    // book_subscription.hpp's IdleTimeoutConfig) can pass a longer
    // per-venue value via the constructor's own parameter. Shared with
    // IdleTimeoutConfig's own default - see idle_timeout.hpp for why
    // that's a single definition, not two.
    VenueSession(Feed feed, VenueId venue, std::vector<NativeSymbol> symbols, SymbolRegistry<Book> registry,
                 net::any_io_executor executor, net::ssl::context* ssl_ctx = nullptr,
                 std::chrono::seconds idle_timeout = kDefaultIdleTimeout)
        : feed_(std::move(feed)),
          venue_(std::move(venue)),
          symbols_(std::move(symbols)),
          registry_(std::move(registry)),
          ssl_ctx_(ssl_ctx),
          idle_timeout_(idle_timeout),
          strand_(net::make_strand(executor)) {
        for (const auto& symbol : symbols_) symbol_syncs_.try_emplace(symbol);
    }

    const VenueId& venue() const { return venue_; }

    // Precondition (not enforced at runtime): call at most once per
    // session. A second call would silently orphan the run() already in
    // flight - stop() would then only ever reach the new one. `on_done`
    // fires when run() itself returns (after any in-flight snapshot fetch
    // has drained via stop()), not when stop() returns.
    //
    // The net::post is required, not just an optimization alongside the
    // stopping_ check below: binding run_sig_ directly inside co_spawn
    // would let a stop() already queued on strand_ abort run() before its
    // body executes a single line. Posting the bind onto strand_ instead
    // orders it after any pending stop(), so run() always starts from a
    // fresh, uncancelled state.
    template <typename CompletionHandler>
    void start(CompletionHandler&& on_done) {
        net::post(strand_, [this, on_done = std::forward<CompletionHandler>(on_done)]() mutable {
            if (stopping_) {
                on_done(nullptr);
                return;
            }
            net::co_spawn(strand_, run(),
                          net::bind_cancellation_slot(run_sig_.slot(), std::move(on_done)));
        });
    }

    // Requests a full stop: aborts the in-flight read/backoff wait and
    // every in-flight snapshot fetch, invalidates every covered symbol one
    // last time (the same on_disconnected() contract as any other drop),
    // and returns without reconnecting - run() only actually finishes once
    // every in-flight snapshot fetch has drained (see
    // drain_pending_snapshots()), so it's safe to destroy this session as
    // soon as start()'s on_done fires.
    //
    // Safe to call from any thread: the flag + both signals' emit() happen
    // on this session's strand via net::post rather than directly, since
    // emit() isn't documented as thread-safe with respect to the
    // operation(s) it cancels, and stopping_/snapshot_sigs_ are otherwise
    // only ever touched from the strand.
    void stop() {
        net::post(strand_, [this] {
            stopping_ = true;
            run_sig_.emit(net::cancellation_type::terminal);
            // A plain range-for is safe here only because emit() can
            // synchronously resume the cancelled operation to completion
            // (confirmed for the WS read/timer case; not specifically
            // ruled out for the http_get resolve/connect/read chain a
            // snapshot fetch runs through), and its completion handler
            // (execute_action()'s RequestSnapshot branch) defers
            // snapshot_sigs_.erase() with its own net::post instead of
            // erasing inline - nothing can mutate snapshot_sigs_ while
            // this loop is still walking it.
            for (auto& sig : snapshot_sigs_) sig.emit(net::cancellation_type::terminal);
        });
    }

  private:
    // Runs until stop()ed: connect, subscribe, read/dispatch loop; on any
    // failure (including a failed first connect, or stop() aborting the
    // read or the backoff wait below), invalidate every symbol this
    // session covers. Reconnects after a backoff, unless stopping_ is set,
    // in which case it drains any in-flight snapshot fetch and returns for
    // good instead.
    net::awaitable<void> run() {
        int attempt = 0;
        while (true) {
            // Defense in depth, not the primary guard: start() already
            // refuses to spawn run() at all once stopping_ is set. This
            // covers whatever narrow window sits between run()'s first
            // resumption and that abort taking effect.
            if (stopping_) co_return;

            // Set only once connect+subscribe succeed (below); read again
            // near the backoff decision past the catch, which is what
            // decides whether this connection counted as healthy.
            std::optional<std::chrono::steady_clock::time_point> connected_at;

            try {
                auto executor = co_await net::this_coro::executor;
                auto connection = make_connection(executor);
                co_await connection.connect(feed_.ws_host(), feed_.ws_port(), feed_.ws_target(), idle_timeout_);
                co_await connection.send(feed_.subscribe_message(symbols_));
                connected_at = std::chrono::steady_clock::now();

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
                // The only way out of the try block above (connect
                // failure, a thrown read, or stop()'s terminal
                // cancellation surfacing as connection.read() throwing
                // operation_aborted). Either way the connection is gone,
                // so invalidation below runs unconditionally; stopping_,
                // not the exception type, is what tells a real drop apart
                // from stop() below.
            }

            // Must happen here, in run()'s own frame, immediately before
            // the call - not inside invalidate_all() itself. Entering any
            // nested co_await'ed coroutine while the ambient cancellation
            // state is already latched throws before the callee's body
            // runs at all, so a reset at the top of the callee is too
            // late under Boost.Asio's cancellation model.
            co_await net::this_coro::reset_cancellation_state();
            try {
                co_await invalidate_all();
            } catch (const std::exception&) {
                // invalidate_venue()/apply_*() only touch in-memory state
                // and shouldn't normally throw, but if one ever does, the
                // stopping_ check and drain_pending_snapshots() below
                // still have to run - an uncaught exception here would
                // escape run() entirely while a snapshot fetch might
                // still be in flight.
            }

            if (stopping_) break;

            // Only reset the backoff counter if the connection that just
            // ended actually stayed up for a while (connected_at is set
            // only once connect+subscribe succeed, so a failed connect
            // never resets `attempt` either). Resetting unconditionally on
            // every successful connect would let a venue that force-
            // reconnects right after every connect (e.g. a
            // kTrustsConnectionOrder venue whose SymbolSync keeps
            // reporting InvalidateVenue) hammer the exchange at a fixed
            // rate instead of backing off.
            if (connected_at && std::chrono::steady_clock::now() - *connected_at >= kMinHealthyUptime) {
                attempt = 0;
            }
            ++attempt;
            try {
                co_await backoff(attempt);
            } catch (const std::exception&) {
                // stop() cancelled the backoff wait itself (no connection
                // was open to drop) - fall through to the stopping_ check
                // below instead of looping back to reconnect. Left
                // uncaught, this would escape run() entirely and skip
                // draining snapshot_sigs_ below.
            }
            if (stopping_) break;
        }

        // Only reachable via stopping_, so this is the one place run()
        // returns for good. A handle_request_snapshot() spawned before
        // stop() may still be in flight - its coroutine frame holds a
        // `this` pointing at this session until it unwinds, so returning
        // before it drains would let whatever destroys this session next
        // do so while that frame is still pending.
        //
        // run_sig_'s terminal emit() (that's how this point is reached)
        // leaves the ambient cancellation state latched, so the reset must
        // happen here, in run()'s own frame, immediately before this call -
        // resetting inside drain_pending_snapshots() itself would be too
        // late (see the same reset earlier in this function).
        co_await net::this_coro::reset_cancellation_state();
        co_await drain_pending_snapshots();
    }

    // Invalidates every covered symbol - the on_disconnected() contract
    // any drop gets, whether that drop was a genuine failure or stop()'s
    // own cancellation of the read/connect that was in flight.
    net::awaitable<void> invalidate_all() {
        for (auto& [symbol, sync] : symbol_syncs_) {
            co_await execute_actions(symbol, sync.on_disconnected());
        }
    }

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

    // Executes every action in `actions`, always - never stops partway
    // through just because an earlier one triggered a nested resync (see
    // execute_action()'s own apply_failed branch) - and returns whether any
    // of them, directly or via that nested resync, was InvalidateVenue.
    // The caller decides what a true result means; execute_actions() itself
    // has no policy-specific behavior.
    net::awaitable<bool> execute_actions(const NativeSymbol& symbol, std::vector<SyncAction> actions) {
        bool gap_detected = false;
        for (auto& action : actions) {
            if (co_await execute_action(symbol, std::move(action))) gap_detected = true;
        }
        co_return gap_detected;
    }

    // The kTrustsConnectionOrder == false counterpart to
    // execute_actions_and_maybe_force_reconnect()'s forced-reconnect
    // branch below: re-requests a snapshot directly, since VenueSession
    // never forces a reconnect for these venues and on_connected() - the
    // only other thing that emits RequestSnapshot - only fires on a real
    // reconnect. Mirrors on_connected()'s own "Buffering and not yet
    // requested" logic; that decision belongs only here (and in
    // execute_action()'s apply_failed branch below) - SymbolSync must not
    // duplicate it internally.
    net::awaitable<void> resync_rest_venue_after_gap(const NativeSymbol& symbol) {
        if (auto it = symbol_syncs_.find(symbol); it != symbol_syncs_.end()) {
            co_await execute_actions(symbol, it->second.on_connected());
        }
    }

    // Executes `actions`, then - only for a SequencePolicy that trusts the
    // WebSocket connection's own ordering to deliver its snapshot as the
    // first message (kTrustsConnectionOrder) - forces this whole
    // connection to drop and reconnect if any of them was InvalidateVenue.
    // Necessary because on_connected() is the only thing that ever
    // re-requests a snapshot, and kTrustsConnectionOrder implies
    // kSnapshotViaRest == false (see symbol_sync.hpp), so RequestSnapshot
    // on an already-open connection is a no-op: without a genuine
    // reconnect, a gapped symbol would buffer live events forever.
    //
    // Throws rather than adding a dedicated SyncAction so this reuses
    // run()'s existing reconnect machinery verbatim - every symbol sharing
    // this connection needs the same fresh start a real disconnect gives
    // them, since one ordered stream per connection is the whole premise
    // kTrustsConnectionOrder relies on.
    //
    // Only usable by a caller directly co_await'ed from inside run()'s own
    // try block (dispatch_snapshot()/dispatch_depth_update()) - the throw
    // needs run()'s try/catch to reach it. handle_request_snapshot() runs
    // detached, so it can't use this; it has its own near-identical
    // wrapper below that emits instead of throwing.
    net::awaitable<void> execute_actions_and_maybe_force_reconnect(const NativeSymbol& symbol,
                                                                     std::vector<SyncAction> actions) {
        bool gap_detected = co_await execute_actions(symbol, std::move(actions));
        if (!gap_detected) co_return;
        if constexpr (Policy::kTrustsConnectionOrder) {
            throw std::runtime_error(
                "SymbolSync gap for a kTrustsConnectionOrder venue - forcing full reconnect");
        } else {
            co_await resync_rest_venue_after_gap(symbol);
        }
    }

    // Named coroutines for the two ParsedMessage alternatives, rather than
    // one generic lambda passed to std::visit - easier to read/step
    // through, and keeps std::get_if's dispatch (below) simple.
    net::awaitable<void> dispatch_snapshot(SnapshotMessage message) {
        NativeSymbol symbol = message.symbol;
        auto it = symbol_syncs_.find(symbol);
        if (it == symbol_syncs_.end()) co_return;  // not ours
        co_await execute_actions_and_maybe_force_reconnect(symbol, it->second.on_snapshot(std::move(message)));
    }

    net::awaitable<void> dispatch_depth_update(DepthUpdate message) {
        NativeSymbol symbol = message.symbol;
        auto it = symbol_syncs_.find(symbol);
        if (it == symbol_syncs_.end()) co_return;  // not ours
        co_await execute_actions_and_maybe_force_reconnect(symbol, it->second.on_depth_update(std::move(message)));
    }

    // Returns whether executing `action` means this symbol just saw
    // InvalidateVenue - directly (the action itself) or via the nested
    // resync the apply_failed branch below triggers. execute_actions()
    // aggregates this across a whole batch; only its caller decides what a
    // true result should do (see execute_actions_and_maybe_force_reconnect()
    // and handle_request_snapshot()).
    net::awaitable<bool> execute_action(const NativeSymbol& symbol, SyncAction action) {
        if (std::holds_alternative<RequestSnapshot>(action)) {
            // Spawned rather than co_await'ed: buffering only works if
            // live events keep being read *while the fetch is in flight*.
            // Awaiting it inline here would starve the read loop below
            // until the fetch completed, so on_snapshot() would always
            // find an empty buffer.
            //
            // Don't start a new one once stop() has been requested: its
            // result could never reach a live connection, so it would
            // just be one more thing drain_pending_snapshots() waits out.
            if (stopping_) co_return false;

            // Its own cancellation_signal, not run_sig_: a
            // cancellation_slot only forwards to the most recently bound
            // operation, so sharing run_sig_ here would silently steal
            // run()'s own cancellation registration the moment a second
            // snapshot fetch was spawned, and clobber it again for every
            // fetch after that. snapshot_sigs_ is a std::list so this
            // iterator, captured below for the fetch's own completion
            // handler to erase itself with, stays valid across
            // insertions/erasures of every other element.
            auto sig_it = snapshot_sigs_.emplace(snapshot_sigs_.end());
            auto executor = co_await net::this_coro::executor;
            net::co_spawn(
                executor, handle_request_snapshot(symbol),
                net::bind_cancellation_slot(sig_it->slot(), [this, sig_it](std::exception_ptr e) {
                    // Deferred via net::post, not erased inline: this
                    // handler runs on strand_, and stop()'s emit() loop
                    // over snapshot_sigs_ can synchronously resume this
                    // very coroutine to completion. Erasing sig_it
                    // reentrantly, mid-loop, would mutate the list stop()
                    // is still walking; posting the erase guarantees it
                    // only runs once the current strand task has finished.
                    net::post(strand_, [this, sig_it] { snapshot_sigs_.erase(sig_it); });
                    log_exception("venue_session", "snapshot fetch failed", e);
                }));
            co_return false;
        }

        // RequestSnapshot returned above before reaching here. Everything
        // below is synchronous (SymbolBook's own methods never suspend),
        // so this visitor must stay a plain function, not be declared to
        // return net::awaitable<void>: with no co_await/co_return in its
        // body it would fall off the end without producing one, which
        // traps (SIGTRAP) rather than silently returning garbage.
        //
        // `apply_failed`, set inside the visitor and checked after it
        // returns, is how a book-level rejection (malformed venue data,
        // not a sequence gap SymbolSync would have already caught) reaches
        // the resync logic below without the visitor itself co_awaiting
        // anything. apply_snapshot()/apply_batch() each validate every
        // level across both sides before applying any of them, so a
        // rejection here means nothing from this action was applied - not
        // a case of one side landing while the other didn't (see
        // AggregateOrderBook::apply_snapshot()'s own doc comment).
        //
        // Captured before std::visit() below moves out of `action`:
        // holds_alternative() only inspects the active alternative, not the
        // value, but checking it before the move keeps that non-dependency
        // obvious rather than relying on it.
        bool is_invalidate = std::holds_alternative<InvalidateVenue>(action);

        bool apply_failed = false;
        std::visit(
            [this, &symbol, &apply_failed](auto&& a) {
                using T = std::decay_t<decltype(a)>;
                if constexpr (std::is_same_v<T, ApplySnapshot>) {
                    if (auto* book = registry_.book(symbol)) {
                        apply_failed = !book->apply_snapshot(venue_, a.bids, a.asks);
                    }
                } else if constexpr (std::is_same_v<T, ApplyDelta>) {
                    // One call, not one per level: a.bids/a.asks together
                    // are everything one upstream exchange message carried,
                    // and apply_batch() is what keeps that one atomic
                    // update from becoming several separate seq bumps/
                    // broadcasts on our own wire protocol.
                    if (auto* book = registry_.book(symbol)) {
                        apply_failed = !book->apply_batch(venue_, a.bids, a.asks);
                    }
                } else if constexpr (std::is_same_v<T, InvalidateVenue>) {
                    if (auto* book = registry_.book(symbol)) book->invalidate_venue(venue_);
                }
            },
            std::move(action));

        if (!apply_failed) co_return is_invalidate;

        // A rejected level means this venue's data for `symbol` can no
        // longer be trusted, so this goes through
        // SymbolSync::on_disconnected() exactly like a real drop does.
        // What a kTrustsConnectionOrder == false venue does in response is
        // decided once, centrally, by resync_rest_venue_after_gap() at the
        // outermost caller below - not duplicated into SymbolSync itself.
        // Logged here, not inside the visitor, to keep I/O out of it (see
        // log_exception).
        // Single std::cerr call, same reasoning as log_exception() above:
        // two VenueSessions on different io_threads can hit this at once.
        {
            std::ostringstream line;
            line << "[venue_session] " << symbol
                 << ": rejected level(s) from this venue (malformed data) - invalidating and "
                    "resyncing\n";
            std::cerr << line.str();
        }
        // Recurses into execute_action() exactly one level deep, not
        // further: on_disconnected() only ever returns {InvalidateVenue{}},
        // whose own branch above never sets apply_failed. Routed through
        // the plain execute_actions(), not the throwing
        // execute_actions_and_maybe_force_reconnect(): this call can be
        // reached partway through the *outer* batch execute_actions() is
        // still iterating, and throwing here would abort that outer loop
        // and drop whatever's still queued after this action. Instead this
        // just reports the gap upward, and the outermost caller decides
        // once, after every action in the batch has run, whether to force
        // a reconnect or resync in place.
        if (auto it = symbol_syncs_.find(symbol); it != symbol_syncs_.end()) {
            bool nested_gap = co_await execute_actions(symbol, it->second.on_disconnected());
            co_return nested_gap;
        }
        co_return false;
    }

    // RequestSnapshot's fulfillment: for a Feed whose snapshot arrives over
    // REST (kSnapshotViaRest), fetch it and feed the result back into that
    // symbol's SymbolSync, executing whatever actions that in turn
    // produces. For a Feed where the exchange pushes its own snapshot over
    // the WebSocket instead, this is a no-op - nothing to fetch.
    //
    // `symbol` is taken *by value*, not by reference: this coroutine is
    // handed to co_spawn() rather than co_await'ed directly, so it outlives
    // its caller's coroutine frame - a reference parameter would dangle
    // once that frame is destroyed, well before this resumes from
    // `co_await fetch(...)` below.
    net::awaitable<void> handle_request_snapshot(NativeSymbol symbol) {
        if constexpr (!Feed::kSnapshotViaRest) {
            co_return;
        } else {
            auto spec = feed_.snapshot_request(symbol);
            std::expected<std::string, std::errc> body;
            // Retries the fetch itself in place (same connection, no WS
            // involvement) rather than giving up after one failure: if
            // this very first snapshot fetch fails, the symbol never
            // reaches Live at all (on_depth_update() while Buffering just
            // buffers, with no gap check), so nothing but an unrelated
            // transport-level disconnect would otherwise ever re-request.
            //
            // Deliberately not run_sig_.emit() (the kTrustsConnectionOrder
            // == true treatment below): kSnapshotViaRest == true implies
            // kTrustsConnectionOrder == false for every venue that reaches
            // this branch, and a REST venue's fix for a failed fetch is to
            // retry in place, not tear down the whole connection over one
            // HTTP failure. Reuses backoff(), the same helper run()'s own
            // reconnect loop uses.
            for (int attempt = 0;; ++attempt) {
                if (stopping_) co_return;
                body = co_await fetch(spec.host, spec.port, spec.target);
                if (body) break;
                co_await backoff(attempt);
            }

            // stop() emits on this fetch's own cancellation_signal too
            // (see stop()), but if that cancellation doesn't land before
            // this response arrives, the result must still be discarded:
            // invalidate_all() may have already invalidated this venue's
            // contribution for this shutdown, and applying a snapshot
            // after that would silently revive it.
            if (stopping_) co_return;

            auto snapshot = feed_.parse_snapshot_response(symbol, *body);
            if (!snapshot) co_return;

            auto it = symbol_syncs_.find(symbol);
            if (it == symbol_syncs_.end()) co_return;
            // Can't use execute_actions_and_maybe_force_reconnect() here -
            // it throws to reach run()'s try/catch, but this coroutine is
            // net::co_spawn'ed detached, not co_await'ed by run(). A throw
            // here would only reach this spawn's own completion handler,
            // which logs and discards it rather than forcing a reconnect.
            //
            // A no-op today for the kTrustsConnectionOrder == true branch
            // (that combination never occurs in this codebase, so
            // gap_detected is always false here), kept working anyway for
            // a future venue that combines both flags. The
            // kTrustsConnectionOrder == false branch is live: this is how
            // a gap discovered while resyncing gets a second snapshot
            // fetch requested for a REST venue.
            bool gap_detected = co_await execute_actions(symbol, it->second.on_snapshot(std::move(*snapshot)));
            if (!gap_detected) co_return;
            if constexpr (Policy::kTrustsConnectionOrder) {
                // Same outcome as the thrown exception at every other call
                // site, reached the other way: this coroutine runs on
                // strand_ itself (the executor it was spawned with, in
                // execute_action()), the same strand stop() emits
                // run_sig_ from - so emitting directly here is exactly as
                // safe as stop()'s own emit() call.
                run_sig_.emit(net::cancellation_type::terminal);
            } else {
                co_await resync_rest_venue_after_gap(symbol);
            }
        }
    }

    // How long a connection has to stay up before run() treats it as
    // "was healthy" and resets the backoff counter on its next reconnect -
    // see run()'s own use of this against connected_at. Provisional, same
    // as backoff()'s own numbers below.
    static constexpr std::chrono::seconds kMinHealthyUptime{10};

    // Exponential-ish backoff with jitter before a reconnect attempt. The
    // exact numbers below are provisional.
    net::awaitable<void> backoff(int attempt) {
        auto base_ms = std::min(30'000, 500 * (1 << std::min(attempt, 6)));
        thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<int> jitter(0, base_ms / 5);
        auto delay = std::chrono::milliseconds(base_ms + jitter(rng));

        auto executor = co_await net::this_coro::executor;
        net::steady_timer timer(executor, delay);
        co_await timer.async_wait(net::use_awaitable);
    }

    // Polls until every in-flight handle_request_snapshot() has unwound.
    // stop() cancels them, but cancellation only takes effect at their
    // next suspension point, so run() awaits this before returning - the
    // caller of start()'s on_done must never see "done" while one of those
    // coroutines still holds a `this` pointing at this session. A short
    // poll rather than an event/condition variable: these are single HTTP
    // GETs already being cancelled, so the wait is at most a couple of
    // ticks. Caller (run()) must reset the ambient cancellation state
    // immediately before calling this. net::redirect_error on each poll
    // wait besides, as a second line of defense in case a stop() lands
    // during this loop.
    //
    // snapshot_sigs_.empty() is the drain condition directly, not a
    // separate counter that could drift out of sync with it.
    net::awaitable<void> drain_pending_snapshots() {
        while (!snapshot_sigs_.empty()) {
            auto executor = co_await net::this_coro::executor;
            net::steady_timer poll(executor, std::chrono::milliseconds(5));
            boost::system::error_code ec;
            co_await poll.async_wait(net::redirect_error(net::use_awaitable, ec));
        }
    }

    Feed feed_;
    VenueId venue_;
    std::vector<NativeSymbol> symbols_;
    SymbolRegistry<Book> registry_;
    net::ssl::context* ssl_ctx_;
    // Passed to WebSocketConnection::connect() on every (re)connect - see
    // its own doc comment for why this exists (WS idle-read detection) and
    // idle_timeout.hpp's kDefaultIdleTimeout for the default this
    // constructor falls back to when a caller doesn't override it.
    std::chrono::seconds idle_timeout_;
    std::unordered_map<NativeSymbol, SymbolSync<Policy>> symbol_syncs_;

    // Everything below is only ever touched while running on strand_ (run()
    // itself, its dispatch/execute_action helpers, and every
    // handle_request_snapshot() spawn all run on it - see start()) or via
    // net::post(strand_, ...) from stop(), so none of it needs its own
    // synchronization despite stop() being callable from any thread.
    net::strand<net::any_io_executor> strand_;
    net::cancellation_signal run_sig_;
    std::list<net::cancellation_signal> snapshot_sigs_;
    // Two independently-maintained "we're shutting down" mechanisms, not
    // one: stopping_ answers "should new work start" (checked at the top
    // of run()'s loop and in execute_action()'s RequestSnapshot branch),
    // while run_sig_/snapshot_sigs_ answer "abort whatever's in flight
    // right now" (emitted in stop()). A future kind of spawned background
    // operation beyond the read/backoff loop and snapshot fetches would
    // need to remember to wire in *both* by hand - there's no single
    // primitive today that covers "don't start" and "abort in flight" at
    // once, so it's easy to add one and forget the other.
    bool stopping_ = false;
};

}  // namespace bobby::hermeneutic::ingestion

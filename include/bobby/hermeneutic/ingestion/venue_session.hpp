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
#include <random>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/net/http_client.hpp"
#include "bobby/hermeneutic/net/net_traits.hpp"
#include "bobby/hermeneutic/net/websocket_connection.hpp"
#include "bobby/hermeneutic/service/aggregator_service.hpp"

namespace bobby::hermeneutic::ingestion {

using bobby::hermeneutic::Side;
using bobby::hermeneutic::VenueId;
using bobby::hermeneutic::aggregator::SymbolBook;

// Logs `e` (if non-null) as "[component] action: <what>" - the shape a
// background coroutine's completion handler needs when an exception there
// should be reported but must never propagate further (a stopped session
// or a single failed snapshot fetch isn't a reason to crash the process).
// Shared by VenueSession's own snapshot-fetch handler and
// VenueSessionAdapter's start() handler (ingestion_runner.hpp) rather than
// each repeating the same rethrow/catch.
inline void log_exception(std::string_view component, std::string_view action, std::exception_ptr e) {
    if (!e) return;
    try {
        std::rethrow_exception(e);
    } catch (const std::exception& ex) {
        std::cerr << "[" << component << "] " << action << ": " << ex.what() << '\n';
    }
}

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
                 net::any_io_executor executor, net::ssl::context* ssl_ctx = nullptr)
        : feed_(std::move(feed)),
          venue_(std::move(venue)),
          symbols_(std::move(symbols)),
          registry_(std::move(registry)),
          ssl_ctx_(ssl_ctx),
          strand_(net::make_strand(executor)) {
        for (const auto& symbol : symbols_) symbol_syncs_.try_emplace(symbol);
    }

    const VenueId& venue() const { return venue_; }

    // Spawns run() on this session's own strand, bound to run_sig_ so
    // stop() can actually abort an in-flight read or backoff wait rather
    // than only asking cooperatively. `on_done` fires once run() actually
    // returns - i.e. once shutdown (including draining any in-flight
    // snapshot fetch, see stop()) is fully complete, not when stop() itself
    // returns.
    //
    // Precondition: call at most once per session. A second call would
    // bind_cancellation_slot() a fresh run_sig_ registration on top of the
    // first (a slot only forwards to the most recently bound operation -
    // the same reasoning behind giving each snapshot fetch its own signal
    // below), silently orphaning whichever run() was already in flight:
    // stop() would then only ever reach the second one. Not enforced at
    // runtime - IngestionRunner's only caller today calls this exactly
    // once per session by construction (see add()) - but a future caller
    // adding e.g. a retry/reload path must not call start() again on a
    // session that already has one.
    //
    // Deferred through net::post rather than co_spawn'ing directly here -
    // this is the actual fix, not the stopping_ check below. Binding
    // run_sig_'s slot to a coroutine (what co_spawn + bind_cancellation_slot
    // does) and then having stop() emit() on it *before* that coroutine has
    // taken its first step aborts the whole co_spawn'd operation before
    // run()'s body ever executes a single line (verified empirically two
    // ways: a stopping_ check at the top of run()'s own loop never even
    // runs in that case; reverting just this net::post back to a direct
    // co_spawn() call, with the stopping_ check below left in place, still
    // reproduces the failure). Posting the bind itself onto strand_ orders
    // it against any stop() already queued there - if stop() ran first,
    // run_sig_.emit() lands on an unbound slot (a true no-op), so binding
    // afterward starts run() from a genuinely fresh, uncancelled state.
    // The stopping_ check is then just an optimization on top of that
    // correct ordering: no need to spawn a coroutine at all if we already
    // know it would immediately co_return - see
    // docs/ingestion_design.md 第 10 節第 2 項 for the full account.
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
            // A plain range-for is safe here only because the fetches'
            // completion handler (see execute_action()'s RequestSnapshot
            // branch) defers its snapshot_sigs_.erase() with its own
            // net::post rather than erasing inline: emit() can
            // synchronously resume the cancelled operation to completion
            // (confirmed for the WS read/timer case, not specifically
            // ruled out for the http_get resolve/connect/read chain a
            // snapshot fetch runs through), and a `cancellation_signal`
            // can't be copied or moved out of harm's way first - the only
            // thing that actually keeps this loop safe against a
            // reentrant erase of *any* node (not just the one currently
            // being visited) is that nothing can erase from snapshot_sigs_
            // while this task is still running on the strand.
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
            // Defense in depth, not the primary guard against "stop()
            // before this coroutine has properly started" - start() itself
            // already refuses to spawn run() at all once stopping_ is set
            // (see there for why: emitting on an already-bound-but-never-
            // resumed coroutine aborts it before this line would ever run
            // anyway). This check exists for whatever narrow window, if
            // any, sits between run()'s first resumption actually
            // beginning and that same abort taking effect.
            if (stopping_) co_return;

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
                // The inner while(true) above has no break/return of its
                // own, so the only way this try block is ever left is
                // through this catch - connect failed, the read loop
                // threw, or a coroutine we co_await'ed inside it did.
                // Either way the connection is gone and every symbol it
                // carried is now untrustworthy; co_await isn't allowed
                // inside a catch handler itself (coroutine suspension
                // can't interleave with unwinding), so invalidation
                // happens once, unconditionally, right below instead of
                // behind a "did we actually disconnect" flag that would
                // never actually be false here. stop()'s terminal
                // cancellation surfaces exactly the same way
                // (connection.read() throwing operation_aborted), which is
                // why it's stopping_ - not a separate exception type -
                // that tells the two apart below.
            }

            // Must happen here, in run()'s own frame, immediately before
            // the call - not as invalidate_all()'s own first statement.
            // Verified the hard way: entering *any* nested co_await'ed
            // coroutine while the caller's ambient cancellation state is
            // already latched throws immediately, before the callee's own
            // body - including a reset sitting at its top - ever runs. A
            // "self-resetting helper" looks structurally safer but doesn't
            // actually work under Boost.Asio's cancellation model; see
            // docs/ingestion_design.md 第 10 節第 2 項 for the full account
            // of both directions of this mistake.
            co_await net::this_coro::reset_cancellation_state();
            try {
                co_await invalidate_all();
            } catch (const std::exception&) {
                // invalidate_venue()/apply_*() only touch in-memory
                // SymbolBook state and shouldn't normally throw, but if
                // one ever does (e.g. bad_alloc), the stopping_ check and
                // drain_pending_snapshots() below still have to run - an
                // uncaught exception here would otherwise escape run()
                // entirely, taking the "safe to destroy this session once
                // on_done fires" contract with it while a snapshot fetch
                // might still be in flight (第 7 節坑 2, again).
            }

            if (stopping_) break;

            ++attempt;
            try {
                co_await backoff(attempt);
            } catch (const std::exception&) {
                // stop() cancelled the backoff wait itself (no connection
                // was even open to drop) - fall through to the stopping_
                // check below instead of looping back to reconnect. Without
                // this, a stop() that lands mid-backoff would be silently
                // ignored: the timer is what stop() actually cancelled, so
                // if this exception weren't caught here it would escape
                // run() entirely and skip draining snapshot_sigs_ below.
            }
            if (stopping_) break;
        }

        // Only reachable via stopping_, so this is the one place run()
        // returns for good rather than looping back to reconnect. A
        // handle_request_snapshot() spawned before stop() was called may
        // still be in flight (see execute_action()'s RequestSnapshot
        // branch) - it was handed the same cancellation treatment as the
        // read/backoff above, but until it has actually unwound, its
        // coroutine frame still holds a `this` pointing at this session.
        // Returning before it drains would let whatever destroys this
        // session next (IngestionRunner::stop_all()'s caller) do so while
        // that frame is still pending - the exact use-after-free
        // docs/ingestion_design.md's 第 7 節坑 2 already describes, just at
        // a different trigger.
        // run_sig_ has already emit()ed terminal by this point (that's how
        // we got here), which - as established above - leaves the ambient
        // cancellation state latched. The reset has to happen here, in
        // run()'s own frame, immediately before this call - resetting
        // inside drain_pending_snapshots() itself would be too late, for
        // the same reason explained at the reset above.
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
            //
            // Don't start a new one once stop() has been requested: a
            // fetch begun after that point would just be one more thing
            // drain_pending_snapshots() has to wait out below, for no
            // benefit (its result can never reach a live connection).
            if (stopping_) co_return;

            // Its own cancellation_signal, not run_sig_: a
            // cancellation_slot only forwards to the most recently bound
            // operation, so sharing run_sig_ here would silently steal
            // run()'s own cancellation registration the moment a second
            // snapshot fetch (a second symbol also gapped, say) was
            // spawned - and clobber it again for every fetch after that.
            // snapshot_sigs_ is a std::list so this iterator - captured
            // below for the fetch's own completion handler to erase itself
            // with - stays valid across insertions/erasures of every other
            // element.
            auto sig_it = snapshot_sigs_.emplace(snapshot_sigs_.end());
            auto executor = co_await net::this_coro::executor;
            net::co_spawn(
                executor, handle_request_snapshot(symbol),
                net::bind_cancellation_slot(sig_it->slot(), [this, sig_it](std::exception_ptr e) {
                    // Deferred via net::post, not erased inline: this
                    // handler runs on strand_ (same as everything else
                    // here - see execute_action()'s own executor lookup
                    // above), and stop()'s emit() loop over snapshot_sigs_
                    // can synchronously resume this very coroutine to
                    // completion. Erasing sig_it reentrant, mid-loop,
                    // would mutate the list stop() (or a sibling
                    // completion handler) is still walking. Posting the
                    // erase guarantees it only runs once the current
                    // strand task has fully finished - snapshot_sigs_ is
                    // then never mutated while anything is iterating it.
                    net::post(strand_, [this, sig_it] { snapshot_sigs_.erase(sig_it); });
                    log_exception("venue_session", "snapshot fetch failed", e);
                }));
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

            // stop() emits on this fetch's own cancellation_signal too
            // (see stop()), but per-op cancellation for the resolve/
            // connect/read chain fetch() runs through hasn't been
            // specifically verified the way the WS read/timer path has -
            // if it doesn't land before this response arrives, this
            // result must still be discarded rather than applied:
            // invalidate_all() may have already invalidated this venue's
            // contribution for this exact shutdown, and applying a
            // snapshot after that would silently revive it.
            if (stopping_) co_return;

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

    // Polls until every in-flight handle_request_snapshot() spawned by
    // execute_action() has actually unwound. stop() cancels them, but
    // cancellation only takes effect at their next suspension point - this
    // is what run() awaits before actually returning, so the caller of
    // start()'s on_done never sees "done" while one of those coroutines
    // still holds a `this` pointing at this session. A short poll rather
    // than a proper async event/condition variable: these fetches are
    // single HTTP GETs already being cancelled, so the wait is expected to
    // be at most a couple of poll ticks, and this session has no other use
    // for a general-purpose async wait primitive.
    //
    // Caller (run()) must reset the ambient cancellation state immediately
    // before calling this - see the comment on that call. net::redirect_error
    // on each poll wait besides, as a second line of defense: if a future
    // stop() lands *during* this loop (unlikely - stopping_ is already
    // true and nothing schedules another emit - but not provably
    // impossible), an aborted poll tick just becomes an immediate
    // re-check instead of an unhandled throw escaping run() before
    // snapshot_sigs_ is actually drained.
    //
    // snapshot_sigs_.empty() is the drain condition directly - no separate
    // counter alongside it. A counter incremented/decremented at the exact
    // same two call sites as this list's insert/erase would just be a
    // second piece of state that could drift out of sync with it.
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
    std::vector<SymbolId> symbols_;
    SymbolRegistry registry_;
    net::ssl::context* ssl_ctx_;
    std::unordered_map<SymbolId, SymbolSync<Policy>> symbol_syncs_;

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

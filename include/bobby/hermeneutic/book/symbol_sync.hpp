#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/core/fixed_point.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic {

using NativeSymbol = bobby::hermeneutic::symbol::NativeSymbol;

// One parsed depth-diff message from an exchange's WebSocket feed. This is
// a *batch* of bid/ask level changes sharing one sequencing envelope -
// mirroring the exchange's own wire shape (e.g. Binance's depthUpdate
// carries multiple b/a level changes per message, not one level per
// message) - not a single (side, price, size) triple.
struct DepthUpdate {
    NativeSymbol symbol;
    std::uint64_t first_id;       // Binance: U
    std::uint64_t final_id;       // Binance: u
    std::uint64_t prev_final_id;  // Binance USDS-M Futures: pu (no Spot equivalent)
    std::vector<std::pair<Price, Size>> bids;
    std::vector<std::pair<Price, Size>> asks;
};

// One parsed order-book snapshot, whether it came from a REST call or was
// pushed by the exchange over the same WebSocket connection.
struct SnapshotMessage {
    NativeSymbol symbol;
    std::vector<std::pair<Price, Size>> bids;
    std::vector<std::pair<Price, Size>> asks;
    std::uint64_t last_update_id;
};

// Actions SymbolSync emits for its driver to actually carry out -
// SymbolSync itself never touches a socket, a timer, or SymbolBook
// directly.
struct RequestSnapshot {};

struct ApplySnapshot {
    std::vector<std::pair<Price, Size>> bids;
    std::vector<std::pair<Price, Size>> asks;
};

struct ApplyDelta {
    std::vector<std::pair<Price, Size>> bids;
    std::vector<std::pair<Price, Size>> asks;
};

struct InvalidateVenue {};

using SyncAction = std::variant<RequestSnapshot, ApplySnapshot, ApplyDelta, InvalidateVenue>;

// Sans-io resync state machine for one (venue, symbol) pair: buffers
// depth-diff events until a snapshot can be bridged into them without a
// gap, then tracks steady-state contiguity and re-resyncs on any gap or
// disconnect. Never touches a socket, a timer, or a thread - a driver
// feeds it parsed events (on_connected/on_depth_update/on_snapshot/
// on_disconnected) and executes whatever SyncAction(s) come back. This is
// what makes it fully unit-testable with a scripted event sequence and no
// real I/O.
//
// `SequencePolicy` supplies three static predicates plus one compile-time
// flag - the only venue-specific part of this algorithm:
//   - should_drop_buffered(const DepthUpdate&, const SnapshotMessage&) -> bool
//   - bridges_snapshot(const DepthUpdate&, const SnapshotMessage&) -> bool
//   - is_contiguous(const DepthUpdate&, std::uint64_t last_applied_final_id) -> bool
//   - kTrustsConnectionOrder: true for a venue whose Feed pushes its own
//     snapshot as the first message on an ordered WS connection, ahead of
//     every diff (kSnapshotViaRest == false in practice - see
//     venue_session.hpp); false for a venue whose snapshot arrives via a
//     separate REST call raced against the already-flowing diff stream.
//     Governs on_snapshot()'s empty-buffer branch below.
// Everything else here is venue-agnostic; see
// exchange/binance/binance_futures_sequence_policy.hpp's
// BinanceFuturesSequencePolicy for a concrete example.
template <typename SequencePolicy>
class SymbolSync {
  public:
    // Driver calls this once, right after a (re)connect and resubscribe
    // succeed. Deliberately not called from on_disconnected() itself:
    // fetching a snapshot before the connection is actually back up would
    // just go stale before any live event arrives to bridge it.
    std::vector<SyncAction> on_connected() {
        if (state_ == State::Buffering && !snapshot_requested_) {
            snapshot_requested_ = true;
            return {RequestSnapshot{}};
        }
        return {};
    }

    std::vector<SyncAction> on_depth_update(DepthUpdate update) {
        if (state_ == State::Buffering) {
            buffer_.push_back(std::move(update));
            return {};
        }

        if (!SequencePolicy::is_contiguous(update, last_final_id_)) {
            // buffer_ is always empty here (Live state never buffers) -
            // push this gap-triggering event in first so handle_gap() has
            // something to keep; buffer_.begin() then erases nothing.
            buffer_.push_back(std::move(update));
            return handle_gap(buffer_.begin());
        }

        // Not `std::vector<SyncAction> actions{ApplyDelta{...}}`: a braced
        // initializer-list constructor copy-constructs its element from the
        // (necessarily const) std::initializer_list entry, so the
        // std::move()s below would silently deep-copy update.bids/asks
        // instead of moving them - the vector ends up with the right
        // *values* either way, so nothing catches this by behavior, only by
        // profiling or reading the generated code. emplace_back constructs
        // the element in place from an rvalue, so the moves inside ApplyDelta
        // actually move.
        std::vector<SyncAction> actions;
        actions.emplace_back(ApplyDelta{std::move(update.bids), std::move(update.asks)});
        last_final_id_ = update.final_id;
        return actions;
    }

    std::vector<SyncAction> on_snapshot(SnapshotMessage snapshot) {
        // Captured *before* should_drop_buffered() can empty buffer_ out -
        // "nothing was ever buffered" (this really is the first message
        // ever seen for this symbol) and "something was buffered but every
        // event in it got dropped as stale" both leave buffer_ empty
        // afterward, but only the former is safe for
        // kTrustsConnectionOrder's shortcut below to treat as "go live
        // directly" - the latter is a real gap (something arrived despite
        // this venue's ordering guarantee) that must still retry.
        bool nothing_was_ever_buffered = buffer_.empty();

        std::erase_if(buffer_, [&snapshot](const DepthUpdate& event) {
            return SequencePolicy::should_drop_buffered(event, snapshot);
        });

        auto bridge = std::find_if(buffer_.begin(), buffer_.end(), [&snapshot](const DepthUpdate& event) {
            return SequencePolicy::bridges_snapshot(event, snapshot);
        });

        if (bridge == buffer_.end()) {
            // For a venue whose Feed pushes its own snapshot as the first
            // message on an ordered connection (SequencePolicy::
            // kTrustsConnectionOrder), an *empty* buffer here isn't a gap -
            // it's the expected, common case: the snapshot IS the first
            // thing this symbol has ever seen. Trust transport order and go
            // live directly from the snapshot. A *non-empty* buffer that
            // still doesn't bridge is still a real gap below (some event
            // arrived despite the snapshot supposedly being first).
            //
            // Without this branch, a venue like this would never leave
            // Buffering: kSnapshotViaRest is false for these venues (see
            // venue_session.hpp), so the RequestSnapshot this would
            // otherwise return is a no-op, and every subsequent depth
            // update would buffer forever instead of applying.
            if constexpr (SequencePolicy::kTrustsConnectionOrder) {
                if (nothing_was_ever_buffered) {
                    last_final_id_ = snapshot.last_update_id;
                    state_ = State::Live;
                    // Not `return {ApplySnapshot{std::move(...)}};` - same
                    // reasoning as on_depth_update()'s own comment above,
                    // and the wasted copy is larger here: a snapshot is a
                    // full order book, not one diff's worth.
                    std::vector<SyncAction> actions;
                    actions.emplace_back(ApplySnapshot{std::move(snapshot.bids), std::move(snapshot.asks)});
                    return actions;
                }
                // A *non-empty* buffer that still doesn't bridge is a real
                // gap - some event arrived despite this venue's ordering
                // guarantee. The plain RequestSnapshot retry below is a
                // dead end here: it's a no-op for these venues
                // (kSnapshotViaRest == false - see venue_session.hpp), so
                // this symbol must go through handle_gap() (force a real
                // reconnect via VenueSession) instead of staying in
                // Buffering forever waiting on a snapshot that never
                // arrives. buffer_.begin() erases nothing - the whole
                // (post-drop-filter) buffer survives, in case it bridges
                // the *next* (post-reconnect) snapshot.
                return handle_gap(buffer_.begin());
            }
            // No buffered event bridges this snapshot - either nothing
            // survived the drop filter, or everything that did starts
            // after a gap this snapshot doesn't cover. Retry: stay in
            // Buffering, keep whatever's left (a future event might still
            // bridge a fresher snapshot), ask for another one.
            return {RequestSnapshot{}};
        }
        // Do not assert bridge == buffer_.begin() here. For a
        // monotonic-sequence policy (Binance, Bybit) it always holds, but
        // for a policy with a documented sequence-reset case (see
        // exchange/okx/okx_sequence_policy.hpp's OkxSequencePolicy),
        // should_drop_buffered's numeric comparison can under-drop across
        // a reset and leave a stale earlier survivor in front of the real
        // bridge. The loop below doesn't need bridge == begin() to be
        // correct anyway: applying from bridge onward and discarding
        // anything before it is correct regardless of bridge's position,
        // since the snapshot is authoritative as of its own sequence
        // number and supersedes everything buffered before the bridge.

        std::vector<SyncAction> actions;
        actions.emplace_back(ApplySnapshot{std::move(snapshot.bids), std::move(snapshot.asks)});

        // bridge itself is only guaranteed to bridge the *snapshot*
        // (SequencePolicy::bridges_snapshot), not to be contiguous with
        // whatever buffered event follows it - two events that both
        // survived should_drop_buffered's numeric filter can still have a
        // real gap between them. Every tail event except bridge itself
        // must pass the same is_contiguous() check on_depth_update()
        // applies in Live state, or last_final_id_ would silently jump
        // past a missing update; `it != bridge` skips that check for
        // exactly the first iteration, letting one loop apply the whole
        // tail instead of bridge needing its own separate apply step.
        for (auto it = bridge; it != buffer_.end(); ++it) {
            if (it != bridge && !SequencePolicy::is_contiguous(*it, last_final_id_)) {
                // Same treatment as on_depth_update()'s Live-state gap
                // branch: invalidate, reset, and keep everything from this
                // point on buffered - it may still bridge the *next*
                // snapshot.
                auto gap_actions = handle_gap(it);
                actions.insert(actions.end(), std::make_move_iterator(gap_actions.begin()),
                                std::make_move_iterator(gap_actions.end()));
                return actions;
            }
            last_final_id_ = it->final_id;
            actions.emplace_back(ApplyDelta{std::move(it->bids), std::move(it->asks)});
        }

        buffer_.clear();
        state_ = State::Live;
        return actions;
    }

    // Driver calls this when the underlying connection drops, for any
    // reason. The feed is untrustworthy from this point until a fresh
    // snapshot is bridged in, same reasoning as AggregateOrderBook's own
    // invalidate_venue() doc comment.
    std::vector<SyncAction> on_disconnected() {
        reset();
        return {InvalidateVenue{}};
    }

  private:
    enum class State { Buffering, Live };

    // Shared by on_depth_update()'s Live-state gap branch and both of
    // on_snapshot()'s gap checks - all three invalidate, reset back to
    // Buffering, and keep buffer_'s unresolved suffix around (it may still
    // bridge the *next* snapshot) via a single helper so they can't drift
    // out of sync on a future change to any one of them.
    //
    // Takes an iterator *into buffer_ itself* and erases the resolved
    // prefix in place rather than building a separate vector to swap in:
    // a venue like Bybit hits this path as a routine, not rare, event, so
    // the allocation/copy a separate vector would cost is worth avoiding.
    // on_depth_update()'s call site passes buffer_.begin() (buffer_ is
    // always empty in Live state before it pushes its one gap-triggering
    // event), making the erase a no-op there.
    //
    // Deliberately venue-shape-agnostic: this class never decides what a
    // gap *means* for the driver (force a reconnect vs. re-request a
    // snapshot in place - both are wrong for the other venue shape) - it
    // only ever reports that one happened. VenueSession is the one place
    // that decision is made, for every trigger point (execute_actions_
    // and_maybe_force_reconnect(), handle_request_snapshot(), and
    // resync_rest_venue_after_gap() as the kTrustsConnectionOrder ==
    // false counterpart both of those call) - not split between here and
    // there.
    std::vector<SyncAction> handle_gap(std::vector<DepthUpdate>::iterator first_unresolved) {
        buffer_.erase(buffer_.begin(), first_unresolved);
        state_ = State::Buffering;
        last_final_id_ = 0;
        snapshot_requested_ = false;
        return {InvalidateVenue{}};
    }

    void reset() {
        state_ = State::Buffering;
        buffer_.clear();
        last_final_id_ = 0;
        snapshot_requested_ = false;
    }

    State state_ = State::Buffering;
    std::vector<DepthUpdate> buffer_;
    std::uint64_t last_final_id_ = 0;
    bool snapshot_requested_ = false;
};

}  // namespace bobby::hermeneutic

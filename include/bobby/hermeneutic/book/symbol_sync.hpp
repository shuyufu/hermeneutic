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
// directly. See docs/ingestion_design.md for the full design.
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
//     Governs on_snapshot()'s empty-buffer branch below - see there for why
//     this needs to exist at all.
// Everything else here is venue-agnostic; see
// exchange/binance/binance_futures_sequence_policy.hpp's
// BinanceFuturesSequencePolicy for the concrete USDS-M Futures predicates
// this was designed against.
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
            std::vector<SyncAction> actions{InvalidateVenue{}};
            reset();
            // This event may still be the one that bridges the *next*
            // snapshot, so it's folded into the fresh buffer rather than
            // discarded.
            buffer_.push_back(std::move(update));
            return actions;
        }

        std::vector<SyncAction> actions{ApplyDelta{std::move(update.bids), std::move(update.asks)}};
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
            // thing this symbol has ever seen, so there was never anything
            // to buffer in the first place. Trust transport order and go
            // live directly from the snapshot. A *non-empty* buffer that
            // still doesn't bridge is still treated as a real gap below
            // (some event arrived despite the snapshot supposedly being
            // first - genuinely unexpected for this kind of venue, not
            // something to silently paper over).
            //
            // Without this branch, a venue like this would never leave
            // Buffering at all: kSnapshotViaRest is false for these venues
            // (see venue_session.hpp), so the RequestSnapshot this would
            // otherwise return is a no-op - nothing would ever re-request,
            // and every subsequent depth update would buffer forever
            // instead of applying. Caught by a test
            // (SnapshotArrivingBeforeAnyBufferedEventGoesLiveDirectly) that
            // fails without this, not found by inspection - every existing
            // SymbolSync test drove events in Binance's REST-race order
            // (on_depth_update() before on_snapshot()), which never
            // exercises an empty buffer at snapshot time.
            if constexpr (SequencePolicy::kTrustsConnectionOrder) {
                if (nothing_was_ever_buffered) {
                    last_final_id_ = snapshot.last_update_id;
                    state_ = State::Live;
                    return {ApplySnapshot{std::move(snapshot.bids), std::move(snapshot.asks)}};
                }
            }
            // No buffered event bridges this snapshot - either nothing
            // survived the drop filter, or everything that did starts
            // after a gap this snapshot doesn't cover. Retry: stay in
            // Buffering, keep whatever's left (a future event might still
            // bridge a fresher snapshot), ask for another one.
            return {RequestSnapshot{}};
        }
        // For a policy whose sequence numbers only increase (Binance,
        // Bybit), should_drop_buffered() already removed everything below
        // the snapshot's coverage, so the first surviving event always
        // either bridges or nothing does - bridge lands on buffer_.begin()
        // whenever it's found at all. This is NOT a precondition the loop
        // below actually depends on, though: applying from bridge onward
        // and discarding anything before it (in arrival order) is correct
        // regardless of bridge's position, because the snapshot itself is
        // the authoritative state as of its own sequence number - anything
        // buffered before the bridge event is superseded by the snapshot no
        // matter why it didn't survive should_drop_buffered's own filter.
        // This matters for a policy whose sequence numbers are explicitly
        // NOT assumed monotonic (see exchange/okx/okx_sequence_policy.hpp's
        // OkxSequencePolicy documented sequence-reset case): should_drop_buffered's numeric comparison
        // can under-drop across a reset, leaving a stale earlier survivor
        // in front of the real bridge - asserting bridge == begin() here
        // would be a false alarm in a debug build (or, worse, would have
        // silently been relied upon to always hold), not a real invariant
        // violation. There used to be an assert(bridge == buffer_.begin())
        // here for exactly that now-incorrect reason - removed rather than
        // conditioned on a new policy trait, since the loop needs no such
        // guarantee to behave correctly either way.

        std::vector<SyncAction> actions;
        actions.emplace_back(ApplySnapshot{std::move(snapshot.bids), std::move(snapshot.asks)});
        for (auto it = bridge; it != buffer_.end(); ++it) {
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

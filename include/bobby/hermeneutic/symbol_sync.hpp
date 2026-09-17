#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/fixed_point.hpp"

namespace bobby::hermeneutic {

using SymbolId = std::string;

// One parsed depth-diff message from an exchange's WebSocket feed. This is
// a *batch* of bid/ask level changes sharing one sequencing envelope -
// mirroring the exchange's own wire shape (e.g. Binance's depthUpdate
// carries multiple b/a level changes per message, not one level per
// message) - not a single (side, price, size) triple.
struct DepthUpdate {
    SymbolId symbol;
    std::uint64_t first_id;       // Binance: U
    std::uint64_t final_id;       // Binance: u
    std::uint64_t prev_final_id;  // Binance USDS-M Futures: pu (no Spot equivalent)
    std::vector<std::pair<Price, Size>> bids;
    std::vector<std::pair<Price, Size>> asks;
};

// One parsed order-book snapshot, whether it came from a REST call or was
// pushed by the exchange over the same WebSocket connection.
struct SnapshotMessage {
    SymbolId symbol;
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
// `SequencePolicy` supplies three static predicates that are the only
// venue-specific part of this algorithm:
//   - should_drop_buffered(const DepthUpdate&, const SnapshotMessage&) -> bool
//   - bridges_snapshot(const DepthUpdate&, const SnapshotMessage&) -> bool
//   - is_contiguous(const DepthUpdate&, std::uint64_t last_applied_final_id) -> bool
// Everything else here is venue-agnostic; see BinanceFuturesSequencePolicy
// for the concrete USDS-M Futures predicates this was designed against.
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
        std::erase_if(buffer_, [&snapshot](const DepthUpdate& event) {
            return SequencePolicy::should_drop_buffered(event, snapshot);
        });

        auto bridge = std::find_if(buffer_.begin(), buffer_.end(), [&snapshot](const DepthUpdate& event) {
            return SequencePolicy::bridges_snapshot(event, snapshot);
        });

        if (bridge == buffer_.end()) {
            // No buffered event bridges this snapshot - either nothing
            // survived the drop filter, or everything that did starts
            // after a gap this snapshot doesn't cover. Retry: stay in
            // Buffering, keep whatever's left (a future event might still
            // bridge a fresher snapshot), ask for another one.
            return {RequestSnapshot{}};
        }
        // Sequence IDs only increase, and should_drop_buffered() already
        // removed everything below the snapshot's coverage, so the first
        // surviving event either bridges or nothing does - never a later
        // one while an earlier survivor is silently skipped.
        assert(bridge == buffer_.begin());

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

// Binance USDS-M Futures' documented local-order-book procedure
// (developers.binance.com/docs/derivatives/usds-margined-futures/
// websocket-market-streams/How-to-manage-a-local-order-book-correctly),
// verified against the primary source rather than assumed from Spot's
// (different) rules:
//   - drop buffered events with final_id < snapshot.last_update_id (strict <)
//   - the bridging event needs first_id <= last_update_id && final_id >= last_update_id
//     (no +1 offset, unlike Spot's U == lastUpdateId+1 model)
//   - steady state validates via the explicit `pu` back-pointer, not an
//     assumed U == last_u+1 continuity
struct BinanceFuturesSequencePolicy {
    static bool should_drop_buffered(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.final_id < snapshot.last_update_id;
    }

    static bool bridges_snapshot(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.first_id <= snapshot.last_update_id && event.final_id >= snapshot.last_update_id;
    }

    static bool is_contiguous(const DepthUpdate& event, std::uint64_t last_applied_final_id) {
        return event.prev_final_id == last_applied_final_id;
    }
};

}  // namespace bobby::hermeneutic

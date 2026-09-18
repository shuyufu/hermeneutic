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
    // Snapshot comes from a separate REST call raced against the
    // already-flowing WS diff stream (kSnapshotViaRest == true) - the
    // buffer accumulated before that REST response lands is exactly what
    // on_snapshot()'s bridge search is for, so an empty buffer there is a
    // real "nothing to bridge yet" case, not something to special-case.
    static constexpr bool kTrustsConnectionOrder = false;

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

// Binance Spot's documented local-order-book procedure
// (developers.binance.com/en/docs/products/spot/web-socket-streams#how-to-manage-a-local-order-book-correctly),
// verified against the primary source directly (2026-09-18), not assumed
// from Futures' (different) rules:
//   - drop buffered events with final_id <= snapshot.last_update_id (the
//     doc's step 5, non-strict <=, unlike Futures' strict <)
//   - the bridging event needs first_id <= last_update_id+1 && final_id >=
//     last_update_id+1 (the +1 offset Futures' bridge condition lacks)
//   - steady state validates via first_id == last_applied_final_id + 1
//     ("normally U of the next event equals u+1 of the previous"), not an
//     explicit back-pointer field - Spot's depthUpdate carries no `pu`
//     equivalent, so DepthUpdate::prev_final_id is simply unused here
//     (BinanceSpotFeed::parse_message leaves it at 0)
//
// Spot's update procedure also documents a third branch this policy
// doesn't model: "if u is less than your local book's update ID, ignore
// the event" (silently drop, no resync). SymbolSync has no such ignore
// action - an event that stale fails is_contiguous() and triggers a full
// resync instead. Unreachable in practice on a single ordered TCP stream
// (it would require the exchange delivering events out of order), so not
// modeled; noted here so this isn't mistaken for an oversight.
struct BinanceSpotSequencePolicy {
    // Same reasoning as BinanceFuturesSequencePolicy: Spot's snapshot is
    // also a separate REST call racing the diff stream, not pushed as the
    // first WS message - see BybitSequencePolicy below for the venue this
    // flag was actually added for.
    static constexpr bool kTrustsConnectionOrder = false;

    static bool should_drop_buffered(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.final_id <= snapshot.last_update_id;
    }

    static bool bridges_snapshot(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.first_id <= snapshot.last_update_id + 1 && event.final_id >= snapshot.last_update_id + 1;
    }

    static bool is_contiguous(const DepthUpdate& event, std::uint64_t last_applied_final_id) {
        return event.first_id == last_applied_final_id + 1;
    }
};

// Bybit v5 public orderbook stream
// (bybit-exchange.github.io/docs/v5/websocket/public/orderbook), verified
// two ways rather than assumed from the doc text alone: the doc itself does
// not spell out an explicit gap-detection rule (only that receiving `u=1`
// mid-stream signals a server-side restart, requiring a fresh local book),
// so this project also ran a live probe against
// wss://stream.bybit.com/v5/public/linear (orderbook.50.BTCUSDT,
// 2026-09-18): 30 consecutive messages showed `u` incrementing by exactly 1
// on every delta following the snapshot, with no gaps. This project chose
// to actually enforce that stricter invariant rather than the doc's more
// permissive framing ("transport order can be trusted") - see
// docs/ingestion_design.md 第 5 節's TrustConnectionOrderPolicy sketch for
// the fully-trivial version (unconditionally-true predicates) this
// deliberately isn't, now that a real venue with its own sequence field is
// actually being implemented:
//   - Bybit gives one update id per message (`u`), not Binance's
//     first_id/final_id range + `pu` back-pointer - DepthUpdate::first_id
//     and ::final_id both carry Bybit's `u` (BybitLinearFeed::parse_message
//     sets both), so either field reads the same value; ::prev_final_id is
//     unused (set to 0, not left default).
//   - `seq` ("cross sequence") is *not* a per-topic gap signal - Bybit's
//     docs describe it as comparing freshness across different depth
//     subscriptions of the same symbol, and it jumps by an arbitrary amount
//     between consecutive messages on a single topic (confirmed live: jumps
//     from 39 to 873 within the same 30-message sample) - it plays no role
//     in this policy.
//   - The documented "u=1 mid-stream = forced resnapshot" case needs no
//     special-casing: once a real book is live, u=1 can never equal
//     last_applied_final_id+1, so it already fails is_contiguous() and
//     triggers the same InvalidateVenue + resync path as any other gap.
//   - kTrustsConnectionOrder = true: Bybit pushes its own snapshot as the
//     first message on this same ordered WS connection (kSnapshotViaRest ==
//     false), so the buffer is empty when on_snapshot() first runs for a
//     symbol - see SymbolSync::on_snapshot()'s empty-buffer branch for why
//     this flag has to exist. Caught by a real bug, not designed in from
//     the start: every existing SymbolSync test drove events in Binance's
//     REST-race order (a depth update buffered before the snapshot
//     arrives), which never exercises Bybit's actual message order and so
//     never exposed that on_snapshot() would otherwise stay in Buffering
//     forever - RequestSnapshot is a no-op for a kSnapshotViaRest == false
//     Feed, so nothing would ever re-request, and the Bybit venue would
//     silently contribute zero levels to the book.
struct BybitSequencePolicy {
    static constexpr bool kTrustsConnectionOrder = true;

    static bool should_drop_buffered(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.final_id <= snapshot.last_update_id;
    }

    static bool bridges_snapshot(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.final_id == snapshot.last_update_id + 1;
    }

    static bool is_contiguous(const DepthUpdate& event, std::uint64_t last_applied_final_id) {
        return event.final_id == last_applied_final_id + 1;
    }
};

}  // namespace bobby::hermeneutic

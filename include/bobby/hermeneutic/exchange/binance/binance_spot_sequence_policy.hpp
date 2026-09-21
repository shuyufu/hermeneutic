#pragma once

#include <cstdint>

#include "bobby/hermeneutic/book/symbol_sync.hpp"

namespace bobby::hermeneutic {

// Binance Spot's documented local-order-book procedure
// (developers.binance.com/en/docs/products/spot/web-socket-streams#how-to-manage-a-local-order-book-correctly):
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
//
// Deliberately zero-dependency beyond book/symbol_sync.hpp - see
// binance_futures_sequence_policy.hpp's comment for why this isn't merged
// into binance_spot_feed.hpp.
struct BinanceSpotSequencePolicy {
    // Same reasoning as BinanceFuturesSequencePolicy: Spot's snapshot is
    // also a separate REST call racing the diff stream, not pushed as the
    // first WS message - see BybitSequencePolicy for the venue this flag
    // was actually added for.
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

}  // namespace bobby::hermeneutic

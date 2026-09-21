#pragma once

#include <cstdint>

#include "bobby/hermeneutic/book/symbol_sync.hpp"

namespace bobby::hermeneutic {

// Binance USDS-M Futures' documented local-order-book procedure
// (developers.binance.com/docs/derivatives/usds-margined-futures/
// websocket-market-streams/How-to-manage-a-local-order-book-correctly):
//   - drop buffered events with final_id < snapshot.last_update_id (strict <)
//   - the bridging event needs first_id <= last_update_id && final_id >= last_update_id
//     (no +1 offset, unlike Spot's U == lastUpdateId+1 model)
//   - steady state validates via the explicit `pu` back-pointer, not an
//     assumed U == last_u+1 continuity
//
// Deliberately zero-dependency beyond book/symbol_sync.hpp - not merged
// into exchange/binance/binance_futures_feed.hpp, which pulls in simdjson
// for its actual JSON parsing. A consumer that only needs SymbolSync's
// resync semantics (e.g. a test driving a fake Feed, see
// VenueSessionTest's FakeHttpRequestSpec comment) must not be forced to
// drag simdjson in just to get this struct.
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

}  // namespace bobby::hermeneutic

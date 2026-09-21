#pragma once

#include <cstdint>

#include "bobby/hermeneutic/book/symbol_sync.hpp"

namespace bobby::hermeneutic {

// Bybit v5 public orderbook stream
// (bybit-exchange.github.io/docs/v5/websocket/public/orderbook): `u`
// increments by exactly 1 on every delta following the snapshot, with no
// gaps, except when a server-side restart resets it to 1 (see the u=1
// case below).
//   - Bybit gives one update id per message (`u`), not Binance's
//     first_id/final_id range + `pu` back-pointer - DepthUpdate::first_id
//     and ::final_id both carry Bybit's `u` (BybitLinearFeed::parse_message
//     sets both), so either field reads the same value; ::prev_final_id is
//     unused (set to 0, not left default).
//   - `seq` ("cross sequence") is *not* a per-topic gap signal - it
//     compares freshness across different depth subscriptions of the same
//     symbol and can jump by an arbitrary amount between consecutive
//     messages on a single topic - it plays no role in this policy.
//   - The documented "u=1 mid-stream = forced resnapshot" case needs no
//     special-casing: once a real book is live, u=1 can never equal
//     last_applied_final_id+1, so it already fails is_contiguous() and
//     triggers the same InvalidateVenue + resync path as any other gap.
//   - kTrustsConnectionOrder = true: Bybit pushes its own snapshot as the
//     first message on this same ordered WS connection (kSnapshotViaRest ==
//     false), so the buffer is empty when on_snapshot() first runs for a
//     symbol - see SymbolSync::on_snapshot()'s empty-buffer branch. This
//     must stay true for this policy: if it were false, on_snapshot()
//     would stay in Buffering forever, since RequestSnapshot is a no-op
//     for a kSnapshotViaRest == false Feed and nothing would ever
//     re-request.
//
// Shared by both BybitLinearFeed and BybitSpotFeed (exchange/bybit/
// bybit_feed.hpp) - Bybit's v5 public orderbook stream is protocol-
// identical across market segments, only the WS path differs. Deliberately
// zero-dependency beyond book/symbol_sync.hpp - see
// binance_futures_sequence_policy.hpp's comment for why this isn't merged
// into the feed header, which pulls in simdjson.
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

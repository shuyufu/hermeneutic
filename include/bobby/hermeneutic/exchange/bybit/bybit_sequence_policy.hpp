#pragma once

#include <cstdint>

#include "bobby/hermeneutic/book/symbol_sync.hpp"

namespace bobby::hermeneutic {

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

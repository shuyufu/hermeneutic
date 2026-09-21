#pragma once

#include <cstdint>

#include "bobby/hermeneutic/book/symbol_sync.hpp"

namespace bobby::hermeneutic {

// OKX v5 public `books` channel. Traced against OKX's own worked example:
//   snapshot: prevSeqId=-1, seqId=10
//   normal update:          prevSeqId=10, seqId=15
//   idle heartbeat:         prevSeqId=15, seqId=15   (empty bids/asks)
//   sequence reset:         prevSeqId=15, seqId=3    (seqId itself resets down)
//   normal update:          prevSeqId=3,  seqId=5
// Every step checks out under a pure prevSeqId == last-applied-seqId chain,
// the same shape as Binance Futures' `pu` back-pointer - no special-casing
// needed for either the heartbeat (chains from itself: prevSeqId==seqId,
// applies as a harmless empty ApplyDelta) or the reset (prevSeqId still
// chains correctly from the prior seqId; only the numeric value of seqId
// itself moves backward, which nothing here assumes is monotonic).
//   - OKX gives one seqId per message, not Binance's first_id/final_id
//     range - DepthUpdate::first_id and ::final_id both carry seqId
//     (OkxFeed::parse_message sets both), so either field reads
//     consistently; first_id has no distinct meaning for OKX.
//   - should_drop_buffered()'s `<` (not `<=`) is a buffer-hygiene detail,
//     not a correctness-critical gap check - the real gap detection is
//     is_contiguous()'s exact chain match. Under the documented sequence
//     reset, a buffered event could in principle have a final_id that
//     doesn't compare cleanly against a post-reset snapshot's
//     last_update_id; if that ever under-drops, on_snapshot() just fails to
//     find a bridge and safely retries (RequestSnapshot), it does not apply
//     wrong data - resets are documented as maintenance-only and rare.
//   - Checksum (CRC32 over the top book levels) is deliberately not
//     implemented - this relies on the seqId/prevSeqId chain alone, the
//     same rigor Binance's policies already operate at with no extra
//     integrity layer.
//   - kTrustsConnectionOrder = true: OKX pushes its own snapshot as the
//     first message on a fresh (re)subscribe (kSnapshotViaRest == false),
//     same reasoning as BybitSequencePolicy.
//
// Deliberately zero-dependency beyond book/symbol_sync.hpp - see
// binance_futures_sequence_policy.hpp's comment for why this isn't merged
// into okx_feed.hpp, which pulls in simdjson.
struct OkxSequencePolicy {
    static constexpr bool kTrustsConnectionOrder = true;

    static bool should_drop_buffered(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.final_id < snapshot.last_update_id;
    }

    static bool bridges_snapshot(const DepthUpdate& event, const SnapshotMessage& snapshot) {
        return event.prev_final_id == snapshot.last_update_id;
    }

    static bool is_contiguous(const DepthUpdate& event, std::uint64_t last_applied_final_id) {
        return event.prev_final_id == last_applied_final_id;
    }
};

}  // namespace bobby::hermeneutic

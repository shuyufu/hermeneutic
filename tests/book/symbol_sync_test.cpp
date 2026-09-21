#include "bobby/hermeneutic/book/symbol_sync.hpp"

#include <gtest/gtest.h>

#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/exchange/binance/binance_futures_sequence_policy.hpp"
#include "bobby/hermeneutic/exchange/binance/binance_spot_sequence_policy.hpp"
#include "bobby/hermeneutic/exchange/bybit/bybit_sequence_policy.hpp"
#include "bobby/hermeneutic/exchange/okx/okx_sequence_policy.hpp"

namespace bobby::hermeneutic {
namespace {

using Sync = SymbolSync<BinanceFuturesSequencePolicy>;

DepthUpdate make_update(std::uint64_t first_id, std::uint64_t final_id, std::uint64_t prev_final_id,
                         std::vector<std::pair<Price, Size>> bids = {},
                         std::vector<std::pair<Price, Size>> asks = {}) {
    return DepthUpdate{"BTCUSDT", first_id, final_id, prev_final_id, std::move(bids), std::move(asks)};
}

SnapshotMessage make_snapshot(std::uint64_t last_update_id,
                               std::vector<std::pair<Price, Size>> bids = {},
                               std::vector<std::pair<Price, Size>> asks = {}) {
    return SnapshotMessage{"BTCUSDT", std::move(bids), std::move(asks), last_update_id};
}

enum class Kind { RequestSnapshot, ApplySnapshot, ApplyDelta, InvalidateVenue };

Kind kind_of(const SyncAction& action) {
    return std::visit(
        [](auto&& a) -> Kind {
            using T = std::decay_t<decltype(a)>;
            if constexpr (std::is_same_v<T, bobby::hermeneutic::RequestSnapshot>) {
                return Kind::RequestSnapshot;
            } else if constexpr (std::is_same_v<T, bobby::hermeneutic::ApplySnapshot>) {
                return Kind::ApplySnapshot;
            } else if constexpr (std::is_same_v<T, bobby::hermeneutic::ApplyDelta>) {
                return Kind::ApplyDelta;
            } else {
                return Kind::InvalidateVenue;
            }
        },
        action);
}

std::vector<Kind> kinds_of(const std::vector<SyncAction>& actions) {
    std::vector<Kind> kinds;
    kinds.reserve(actions.size());
    for (const auto& action : actions) kinds.push_back(kind_of(action));
    return kinds;
}

TEST(SymbolSyncTest, OnConnectedRequestsSnapshotOnceThenStaysQuietUntilResync) {
    Sync sync;
    EXPECT_EQ(kinds_of(sync.on_connected()), (std::vector{Kind::RequestSnapshot}));
    // Already requested for this episode - calling again must not re-request.
    EXPECT_TRUE(sync.on_connected().empty());
    EXPECT_TRUE(sync.on_connected().empty());
}

TEST(SymbolSyncTest, DepthUpdatesWhileBufferingProduceNoActions) {
    Sync sync;
    sync.on_connected();
    EXPECT_TRUE(sync.on_depth_update(make_update(1, 5, 0)).empty());
    EXPECT_TRUE(sync.on_depth_update(make_update(6, 10, 5)).empty());
}

TEST(SymbolSyncTest, BridgingSnapshotAppliesSnapshotThenBufferedTailInOrder) {
    Sync sync;
    sync.on_connected();

    // Dropped: final_id (150) < snapshot.last_update_id (160).
    sync.on_depth_update(make_update(100, 150, 99, {{Price(1.0), Size(1.0)}}));
    // Bridges: first_id (151) <= 160 <= final_id (160), no +1 offset needed.
    sync.on_depth_update(make_update(151, 160, 150, {{Price(2.0), Size(2.0)}}));
    // Tail: contiguous with the bridge event, applied right after it.
    sync.on_depth_update(make_update(161, 165, 160, {}, {{Price(3.0), Size(3.0)}}));

    auto actions = sync.on_snapshot(make_snapshot(160, {{Price(50.0), Size(5.0)}}));

    ASSERT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta, Kind::ApplyDelta}));
    EXPECT_EQ(std::get<ApplySnapshot>(actions[0]).bids, (std::vector<std::pair<Price, Size>>{
                                                             {Price(50.0), Size(5.0)}}));
    EXPECT_EQ(std::get<ApplyDelta>(actions[1]).bids,
              (std::vector<std::pair<Price, Size>>{{Price(2.0), Size(2.0)}}));
    EXPECT_EQ(std::get<ApplyDelta>(actions[2]).asks,
              (std::vector<std::pair<Price, Size>>{{Price(3.0), Size(3.0)}}));

    // Now live: a contiguous event (pu == 165, the last applied final_id)
    // must apply directly, no re-buffering.
    auto live_actions = sync.on_depth_update(make_update(166, 170, 165, {{Price(4.0), Size(4.0)}}));
    ASSERT_EQ(kinds_of(live_actions), (std::vector{Kind::ApplyDelta}));
    EXPECT_EQ(std::get<ApplyDelta>(live_actions[0]).bids,
              (std::vector<std::pair<Price, Size>>{{Price(4.0), Size(4.0)}}));
}

TEST(SymbolSyncTest, BridgingSnapshotDetectsAGapBetweenTwoBufferedTailEvents) {
    // Both tail events individually survive should_drop_buffered (neither
    // final_id is < the snapshot's last_update_id) and the first of the two
    // bridges the snapshot - but there's a real gap between them (a message
    // this SymbolSync never saw). Replaying the tail must catch this the
    // same way on_depth_update() catches a Live-state gap, not silently
    // apply across it.
    Sync sync;
    sync.on_connected();

    // Bridges: first_id (151) <= 160 <= final_id (160), no +1 offset needed.
    sync.on_depth_update(make_update(151, 160, 150, {{Price(2.0), Size(2.0)}}));
    // Gap: prev_final_id (162) != the bridge event's final_id (160) - a
    // message covering 161 was missed.
    sync.on_depth_update(make_update(163, 165, 162, {}, {{Price(3.0), Size(3.0)}}));

    auto actions = sync.on_snapshot(make_snapshot(160, {{Price(50.0), Size(5.0)}}));

    // Snapshot applies, then the bridge event (contiguous with it by
    // construction) - then the gap is caught before the second tail event is
    // applied, not after.
    ASSERT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta, Kind::InvalidateVenue}));
    EXPECT_EQ(std::get<ApplyDelta>(actions[1]).bids,
              (std::vector<std::pair<Price, Size>>{{Price(2.0), Size(2.0)}}));

    // Back in Buffering: a fresh on_connected() must request a snapshot
    // again, exactly like any other gap.
    EXPECT_EQ(kinds_of(sync.on_connected()), (std::vector{Kind::RequestSnapshot}));

    // The gap-triggering event must have been folded into the new buffer,
    // not discarded - a snapshot landing inside its range bridges
    // immediately.
    auto resync_actions = sync.on_snapshot(make_snapshot(163));
    ASSERT_EQ(kinds_of(resync_actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
    EXPECT_EQ(std::get<ApplyDelta>(resync_actions[1]).asks,
              (std::vector<std::pair<Price, Size>>{{Price(3.0), Size(3.0)}}));
}

TEST(SymbolSyncTest, SnapshotThatDoesNotBridgeRetriesAndKeepsBufferedEvent) {
    Sync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(200, 210, 199));

    // last_update_id (100) is far behind the only buffered event (200-210):
    // that event isn't dropped (final_id 210 is not < 100) but also doesn't
    // bridge (first_id 200 > 100) - a real gap, must retry rather than
    // silently pick a later event.
    auto retry = sync.on_snapshot(make_snapshot(100));
    EXPECT_EQ(kinds_of(retry), (std::vector{Kind::RequestSnapshot}));

    // A fresher snapshot whose last_update_id actually falls inside that
    // same buffered event's range must now succeed - proving the retry
    // above didn't discard it.
    auto success = sync.on_snapshot(make_snapshot(205));
    EXPECT_EQ(kinds_of(success), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
}

TEST(SymbolSyncTest, DropBoundaryIsStrictLessThan) {
    Sync sync;
    sync.on_connected();
    // final_id == last_update_id exactly: must survive the drop filter
    // (Futures drops only final_id < last_update_id, strictly).
    sync.on_depth_update(make_update(150, 160, 149));

    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
}

TEST(SymbolSyncTest, BridgeHasNoPlusOneOffsetUnlikeSpot) {
    Sync sync;
    sync.on_connected();
    // Under Spot's "U == lastUpdateId + 1" model this would bridge; Futures
    // requires first_id <= last_update_id, which 161 <= 160 fails - a gap.
    sync.on_depth_update(make_update(161, 165, 160));

    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::RequestSnapshot}));
}

TEST(SymbolSyncTest, SnapshotWithEmptyBufferStillRetriesUnderFuturesPolicy) {
    // kTrustsConnectionOrder == false for Futures (its snapshot is a
    // separate REST call raced against the diff stream): an empty buffer
    // at snapshot time is a real "nothing to bridge yet" case, not the
    // special "this venue's snapshot IS the first message" case
    // BybitSequencePolicy's kTrustsConnectionOrder == true handles.
    // Must stay in Buffering and retry, exactly as before that flag
    // existed - proves adding it didn't change Futures' behavior.
    Sync sync;
    sync.on_connected();

    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::RequestSnapshot}));

    // Still Buffering: a depth update now must buffer, not apply directly.
    EXPECT_TRUE(sync.on_depth_update(make_update(161, 165, 160)).empty());
}

TEST(SymbolSyncTest, LiveGapInvalidatesAndReBuffersTheTriggeringEvent) {
    Sync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(1, 100, 0));
    auto bridge_actions = sync.on_snapshot(make_snapshot(100));
    ASSERT_EQ(kinds_of(bridge_actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
    // Now live with last_final_id_ == 100.

    // pu (50) doesn't match the last applied final_id (100): a gap.
    auto gap_actions = sync.on_depth_update(make_update(151, 160, 50, {{Price(9.0), Size(9.0)}}));
    EXPECT_EQ(kinds_of(gap_actions), (std::vector{Kind::InvalidateVenue}));

    // Reconnect-equivalent: state reset, so on_connected() must request a
    // fresh snapshot again (not suppressed as "already requested").
    EXPECT_EQ(kinds_of(sync.on_connected()), (std::vector{Kind::RequestSnapshot}));

    // The event that triggered the gap must have been folded into the new
    // buffer, not discarded - a snapshot landing inside its range bridges
    // immediately, with no further on_depth_update() call needed.
    auto resync_actions = sync.on_snapshot(make_snapshot(155));
    ASSERT_EQ(kinds_of(resync_actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
    EXPECT_EQ(std::get<ApplyDelta>(resync_actions[1]).bids,
              (std::vector<std::pair<Price, Size>>{{Price(9.0), Size(9.0)}}));
}

TEST(SymbolSyncTest, DisconnectInvalidatesAndDoesNotRequestSnapshotUntilReconnected) {
    Sync sync;
    sync.on_connected();  // consumes this episode's "not yet requested" flag

    auto actions = sync.on_disconnected();
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));

    // Must request a fresh snapshot on the next reconnect, not stay quiet
    // because of the pre-disconnect request.
    EXPECT_EQ(kinds_of(sync.on_connected()), (std::vector{Kind::RequestSnapshot}));
}

TEST(SymbolSyncTest, DisconnectWhileLiveInvalidatesAndReturnsToBuffering) {
    Sync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(1, 100, 0));
    ASSERT_EQ(kinds_of(sync.on_snapshot(make_snapshot(100))).size(), 2u);  // now Live

    EXPECT_EQ(kinds_of(sync.on_disconnected()), (std::vector{Kind::InvalidateVenue}));

    // Back in Buffering: a depth update now must be buffered, not applied
    // directly as if still Live.
    EXPECT_TRUE(sync.on_depth_update(make_update(101, 105, 100)).empty());
}

// Spot-specific tests: each one exercises a rule that differs from
// BinanceFuturesSequencePolicy (see the mirrored Futures test named in each
// comment) - a policy that was copy-pasted from Futures without actually
// implementing Spot's own (documented) rules would fail these.
namespace spot {

using SpotSync = SymbolSync<BinanceSpotSequencePolicy>;

TEST(SymbolSyncSpotTest, DropBoundaryIsNonStrictLessThanOrEqualUnlikeFutures) {
    // Mirrors SymbolSyncTest.DropBoundaryIsStrictLessThan, inverted: Spot
    // drops final_id == last_update_id (non-strict <=); Futures keeps it.
    SpotSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(150, 160, /*prev_final_id=*/0));

    // final_id (160) == last_update_id (160): dropped under Spot's rule, so
    // nothing survives to bridge - must retry, not apply.
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::RequestSnapshot}));
}

TEST(SymbolSyncSpotTest, BridgeHasPlusOneOffsetUnlikeFutures) {
    // Mirrors SymbolSyncTest.BridgeHasNoPlusOneOffsetUnlikeSpot, inverted:
    // under Futures' "no +1 offset" model this would be a gap; Spot's
    // U <= lastUpdateId+1 <= u model bridges it.
    SpotSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(161, 165, /*prev_final_id=*/0));

    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
}

TEST(SymbolSyncSpotTest, ContinuityIgnoresPrevFinalIdAndUsesFirstIdInstead) {
    // Proves the policy never reads prev_final_id: a live-state event whose
    // prev_final_id is garbage (not the last applied final_id, and not even
    // set by anything - Spot depthUpdate has no `pu` field) must still
    // apply, because Spot's continuity check is first_id == last_u+1, not a
    // pu back-pointer comparison.
    SpotSync sync;
    sync.on_connected();
    // last_update_id (99), not 100: Spot drops final_id <= last_update_id
    // (non-strict), so a snapshot of 100 here would drop this very event
    // instead of bridging on it.
    sync.on_depth_update(make_update(1, 100, /*prev_final_id=*/0));
    ASSERT_EQ(kinds_of(sync.on_snapshot(make_snapshot(99))).size(), 2u);  // now Live, last_final_id_ == 100

    constexpr std::uint64_t kGarbagePrevFinalId = 999'999;
    auto actions = sync.on_depth_update(
        make_update(101, 105, kGarbagePrevFinalId, {{Price(9.0), Size(9.0)}}));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::ApplyDelta}));
}

}  // namespace spot

// Bybit-specific tests: each one exercises a rule that differs from
// BinanceFuturesSequencePolicy (see the mirrored Futures test named in each
// comment) - a policy copy-pasted from Futures without actually
// implementing Bybit's own (verified) single-update-id rules would fail
// these. make_update()'s first_id/final_id are always equal here, matching
// what BybitLinearFeed::parse_message actually produces (see
// BybitSequencePolicy's doc comment).
namespace bybit {

using BybitSync = SymbolSync<BybitSequencePolicy>;

TEST(SymbolSyncBybitTest, SnapshotArrivingBeforeAnyBufferedEventGoesLiveDirectly) {
    // This is Bybit's actual message order, not a hypothetical: the
    // snapshot is the first message VenueSession ever reads for a topic -
    // on_depth_update() has never been called at all when on_snapshot()
    // runs, so buffer_ is empty. Without kTrustsConnectionOrder's
    // empty-buffer branch, this fell into the "no bridge, retry" path
    // forever (RequestSnapshot is a no-op for a kSnapshotViaRest == false
    // Feed - nothing would ever re-request), leaving the venue stuck in
    // Buffering permanently. Every other SymbolSyncBybitTest in this file
    // calls on_depth_update() before on_snapshot() (Binance's REST-race
    // order) and so never exercised this - this is the one that would have
    // caught it.
    BybitSync sync;
    sync.on_connected();

    auto actions = sync.on_snapshot(make_snapshot(160));
    ASSERT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot}));
    EXPECT_TRUE(std::get<ApplySnapshot>(actions[0]).bids.empty());

    // Now live with last_final_id_ == snapshot.last_update_id (160): the
    // very next delta, exactly the shape Bybit actually sends (u == 161),
    // must apply directly, not buffer.
    auto live_actions =
        sync.on_depth_update(make_update(161, 161, /*prev_final_id=*/0, {{Price(2.0), Size(2.0)}}));
    ASSERT_EQ(kinds_of(live_actions), (std::vector{Kind::ApplyDelta}));
    EXPECT_EQ(std::get<ApplyDelta>(live_actions[0]).bids,
              (std::vector<std::pair<Price, Size>>{{Price(2.0), Size(2.0)}}));
}

TEST(SymbolSyncBybitTest, NonEmptyBufferThatDoesNotBridgeForcesAGapDespiteTrustingConnectionOrder) {
    // The empty-buffer branch is deliberately narrow: a *non-empty* buffer
    // that still doesn't bridge is not silently trusted just because this
    // policy sets kTrustsConnectionOrder - that would mean some event
    // arrived before the snapshot despite Bybit's ordering guarantee, which
    // is unexpected enough to require a real resync. Unlike
    // kTrustsConnectionOrder == false (Binance), a plain RequestSnapshot
    // retry here would be a dead end: it's a no-op for this venue (its
    // snapshot only ever arrives pushed on a fresh connection), so nothing
    // would ever actually re-request - this must be InvalidateVenue, the
    // same treatment as every other gap in this class (see handle_gap()'s
    // own comment), not just a retry that papers over the eventual "stuck
    // in Buffering forever" outcome. Caught by a /code-review pass, not by
    // this test before it was rewritten - the previous version asserted
    // exactly that dead-end RequestSnapshot-only behavior as correct.
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(165, 165, /*prev_final_id=*/0));

    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));

    // Reconnect-equivalent: state reset, so on_connected() must request a
    // fresh snapshot again (not suppressed as "already requested").
    EXPECT_EQ(kinds_of(sync.on_connected()), (std::vector{Kind::RequestSnapshot}));

    // The buffered event must have been kept, not discarded - a snapshot
    // landing inside its range now bridges immediately.
    auto resync_actions = sync.on_snapshot(make_snapshot(164));
    ASSERT_EQ(kinds_of(resync_actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
}

TEST(SymbolSyncBybitTest, PostGapBufferedEventIsNotDiscardedByTheEmptyBufferShortcut) {
    // buffer_ has two push sites: the Buffering branch of on_depth_update()
    // AND the live-gap branch (which reset()s state_/buffer_ back to
    // Buffering, then re-buffers the very event that triggered the gap).
    // A version of the empty-buffer shortcut that checked buffer_.empty()
    // *after* should_drop_buffered() - or that only tracked "was anything
    // buffered" from the first push site - would still be fooled here: it
    // has no way to tell "genuinely nothing buffered yet" apart from "the
    // one buffered event just hasn't been observed by this check yet",
    // and would go live from the fresher snapshot while silently dropping
    // the still-buffered post-gap event instead of retrying with it intact.
    BybitSync sync;
    sync.on_connected();
    ASSERT_EQ(kinds_of(sync.on_snapshot(make_snapshot(100))).size(), 1u);  // live, last_final_id_ == 100

    // A gap: 105 != 100+1. Resets to Buffering, re-buffers this event.
    auto gap_actions = sync.on_depth_update(make_update(105, 105, /*prev_final_id=*/0));
    EXPECT_EQ(kinds_of(gap_actions), (std::vector{Kind::InvalidateVenue}));

    // A fresher snapshot arrives whose u+1 (161) doesn't match the
    // buffered event's u (105) - a real gap the buffered event doesn't
    // bridge, not the "first message ever" case. buffer_ was non-empty at
    // entry (the re-buffered gap event), so this is InvalidateVenue - a
    // plain RequestSnapshot retry would be a dead end for this venue (see
    // NonEmptyBufferThatDoesNotBridgeForcesAGapDespiteTrustingConnectionOrder's
    // own comment).
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));
}

TEST(SymbolSyncBybitTest, DropBoundaryIsNonStrictLessThanOrEqualUnlikeFutures) {
    // Mirrors SymbolSyncTest.DropBoundaryIsStrictLessThan, inverted: Bybit
    // drops final_id == last_update_id (non-strict <=); Futures keeps it.
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(160, 160, /*prev_final_id=*/0));

    // final_id (160) == last_update_id (160): dropped under Bybit's rule,
    // so nothing survives to bridge - but buffer_ was non-empty at entry
    // (the on_depth_update() above), so this is the real-gap branch
    // (InvalidateVenue), not the "nothing ever buffered" shortcut.
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));
}

TEST(SymbolSyncBybitTest, BridgeIsExactUPlusOneNotARange) {
    // Unlike Futures' first_id<=last_update_id<=final_id range check, Bybit
    // has only one id per message, so bridging is an exact u == snapshot.u+1
    // equality - a later, non-adjacent event does not bridge even though it
    // would satisfy Futures' range-based condition.
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(165, 165, /*prev_final_id=*/0));

    // Not a match: snapshot.last_update_id+1 (161) != this event's u (165) -
    // buffer_ non-empty at entry, so InvalidateVenue (see
    // NonEmptyBufferThatDoesNotBridgeForcesAGapDespiteTrustingConnectionOrder).
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));
}

TEST(SymbolSyncBybitTest, ExactUPlusOneBridgesAndContinuityUsesFinalIdEquality) {
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(161, 161, /*prev_final_id=*/0, {{Price(2.0), Size(2.0)}}));

    auto actions = sync.on_snapshot(make_snapshot(160));
    ASSERT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));

    // Now live with last_final_id_ == 161: the next event's u must be
    // exactly 162 (last_applied_final_id + 1), not merely "greater than".
    auto live_actions = sync.on_depth_update(make_update(163, 163, /*prev_final_id=*/0));
    EXPECT_EQ(kinds_of(live_actions), (std::vector{Kind::InvalidateVenue}));  // 163 != 162: a gap
}

TEST(SymbolSyncBybitTest, ContinuityIgnoresPrevFinalIdAndFirstIdEntirely) {
    // Proves the policy never reads prev_final_id or first_id: a live-state
    // event whose first_id/prev_final_id are garbage must still apply,
    // because Bybit's continuity check is final_id == last_applied+1 only -
    // BybitLinearFeed::parse_message never sets them to anything meaningful
    // in the first place (see its comment).
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(100, 100, /*prev_final_id=*/0));
    ASSERT_EQ(kinds_of(sync.on_snapshot(make_snapshot(99))).size(), 2u);  // now Live, last_final_id_ == 100

    constexpr std::uint64_t kGarbageFirstId = 1;
    constexpr std::uint64_t kGarbagePrevFinalId = 999'999;
    auto actions = sync.on_depth_update(
        make_update(kGarbageFirstId, 101, kGarbagePrevFinalId, {{Price(9.0), Size(9.0)}}));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::ApplyDelta}));
}

}  // namespace bybit

// OKX-specific tests, using OkxSequencePolicy directly (not the generic
// TrustingSequencePolicy in the trust_connection_order namespace above,
// which only proves the shared kTrustsConnectionOrder mechanism in the
// abstract) - see OkxSequencePolicy's doc comment in symbol_sync.hpp for
// the seqId/prevSeqId semantics these exercise, verified against the OKX
// documentation text the user quoted directly. make_update()'s first_id is
// irrelevant here (OKX has no distinct range start - OkxFeed::parse_message
// sets first_id == final_id, and OkxSequencePolicy never reads first_id at
// all), so it's set to whatever value each test finds clearest.
namespace okx {

using OkxSync = SymbolSync<OkxSequencePolicy>;

TEST(SymbolSyncOkxTest, SnapshotArrivingBeforeAnyBufferedEventGoesLiveDirectly) {
    // OKX's actual message order: the snapshot is the first message
    // VenueSession ever reads for a topic, so buffer_ is empty when
    // on_snapshot() first runs.
    OkxSync sync;
    sync.on_connected();

    auto actions = sync.on_snapshot(make_snapshot(10));
    ASSERT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot}));

    // Now live with last_final_id_ == snapshot.last_update_id (10): a
    // directly-chaining update (prevSeqId == 10) applies without buffering.
    auto live_actions = sync.on_depth_update(make_update(15, 15, /*prev_final_id=*/10, {{Price(2.0), Size(2.0)}}));
    ASSERT_EQ(kinds_of(live_actions), (std::vector{Kind::ApplyDelta}));
    EXPECT_EQ(std::get<ApplyDelta>(live_actions[0]).bids,
              (std::vector<std::pair<Price, Size>>{{Price(2.0), Size(2.0)}}));
}

TEST(SymbolSyncOkxTest, NonEmptyBufferThatDoesNotBridgeForcesAGapDespiteTrustingConnectionOrder) {
    // See BybitSequencePolicy's own version of this test for the full
    // reasoning: a plain RequestSnapshot retry is a dead end for a
    // kTrustsConnectionOrder venue (its snapshot only ever arrives pushed
    // on a fresh connection), so this must be InvalidateVenue - a real
    // resync, not just a retry.
    OkxSync sync;
    sync.on_connected();
    // Buffered, but neither dropped (final_id 15 is not < 5) nor bridging
    // (prev_final_id 10 != snapshot's last_update_id 5).
    sync.on_depth_update(make_update(15, 15, /*prev_final_id=*/10));

    auto actions = sync.on_snapshot(make_snapshot(5));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));

    // Reconnect-equivalent: state reset, so on_connected() must request a
    // fresh snapshot again.
    EXPECT_EQ(kinds_of(sync.on_connected()), (std::vector{Kind::RequestSnapshot}));

    // The buffered event must have been kept, not discarded - a snapshot
    // landing inside its range now bridges immediately.
    auto resync_actions = sync.on_snapshot(make_snapshot(10));
    ASSERT_EQ(kinds_of(resync_actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
}

TEST(SymbolSyncOkxTest, PostGapBufferedEventIsNotDiscardedByTheEmptyBufferShortcut) {
    OkxSync sync;
    sync.on_connected();
    ASSERT_EQ(kinds_of(sync.on_snapshot(make_snapshot(10))),
              (std::vector{Kind::ApplySnapshot}));  // live, last_final_id_ == 10

    // A gap: prev_final_id (999) doesn't match the last applied final_id (10).
    auto gap_actions = sync.on_depth_update(make_update(1000, 1000, /*prev_final_id=*/999));
    EXPECT_EQ(kinds_of(gap_actions), (std::vector{Kind::InvalidateVenue}));

    // A fresher snapshot (2000) drops that buffered event as stale
    // (1000 < 2000) - buffer_ ends up empty *after* dropping, but was
    // non-empty *at entry* (that's the whole point of this test - the
    // empty-buffer shortcut must not be fooled by the post-drop state), so
    // this is InvalidateVenue, not "nothing was ever buffered" territory.
    auto actions = sync.on_snapshot(make_snapshot(2000));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));
}

TEST(SymbolSyncOkxTest, DropBoundaryIsStrictLessThan) {
    OkxSync sync;
    sync.on_connected();
    // final_id == last_update_id exactly: must survive the drop filter
    // (should_drop_buffered is strict <, not <=).
    sync.on_depth_update(make_update(160, 160, /*prev_final_id=*/149));

    // Survives the drop filter but doesn't bridge (prev_final_id 149 !=
    // snapshot's last_update_id 160) - buffer_ was non-empty at entry, so
    // InvalidateVenue.
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));
}

TEST(SymbolSyncOkxTest, BridgeCanLandAfterAnEarlierNonBridgingSurvivorAcrossASequenceReset) {
    // OkxSequencePolicy's seqId is explicitly not assumed monotonic (see its
    // doc comment) - unlike Binance/Bybit, should_drop_buffered()'s numeric
    // `<` comparison can under-drop across a documented sequence reset,
    // leaving a stale, non-bridging survivor sitting *before* the real
    // bridge in arrival order. on_snapshot() must still find and apply from
    // that later bridge, silently discarding the earlier survivor, rather
    // than assuming (or asserting) the first survivor is always the bridge.
    //
    // Buffered while still connecting: A{prev_final_id=15, final_id=3} (the
    // reset event itself - seqId drops to 3 but still chains from a prior
    // seqId of 15), then B{prev_final_id=3, final_id=5} (chains from A).
    // A snapshot with last_update_id=3 arrives: should_drop_buffered keeps
    // both (3<3 and 5<3 are both false), but only B bridges (prev_final_id
    // 3 == 3) - A does not (prev_final_id 15 != 3). Only B's data must
    // apply; A is superseded by the snapshot's own state and must not be.
    OkxSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(3, 3, /*prev_final_id=*/15, {{Price(1.0), Size(1.0)}}));
    sync.on_depth_update(make_update(5, 5, /*prev_final_id=*/3, {{Price(2.0), Size(2.0)}}));

    auto actions = sync.on_snapshot(make_snapshot(3));
    ASSERT_EQ(kinds_of(actions), (std::vector{Kind::ApplySnapshot, Kind::ApplyDelta}));
    EXPECT_EQ(std::get<ApplyDelta>(actions[1]).bids,
              (std::vector<std::pair<Price, Size>>{{Price(2.0), Size(2.0)}}));  // B, not A

    // last_final_id_ must be B's final_id (5), not A's (3): a directly
    // chaining update now applies live.
    auto live = sync.on_depth_update(make_update(6, 6, /*prev_final_id=*/5));
    EXPECT_EQ(kinds_of(live), (std::vector{Kind::ApplyDelta}));
}

TEST(SymbolSyncOkxTest, BridgeIsExactPrevSeqIdEqualityNotARange) {
    // Unlike Futures' first_id<=last_update_id<=final_id range check, OKX
    // has an explicit prevSeqId back-pointer - bridging is an exact
    // prev_final_id == snapshot.last_update_id equality.
    OkxSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(165, 165, /*prev_final_id=*/12));

    // Not a match: this event's prev_final_id (12) != snapshot's
    // last_update_id (10) - buffer_ non-empty at entry, so InvalidateVenue
    // (see
    // NonEmptyBufferThatDoesNotBridgeForcesAGapDespiteTrustingConnectionOrder).
    auto actions = sync.on_snapshot(make_snapshot(10));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::InvalidateVenue}));
}

// Traces the exact four-message worked example from OKX's own
// documentation (quoted directly by the user, not assumed from training
// data): a normal update, an idle-heartbeat (prevSeqId == seqId, empty
// bids/asks), then a sequence reset (seqId itself moves backward, but
// prevSeqId still chains from the prior seqId) - all four must apply as
// ordinary contiguous updates, with no special-casing for the heartbeat or
// the reset. This is the scenario OkxSequencePolicy's doc comment in
// symbol_sync.hpp describes by hand; this test is what actually proves it
// against the real state machine rather than a comment alone.
TEST(SymbolSyncOkxTest, IdleHeartbeatAndSequenceResetChainLikeOkxsDocumentedExample) {
    OkxSync sync;
    sync.on_connected();

    // Snapshot: prevSeqId=-1, seqId=10.
    ASSERT_EQ(kinds_of(sync.on_snapshot(make_snapshot(10))), (std::vector{Kind::ApplySnapshot}));

    // Normal update: prevSeqId=10, seqId=15.
    auto normal = sync.on_depth_update(make_update(15, 15, /*prev_final_id=*/10, {{Price(1.0), Size(1.0)}}));
    ASSERT_EQ(kinds_of(normal), (std::vector{Kind::ApplyDelta}));

    // Idle heartbeat: prevSeqId=15, seqId=15 (chains from itself), empty
    // bids/asks - must still apply as a normal (harmless) delta, not be
    // treated as a gap or silently dropped (dropping it would break the
    // prevSeqId chain for the next message).
    auto heartbeat = sync.on_depth_update(make_update(15, 15, /*prev_final_id=*/15));
    ASSERT_EQ(kinds_of(heartbeat), (std::vector{Kind::ApplyDelta}));
    EXPECT_TRUE(std::get<ApplyDelta>(heartbeat[0]).bids.empty());
    EXPECT_TRUE(std::get<ApplyDelta>(heartbeat[0]).asks.empty());

    // Sequence reset: prevSeqId=15 (still chains from the last applied
    // seqId), seqId=3 (the counter itself moves backward) - is_contiguous
    // only compares prev_final_id against last_applied_final_id, so this
    // is not treated as a gap despite seqId decreasing.
    auto reset = sync.on_depth_update(make_update(3, 3, /*prev_final_id=*/15, {{Price(2.0), Size(2.0)}}));
    ASSERT_EQ(kinds_of(reset), (std::vector{Kind::ApplyDelta}));

    // Normal update after the reset: prevSeqId=3, seqId=5.
    auto after_reset = sync.on_depth_update(make_update(5, 5, /*prev_final_id=*/3, {{Price(3.0), Size(3.0)}}));
    EXPECT_EQ(kinds_of(after_reset), (std::vector{Kind::ApplyDelta}));
}

}  // namespace okx

}  // namespace
}  // namespace bobby::hermeneutic

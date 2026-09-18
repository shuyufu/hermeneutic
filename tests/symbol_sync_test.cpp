#include "bobby/hermeneutic/symbol_sync.hpp"

#include <gtest/gtest.h>

#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

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

TEST(SymbolSyncBybitTest, NonEmptyBufferThatDoesNotBridgeStillRetriesDespiteTrustingConnectionOrder) {
    // The empty-buffer branch is deliberately narrow: a *non-empty* buffer
    // that still doesn't bridge is not silently trusted just because this
    // policy sets kTrustsConnectionOrder - that would mean some event
    // arrived before the snapshot despite Bybit's ordering guarantee,
    // which is unexpected enough to fall back to the ordinary retry path
    // rather than being papered over.
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(165, 165, /*prev_final_id=*/0));

    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::RequestSnapshot}));
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
    // bridge, not the "first message ever" case. Must retry, not apply.
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::RequestSnapshot}));
}

TEST(SymbolSyncBybitTest, DropBoundaryIsNonStrictLessThanOrEqualUnlikeFutures) {
    // Mirrors SymbolSyncTest.DropBoundaryIsStrictLessThan, inverted: Bybit
    // drops final_id == last_update_id (non-strict <=); Futures keeps it.
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(160, 160, /*prev_final_id=*/0));

    // final_id (160) == last_update_id (160): dropped under Bybit's rule,
    // so nothing survives to bridge - must retry, not apply.
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::RequestSnapshot}));
}

TEST(SymbolSyncBybitTest, BridgeIsExactUPlusOneNotARange) {
    // Unlike Futures' first_id<=last_update_id<=final_id range check, Bybit
    // has only one id per message, so bridging is an exact u == snapshot.u+1
    // equality - a later, non-adjacent event does not bridge even though it
    // would satisfy Futures' range-based condition.
    BybitSync sync;
    sync.on_connected();
    sync.on_depth_update(make_update(165, 165, /*prev_final_id=*/0));

    // Not a match: snapshot.last_update_id+1 (161) != this event's u (165).
    auto actions = sync.on_snapshot(make_snapshot(160));
    EXPECT_EQ(kinds_of(actions), (std::vector{Kind::RequestSnapshot}));
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

}  // namespace
}  // namespace bobby::hermeneutic

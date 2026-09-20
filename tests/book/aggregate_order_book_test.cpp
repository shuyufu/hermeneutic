#include "bobby/hermeneutic/book/aggregate_order_book.hpp"

#include <gtest/gtest.h>

#include <array>
#include <system_error>
#include <utility>

#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic {
namespace {

using bobby::hermeneutic::symbol::Exchange;
using bobby::hermeneutic::symbol::MarketType;

// Two arbitrary, distinct VenueIds - these tests only need "two different
// venues" to exercise per-venue isolation, never the real to_string(VenueId)
// spelling, so MarketType::Spot on both is an arbitrary (but fixed) choice,
// not a claim about what market either actually covers.
constexpr VenueId kBinance{Exchange::Binance, MarketType::Spot};
constexpr VenueId kOkx{Exchange::Okx, MarketType::Spot};

TEST(AggregateOrderBook, SingleVenueAddUpdateRemove) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Bid, Price(100.0), Size(1.0));
    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(1.0));

    book.apply_delta(kBinance, Side::Bid, Price(100.0), Size(2.5));
    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(2.5));

    book.apply_delta(kBinance, Side::Bid, Price(100.0), Size(0.0));
    EXPECT_EQ(book.aggregate().bids.count(Price(100.0)), 0u);
}

TEST(AggregateOrderBook, MultipleVenuesAtSamePriceSum) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(1.0));
    book.apply_delta(kOkx, Side::Ask, Price(101.0), Size(2.0));

    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(3.0));
}

TEST(AggregateOrderBook, RemovingOneVenueKeepsOthersContribution) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(1.0));
    book.apply_delta(kOkx, Side::Ask, Price(101.0), Size(2.0));

    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(0.0));

    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(2.0));

    book.apply_delta(kOkx, Side::Ask, Price(101.0), Size(0.0));
    EXPECT_EQ(book.aggregate().asks.count(Price(101.0)), 0u);
}

TEST(AggregateOrderBook, PerVenueBookIsIndependentlyQueryable) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Bid, Price(99.0), Size(1.0));
    book.apply_delta(kOkx, Side::Bid, Price(99.5), Size(2.0));

    ASSERT_EQ(book.venues().count(kBinance), 1u);
    ASSERT_EQ(book.venues().count(kOkx), 1u);
    EXPECT_EQ(book.venues().at(kBinance).bids.at(Price(99.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kOkx).bids.at(Price(99.5)), Size(2.0));

    // Aggregate keeps the same ordering guarantees as a single L2OrderBook.
    EXPECT_EQ(book.aggregate().bids.begin()->first, Price(99.5));
}

TEST(AggregateOrderBook, AggregateAsksAscendingBidsDescendingAcrossVenues) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(1.0));
    book.apply_delta(kOkx, Side::Ask, Price(100.5), Size(1.0));
    book.apply_delta(kBinance, Side::Ask, Price(102.0), Size(1.0));

    book.apply_delta(kBinance, Side::Bid, Price(99.0), Size(1.0));
    book.apply_delta(kOkx, Side::Bid, Price(99.5), Size(1.0));
    book.apply_delta(kBinance, Side::Bid, Price(98.0), Size(1.0));

    auto ask_it = book.aggregate().asks.begin();
    EXPECT_EQ(ask_it->first, Price(100.5));
    ++ask_it;
    EXPECT_EQ(ask_it->first, Price(101.0));
    ++ask_it;
    EXPECT_EQ(ask_it->first, Price(102.0));

    auto bid_it = book.aggregate().bids.begin();
    EXPECT_EQ(bid_it->first, Price(99.5));
    ++bid_it;
    EXPECT_EQ(bid_it->first, Price(99.0));
    ++bid_it;
    EXPECT_EQ(bid_it->first, Price(98.0));
}

TEST(AggregateOrderBook, InvalidateSoleVenueClearsAggregateLevels) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Bid, Price(99.0), Size(1.0));
    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(2.0));

    book.invalidate_venue(kBinance);

    EXPECT_TRUE(book.aggregate().bids.empty());
    EXPECT_TRUE(book.aggregate().asks.empty());
    EXPECT_EQ(book.venues().count(kBinance), 0u);
}

TEST(AggregateOrderBook, InvalidateVenueKeepsOtherVenuesContribution) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(1.0));
    book.apply_delta(kOkx, Side::Ask, Price(101.0), Size(2.0));
    book.apply_delta(kOkx, Side::Ask, Price(102.0), Size(5.0));

    book.invalidate_venue(kBinance);

    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(2.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(102.0)), Size(5.0));
    EXPECT_EQ(book.venues().count(kBinance), 0u);
    ASSERT_EQ(book.venues().count(kOkx), 1u);
}

TEST(AggregateOrderBook, InvalidateUnknownVenueIsNoOp) {
    AggregateOrderBook book;
    book.apply_delta(kOkx, Side::Bid, Price(99.0), Size(1.0));

    book.invalidate_venue(kBinance);

    EXPECT_EQ(book.aggregate().bids.at(Price(99.0)), Size(1.0));
}

TEST(AggregateOrderBook, ApplySnapshotReplacesVenueSideWholesale) {
    AggregateOrderBook book;

    // Stale state before resync: 100.0 and 101.0 from a diff stream.
    book.apply_delta(kBinance, Side::Ask, Price(100.0), Size(1.0));
    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(2.0));
    book.apply_delta(kOkx, Side::Ask, Price(100.0), Size(4.0));

    // REST snapshot: 100.0 unchanged, 101.0 gone, 102.0 new. binance has no
    // bids anywhere in this test, so an empty bids span here is a genuine
    // no-op, not accidentally wiping real data.
    const std::array snapshot = {std::pair{Price(100.0), Size(1.0)}, std::pair{Price(102.0), Size(3.0)}};
    book.apply_snapshot(kBinance, {}, snapshot);

    EXPECT_EQ(book.venues().at(kBinance).asks.count(Price(101.0)), 0u);
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(100.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(102.0)), Size(3.0));

    // okx's share at 100.0 must be untouched by binance's resync.
    EXPECT_EQ(book.aggregate().asks.at(Price(100.0)), Size(5.0));
    EXPECT_EQ(book.aggregate().asks.count(Price(101.0)), 0u);
    EXPECT_EQ(book.aggregate().asks.at(Price(102.0)), Size(3.0));
}

TEST(AggregateOrderBook, InvalidateThenApplySnapshotResyncsCleanly) {
    AggregateOrderBook book;

    book.apply_delta(kBinance, Side::Bid, Price(99.0), Size(1.0));
    book.apply_delta(kBinance, Side::Bid, Price(98.5), Size(2.0));

    // Disconnect: drop everything binance had contributed.
    book.invalidate_venue(kBinance);
    ASSERT_TRUE(book.aggregate().bids.empty());

    // Resync from a fresh REST snapshot. binance has no asks anywhere in
    // this test, so an empty asks span here is a genuine no-op.
    const std::array snapshot = {std::pair{Price(99.0), Size(1.5)}, std::pair{Price(97.0), Size(1.0)}};
    book.apply_snapshot(kBinance, snapshot, {});

    EXPECT_EQ(book.aggregate().bids.at(Price(99.0)), Size(1.5));
    EXPECT_EQ(book.aggregate().bids.at(Price(97.0)), Size(1.0));
    EXPECT_EQ(book.aggregate().bids.count(Price(98.5)), 0u);
}

TEST(AggregateOrderBook, ApplyDeltaRejectsNegativeSize) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Bid, Price(99.0), Size(1.0));

    auto result = book.apply_delta(kBinance, Side::Bid, Price(98.0), Size(-1.0));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);

    // The rejected call must not have mutated any state.
    EXPECT_EQ(book.aggregate().bids.count(Price(98.0)), 0u);
    EXPECT_EQ(book.aggregate().bids.at(Price(99.0)), Size(1.0));
}

TEST(AggregateOrderBook, ApplySnapshotRejectsNegativeSizeAtomically) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Ask, Price(100.0), Size(1.0));

    const std::array snapshot = {std::pair{Price(100.0), Size(2.0)},
                                  std::pair{Price(101.0), Size(-1.0)}};
    auto result = book.apply_snapshot(kBinance, {}, snapshot);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);

    // Validation must run before any level is touched.
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(100.0)), Size(1.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(100.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kBinance).asks.count(Price(101.0)), 0u);
}

// A non-positive price used to pass through unchecked (only size was
// validated) - see require_valid_level()'s own comment. Both zero and
// negative are rejected the same way size's own <0 check is: with
// std::errc::invalid_argument and no state mutated.
TEST(AggregateOrderBook, ApplyDeltaRejectsNonPositivePrice) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Bid, Price(99.0), Size(1.0));

    auto zero_result = book.apply_delta(kBinance, Side::Bid, Price::from_raw(0), Size(1.0));
    ASSERT_FALSE(zero_result.has_value());
    EXPECT_EQ(zero_result.error(), std::errc::invalid_argument);

    auto negative_result = book.apply_delta(kBinance, Side::Bid, Price::from_raw(-1), Size(1.0));
    ASSERT_FALSE(negative_result.has_value());
    EXPECT_EQ(negative_result.error(), std::errc::invalid_argument);

    // Neither rejected call mutated any state.
    EXPECT_EQ(book.aggregate().bids.count(Price::from_raw(0)), 0u);
    EXPECT_EQ(book.aggregate().bids.count(Price::from_raw(-1)), 0u);
    EXPECT_EQ(book.aggregate().bids.at(Price(99.0)), Size(1.0));
}

TEST(AggregateOrderBook, ApplySnapshotRejectsNonPositivePriceAtomically) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Ask, Price(100.0), Size(1.0));

    const std::array snapshot = {std::pair{Price(100.0), Size(2.0)},
                                  std::pair{Price::from_raw(0), Size(1.0)}};
    auto result = book.apply_snapshot(kBinance, {}, snapshot);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);

    // Validation must run before any level is touched.
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(100.0)), Size(1.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(100.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kBinance).asks.count(Price::from_raw(0)), 0u);
}

TEST(AggregateOrderBook, ApplySnapshotAppliesBothSidesInOneCall) {
    AggregateOrderBook book;

    const std::array bids = {std::pair{Price(99.0), Size(1.0)}, std::pair{Price(98.0), Size(2.0)}};
    const std::array asks = {std::pair{Price(101.0), Size(3.0)}};
    ASSERT_TRUE(book.apply_snapshot(kBinance, bids, asks).has_value());

    EXPECT_EQ(book.venues().at(kBinance).bids.at(Price(99.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kBinance).bids.at(Price(98.0)), Size(2.0));
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(101.0)), Size(3.0));
    EXPECT_EQ(book.aggregate().bids.at(Price(99.0)), Size(1.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(3.0));
}

// The bug this guards against: apply_snapshot() used to take one side at a
// time, so a caller resyncing both sides of a venue made two independent
// calls - a bad level on one side could be rejected while the other side's
// (perfectly valid) resync had already gone through, leaving the venue's
// book genuinely half-resynced with nothing downstream able to tell (see
// venue_session.hpp's own historical comment on this). Taking both sides
// in one call and validating both before touching either - the same shape
// apply_batch() already used - closes that gap: a bad level anywhere
// rejects the whole snapshot, valid side included.
TEST(AggregateOrderBook, ApplySnapshotRejectsBadLevelOnEitherSideWithoutTouchingTheOther) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Bid, Price(50.0), Size(1.0));
    book.apply_delta(kBinance, Side::Ask, Price(100.0), Size(1.0));

    // A perfectly good bids resync, paired with an asks resync containing
    // one bad level.
    const std::array good_bids = {std::pair{Price(99.0), Size(2.0)}};
    const std::array bad_asks = {std::pair{Price(101.0), Size(1.0)}, std::pair{Price(102.0), Size(-1.0)}};
    auto result = book.apply_snapshot(kBinance, good_bids, bad_asks);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);

    // Neither side was touched - not even the bids side, whose own data
    // was entirely valid.
    EXPECT_EQ(book.venues().at(kBinance).bids.count(Price(99.0)), 0u);
    EXPECT_EQ(book.venues().at(kBinance).bids.at(Price(50.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kBinance).asks.count(Price(101.0)), 0u);
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(100.0)), Size(1.0));
}

// Unlike apply_batch(), where an empty span means "no changes on this
// side" (an exchange delta message can legitimately touch only one side),
// apply_snapshot()'s levels are the *complete* state for that side - an
// empty span means this venue now holds nothing there, the same as if
// every existing level had been explicitly dropped from the snapshot.
// This matches a real REST snapshot response, which always reports both
// sides' complete current state at once.
TEST(AggregateOrderBook, ApplySnapshotWithEmptySpanClearsThatSide) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Bid, Price(99.0), Size(1.0));
    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(1.0));

    const std::array new_bids = {std::pair{Price(98.0), Size(1.0)}};
    ASSERT_TRUE(book.apply_snapshot(kBinance, new_bids, {}).has_value());

    EXPECT_EQ(book.venues().at(kBinance).bids.count(Price(99.0)), 0u);
    EXPECT_EQ(book.venues().at(kBinance).bids.at(Price(98.0)), Size(1.0));
    EXPECT_TRUE(book.venues().at(kBinance).asks.empty());
    EXPECT_TRUE(book.aggregate().asks.empty());
}

// apply_batch()'s own aggregation/atomicity behavior belongs here, not in
// aggregator_service_test.cpp's AggregatorServiceTest.ApplyBatch* cases:
// those go through a real SymbolBook over an actual gRPC server to check
// SymbolBook's own diff/broadcast wiring (one seq bump per batch, diff
// ordering, no broadcast on rejection) - concepts that don't exist on
// AggregateOrderBook itself. That file is also gated behind
// HERMENEUTIC_BUILD_SERVICE (needs the gRPC/protobuf vcpkg toolchain), so
// it isn't part of the always-built hermeneutic_tests binary - these
// cases are what actually exercise apply_batch()'s own correctness by
// default.
TEST(AggregateOrderBook, ApplyBatchAppliesBidsAndAsksInOneCall) {
    AggregateOrderBook book;

    std::array<std::pair<Price, Size>, 2> bids{{
        {Price(102.0), Size(1.0)},
        {Price(100.0), Size(2.0)},
    }};
    std::array<std::pair<Price, Size>, 1> asks{{
        {Price(101.0), Size(3.0)},
    }};
    ASSERT_TRUE(book.apply_batch(kBinance, bids, asks).has_value());

    EXPECT_EQ(book.aggregate().bids.at(Price(102.0)), Size(1.0));
    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(2.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(3.0));
    EXPECT_EQ(book.venues().at(kBinance).bids.at(Price(102.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(101.0)), Size(3.0));
}

TEST(AggregateOrderBook, ApplyBatchAggregatesAcrossVenuesLikeApplyDelta) {
    AggregateOrderBook book;
    book.apply_delta(kOkx, Side::Bid, Price(100.0), Size(5.0));

    std::array<std::pair<Price, Size>, 2> bids{{
        {Price(100.0), Size(3.0)},
        {Price(99.0), Size(1.0)},
    }};
    ASSERT_TRUE(book.apply_batch(kBinance, bids, {}).has_value());

    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(8.0));  // okx's 5 + binance's 3
    EXPECT_EQ(book.aggregate().bids.at(Price(99.0)), Size(1.0));   // binance only
}

TEST(AggregateOrderBook, ApplyBatchWithOneSideEmptyOnlyTouchesTheOtherSide) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Ask, Price(101.0), Size(1.0));

    std::array<std::pair<Price, Size>, 1> bids{{
        {Price(100.0), Size(2.0)},
    }};
    ASSERT_TRUE(book.apply_batch(kBinance, bids, {}).has_value());

    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(2.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(1.0));  // untouched
}

TEST(AggregateOrderBook, ApplyBatchRejectsNegativeSizeAtomically) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Bid, Price(100.0), Size(1.0));

    std::array<std::pair<Price, Size>, 2> bad_bids{{
        {Price(99.0), Size(1.0)},
        {Price(98.0), Size(-1.0)},
    }};
    std::array<std::pair<Price, Size>, 1> asks{{
        {Price(101.0), Size(1.0)},
    }};
    auto result = book.apply_batch(kBinance, bad_bids, asks);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);

    // Nothing from the rejected batch was applied - not even the good bid
    // ahead of the bad one, nor the ask side checked after it.
    EXPECT_EQ(book.aggregate().bids.count(Price(99.0)), 0u);
    EXPECT_EQ(book.aggregate().asks.count(Price(101.0)), 0u);
    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(1.0));
}

TEST(AggregateOrderBook, ApplyBatchRejectsNonPositivePriceAtomically) {
    AggregateOrderBook book;
    book.apply_delta(kBinance, Side::Ask, Price(100.0), Size(1.0));

    std::array<std::pair<Price, Size>, 1> bids{{
        {Price(99.0), Size(1.0)},
    }};
    std::array<std::pair<Price, Size>, 1> bad_asks{{
        {Price::from_raw(0), Size(1.0)},
    }};
    auto result = book.apply_batch(kBinance, bids, bad_asks);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);

    EXPECT_EQ(book.aggregate().bids.count(Price(99.0)), 0u);
    EXPECT_EQ(book.aggregate().asks.at(Price(100.0)), Size(1.0));
}

}  // namespace
}  // namespace bobby::hermeneutic

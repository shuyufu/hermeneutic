#include "bobby/hermeneutic/book/aggregate_order_book.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <system_error>
#include <utility>
#include <vector>

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

// AggregateOrderBook has no single-level apply_delta() (see apply_batch()'s
// own doc comment on why). This gives these tests that single-level
// convenience via apply_batch() with a one-element span on the requested
// side and an empty span on the other.
std::expected<void, std::errc> apply_one(AggregateOrderBook& book, const VenueId& venue, Side side,
                                          Price price, Size size) {
    std::array<std::pair<Price, Size>, 1> level{{{price, size}}};
    if (side == Side::Bid) return book.apply_batch(venue, level, {});
    return book.apply_batch(venue, {}, level);
}

// Same as above, but forwarding a Sink to the requested side (and a no-op
// to the other) - for the tests that exercise apply_batch()'s Sink contract
// through a single level.
template <typename Sink>
std::expected<void, std::errc> apply_one(AggregateOrderBook& book, const VenueId& venue, Side side,
                                          Price price, Size size, Sink&& on_change) {
    std::array<std::pair<Price, Size>, 1> level{{{price, size}}};
    if (side == Side::Bid) return book.apply_batch(venue, level, {}, on_change, [](Price, Size) {});
    return book.apply_batch(venue, {}, level, [](Price, Size) {}, on_change);
}

TEST(AggregateOrderBook, SingleVenueAddUpdateRemove) {
    AggregateOrderBook book;

    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(1.0));
    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(1.0));

    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(2.5));
    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(2.5));

    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(0.0));
    EXPECT_EQ(book.aggregate().bids.count(Price(100.0)), 0u);
}

TEST(AggregateOrderBook, MultipleVenuesAtSamePriceSum) {
    AggregateOrderBook book;

    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(1.0));
    apply_one(book, kOkx, Side::Ask, Price(101.0), Size(2.0));

    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(3.0));
}

TEST(AggregateOrderBook, RemovingOneVenueKeepsOthersContribution) {
    AggregateOrderBook book;

    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(1.0));
    apply_one(book, kOkx, Side::Ask, Price(101.0), Size(2.0));

    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(0.0));

    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(2.0));

    apply_one(book, kOkx, Side::Ask, Price(101.0), Size(0.0));
    EXPECT_EQ(book.aggregate().asks.count(Price(101.0)), 0u);
}

TEST(AggregateOrderBook, PerVenueBookIsIndependentlyQueryable) {
    AggregateOrderBook book;

    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(1.0));
    apply_one(book, kOkx, Side::Bid, Price(99.5), Size(2.0));

    ASSERT_EQ(book.venues().count(kBinance), 1u);
    ASSERT_EQ(book.venues().count(kOkx), 1u);
    EXPECT_EQ(book.venues().at(kBinance).bids.at(Price(99.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kOkx).bids.at(Price(99.5)), Size(2.0));

    // Aggregate keeps the same ordering guarantees as a single L2OrderBook.
    EXPECT_EQ(book.aggregate().bids.begin()->first, Price(99.5));
}

TEST(AggregateOrderBook, AggregateAsksAscendingBidsDescendingAcrossVenues) {
    AggregateOrderBook book;

    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(1.0));
    apply_one(book, kOkx, Side::Ask, Price(100.5), Size(1.0));
    apply_one(book, kBinance, Side::Ask, Price(102.0), Size(1.0));

    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(1.0));
    apply_one(book, kOkx, Side::Bid, Price(99.5), Size(1.0));
    apply_one(book, kBinance, Side::Bid, Price(98.0), Size(1.0));

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

    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(1.0));
    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(2.0));

    book.invalidate_venue(kBinance);

    EXPECT_TRUE(book.aggregate().bids.empty());
    EXPECT_TRUE(book.aggregate().asks.empty());
    EXPECT_EQ(book.venues().count(kBinance), 0u);
}

TEST(AggregateOrderBook, InvalidateVenueKeepsOtherVenuesContribution) {
    AggregateOrderBook book;

    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(1.0));
    apply_one(book, kOkx, Side::Ask, Price(101.0), Size(2.0));
    apply_one(book, kOkx, Side::Ask, Price(102.0), Size(5.0));

    book.invalidate_venue(kBinance);

    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(2.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(102.0)), Size(5.0));
    EXPECT_EQ(book.venues().count(kBinance), 0u);
    ASSERT_EQ(book.venues().count(kOkx), 1u);
}

TEST(AggregateOrderBook, InvalidateUnknownVenueIsNoOp) {
    AggregateOrderBook book;
    apply_one(book, kOkx, Side::Bid, Price(99.0), Size(1.0));

    book.invalidate_venue(kBinance);

    EXPECT_EQ(book.aggregate().bids.at(Price(99.0)), Size(1.0));
}

TEST(AggregateOrderBook, ApplySnapshotReplacesVenueSideWholesale) {
    AggregateOrderBook book;

    // Stale state before resync: 100.0 and 101.0 from a diff stream.
    apply_one(book, kBinance, Side::Ask, Price(100.0), Size(1.0));
    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(2.0));
    apply_one(book, kOkx, Side::Ask, Price(100.0), Size(4.0));

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

    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(1.0));
    apply_one(book, kBinance, Side::Bid, Price(98.5), Size(2.0));

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

TEST(AggregateOrderBook, ApplySnapshotRejectsNegativeSizeAtomically) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Ask, Price(100.0), Size(1.0));

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

TEST(AggregateOrderBook, ApplySnapshotRejectsNonPositivePriceAtomically) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Ask, Price(100.0), Size(1.0));

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

// Regression test: apply_snapshot() must take both sides in one call and
// validate both before touching either, the same shape apply_batch()
// uses - a caller resyncing both sides of a venue via two independent
// calls could have a bad level on one side rejected while the other
// side's valid resync already went through, leaving the book genuinely
// half-resynced with nothing downstream able to tell.
TEST(AggregateOrderBook, ApplySnapshotRejectsBadLevelOnEitherSideWithoutTouchingTheOther) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Bid, Price(50.0), Size(1.0));
    apply_one(book, kBinance, Side::Ask, Price(100.0), Size(1.0));

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
    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(1.0));
    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(1.0));

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

TEST(AggregateOrderBook, ApplyBatchAggregatesAcrossVenues) {
    AggregateOrderBook book;
    apply_one(book, kOkx, Side::Bid, Price(100.0), Size(5.0));

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
    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(1.0));

    std::array<std::pair<Price, Size>, 1> bids{{
        {Price(100.0), Size(2.0)},
    }};
    ASSERT_TRUE(book.apply_batch(kBinance, bids, {}).has_value());

    EXPECT_EQ(book.aggregate().bids.at(Price(100.0)), Size(2.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(101.0)), Size(1.0));  // untouched
}

TEST(AggregateOrderBook, ApplyBatchRejectsNegativeSizeAtomically) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(1.0));

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

// Regression test: a non-positive price must be rejected, not just size.
// Both zero and negative are rejected the same way size's own <0 check
// is: with std::errc::invalid_argument and no state mutated, atomically
// across the whole batch.
TEST(AggregateOrderBook, ApplyBatchRejectsNonPositivePriceAtomically) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Ask, Price(100.0), Size(1.0));

    std::array<std::pair<Price, Size>, 1> bids{{
        {Price(99.0), Size(1.0)},
    }};
    std::array<std::pair<Price, Size>, 1> zero_asks{{
        {Price::from_raw(0), Size(1.0)},
    }};
    auto zero_result = book.apply_batch(kBinance, bids, zero_asks);
    ASSERT_FALSE(zero_result.has_value());
    EXPECT_EQ(zero_result.error(), std::errc::invalid_argument);

    std::array<std::pair<Price, Size>, 1> negative_asks{{
        {Price::from_raw(-1), Size(1.0)},
    }};
    auto negative_result = book.apply_batch(kBinance, bids, negative_asks);
    ASSERT_FALSE(negative_result.has_value());
    EXPECT_EQ(negative_result.error(), std::errc::invalid_argument);

    EXPECT_EQ(book.aggregate().bids.count(Price(99.0)), 0u);
    EXPECT_EQ(book.aggregate().asks.at(Price(100.0)), Size(1.0));
}

// The `Sink` callbacks below are how aggregator::SymbolBook observes what
// changed - see aggregate_order_book.hpp's own class comment. These cases
// exercise that contract directly, on the always-built binary, since the
// SymbolBook-level tests that also cover it are gRPC-gated.

TEST(AggregateOrderBook, SinkInvokedWithResultingAggregateSize) {
    AggregateOrderBook book;
    apply_one(book, kOkx, Side::Bid, Price(100.0), Size(2.0));

    std::vector<std::pair<Price, Size>> changes;
    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(1.0),
              [&](Price price, Size new_size) { changes.emplace_back(price, new_size); });

    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].first, Price(100.0));
    EXPECT_EQ(changes[0].second, Size(3.0));  // okx's 2 + binance's new 1
}

TEST(AggregateOrderBook, SinkReportsRemovalAsZeroNotNegative) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Ask, Price(100.0), Size(1.0));

    std::vector<std::pair<Price, Size>> changes;
    apply_one(book, kBinance, Side::Ask, Price(100.0), Size(0.0),
              [&](Price price, Size new_size) { changes.emplace_back(price, new_size); });

    ASSERT_EQ(changes.size(), 1u);
    EXPECT_EQ(changes[0].first, Price(100.0));
    EXPECT_EQ(changes[0].second, Size(0.0));
    EXPECT_EQ(book.aggregate().asks.count(Price(100.0)), 0u);
}

TEST(AggregateOrderBook, SinkDoesNotFireForANoOp) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(1.0));

    std::vector<std::pair<Price, Size>> changes;
    // Re-applying the exact same size changes nothing in the aggregate.
    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(1.0),
              [&](Price price, Size new_size) { changes.emplace_back(price, new_size); });

    EXPECT_TRUE(changes.empty());
}

TEST(AggregateOrderBook, InvalidateVenueInvokesSinkOncePerPriceItDrops) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(1.0));
    apply_one(book, kBinance, Side::Ask, Price(101.0), Size(2.0));
    apply_one(book, kOkx, Side::Ask, Price(101.0), Size(5.0));  // survives binance's invalidation

    std::vector<std::pair<Price, Size>> bid_changes, ask_changes;
    book.invalidate_venue(
        kBinance, [&](Price price, Size new_size) { bid_changes.emplace_back(price, new_size); },
        [&](Price price, Size new_size) { ask_changes.emplace_back(price, new_size); });

    ASSERT_EQ(bid_changes.size(), 1u);
    EXPECT_EQ(bid_changes[0].first, Price(99.0));
    EXPECT_EQ(bid_changes[0].second, Size(0.0));  // no one else held it

    ASSERT_EQ(ask_changes.size(), 1u);
    EXPECT_EQ(ask_changes[0].first, Price(101.0));
    EXPECT_EQ(ask_changes[0].second, Size(5.0));  // okx's remaining share, not a removal
}

TEST(AggregateOrderBook, ApplySnapshotSinkFiresForRemovalsAndAdditions) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(1.0));
    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(2.0));

    // 100 is dropped (absent from the new snapshot), 99 is untouched (same
    // size), 98 is a brand new price.
    const std::array snapshot = {std::pair{Price(99.0), Size(2.0)}, std::pair{Price(98.0), Size(3.0)}};
    std::vector<std::pair<Price, Size>> changes;
    book.apply_snapshot(kBinance, snapshot, {},
                         [&](Price price, Size new_size) { changes.emplace_back(price, new_size); },
                         [](Price, Size) {});

    // 99 doesn't fire: adjust_aggregate() never calls sink for a zero delta.
    ASSERT_EQ(changes.size(), 2u);
    EXPECT_NE(std::find(changes.begin(), changes.end(), std::pair{Price(100.0), Size(0.0)}),
              changes.end());
    EXPECT_NE(std::find(changes.begin(), changes.end(), std::pair{Price(98.0), Size(3.0)}),
              changes.end());
}

TEST(AggregateOrderBook, ApplySnapshotReportsAllRemovalsBeforeAnyAddition) {
    AggregateOrderBook book;
    apply_one(book, kBinance, Side::Bid, Price(100.0), Size(1.0));
    apply_one(book, kBinance, Side::Bid, Price(99.0), Size(2.0));
    apply_one(book, kBinance, Side::Bid, Price(97.0), Size(4.0));

    // 100 and 97 are dropped, 99 stays, 96 and 95 are brand new - two
    // removals and two additions in one apply_snapshot() call, enough that
    // an accidental interleaving (unlike ApplySnapshotSinkFiresForRemovals
    // AndAdditions above, whose single removal/single addition can't tell
    // "always before" apart from "happens to land first") would be caught
    // here. resync_side()'s own doc comment: every removal is reported
    // before any addition, regardless of price order.
    const std::array snapshot = {std::pair{Price(99.0), Size(2.0)}, std::pair{Price(96.0), Size(5.0)},
                                  std::pair{Price(95.0), Size(6.0)}};
    std::vector<std::pair<Price, Size>> changes;
    book.apply_snapshot(kBinance, snapshot, {},
                         [&](Price price, Size new_size) { changes.emplace_back(price, new_size); },
                         [](Price, Size) {});

    ASSERT_EQ(changes.size(), 4u);
    // The first two entries are the two removals (100, 97), the last two
    // the two additions (96, 95) - not a value-based rule (a genuine size-0
    // addition is possible in general; see l2_order_book.hpp's
    // is_valid_level()), just what this fixture's own chosen sizes happen
    // to produce, used here only to tell "removal" and "addition" apart in
    // the assertions below without re-deriving each price's identity.
    // A removal's reported size is always 0 (nobody else held that price).
    EXPECT_EQ(changes[0].second, Size(0.0));
    EXPECT_EQ(changes[1].second, Size(0.0));
    EXPECT_NE(changes[2].second, Size(0.0));
    EXPECT_NE(changes[3].second, Size(0.0));
}

}  // namespace
}  // namespace bobby::hermeneutic

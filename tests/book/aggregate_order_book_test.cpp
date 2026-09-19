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

    // REST snapshot: 100.0 unchanged, 101.0 gone, 102.0 new.
    const std::array snapshot = {std::pair{Price(100.0), Size(1.0)}, std::pair{Price(102.0), Size(3.0)}};
    book.apply_snapshot(kBinance, Side::Ask, snapshot);

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

    // Resync from a fresh REST snapshot.
    const std::array snapshot = {std::pair{Price(99.0), Size(1.5)}, std::pair{Price(97.0), Size(1.0)}};
    book.apply_snapshot(kBinance, Side::Bid, snapshot);

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
    auto result = book.apply_snapshot(kBinance, Side::Ask, snapshot);
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
    auto result = book.apply_snapshot(kBinance, Side::Ask, snapshot);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);

    // Validation must run before any level is touched.
    EXPECT_EQ(book.venues().at(kBinance).asks.at(Price(100.0)), Size(1.0));
    EXPECT_EQ(book.aggregate().asks.at(Price(100.0)), Size(1.0));
    EXPECT_EQ(book.venues().at(kBinance).asks.count(Price::from_raw(0)), 0u);
}

}  // namespace
}  // namespace bobby::hermeneutic

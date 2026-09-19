#include "bobby/hermeneutic/book/price_bands.hpp"

#include <array>
#include <span>
#include <system_error>

#include <gtest/gtest.h>

namespace bobby::hermeneutic {
namespace {

TEST(PriceBands, OffsetByBpsUpAndDown) {
    EXPECT_EQ(detail::offset_by_bps(Price(100.0), 50, /*round_down=*/true), Price(100.5));
    EXPECT_EQ(detail::offset_by_bps(Price(100.0), -50, /*round_down=*/false), Price(99.5));
    EXPECT_EQ(detail::offset_by_bps(Price(100.0), 0, /*round_down=*/true), Price(100.0));
}

TEST(PriceBands, OffsetByBpsRoundsInwardRatherThanToNearest) {
    // raw(5) * (10000 + 1000) = 55000, /10000 = 5.5 exactly: rounding down
    // (ask) keeps the boundary at 5, not 6 — 6 would be outside the true
    // 5.5 boundary.
    EXPECT_EQ(detail::offset_by_bps(Price::from_raw(5), 1000, /*round_down=*/true).raw(), 5);
    // raw(6) * (10000 - 1000) = 54000, /10000 = 5.4: rounding up (bid)
    // keeps the boundary at 6, not 5 — 5 would be outside the true 5.4
    // boundary (5 < 5.4).
    EXPECT_EQ(detail::offset_by_bps(Price::from_raw(6), -1000, /*round_down=*/false).raw(), 6);
}

// Regression test: rounding the boundary to *nearest* (rather than inward)
// would compute boundary_raw=6 here (exact boundary is 5.5), wrongly
// admitting the level at raw=6, which is actually beyond the true
// threshold. Confirmed this fails against the pre-fix implementation
// before the inward-rounding fix was applied.
TEST(PriceBands, AskExcludesLevelJustBeyondTheExactBpsBoundary) {
    L2OrderBook book;
    book.asks[Price::from_raw(5)] = Size(1.0);
    book.asks[Price::from_raw(6)] = Size(1.0);

    auto result = ask_price_band_depths(book, std::array{1000});
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 1u);
    EXPECT_EQ(bands[0].boundary_price.raw(), 5);
    EXPECT_EQ(bands[0].cumulative_size, Size::from_raw(1'000'000));  // only raw=5 included
}

// Mirror of the above for the bid side: exact boundary is 5.4 (raw), so a
// level at raw=5 must be excluded. Rounding to nearest would compute
// boundary_raw=5 here and wrongly admit it.
TEST(PriceBands, BidExcludesLevelJustBeyondTheExactBpsBoundary) {
    L2OrderBook book;
    book.bids[Price::from_raw(6)] = Size(1.0);
    book.bids[Price::from_raw(5)] = Size(1.0);

    auto result = bid_price_band_depths(book, std::array{1000});
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 1u);
    EXPECT_EQ(bands[0].boundary_price.raw(), 6);
    EXPECT_EQ(bands[0].cumulative_size, Size::from_raw(1'000'000));  // only raw=6 included
}

// Pins that membership is decided by the exact detail::within_bps(), not by
// comparing against the (rounded) boundary_price. Exact boundary here is
// 5.5 (raw(5)*(10000+1000)/10000); raw=6 lies strictly beyond it and must
// be excluded regardless of how offset_by_bps() rounds its display value.
TEST(PriceBands, WithinBpsIsExactRegardlessOfBoundaryDisplayRounding) {
    EXPECT_FALSE(detail::within_bps(Price::from_raw(6), Price::from_raw(5), 1000, /*ge=*/false));
    EXPECT_TRUE(detail::within_bps(Price::from_raw(5), Price::from_raw(5), 1000, /*ge=*/false));

    // Mirror on the bid side: exact boundary is 5.4 (raw(6)*(10000-1000)/10000);
    // raw=5 lies strictly beyond it (below) and must be excluded.
    EXPECT_FALSE(detail::within_bps(Price::from_raw(5), Price::from_raw(6), -1000, /*ge=*/true));
    EXPECT_TRUE(detail::within_bps(Price::from_raw(6), Price::from_raw(6), -1000, /*ge=*/true));
}

TEST(PriceBands, AskBandsWalkBestAskFirstAndIncludeBoundaryInclusive) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);  // best ask
    book.asks[Price(100.5)] = Size(3.0);  // exactly at the 50bps boundary
    book.asks[Price(100.8)] = Size(2.0);  // beyond 50bps, within 100bps
    book.asks[Price(101.0)] = Size(4.0);  // exactly at the 100bps boundary
    book.asks[Price(101.5)] = Size(1.0);  // beyond 100bps, within 200bps
    book.asks[Price(103.0)] = Size(10.0);  // beyond every threshold

    std::vector<int> bps_thresholds = {50, 100, 200};
    auto result = ask_price_band_depths(book, bps_thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 3u);

    EXPECT_EQ(bands[0].bps, 50);
    EXPECT_EQ(bands[0].boundary_price, Price(100.5));
    EXPECT_EQ(bands[0].cumulative_size, Size(8.0));
    EXPECT_EQ(bands[0].cumulative_notional, Notional(801.5));

    EXPECT_EQ(bands[1].bps, 100);
    EXPECT_EQ(bands[1].boundary_price, Price(101.0));
    EXPECT_EQ(bands[1].cumulative_size, Size(14.0));
    EXPECT_EQ(bands[1].cumulative_notional, Notional(1'407.1));

    EXPECT_EQ(bands[2].bps, 200);
    EXPECT_EQ(bands[2].boundary_price, Price(102.0));
    EXPECT_EQ(bands[2].cumulative_size, Size(15.0));
    EXPECT_EQ(bands[2].cumulative_notional, Notional(1'508.6));
}

TEST(PriceBands, BidBandsWalkBestBidFirstAndOffsetDownward) {
    L2OrderBook book;
    book.bids[Price(100.0)] = Size(5.0);  // best bid
    book.bids[Price(99.5)] = Size(3.0);   // exactly at the 50bps boundary
    book.bids[Price(99.0)] = Size(2.0);   // exactly at the 100bps boundary
    book.bids[Price(98.5)] = Size(1.0);   // beyond 100bps

    std::vector<int> bps_thresholds = {50, 100};
    auto result = bid_price_band_depths(book, bps_thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 2u);

    EXPECT_EQ(bands[0].boundary_price, Price(99.5));
    EXPECT_EQ(bands[0].cumulative_size, Size(8.0));
    EXPECT_EQ(bands[0].cumulative_notional, Notional(798.5));

    EXPECT_EQ(bands[1].boundary_price, Price(99.0));
    EXPECT_EQ(bands[1].cumulative_size, Size(10.0));
    EXPECT_EQ(bands[1].cumulative_notional, Notional(996.5));
}

TEST(PriceBands, ThresholdsBeyondBookDepthReportTheWholeBookNotAFailure) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);

    std::vector<int> bps_thresholds = {50, 10'000};
    auto result = ask_price_band_depths(book, bps_thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 2u);
    EXPECT_EQ(bands[0].cumulative_size, Size(5.0));
    EXPECT_EQ(bands[1].boundary_price, Price(200.0));
    EXPECT_EQ(bands[1].cumulative_size, Size(5.0));
    EXPECT_EQ(bands[1].cumulative_notional, Notional(500.0));
}

// Pins that best_price is derived from levels.begin()->first (the highest
// bid, since bids iterate descending) rather than some other element —
// e.g. a future refactor mistakenly reaching for levels.rbegin() (the
// lowest bid, 98.0 here) would compute boundary = 98.0 * 0.995 = 97.51,
// not 99.5, and this test would catch it.
TEST(PriceBands, BidBoundaryIsDerivedFromTheHighestBidNotAnyOtherLevel) {
    L2OrderBook book;
    book.bids[Price(100.0)] = Size(1.0);
    book.bids[Price(99.0)] = Size(1.0);
    book.bids[Price(98.0)] = Size(1.0);

    auto result = bid_price_band_depths(book, std::array{50});
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 1u);
    EXPECT_EQ(bands[0].boundary_price, Price(99.5));
    EXPECT_EQ(bands[0].cumulative_size, Size(1.0));  // only the 100.0 level qualifies
}

TEST(PriceBands, AcceptsThresholdsFromNonVectorContiguousStorage) {
    // bps_thresholds is std::span<const int>: a std::array (fixed, no heap
    // allocation) works directly, as does a sub-range of a larger buffer —
    // this is the whole point of taking span over a fixed const vector&.
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);
    book.asks[Price(101.0)] = Size(4.0);

    std::array<int, 2> bps_thresholds = {50, 100};
    auto result = ask_price_band_depths(book, bps_thresholds);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 2u);

    std::array<int, 4> wider_buffer = {0, 0, 50, 100};
    std::span<const int> sub(wider_buffer.begin() + 2, 2);
    auto from_subspan = ask_price_band_depths(book, sub);
    ASSERT_TRUE(from_subspan.has_value());
    ASSERT_EQ(from_subspan->size(), 2u);
    EXPECT_EQ((*from_subspan)[0].bps, 50);
    EXPECT_EQ((*from_subspan)[1].bps, 100);
}

TEST(PriceBands, EmptyBookReturnsNoBands) {
    L2OrderBook book;
    std::vector<int> bps_thresholds = {50, 100};

    auto ask_result = ask_price_band_depths(book, bps_thresholds);
    auto bid_result = bid_price_band_depths(book, bps_thresholds);
    ASSERT_TRUE(ask_result.has_value());
    ASSERT_TRUE(bid_result.has_value());
    EXPECT_TRUE(ask_result->empty());
    EXPECT_TRUE(bid_result->empty());
}

TEST(PriceBands, ZeroBpsIncludesOnlyTheAggregatedBboLevel) {
    // 0bps means "exactly at BBO": the whole (already-aggregated, e.g. by
    // AggregateOrderBook combining multiple venues) size at the best price,
    // not some smaller partial amount and not the next level up.
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(3.0);
    book.asks[Price(100.1)] = Size(3.0);

    auto result = ask_price_band_depths(book, std::array{0});
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 1u);
    EXPECT_EQ(bands[0].boundary_price, Price(100.0));
    EXPECT_EQ(bands[0].cumulative_size, Size(3.0));
}

TEST(PriceBands, DuplicateThresholdsProduceOneBandEachNotDeduplicated) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);
    book.asks[Price(101.0)] = Size(4.0);  // beyond 50bps (100.5), within 100bps

    auto result = ask_price_band_depths(book, std::array{50, 50, 100});
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 3u);
    EXPECT_EQ(bands[0].bps, 50);
    EXPECT_EQ(bands[1].bps, 50);
    EXPECT_EQ(bands[0].cumulative_size, bands[1].cumulative_size);
    EXPECT_EQ(bands[0].boundary_price, bands[1].boundary_price);
    EXPECT_EQ(bands[2].bps, 100);
    EXPECT_EQ(bands[2].cumulative_size, Size(9.0));
}

TEST(PriceBands, BidBps9999IsAcceptedAtTheEdgeOfTheValidRange) {
    // bps must stay strictly under 10000 on the bid side (see
    // price_band_depth()'s asserts); 9999 is the largest valid value and
    // must not trip that precondition. (bps == 10000 tripping it was
    // confirmed separately via a throwaway program, not a gtest death
    // test -- this codebase's convention for assert-based preconditions.)
    L2OrderBook book;
    book.bids[Price(100.0)] = Size(1.0);

    auto result = bid_price_band_depths(book, std::array{9999});
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 1u);
    EXPECT_EQ(bands[0].cumulative_size, Size(1.0));
}

// Mirrors volume_bands_test.cpp's own RejectsNegativePriceLevel/
// ZeroPriceLevelReportsArgumentOutOfDomainInsteadOfCrashing: a level with a
// non-positive price must be rejected with std::errc::argument_out_of_domain
// (a real runtime check), not silently walked over into offset_by_bps()/
// within_bps(), whose own price>0 precondition is only assert()-checked
// (compiled out under NDEBUG).
TEST(PriceBands, RejectsNegativePriceLevel) {
    L2OrderBook book;
    book.asks[Price::from_raw(-1)] = Size(1.0);

    auto result = ask_price_band_depths(book, std::array{50});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::argument_out_of_domain);
}

TEST(PriceBands, RejectsZeroPriceLevel) {
    L2OrderBook book;
    book.bids[Price::from_raw(0)] = Size(1.0);

    auto result = bid_price_band_depths(book, std::array{50});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::argument_out_of_domain);
}

// The bad level doesn't have to be the best price - price_band_depth()
// checks every level it walks, not just levels.begin()->first. bids sort
// descending (best = highest first, see l2_order_book.hpp), so a
// non-positive price - always numerically the smallest possible - sorts
// last here, unlike on the ask side where it would sort first (ascending)
// and always coincide with the best-price case instead.
TEST(PriceBands, RejectsNonPositivePriceLevelEvenWhenNotTheBest) {
    L2OrderBook book;
    book.bids[Price(100.0)] = Size(1.0);        // valid best bid
    book.bids[Price::from_raw(0)] = Size(1.0);  // malformed, sorts last

    auto result = bid_price_band_depths(book, std::array{50});
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::argument_out_of_domain);
}

}  // namespace
}  // namespace bobby::hermeneutic

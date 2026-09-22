#include "bobby/hermeneutic/book/volume_bands.hpp"

#include <array>
#include <cmath>
#include <span>
#include <system_error>

#include <gtest/gtest.h>

namespace bobby::hermeneutic {
namespace {

TEST(VolumeBands, AskBandsWalkBestAskFirstAndComputeVwap) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);   // notional 500, cumulative 500
    book.asks[Price(125.0)] = Size(20.0);  // notional 2500, cumulative 3000
    book.asks[Price(150.0)] = Size(10.0);  // notional 1500, cumulative 4500

    std::vector<Notional> thresholds = {Notional(500.0), Notional(3000.0), Notional(10'000.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 3u);

    // Exactly consumes the first level.
    ASSERT_TRUE(bands[0].vwap.has_value());
    EXPECT_DOUBLE_EQ(bands[0].vwap->to_double(), 100.0);
    EXPECT_EQ(bands[0].filled_notional, Notional(500.0));

    // Exactly consumes the first two levels; VWAP = 3000 / (5 + 20).
    ASSERT_TRUE(bands[1].vwap.has_value());
    EXPECT_DOUBLE_EQ(bands[1].vwap->to_double(), 120.0);
    EXPECT_EQ(bands[1].filled_notional, Notional(3000.0));

    // Book only holds 4500 of depth, short of the 10000 threshold.
    EXPECT_FALSE(bands[2].vwap.has_value());
    EXPECT_EQ(bands[2].filled_notional, Notional(4500.0));
}

TEST(VolumeBands, BidBandsWalkBestBidFirst) {
    L2OrderBook book;
    book.bids[Price(150.0)] = Size(5.0);   // best bid, notional 750
    book.bids[Price(125.0)] = Size(20.0);  // notional 2500, cumulative 3250
    book.bids[Price(100.0)] = Size(10.0);  // notional 1000, cumulative 4250

    std::vector<Notional> thresholds = {Notional(750.0), Notional(3250.0)};
    auto result = bid_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 2u);

    ASSERT_TRUE(bands[0].vwap.has_value());
    EXPECT_DOUBLE_EQ(bands[0].vwap->to_double(), 150.0);

    ASSERT_TRUE(bands[1].vwap.has_value());
    EXPECT_DOUBLE_EQ(bands[1].vwap->to_double(), 130.0);
}

TEST(VolumeBands, EmptyBookLeavesEveryThresholdUnfilled) {
    L2OrderBook book;
    std::vector<Notional> thresholds = {Notional(1'000'000.0), Notional(5'000'000.0)};

    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 2u);
    for (const auto& band : bands) {
        EXPECT_FALSE(band.vwap.has_value());
        EXPECT_EQ(band.filled_notional, Notional(0.0));
    }
}

TEST(VolumeBands, ThresholdsAreReportedBackOnEachBand) {
    L2OrderBook book;
    book.asks[Price(10.0)] = Size(100.0);

    std::vector<Notional> thresholds = {Notional(500.0), Notional(2000.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 2u);
    EXPECT_EQ(bands[0].notional_threshold, Notional(500.0));
    EXPECT_EQ(bands[1].notional_threshold, Notional(2000.0));
}

// Regression test for a precision bug in an earlier version of this code:
// it computed the partial-fill quantity as `remaining / price` rounded to
// Size's 6-decimal precision *before* the final division. A partial fill
// entirely within a single level must VWAP to exactly that level's price --
// there's only one price paid -- but remaining/price here is 1/3 =
// 0.333..., which doesn't divide evenly at 6 decimals; the old code rounded
// it to Size(0.333333) and got a VWAP of ~3.000003 instead of exactly 3.0.
TEST(VolumeBands, PartialFillVwapEqualsLevelPriceExactlyEvenWhenSizeDoesNotDivideEvenly) {
    L2OrderBook book;
    book.asks[Price(3.0)] = Size(10.0);

    std::vector<Notional> thresholds = {Notional(1.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    EXPECT_EQ(*(*result)[0].vwap, Price(3.0));
}

// Regression test: when a threshold lands exactly at a level's far edge
// (a full-level, not partial, fill), this level's `size` is already known
// exactly and must be used directly. Reconstructing it from
// `level_notional / price` instead is not reliably exact, because
// `level_notional` is `price * size` already rounded to Notional's own
// precision -- the gap between the reconstructed and true `size` is
// inversely proportional to price, so it's large here on purpose: level 1
// (highest bid, iterated first) contributes cum_size = 1.0 at a normal
// price; level 2's price is Price::from_raw(1) with Size::from_raw(1'500'000)
// = 1.5, so level2_notional = round(1 * 1'500'000 / 1'000'000) = round(1.5)
// = Notional::from_raw(2) -- and reconstructing size from that via
// remaining/price gives 2.0, not the true 1.5. Bids (not asks) so the
// tiny-price level sorts after the normal one. The threshold lands exactly
// at the end of level 2, so the correct VWAP uses total size
// 1.0 + 1.5 = 2.5, not the reconstructed 1.0 + 2.0 = 3.0.
TEST(VolumeBands, FullLevelBoundaryUsesTheLevelsExactSizeNotAReconstructedOne) {
    L2OrderBook book;
    book.bids[Price(100.0)] = Size(1.0);                        // notional 100.0, iterated first
    book.bids[Price::from_raw(1)] = Size::from_raw(1'500'000);  // notional round-trips to raw 2

    std::vector<Notional> thresholds = {Notional::from_raw(100'000'000'002)};  // exactly cum after both levels
    auto result = bid_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    // True VWAP: 100.000000002 / 2.5 = 40.0000000008, rounds to
    // Price::from_raw(40'000'000'001).
    EXPECT_EQ((*result)[0].vwap->raw(), 40'000'000'001);
}

TEST(VolumeBands, HandlesTinyPriceWithSizeNearItsCeilingWithoutPrecisionLoss) {
    // Price near its smallest representable tick (1e-9) paired with a Size
    // near its largest representable value (~9.2e12): exercises Notional's
    // operator/ overloads at the edge of Size's representable range.
    L2OrderBook book;
    Price price = Price::from_raw(1);
    book.asks[price] = Size(9'000'000'000'000.0);  // level notional = 9000

    std::vector<Notional> thresholds = {Notional(8000.0), Notional(9000.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());
    const auto& bands = *result;

    ASSERT_EQ(bands.size(), 2u);

    ASSERT_TRUE(bands[0].vwap.has_value());
    EXPECT_TRUE(std::isfinite(bands[0].vwap->to_double()));
    EXPECT_NEAR(bands[0].vwap->to_double(), price.to_double(), 1e-9);

    ASSERT_TRUE(bands[1].vwap.has_value());
    EXPECT_NEAR(bands[1].vwap->to_double(), price.to_double(), 1e-9);
}

TEST(VolumeBands, AcceptsThresholdsFromNonVectorContiguousStorage) {
    // thresholds is std::span<const Notional>: a std::array (fixed, no heap
    // allocation) works directly, as does a sub-range of a larger buffer —
    // this is the whole point of taking span over a fixed const vector&.
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);
    book.asks[Price(125.0)] = Size(20.0);

    std::array<Notional, 2> thresholds = {Notional(500.0), Notional(3000.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->size(), 2u);

    std::array<Notional, 4> wider_buffer = {Notional(0.0), Notional(0.0), Notional(500.0),
                                             Notional(3000.0)};
    std::span<const Notional> sub(wider_buffer.begin() + 2, 2);
    auto from_subspan = ask_volume_band_prices(book, sub);
    ASSERT_TRUE(from_subspan.has_value());
    EXPECT_EQ((*from_subspan)[0].notional_threshold, Notional(500.0));
    EXPECT_EQ((*from_subspan)[1].notional_threshold, Notional(3000.0));
}

TEST(VolumeBands, ZeroPriceLevelReportsArgumentOutOfDomainInsteadOfCrashing) {
    // A zero-price level is malformed book state (never produced by
    // AggregateOrderBook, but L2OrderBook itself doesn't forbid it). This
    // must surface as an error. Checked eagerly per level, so this fires
    // regardless of the threshold -- any valid (strictly positive)
    // threshold exercises it, since a 0 threshold is itself a rejected
    // precondition, not something this function's runtime behavior covers.
    L2OrderBook book;
    book.asks[Price{}] = Size(5.0);

    std::vector<Notional> thresholds = {Notional(1.0)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::argument_out_of_domain);
}

// Regression test: a zero-price level contributes zero notional
// (level_notional = price * size = 0), so it never satisfies the crossing
// condition on its own -- an earlier version of this code only validated
// price inside the crossing branch, so a zero-price level that never
// triggered a crossing was walked right over, silently folding its size
// into cum_size with no corresponding notional. That would dilute the VWAP
// of any later, legitimate crossing. Must be rejected the moment it's
// iterated, not only when a crossing happens to land on it.
TEST(VolumeBands, RejectsNonPositivePriceLevelEvenWhenItNeverTriggersACrossing) {
    L2OrderBook book;
    book.bids[Price(100.0)] = Size(1000.0);  // notional 100,000; never reaches the threshold
    book.bids[Price{}] = Size(1'000'000.0);  // zero price, sorts last among bids

    std::vector<Notional> thresholds = {Notional(1'000'000.0)};
    auto result = bid_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::argument_out_of_domain);
}

TEST(VolumeBands, RejectsNegativeSizeLevel) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(-1.0);

    std::vector<Notional> thresholds = {Notional(1.0)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::argument_out_of_domain);
}

TEST(VolumeBands, RejectsNegativePriceLevel) {
    L2OrderBook book;
    book.asks[Price(-1.0)] = Size(5.0);

    std::vector<Notional> thresholds = {Notional(1.0)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::argument_out_of_domain);
}

TEST(VolumeBands, UnsortedThresholdsAreRejected) {
    // volume_band_prices() walks thresholds with a single
    // monotonically-increasing index - an unsorted list would silently
    // compute wrong VWAPs in a release build (where the old assert()-only
    // check compiled out) instead of failing loudly.
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(10.0);

    std::vector<Notional> thresholds = {Notional(500.0), Notional(100.0)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(VolumeBands, ZeroThresholdIsRejected) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(10.0);

    std::vector<Notional> thresholds = {Notional(0.0)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(VolumeBands, NegativeThresholdIsRejected) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(10.0);

    std::vector<Notional> thresholds = {Notional(-1.0)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

// Basic sanity check: a partial fill entirely within the first (and only)
// level, well short of its full depth.
TEST(VolumeBands, BasicPartialFillWithinFirstLevel) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(10.0);  // more depth than the threshold needs

    std::vector<Notional> thresholds = {Notional(500.0)};  // needs exactly 5 @ 100
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    EXPECT_EQ(*(*result)[0].vwap, Price(100.0));
    EXPECT_EQ((*result)[0].filled_notional, Notional(500.0));
}

// Partial fill spanning two levels: fully consumes the first, then takes
// half of the second. Exercises cumulative-plus-partial accumulation, the
// fixed-point scaling, and rounding a non-terminating decimal (2000/15 =
// 133.333...) to Price's 9 decimals.
TEST(VolumeBands, PartialFillAcrossTwoLevelsProducesCorrectlyRoundedVwap) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(10.0);  // notional 1000, fully consumed
    book.asks[Price(200.0)] = Size(10.0);  // notional 2000; only half needed

    std::vector<Notional> thresholds = {Notional(2000.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    // size = 10 + 1000/200 = 15; VWAP = 2000/15 = 133.333333333... (repeats
    // forever), rounded to Price's nearest representable 1e-9.
    ASSERT_TRUE((*result)[0].vwap.has_value());
    EXPECT_EQ((*result)[0].vwap->raw(), 133'333'333'333);
}

// The direct motivation for vwap_at_partial_fill() never materializing an
// intermediate Size: the true partial-fill quantity here is
// 0.1 / 1,000,000 = 1e-7, below Size's 1e-6 resolution. Rounding that
// intermediate to Size would give exactly 0, and dividing by a zero size
// would fail with argument_out_of_domain instead of succeeding. This must
// succeed, with VWAP exactly the level's own price (the only price paid,
// however little is bought).
TEST(VolumeBands, PartialSizeBelowSizePrecisionStillSucceeds) {
    L2OrderBook book;
    book.asks[Price(1'000'000.0)] = Size(1.0);

    std::vector<Notional> thresholds = {Notional(0.1)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    EXPECT_EQ(*(*result)[0].vwap, Price(1'000'000.0));
}

// Full-level special case on the very first level, with a size that
// doesn't round to a clean number -- protects the
// (cum_notional + remaining) / (cum_size + size) branch independent of the
// tiny-price scenario FullLevelBoundaryUsesTheLevelsExactSizeNotAReconstructedOne
// uses to expose the reconstruction-error bug.
TEST(VolumeBands, FullLevelBoundaryOnTheFirstLevelUsesTheExactGivenSize) {
    L2OrderBook book;
    Price price(100.0);
    Size size(1.234567);
    book.asks[price] = size;

    std::vector<Notional> thresholds = {price * size};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    EXPECT_EQ(*(*result)[0].vwap, price);
}

// Same special case spanning two full levels, with round numbers.
TEST(VolumeBands, FullLevelBoundaryAcrossTwoLevelsAveragesBothExactly) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(1.0);
    book.asks[Price(101.0)] = Size(1.0);

    std::vector<Notional> thresholds = {Notional(201.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    EXPECT_EQ(*(*result)[0].vwap, Price(100.5));
}

// Both thresholds fall within the one level's own notional (10,000); the
// while-loop must not advance cum_size/cum_notional between processing
// them -- if it mistakenly did, the second threshold's VWAP would reflect
// a phantom partial fill instead of this level's own price.
TEST(VolumeBands, MultipleThresholdsInTheSameLevelDoNotAdvanceCumulativeStateBetweenThem) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(100.0);  // notional 10,000

    std::vector<Notional> thresholds = {Notional(2000.0), Notional(5000.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    EXPECT_EQ(*(*result)[0].vwap, Price(100.0));
    ASSERT_TRUE((*result)[1].vwap.has_value());
    EXPECT_EQ(*(*result)[1].vwap, Price(100.0));
}

TEST(VolumeBands, DuplicateThresholdsProduceTheSameResultEachTime) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);   // notional 500
    book.asks[Price(125.0)] = Size(20.0);  // notional 2500, cumulative 3000

    std::vector<Notional> thresholds = {Notional(500.0), Notional(500.0), Notional(3000.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 3u);

    EXPECT_EQ((*result)[0].vwap, (*result)[1].vwap);
    EXPECT_EQ((*result)[0].filled_notional, (*result)[1].filled_notional);
    ASSERT_TRUE((*result)[2].vwap.has_value());
    EXPECT_DOUBLE_EQ((*result)[2].vwap->to_double(), 120.0);
}

TEST(VolumeBands, ZeroSizeLevelIsANoOp) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(5.0);  // notional 500
    book.asks[Price(150.0)] = Size(0.0);  // zero size: contributes nothing
    book.asks[Price(200.0)] = Size(5.0);  // notional 1000, cumulative 1500

    std::vector<Notional> thresholds = {Notional(1500.0)};
    auto result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(result.has_value());

    ASSERT_TRUE((*result)[0].vwap.has_value());
    // VWAP = 1500 / (5 + 5) = 150.0, as if the zero-size level weren't there.
    EXPECT_DOUBLE_EQ((*result)[0].vwap->to_double(), 150.0);
}

// volume_band_prices() is generic over the map and has no bid/ask concept
// of its own -- it entirely trusts that `levels` is already ordered
// best-price first. Same three levels on both sides (only the ordering
// differs, matching L2OrderBook's own comparator), same threshold: ask
// must walk up from the lowest price first, bid must walk down from the
// highest first.
TEST(VolumeBands, AskWalksUpwardAndBidWalksDownwardFromTheSameBookStructure) {
    L2OrderBook book;
    book.asks[Price(100.0)] = Size(1.0);
    book.asks[Price(101.0)] = Size(1.0);
    book.asks[Price(102.0)] = Size(1.0);
    book.bids[Price(100.0)] = Size(1.0);
    book.bids[Price(101.0)] = Size(1.0);
    book.bids[Price(102.0)] = Size(1.0);

    std::vector<Notional> thresholds = {Notional(50.0)};

    auto ask_result = ask_volume_band_prices(book, thresholds);
    ASSERT_TRUE(ask_result.has_value());
    ASSERT_TRUE((*ask_result)[0].vwap.has_value());
    EXPECT_EQ(*(*ask_result)[0].vwap, Price(100.0));  // lowest ask first

    auto bid_result = bid_volume_band_prices(book, thresholds);
    ASSERT_TRUE(bid_result.has_value());
    ASSERT_TRUE((*bid_result)[0].vwap.has_value());
    EXPECT_EQ(*(*bid_result)[0].vwap, Price(102.0));  // highest bid first
}

// Regression test: cum_notional must accumulate via a checked add (see
// volume_band_prices()'s own comment), not a plain operator+=. Each
// level's own price*size here is comfortably in range (~5e9, well under
// Notional::raw_type's ~9.22e9 max), so this specifically exercises the
// *running total* overflowing across levels, not any single level's own
// product.
TEST(VolumeBands, CumulativeNotionalOverflowReportsOutOfRangeInsteadOfWrapping) {
    L2OrderBook book;
    book.asks[Price(1'000'000.0)] = Size(5'000.0);
    book.asks[Price(1'000'001.0)] = Size(5'000.0);

    std::vector<Notional> thresholds = {Notional(1'000.0)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::result_out_of_range);
}

// A caller (e.g. hermeneutic_aggregator_client's --volume-thresholds=
// flag) can now pick a threshold up near Notional's own ~9.22e9 ceiling,
// not just this project's small built-in defaults - a threshold that
// large, crossed mid-level at an ordinary real-instrument price, reaches
// detail::vwap_at_partial_fill()'s own joint notional*price overflow
// guard against __int128 itself. That guard must fail cleanly here
// (std::errc::result_out_of_range), not silently overflow in a release
// build where a plain assert would have been compiled out.
TEST(VolumeBands, LargeThresholdCrossedAtARealisticPriceReportsOutOfRangeInsteadOfOverflowing) {
    L2OrderBook book;
    // Level notional (20'000 * 455'000 = 9.1e9) lands just past the
    // threshold, so the crossing is a genuine partial fill mid-level
    // (remaining != level_notional), not the exact-boundary case.
    book.asks[Price(20'000.0)] = Size(455'000.0);

    std::vector<Notional> thresholds = {Notional(9e9)};
    auto result = ask_volume_band_prices(book, thresholds);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::result_out_of_range);
}

}  // namespace
}  // namespace bobby::hermeneutic

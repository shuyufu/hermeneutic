#include "bobby/hermeneutic/core/notional.hpp"

#include <system_error>

#include <gtest/gtest.h>

namespace bobby::hermeneutic {
namespace {

TEST(Notional, DecimalsMatchPrice) {
    EXPECT_EQ(Notional::decimals, Price::decimals);
}

TEST(Notional, MultiplyComputesPriceTimesSize) {
    Price price(100.0);
    Size size(2.5);

    EXPECT_DOUBLE_EQ((price * size).to_double(), 250.0);
}

TEST(Notional, MultiplyRoundsToNearestTickTiesAwayFromZero) {
    // 1e-9 * 0.5 = 5e-10, exactly half of Notional's tick (1e-9).
    Price price = Price::from_raw(1);
    Size size(0.5);

    EXPECT_EQ((price * size).raw(), 1);
}

TEST(Notional, MultiplyDoesNotOverflowAtLargeMagnitudes) {
    // raw(price) * raw(size) = 5e13 * 1e10 = 5e23, far past int64_t's
    // range (~9.2e18); only safe because the multiply uses a 128-bit
    // intermediate before rescaling down to Notional's precision.
    Price price(50'000.0);
    Size size(10'000.0);

    EXPECT_DOUBLE_EQ((price * size).to_double(), 500'000'000.0);
}

TEST(Notional, MultiplyIsZeroWhenEitherOperandIsZero) {
    EXPECT_EQ((Price(0.0) * Size(5.0)).raw(), 0);
    EXPECT_EQ((Price(5.0) * Size(0.0)).raw(), 0);
}

TEST(Notional, DivideByPriceComputesSize) {
    Notional notional(500.0);
    Price price(100.0);

    auto size = notional / price;
    ASSERT_TRUE(size.has_value());
    EXPECT_DOUBLE_EQ(size->to_double(), 5.0);
}

TEST(Notional, DivideBySizeComputesPrice) {
    Notional notional(3000.0);
    Size size(25.0);

    auto price = notional / size;
    ASSERT_TRUE(price.has_value());
    EXPECT_DOUBLE_EQ(price->to_double(), 120.0);
}

TEST(Notional, DivideRoundsToNearestTick) {
    // 1 / 3 = 0.333333333..., rounds down to Size's 6th decimal (0.333333)
    // and to Price's 9th decimal (Price case below) since the next digit
    // in both cases is a 3.
    EXPECT_EQ((Notional(1.0) / Price(3.0))->raw(), 333'333);
    EXPECT_EQ((Notional(10.0) / Size(3.0))->raw(), 3'333'333'333);
}

TEST(Notional, DivideDoesNotOverflowAtLargeMagnitudes) {
    // raw(notional) * 1e6 = 5e17 * 1e6 = 5e23, far past int64_t's range
    // (~9.2e18); only safe because the division rescales the numerator in
    // a 128-bit intermediate before narrowing the quotient back down.
    Notional notional(500'000'000.0);
    Price price(0.0001);

    auto size = notional / price;
    ASSERT_TRUE(size.has_value());
    EXPECT_DOUBLE_EQ(size->to_double(), 5'000'000'000'000.0);
}

TEST(Notional, DivideByZeroPriceReturnsArgumentOutOfDomain) {
    Notional notional(500.0);
    Price zero_price{};

    auto size = notional / zero_price;
    ASSERT_FALSE(size.has_value());
    EXPECT_EQ(size.error(), std::errc::argument_out_of_domain);
}

TEST(Notional, DivideByZeroSizeReturnsArgumentOutOfDomain) {
    Notional notional(500.0);
    Size zero_size{};

    auto price = notional / zero_size;
    ASSERT_FALSE(price.has_value());
    EXPECT_EQ(price.error(), std::errc::argument_out_of_domain);
}

// Regression test: round_div_nearest_away() requires a strictly positive
// denominator, since "numerator +/- denominator/2, then divide" rounds
// the wrong way for a negative one. A negative price or size must be
// rejected, both because it's not a meaningful price/size and because it
// would otherwise reach the (denominator-must-be-positive) rounding math.
TEST(Notional, DivideByNegativePriceReturnsArgumentOutOfDomain) {
    Notional notional(500.0);
    Price negative_price(-100.0);

    auto size = notional / negative_price;
    ASSERT_FALSE(size.has_value());
    EXPECT_EQ(size.error(), std::errc::argument_out_of_domain);
}

TEST(Notional, DivideByNegativeSizeReturnsArgumentOutOfDomain) {
    Notional notional(500.0);
    Size negative_size(-25.0);

    auto price = notional / negative_size;
    ASSERT_FALSE(price.has_value());
    EXPECT_EQ(price.error(), std::errc::argument_out_of_domain);
}

TEST(Notional, RoundDivNearestAwayRoundsTiesAwayFromZero) {
    EXPECT_EQ(detail::round_div_nearest_away(14, 10), 1);   // 1.4 -> down
    EXPECT_EQ(detail::round_div_nearest_away(15, 10), 2);   // 1.5 -> away from zero (up)
    EXPECT_EQ(detail::round_div_nearest_away(16, 10), 2);   // 1.6 -> up
    EXPECT_EQ(detail::round_div_nearest_away(-14, 10), -1);
    EXPECT_EQ(detail::round_div_nearest_away(-15, 10), -2);  // -1.5 -> away from zero (down)
    EXPECT_EQ(detail::round_div_nearest_away(-16, 10), -2);
    EXPECT_EQ(detail::round_div_nearest_away(0, 10), 0);
}

// The same tie behavior, reached through the actual public operator rather
// than the helper directly: 1,000,000 / 128 = 7812.5 exactly (verified:
// 128 * 7812 = 999,936, remainder 64 = exactly half of 128), so this must
// round up to 7813, not down to 7812.
TEST(Notional, DivideByPriceRoundsAnExactTieAwayFromZero) {
    Notional notional = Notional::from_raw(1);
    Price price = Price::from_raw(128);

    auto size = notional / price;
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(size->raw(), 7813);
}

// Round-trip through operator* then back through both operator/ overloads.
// This is NOT a general guarantee -- operator* already rounds once to
// Notional's precision, so dividing back out doesn't always reconstruct
// the original operand exactly (see the next test for a case where it
// doesn't). For these particular inputs it happens to, which is a useful
// sanity check in its own right.
TEST(Notional, RoundTripThroughMultiplyThenDivideRecoversOriginalsForTheseInputs) {
    Price price = Price::from_raw(100'123'456'789);
    Size size = Size::from_raw(1'234'567);

    Notional notional = price * size;
    EXPECT_EQ(notional.raw(), 123'609'115'678);

    auto recovered_size = notional / price;
    ASSERT_TRUE(recovered_size.has_value());
    EXPECT_EQ(recovered_size->raw(), size.raw());

    auto recovered_price = notional / size;
    ASSERT_TRUE(recovered_price.has_value());
    EXPECT_EQ(recovered_price->raw(), price.raw());
}

// Same round-trip, but chosen so it does NOT reconstruct exactly: a low
// enough price makes operator*'s rounding error (up to 0.5 raw Notional
// units) large relative to the resulting size when divided back out. Not
// a bug -- operator/ is exact given its actual (already-rounded) input;
// the imprecision was already baked in by operator* rounding price * size
// down to a coarser Notional in the first place.
TEST(Notional, RoundTripThroughMultiplyThenDivideIsNotAlwaysExact) {
    Price price = Price::from_raw(1);
    Size size = Size::from_raw(1'500'000);  // 1.5

    Notional notional = price * size;
    EXPECT_EQ(notional.raw(), 2);  // round(1 * 1,500,000 / 1,000,000) = round(1.5) = 2

    auto recovered_size = notional / price;
    ASSERT_TRUE(recovered_size.has_value());
    EXPECT_EQ(recovered_size->raw(), 2'000'000);  // not the original 1,500,000

    auto recovered_price = notional / size;
    ASSERT_TRUE(recovered_price.has_value());
    EXPECT_EQ(recovered_price->raw(), 1);  // this direction happens to land exactly
}

// fixed_multiply/fixed_divide (fixed_point.hpp) must generalize to a
// decimal width that has nothing to do with Price/Size/Notional, not just
// the trio they were extracted from. `Rate` is defined locally, here,
// rather than in production code - the point is to prove the templates
// work for an arbitrary new width without growing the production type
// surface just to demonstrate it.
TEST(FixedPointArithmeticHelpers, GeneralizeToAFixedPointWidthOtherThanPriceSizeNotional) {
    using Rate = BasicFixedPoint<8>;

    // Rate(8) * Size(6) -> Notional(9): shift = 8 + 6 - 9 = 5.
    // raw: 25'000 * 1'000'000'000 / 10^5 = 250'000'000.
    Rate rate = Rate::from_raw(25'000);
    Size size = Size::from_raw(1'000'000'000);
    Notional product = detail::fixed_multiply<Rate, Size, Notional>(rate, size);
    EXPECT_EQ(product.raw(), 250'000'000);
    EXPECT_DOUBLE_EQ(product.to_double(), 0.25);

    // Notional(9) / Rate(8) -> Size(6): shift = 6 + 8 - 9 = 5.
    // raw: 250'000'000 * 10^5 / 25'000 = 1'000'000'000, recovering `size`.
    auto recovered_size = detail::fixed_divide<Notional, Rate, Size>(product, rate);
    ASSERT_TRUE(recovered_size.has_value());
    EXPECT_EQ(recovered_size->raw(), size.raw());
}

}  // namespace
}  // namespace bobby::hermeneutic

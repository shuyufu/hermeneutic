#include "bobby/hermeneutic/core/fixed_point.hpp"

#include <gtest/gtest.h>

namespace bobby::hermeneutic {
namespace {

TEST(BasicFixedPoint, DecimalsAndScale) {
    EXPECT_EQ(Price::decimals, 9);
    EXPECT_EQ(Price::scale, 1'000'000'000);
    EXPECT_EQ(Size::decimals, 6);
    EXPECT_EQ(Size::scale, 1'000'000);
}

TEST(BasicFixedPoint, FromRawRoundTrip) {
    auto p = Price::from_raw(1'500'000'000);
    EXPECT_EQ(p.raw(), 1'500'000'000);
    EXPECT_DOUBLE_EQ(p.to_double(), 1.5);
}

TEST(BasicFixedPoint, ConstructFromDoubleRoundsToNearestTick) {
    Price p(1.5);
    EXPECT_EQ(p.raw(), 1'500'000'000);

    Size s(0.25);
    EXPECT_EQ(s.raw(), 250'000);
}

TEST(BasicFixedPoint, ArithmeticIsExactOnRawTicks) {
    Price a(1.5);
    Price b(0.25);

    EXPECT_EQ((a + b).raw(), a.raw() + b.raw());
    EXPECT_EQ((a - b).raw(), a.raw() - b.raw());
    EXPECT_EQ((-a).raw(), -a.raw());

    Price c(1.0);
    c += b;
    EXPECT_DOUBLE_EQ(c.to_double(), 1.25);
    c -= b;
    EXPECT_DOUBLE_EQ(c.to_double(), 1.0);
}

TEST(BasicFixedPoint, TotalOrdering) {
    Price low(1.0);
    Price high(2.0);
    Price high_copy(2.0);

    EXPECT_LT(low, high);
    EXPECT_GT(high, low);
    EXPECT_EQ(high, high_copy);
    EXPECT_LE(high, high_copy);
    EXPECT_GE(high, high_copy);
}

}  // namespace
}  // namespace bobby::hermeneutic

#include "bobby/hermeneutic/core/rounding.hpp"

#include <gtest/gtest.h>

namespace bobby::hermeneutic::detail {
namespace {

TEST(RoundDivNearestAway, RoundsTiesAwayFromZero) {
    EXPECT_EQ(round_div_nearest_away(14, 10), 1);   // 1.4 -> down
    EXPECT_EQ(round_div_nearest_away(15, 10), 2);   // 1.5 -> away from zero (up)
    EXPECT_EQ(round_div_nearest_away(16, 10), 2);   // 1.6 -> up
    EXPECT_EQ(round_div_nearest_away(-14, 10), -1);
    EXPECT_EQ(round_div_nearest_away(-15, 10), -2);  // -1.5 -> away from zero (down)
    EXPECT_EQ(round_div_nearest_away(-16, 10), -2);
    EXPECT_EQ(round_div_nearest_away(0, 10), 0);
}

// half = denominator/2 + denominator%2 only differs from a plain
// denominator/2 when denominator is odd; every case above uses an even
// denominator (10) and so never exercises that "+ denominator%2" ceiling
// term. With denominator=3, half is 2 (not 1), so a remainder of 1 (1/3,
// well under the true halfway point) must round down, not away from zero.
TEST(RoundDivNearestAway, HandlesOddDenominatorCeiling) {
    EXPECT_EQ(round_div_nearest_away(4, 3), 1);   // 1.33 -> down
    EXPECT_EQ(round_div_nearest_away(5, 3), 2);   // 1.67 -> up
    EXPECT_EQ(round_div_nearest_away(-4, 3), -1);
    EXPECT_EQ(round_div_nearest_away(-5, 3), -2);
}

}  // namespace
}  // namespace bobby::hermeneutic::detail

#include "bobby/hermeneutic/core/fixed_point.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <string_view>
#include <system_error>

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

// operator+/operator-/unary operator- widen to __int128 before computing
// (see fixed_point.hpp's own comment on why: raw_ + other.raw_ overflowing
// int64_t directly is undefined behavior, not just an inexact result), then
// narrow back down through from_raw_checked()'s debug-only assert - same
// shape as notional.hpp's operator*/operator/, and the same reason
// Notional's own "DoesNotOverflowAtLargeMagnitudes" tests exist: proving the
// wide intermediate keeps genuinely large-but-in-range magnitudes exact,
// not just small ones like the test above.
TEST(BasicFixedPoint, AddAndSubtractStayExactNearRawTypeBounds) {
    constexpr auto kRawMax = std::numeric_limits<Price::raw_type>::max();

    Price near_max = Price::from_raw(kRawMax - 1);
    Price one = Price::from_raw(1);

    // kRawMax - 1 + 1 == kRawMax exactly, right at the boundary this fix
    // guards - a direct int64_t add here would already be UB territory for
    // any larger left-hand operand, not just this exact case.
    EXPECT_EQ((near_max + one).raw(), kRawMax);
    EXPECT_EQ((Price::from_raw(kRawMax) - one).raw(), kRawMax - 1);
    EXPECT_EQ((-Price::from_raw(kRawMax)).raw(), -kRawMax);

    Price accumulator = Price::from_raw(kRawMax - 3);
    accumulator += one;
    accumulator += one;
    EXPECT_EQ(accumulator.raw(), kRawMax - 1);
    accumulator -= one;
    EXPECT_EQ(accumulator.raw(), kRawMax - 2);
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

// Independent ground-truth decimal parser: exact __int128 arithmetic,
// written separately from BasicFixedPoint::from_decimal_string (not
// sharing any of its control flow) so a bug shared between the two
// wouldn't be invisible to the comparison test below. Implements "round
// half up, ties away from zero" for however many fractional digits `text`
// has, even when that's more than `decimals` - the general case
// from_decimal_string itself has to handle, unlike the narrower one-off
// script from the original double-vs-exact measurement (that script only
// ever generated exactly-decimals-digit fractional strings, so it never
// needed a rounding rule at all). Assumes a well-formed "[-]digits[.digits]"
// string; malformed input is covered by the dedicated error tests below
// instead.
__int128 exact_decimal_raw(std::string_view text, int decimals) {
    bool negative = false;
    std::size_t i = 0;
    if (!text.empty() && text[0] == '-') {
        negative = true;
        i = 1;
    }
    __int128 int_part = 0;
    while (i < text.size() && text[i] != '.') {
        int_part = int_part * 10 + (text[i] - '0');
        ++i;
    }
    __int128 frac = 0;
    int frac_digits = 0;
    if (i < text.size() && text[i] == '.') {
        ++i;
        while (i < text.size() && frac_digits < decimals) {
            frac = frac * 10 + (text[i] - '0');
            ++frac_digits;
            ++i;
        }
        if (i < text.size() && text[i] >= '5') frac += 1;  // round half up on the next digit only
    }
    while (frac_digits < decimals) {
        frac *= 10;
        ++frac_digits;
    }
    __int128 scale = 1;
    for (int k = 0; k < decimals; ++k) scale *= 10;
    if (frac == scale) {
        int_part += 1;
        frac = 0;
    }
    __int128 raw = int_part * scale + frac;
    return negative ? -raw : raw;
}

TEST(FromDecimalString, GeneralCaseWithIntegerAndFraction) {
    auto result = Price::from_decimal_string("123.45");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 123450000000);
}

TEST(FromDecimalString, NoDecimalPoint) {
    auto result = Price::from_decimal_string("123");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 123000000000);
}

TEST(FromDecimalString, TrailingDotWithNoFractionalDigits) {
    auto result = Price::from_decimal_string("123.");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 123000000000);
}

TEST(FromDecimalString, LeadingDotWithNoIntegerDigits) {
    auto result = Price::from_decimal_string(".5");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 500000000);
}

TEST(FromDecimalString, FewerFractionalDigitsThanDecimalsAreZeroPadded) {
    auto price = Price::from_decimal_string("1.4");
    ASSERT_TRUE(price.has_value());
    EXPECT_EQ(price->raw(), 1400000000);

    auto size = Size::from_decimal_string("1.4");
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(size->raw(), 1400000);
}

TEST(FromDecimalString, ExcessFractionalDigitsRoundDownBelowHalf) {
    // 10th fractional digit is '1' (< 5): truncates, no carry.
    auto result = Price::from_decimal_string("1.1234567891");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 1123456789);
}

TEST(FromDecimalString, ExcessFractionalDigitsRoundUpAboveHalf) {
    // 10th fractional digit is '6' (> 5): rounds the kept 9 digits up.
    auto result = Price::from_decimal_string("1.1234567896");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 1123456790);
}

TEST(FromDecimalString, ExactHalfRoundsAwayFromZero) {
    // 10th fractional digit is exactly '5' with nothing after - a genuine
    // tie, must round up (away from zero), matching rounding.hpp's
    // round_div_nearest_away convention.
    auto result = Price::from_decimal_string("1.1234567895");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 1123456790);
}

TEST(FromDecimalString, DigitsBeyondTheRoundingDigitDoNotChangeTheOutcome) {
    // Same 9 kept digits and same 10th digit ('4', below half) as a
    // round-down case above, but with many more (nonzero) digits after
    // it - must still round down, proving only the first excess digit is
    // ever consulted.
    auto below_half = Price::from_decimal_string("1.1234567894999999999999");
    ASSERT_TRUE(below_half.has_value());
    EXPECT_EQ(below_half->raw(), 1123456789);
    // Same, but the extra trailing digits push what would look like "just
    // under half" arbitrarily close to it - still below half, still
    // rounds down.
    auto still_below_half = Price::from_decimal_string("1.1234567894999999999");
    ASSERT_TRUE(still_below_half.has_value());
    EXPECT_EQ(still_below_half->raw(), 1123456789);
}

TEST(FromDecimalString, RoundingCarriesIntoTheIntegerPart) {
    // All-9s fractional part rounds up to exactly 1.0 unit, which must
    // carry into the integer part (5.999999999 5 -> 6.000000000).
    auto result = Price::from_decimal_string("5.9999999995");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 6000000000);
}

TEST(FromDecimalString, NegativeValuePreservesSignEvenWithZeroIntegerPart) {
    // The case the design explicitly guards against: deriving the sign
    // from the integer part ("0") would lose it here.
    auto size = Size::from_decimal_string("-0.5");
    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(size->raw(), -500000);

    auto price = Price::from_decimal_string("-123.45");
    ASSERT_TRUE(price.has_value());
    EXPECT_EQ(price->raw(), -123450000000);
}

TEST(FromDecimalString, IntegerPartWithinRangeSucceeds) {
    // 9223372036 * 1e9 = 9223372036000000000, just under
    // Price::raw_type's max (9223372036854775807).
    auto result = Price::from_decimal_string("9223372036");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->raw(), 9223372036000000000LL);
}

TEST(FromDecimalString, IntegerPartOverflowIsRejected) {
    // 9223372037 * 1e9 = 9223372037000000000, just over Price::raw_type's
    // max - the "loose per-digit guard" wouldn't catch this on its own
    // (9223372037 < Raw::max by itself), only the final post-scale check
    // does.
    auto result = Price::from_decimal_string("9223372037");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::result_out_of_range);
}

TEST(FromDecimalString, VeryLongIntegerPartIsRejectedWithoutOverflowingInt128) {
    auto result = Price::from_decimal_string(std::string(40, '9'));
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::result_out_of_range);
}

TEST(FromDecimalString, RoundingCarryIntoOverflowIsRejected) {
    // Integer part is exactly Price::raw_type's max in units (so the
    // *unrounded* value would just barely fit), but the fractional part
    // rounds up and carries - pushing it one unit past the max. Must
    // still be rejected, not silently wrap.
    auto result = Price::from_decimal_string("9223372036.9999999995");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::result_out_of_range);
}

TEST(FromDecimalString, EmptyStringIsRejected) {
    auto result = Price::from_decimal_string("");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, LoneMinusSignIsRejected) {
    auto result = Price::from_decimal_string("-");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, LoneDotIsRejected) {
    auto result = Price::from_decimal_string(".");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, MinusDotIsRejected) {
    auto result = Price::from_decimal_string("-.");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, MultipleDotsAreRejected) {
    auto result = Price::from_decimal_string("1.2.3");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, NonDigitCharactersAreRejected) {
    EXPECT_EQ(Price::from_decimal_string("abc").error(), std::errc::invalid_argument);
    EXPECT_EQ(Price::from_decimal_string("12a.5").error(), std::errc::invalid_argument);
    EXPECT_EQ(Price::from_decimal_string("12.5a").error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, LeadingPlusSignIsRejected) {
    // Matches std::from_chars<double>'s own grammar - it rejects a
    // leading '+' too, so this isn't a narrowing versus the path this
    // replaces.
    auto result = Price::from_decimal_string("+5.5");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, ScientificNotationIsRejected) {
    EXPECT_EQ(Price::from_decimal_string("1e5").error(), std::errc::invalid_argument);
    EXPECT_EQ(Price::from_decimal_string("1.5e2").error(), std::errc::invalid_argument);
}

TEST(FromDecimalString, NanAndInfinityTextAreRejected) {
    // std::from_chars<double> parses these successfully; rejecting them
    // here is an incidental fix, not a narrowing of anything real
    // exchange data sends.
    EXPECT_EQ(Price::from_decimal_string("nan").error(), std::errc::invalid_argument);
    EXPECT_EQ(Price::from_decimal_string("inf").error(), std::errc::invalid_argument);
    EXPECT_EQ(Price::from_decimal_string("infinity").error(), std::errc::invalid_argument);
}

// Brute-force cross-check against an independently-written exact ground
// truth (exact_decimal_raw above), across every shape the original
// double-vs-exact measurement covered plus the specific high-risk
// combination it flagged (a 7-digit integer part with a fractional part
// at or beyond the type's own full precision, 16+ total significant
// digits - where the double-intermediate path used to disagree with
// exact decimal rounding up to ~29% of the time). Since both sides here
// compute exact base-10 rounding with no double anywhere, a correct
// implementation matches 100% of the time by construction - this test
// exists to catch an implementation bug in either side, not to measure a
// probabilistic error rate the way the original double-vs-exact
// measurement did.
template <typename FixedPointType>
void assert_matches_ground_truth_for_shape(int int_digits, int frac_digits, int total, std::mt19937_64& rng) {
    constexpr int kDecimals = FixedPointType::decimals;
    for (int t = 0; t < total; ++t) {
        std::string text;
        if (int_digits == 0) {
            text = "0";
        } else {
            text += static_cast<char>('1' + rng() % 9);  // no leading zero, so int_digits is exact
            for (int i = 1; i < int_digits; ++i) text += static_cast<char>('0' + rng() % 10);
        }
        if (frac_digits > 0) {
            text += '.';
            for (int i = 0; i < frac_digits; ++i) text += static_cast<char>('0' + rng() % 10);
        }

        __int128 expected = exact_decimal_raw(text, kDecimals);
        auto actual = FixedPointType::from_decimal_string(text);
        ASSERT_TRUE(actual.has_value()) << "text=" << text << " decimals=" << kDecimals;
        ASSERT_EQ(actual->raw(), static_cast<typename FixedPointType::raw_type>(expected))
            << "text=" << text << " decimals=" << kDecimals;
    }
}

TEST(FromDecimalString, MatchesExactGroundTruthAcrossRealisticShapes) {
    std::mt19937_64 rng(20260920);
    constexpr int kSamples = 200000;

    // BTC-style price: 5-digit integer, 2 decimal digits.
    assert_matches_ground_truth_for_shape<Price>(5, 2, kSamples, rng);
    // Low-price altcoin: 1-digit integer, 8 decimal digits.
    assert_matches_ground_truth_for_shape<Price>(1, 8, kSamples, rng);
    // Full 9-decimal precision at a moderate integer magnitude.
    assert_matches_ground_truth_for_shape<Price>(5, 9, kSamples, rng);
    // Typical size: 2-digit integer, 4 decimals.
    assert_matches_ground_truth_for_shape<Size>(2, 4, kSamples, rng);
    // Size at full 6-decimal precision with a large integer part.
    assert_matches_ground_truth_for_shape<Size>(6, 6, kSamples, rng);
}

TEST(FromDecimalString, MatchesExactGroundTruthForTheOriginalHighRiskCombination) {
    // The exact combination the 2026-09-19 double-vs-exact measurement
    // flagged: a 7-digit integer part combined with Price's full 9
    // fractional digits at once (16 total significant digits, past
    // double's ~15.95-digit exact round-trip range) - measured ~29%
    // mismatch rate for the old string->double->BasicFixedPoint(double)
    // path. The new path has no double anywhere, so this must be 100%,
    // not ~71%.
    std::mt19937_64 rng(20260920);
    assert_matches_ground_truth_for_shape<Price>(7, 9, 2000000, rng);
    // One digit further past that boundary, and with excess fractional
    // precision beyond what Price even keeps (12 digits sent, 9 kept) -
    // exercises the rounding path on top of the high-significant-digit
    // integer part in the same value.
    assert_matches_ground_truth_for_shape<Price>(7, 12, 2000000, rng);
}

}  // namespace
}  // namespace bobby::hermeneutic

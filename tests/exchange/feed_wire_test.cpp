#include "bobby/hermeneutic/exchange/feed_wire.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <random>
#include <string>
#include <string_view>

namespace bobby::hermeneutic::ingestion::detail {
namespace {

using bobby::hermeneutic::Price;
using bobby::hermeneutic::Size;

// Independent ground-truth decimal parser: exact __int128 arithmetic,
// written separately from parse_decimal_string_to_fixed (not sharing any
// of its control flow) so a bug shared between the two wouldn't be
// invisible to the comparison test below. Implements "round half up,
// ties away from zero" for however many fractional digits `text` has,
// even when that's more than `decimals` - the general case
// parse_decimal_string_to_fixed itself has to handle, unlike the
// narrower one-off script from the original double-vs-exact measurement
// (that script only ever generated exactly-decimals-digit fractional
// strings, so it never needed a rounding rule at all). Assumes a
// well-formed "[-]digits[.digits]" string; malformed input is covered by
// the dedicated error tests below instead.
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

TEST(ParseDecimalStringToFixed, GeneralCaseWithIntegerAndFraction) {
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("123.45").raw(), 123450000000);
}

TEST(ParseDecimalStringToFixed, NoDecimalPoint) {
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("123").raw(), 123000000000);
}

TEST(ParseDecimalStringToFixed, TrailingDotWithNoFractionalDigits) {
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("123.").raw(), 123000000000);
}

TEST(ParseDecimalStringToFixed, LeadingDotWithNoIntegerDigits) {
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>(".5").raw(), 500000000);
}

TEST(ParseDecimalStringToFixed, FewerFractionalDigitsThanDecimalsAreZeroPadded) {
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("1.4").raw(), 1400000000);
    EXPECT_EQ(parse_decimal_string_to_fixed<Size>("1.4").raw(), 1400000);
}

TEST(ParseDecimalStringToFixed, ExcessFractionalDigitsRoundDownBelowHalf) {
    // 10th fractional digit is '1' (< 5): truncates, no carry.
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("1.1234567891").raw(), 1123456789);
}

TEST(ParseDecimalStringToFixed, ExcessFractionalDigitsRoundUpAboveHalf) {
    // 10th fractional digit is '6' (> 5): rounds the kept 9 digits up.
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("1.1234567896").raw(), 1123456790);
}

TEST(ParseDecimalStringToFixed, ExactHalfRoundsAwayFromZero) {
    // 10th fractional digit is exactly '5' with nothing after - a genuine
    // tie, must round up (away from zero), matching notional.hpp's
    // round_div_nearest_away convention.
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("1.1234567895").raw(), 1123456790);
}

TEST(ParseDecimalStringToFixed, DigitsBeyondTheRoundingDigitDoNotChangeTheOutcome) {
    // Same 9 kept digits and same 10th digit ('4', below half) as a
    // round-down case above, but with many more (nonzero) digits after
    // it - must still round down, proving only the first excess digit is
    // ever consulted.
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("1.1234567894999999999999").raw(), 1123456789);
    // Same, but the extra trailing digits push what would look like "just
    // under half" arbitrarily close to it - still below half, still
    // rounds down.
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("1.1234567894999999999").raw(), 1123456789);
}

TEST(ParseDecimalStringToFixed, RoundingCarriesIntoTheIntegerPart) {
    // All-9s fractional part rounds up to exactly 1.0 unit, which must
    // carry into the integer part (5.999999999 5 -> 6.000000000).
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("5.9999999995").raw(), 6000000000);
}

TEST(ParseDecimalStringToFixed, NegativeValuePreservesSignEvenWithZeroIntegerPart) {
    // The case the design explicitly guards against: deriving the sign
    // from the integer part ("0") would lose it here.
    EXPECT_EQ(parse_decimal_string_to_fixed<Size>("-0.5").raw(), -500000);
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("-123.45").raw(), -123450000000);
}

TEST(ParseDecimalStringToFixed, IntegerPartWithinRangeSucceeds) {
    // 9223372036 * 1e9 = 9223372036000000000, just under
    // Price::raw_type's max (9223372036854775807).
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("9223372036").raw(), 9223372036000000000LL);
}

TEST(ParseDecimalStringToFixed, IntegerPartOverflowIsRejected) {
    // 9223372037 * 1e9 = 9223372037000000000, just over Price::raw_type's
    // max - the "loose per-digit guard" wouldn't catch this on its own
    // (9223372037 < Raw::max by itself), only the final post-scale check
    // does.
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("9223372037"), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, VeryLongIntegerPartIsRejectedWithoutOverflowingInt128) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>(std::string(40, '9')), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, RoundingCarryIntoOverflowIsRejected) {
    // Integer part is exactly Price::raw_type's max in units (so the
    // *unrounded* value would just barely fit), but the fractional part
    // rounds up and carries - pushing it one unit past the max. Must
    // still be rejected, not silently wrap.
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("9223372036.9999999995"), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, EmptyStringIsRejected) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>(""), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, LoneMinusSignIsRejected) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("-"), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, LoneDotIsRejected) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("."), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, MinusDotIsRejected) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("-."), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, MultipleDotsAreRejected) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("1.2.3"), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, NonDigitCharactersAreRejected) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("abc"), simdjson::simdjson_error);
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("12a.5"), simdjson::simdjson_error);
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("12.5a"), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, LeadingPlusSignIsRejected) {
    // Matches std::from_chars<double>'s own grammar - it rejects a
    // leading '+' too, so this isn't a narrowing versus the path this
    // replaces.
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("+5.5"), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, ScientificNotationIsRejected) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("1e5"), simdjson::simdjson_error);
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("1.5e2"), simdjson::simdjson_error);
}

TEST(ParseDecimalStringToFixed, NanAndInfinityTextAreRejected) {
    // std::from_chars<double> parses these successfully; rejecting them
    // here is an incidental fix, not a narrowing of anything real
    // exchange data sends.
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("nan"), simdjson::simdjson_error);
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("inf"), simdjson::simdjson_error);
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("infinity"), simdjson::simdjson_error);
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
        auto actual = parse_decimal_string_to_fixed<FixedPointType>(text).raw();
        ASSERT_EQ(actual, static_cast<typename FixedPointType::raw_type>(expected))
            << "text=" << text << " decimals=" << kDecimals;
    }
}

TEST(ParseDecimalStringToFixed, MatchesExactGroundTruthAcrossRealisticShapes) {
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

TEST(ParseDecimalStringToFixed, MatchesExactGroundTruthForTheOriginalHighRiskCombination) {
    // The exact combination the 2026-09-19 double-vs-exact measurement
    // flagged: a 7-digit integer part combined with Price's full 9
    // fractional digits at once (16 total significant digits, past
    // double's ~15.95-digit exact round-trip range) - measured ~29%
    // mismatch rate for the old string->double->FixedPointType(double)
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
}  // namespace bobby::hermeneutic::ingestion::detail

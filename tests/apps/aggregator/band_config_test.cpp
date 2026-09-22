#include "apps/aggregator/band_config.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace bobby::hermeneutic::aggregator {
namespace {

TEST(SplitCsv, SplitsOnCommas) {
    EXPECT_EQ(split_csv("1e6,5e6,10e6"), (std::vector<std::string>{"1e6", "5e6", "10e6"}));
}

TEST(SplitCsv, SingleTokenWithNoCommaIsOneElement) {
    EXPECT_EQ(split_csv("1e6"), (std::vector<std::string>{"1e6"}));
}

TEST(SplitCsv, LeadingCommaYieldsALeadingEmptyToken) {
    EXPECT_EQ(split_csv(",1e6"), (std::vector<std::string>{"", "1e6"}));
}

TEST(SplitCsv, DoubledCommaYieldsAnEmptyTokenBetween) {
    EXPECT_EQ(split_csv("1e6,,5e6"), (std::vector<std::string>{"1e6", "", "5e6"}));
}

// A std::getline-based split (this function's first implementation) hits
// eof immediately after consuming the trailing comma, so it never emits
// the empty field that should follow it - silently accepting a malformed
// "1e6,5e6," as if it were the well-formed "1e6,5e6". split_csv() must
// treat every comma as a separator, including the last one.
TEST(SplitCsv, TrailingCommaYieldsATrailingEmptyToken) {
    EXPECT_EQ(split_csv("1e6,5e6,"), (std::vector<std::string>{"1e6", "5e6", ""}));
}

TEST(SplitCsv, EmptyStringIsOneEmptyToken) {
    EXPECT_EQ(split_csv(""), (std::vector<std::string>{""}));
}

TEST(ParseVolumeThresholds, ValidAscendingListParses) {
    auto result = parse_volume_thresholds("1000000,5000000,10000000,25000000,50000000");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 5u);
    EXPECT_EQ((*result)[0], Notional(1e6));
    EXPECT_EQ((*result)[4], Notional(50e6));
}

TEST(ParseVolumeThresholds, TrailingCommaIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("1000000,5000000,").has_value());
}

TEST(ParseVolumeThresholds, UnsortedListIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("5000000,1000000").has_value());
}

// Strictly increasing, not just non-decreasing: two equal adjacent
// thresholds would print two bands with the identical label and value,
// which is almost certainly a copy-paste mistake rather than an
// intentional duplicate.
TEST(ParseVolumeThresholds, DuplicateAdjacentValueIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("1000000,1000000,5000000").has_value());
}

TEST(ParseVolumeThresholds, NonPositiveValueIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("0").has_value());
    EXPECT_FALSE(parse_volume_thresholds("-1000000").has_value());
}

TEST(ParseVolumeThresholds, MalformedTokenIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("1000000,notanumber").has_value());
}

// Integers only - no decimals, no scientific notation. Every real
// threshold this tool cares about is a whole notional amount, so a richer
// grammar would only be more surface to parse and validate for inputs
// nobody needs.
TEST(ParseVolumeThresholds, DecimalNotationIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("1000000.5").has_value());
}

TEST(ParseVolumeThresholds, ScientificNotationIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("1e6").has_value());
}

// A value past Notional's representable range (raw_type::max() / scale,
// roughly 9.22e9 at Notional's 1e-9 resolution) must be rejected before a
// Notional is ever constructed from it: the scaling multiply that would
// produce its raw value overflows raw_type, which from_raw_safe() catches
// rather than letting it silently wrap or invoke undefined behavior.
TEST(ParseVolumeThresholds, ValueBeyondNotionalRangeIsRejected) {
    EXPECT_FALSE(parse_volume_thresholds("9223372037").has_value());
    EXPECT_FALSE(parse_volume_thresholds("999999999999999999999999999999").has_value());
}

TEST(ParseVolumeThresholds, ValueJustUnderTheRangeLimitParses) {
    EXPECT_TRUE(parse_volume_thresholds("9223372036").has_value());
}

TEST(ParsePriceBps, ValidAscendingListParses) {
    auto result = parse_price_bps("50,100,200,500,1000");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, (std::vector<int>{50, 100, 200, 500, 1000}));
}

TEST(ParsePriceBps, UnsortedListIsRejected) {
    EXPECT_FALSE(parse_price_bps("100,50").has_value());
}

TEST(ParsePriceBps, DuplicateAdjacentValueIsRejected) {
    EXPECT_FALSE(parse_price_bps("50,50,100").has_value());
}

TEST(ParsePriceBps, NegativeValueIsRejected) {
    EXPECT_FALSE(parse_price_bps("-1").has_value());
}

// std::from_chars (unlike std::stoi, which wraps the C locale-aware
// strtol) rejects leading whitespace and a leading '+' outright - the same
// strict, locale-independent grammar parse_volume_thresholds() gets from
// Notional::from_decimal_string() for the same class of input.
TEST(ParsePriceBps, LeadingWhitespaceIsRejected) {
    EXPECT_FALSE(parse_price_bps(" 50,100").has_value());
}

TEST(ParsePriceBps, LeadingPlusSignIsRejected) {
    EXPECT_FALSE(parse_price_bps("+50,100").has_value());
}

// >= 10000 is rejected even though only the bid side's own
// bps_thresholds_valid<Bid>() requires it - see kDefaultPriceBandBps's own
// comment for why this list must satisfy both sides at once.
TEST(ParsePriceBps, ValueAtOrAboveTenThousandIsRejected) {
    EXPECT_FALSE(parse_price_bps("10000").has_value());
    EXPECT_FALSE(parse_price_bps("50,20000").has_value());
}

TEST(FormatNotionalLabel, RoundMillionsGetAnMSuffix) {
    EXPECT_EQ(format_notional_label(Notional(1e6)), "1M");
    EXPECT_EQ(format_notional_label(Notional(50e6)), "50M");
}

TEST(FormatNotionalLabel, RoundBillionGetsABSuffix) {
    EXPECT_EQ(format_notional_label(Notional(3e9)), "3B");
}

TEST(FormatNotionalLabel, RoundThousandGetsAKSuffix) {
    EXPECT_EQ(format_notional_label(Notional(2000.0)), "2K");
}

TEST(FormatNotionalLabel, NonRoundValueFallsBackToAPlainTrimmedDecimal) {
    EXPECT_EQ(format_notional_label(Notional(1500.5)), "1500.5");
}

// value.to_double() casts raw() (an int64) through a double, which only
// represents integers exactly up to 2^53 (~9e15) - well within reach of a
// caller-supplied --volume-thresholds= value now that the flag exists
// (Notional's raw units go up to ~9.2e18). Built directly via from_raw()
// rather than Notional(double) so this test doesn't itself depend on
// whether that particular double happens to round-trip exactly - it's
// testing format_notional_label()'s own arithmetic, not the constructor's.
TEST(FormatNotionalLabel, LargeNonRoundValueStaysExact) {
    // 6234567891 * Notional::scale, spelled directly as one integer so no
    // floating-point step is involved anywhere in producing this raw
    // value.
    EXPECT_EQ(format_notional_label(Notional::from_raw(6234567891'000000000LL)), "6234567891");
}

TEST(FormatNotionalLabel, LargeRoundBillionValueStaysExact) {
    // 9e9 units - close to Notional's own representable ceiling (raw_
    // type::max() / scale, ~9.22e9) without exceeding it.
    EXPECT_EQ(format_notional_label(Notional::from_raw(9'000000000'000000000LL)), "9B");
}

TEST(FormatBpsLabel, AppendsBps) {
    EXPECT_EQ(format_bps_label(50), "50bps");
    EXPECT_EQ(format_bps_label(1000), "1000bps");
}

TEST(BuildLabels, OnlyTheLastLabelGetsATrailingPlus) {
    std::vector<Notional> thresholds = {Notional(1e6), Notional(5e6), Notional(50e6)};
    auto labels = build_labels(thresholds, format_notional_label);
    EXPECT_EQ(labels, (std::vector<std::string>{"1M", "5M", "50M+"}));
}

TEST(BuildLabels, SingleThresholdStillGetsThePlus) {
    std::vector<int> bps = {1000};
    auto labels = build_labels(bps, format_bps_label);
    EXPECT_EQ(labels, (std::vector<std::string>{"1000bps+"}));
}

TEST(ExtractFlags, FindsBothFlagsAnywhereInArgs) {
    std::vector<std::string> args = {"addr", "--volume-thresholds=1000000,5000000", "volume-bands",
                                      "--price-bps=50,100", "30",  "BTC_USDT.SPOT"};
    auto flags = extract_flags(args);
    ASSERT_TRUE(flags.has_value());
    ASSERT_TRUE(flags->volume_thresholds_csv.has_value());
    EXPECT_EQ(*flags->volume_thresholds_csv, "1000000,5000000");
    ASSERT_TRUE(flags->price_bps_csv.has_value());
    EXPECT_EQ(*flags->price_bps_csv, "50,100");
    EXPECT_EQ(args, (std::vector<std::string>{"addr", "volume-bands", "30", "BTC_USDT.SPOT"}));
}

TEST(ExtractFlags, AbsentFlagsLeaveArgsUntouched) {
    std::vector<std::string> args = {"addr", "bbo", "30", "BTC_USDT.SPOT"};
    auto flags = extract_flags(args);
    ASSERT_TRUE(flags.has_value());
    EXPECT_FALSE(flags->volume_thresholds_csv.has_value());
    EXPECT_FALSE(flags->price_bps_csv.has_value());
    EXPECT_EQ(args, (std::vector<std::string>{"addr", "bbo", "30", "BTC_USDT.SPOT"}));
}

// A repeated flag is far more likely to be a copy-paste mistake than an
// intentional override, so this is rejected rather than letting the last
// occurrence silently win with no diagnostic.
TEST(ExtractFlags, RepeatedVolumeThresholdsFlagIsRejected) {
    std::vector<std::string> args = {"addr", "volume-bands", "--volume-thresholds=1000000", "30",
                                      "--volume-thresholds=5000000", "BTC_USDT.SPOT"};
    auto flags = extract_flags(args);
    EXPECT_FALSE(flags.has_value());
}

TEST(ExtractFlags, RepeatedPriceBpsFlagIsRejected) {
    std::vector<std::string> args = {"addr", "price-bands", "--price-bps=50", "--price-bps=100", "30",
                                      "BTC_USDT.SPOT"};
    auto flags = extract_flags(args);
    EXPECT_FALSE(flags.has_value());
}

}  // namespace
}  // namespace bobby::hermeneutic::aggregator

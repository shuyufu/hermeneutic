#include "bobby/hermeneutic/exchange/feed_wire.hpp"

#include <gtest/gtest.h>

namespace bobby::hermeneutic::ingestion::detail {
namespace {

using bobby::hermeneutic::Price;

// The decimal-string grammar/rounding/overflow behavior itself is
// BasicFixedPoint::from_decimal_string's own concern, tested exhaustively
// in tests/core/fixed_point_test.cpp. What's specific to this file is the
// translation from that function's std::expected failure into the
// simdjson::simdjson_error every other parse failure in a Feed header
// funnels through - that's all these two tests cover.
TEST(ParseDecimalStringToFixed, SuccessReturnsTheParsedValue) {
    EXPECT_EQ(parse_decimal_string_to_fixed<Price>("123.45").raw(), 123450000000);
}

TEST(ParseDecimalStringToFixed, FailureIsTranslatedToSimdjsonError) {
    EXPECT_THROW(parse_decimal_string_to_fixed<Price>("not-a-number"), simdjson::simdjson_error);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion::detail

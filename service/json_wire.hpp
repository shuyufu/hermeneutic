#pragma once

#include <simdjson.h>

#include <charconv>
#include <string_view>
#include <utility>
#include <vector>

#include "bobby/hermeneutic/fixed_point.hpp"

namespace bobby::hermeneutic::ingestion::detail {

// Several venues (Binance, Bybit) send price/quantity as JSON strings, not
// numbers, to avoid any floating-point ambiguity in the wire format itself.
// std::from_chars (locale-independent, no exceptions of its own) parses the
// decimal text; a malformed string throws simdjson::simdjson_error so every
// parse failure in a Feed header that calls this funnels through the same
// catch site as every other simdjson error there.
//
// Shared here, not defined once per Feed header: VenueSession consumes
// each Feed's parse_message()/parse_snapshot_response() duck-typed, so
// nothing requires these helpers to live in one place - but a single
// translation unit that includes more than one Feed header needing them
// (aggregator_main.cpp, once it wires up a second venue) would otherwise
// get a duplicate-definition (ODR) error from two identical `namespace
// bobby::hermeneutic::ingestion::detail { ... }` blocks.
inline double parse_decimal_string(std::string_view text) {
    double value = 0.0;
    auto result = std::from_chars(text.data(), text.data() + text.size(), value);
    if (result.ec != std::errc{} || result.ptr != text.data() + text.size()) {
        throw simdjson::simdjson_error(simdjson::NUMBER_ERROR);
    }
    return value;
}

inline std::pair<Price, Size> parse_level(simdjson::ondemand::array level) {
    auto it = level.begin();
    std::string_view price_text = (*it).get_string();
    ++it;
    std::string_view size_text = (*it).get_string();
    return {Price(parse_decimal_string(price_text)), Size(parse_decimal_string(size_text))};
}

inline std::vector<std::pair<Price, Size>> parse_levels(simdjson::ondemand::array levels) {
    std::vector<std::pair<Price, Size>> result;
    for (auto level : levels) {
        result.push_back(parse_level(level.get_array()));
    }
    return result;
}

}  // namespace bobby::hermeneutic::ingestion::detail

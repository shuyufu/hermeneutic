#pragma once

#include <simdjson.h>

#include <charconv>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "bobby/hermeneutic/fixed_point.hpp"

namespace bobby::hermeneutic::ingestion {

// A REST request this feed wants made, pure data -- no I/O happens here.
// The driver (VenueSession) is what actually performs it. Shared across
// Binance feeds (Futures, Spot): VenueSession consumes this duck-typed
// (`auto spec = feed.snapshot_request(symbol)`, then `.host`/`.port`/
// `.target`), but a single translation unit including more than one
// Binance feed header needs one definition, not a redefinition per feed.
struct HttpRequestSpec {
    std::string host;    // e.g. "fapi.binance.com"
    std::string port;    // e.g. "443" -- kept separate from "is this TLS"
                          // so a test double can point at a local plain
                          // HTTP server on an arbitrary port.
    std::string target;  // path + query, e.g. "/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
};

namespace detail {

// Binance sends price/quantity as JSON strings (not numbers), to avoid any
// floating-point ambiguity in the wire format itself, on every stream this
// project ingests (Futures and Spot alike). std::from_chars
// (locale-independent, no exceptions of its own) parses the decimal text;
// a malformed string throws simdjson::simdjson_error so every parse
// failure in the feed header that calls this funnels through the same
// catch site.
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

}  // namespace detail

}  // namespace bobby::hermeneutic::ingestion

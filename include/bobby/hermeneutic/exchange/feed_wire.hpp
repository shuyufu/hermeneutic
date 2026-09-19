#pragma once

#include <simdjson.h>

#include <charconv>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/core/fixed_point.hpp"

// Shared vocabulary every VenueFeed implementation (Binance, Bybit, OKX)
// needs, kept in one place so a translation unit that includes more than
// one Feed header (server_main.cpp does) doesn't hit ODR duplicate-
// definition errors from each Feed header redeclaring the same thing.
// Named for what it's shared across (Feed wire-level concerns), not
// "json" specifically - most of what's here is JSON parsing (this file's
// original, and still primary, reason to exist), but HttpRequestSpec
// below is a plain REST-request bundle with nothing JSON about it.
namespace bobby::hermeneutic::ingestion {

// Every VenueFeed's parse_message() returns this: nullopt means the
// message was recognized but not book-relevant (a subscribe ack, a
// heartbeat, ...), otherwise which of a snapshot or an incremental depth
// update it carried. Identical across every venue (Binance, Bybit, OKX) -
// shared here rather than redefined per Feed header for the same ODR
// reason parse_decimal_string/parse_level/parse_levels below moved here:
// a translation unit that includes more than one Feed header
// (server_main.cpp does) would otherwise get a duplicate-definition
// error from two or more identical `using ParsedMessage = ...` in the
// same namespace.
using ParsedMessage = std::optional<std::variant<SnapshotMessage, DepthUpdate>>;

// A REST request a Feed with kSnapshotViaRest == true wants made, pure
// data - no I/O happens here, VenueSession::fetch() is what actually
// performs it (see net/http_client.hpp's http_get(), whose own host/
// port/target parameters this bundles into one duck-typed return value:
// `auto spec = feed.snapshot_request(symbol);` then `.host`/`.port`/
// `.target`). Venue-neutral by construction - the three fields carry no
// Binance/Bybit/OKX-specific meaning at all, only whichever venue's REST
// endpoint the caller filled in - so this lives here rather than under
// exchange/binance/ (its first home, back when Binance Futures/Spot were
// the only two Feeds that needed it at all): any current or future Feed
// with a REST snapshot returns one of these, not a venue-scoped
// lookalike struct. Deliberately NOT defined in net/http_client.hpp
// itself, even though it's exactly that function's own parameter list
// bundled up: http_get() needs Boost.Asio/Beast/OpenSSL
// (HERMENEUTIC_BUILD_SERVICE, vcpkg), while this header - like every
// exchange/ Feed header that returns this struct - only needs simdjson
// (HERMENEUTIC_BUILD_INGESTION, no vcpkg). Putting the struct in
// http_client.hpp would force every Feed header back into needing the
// vcpkg toolchain just to declare a return type, defeating the whole
// point of that build-time split - see CMakeLists.txt's own
// HERMENEUTIC_BUILD_INGESTION/HERMENEUTIC_BUILD_SERVICE option comments,
// and hermeneutic_binance_futures_feed_test's own CMake target, which
// builds and runs with no VCPKG_ROOT/gRPC at all today.
struct HttpRequestSpec {
    std::string host;    // e.g. "fapi.binance.com"
    std::string port;    // e.g. "443" -- kept separate from "is this TLS"
                          // so a test double can point at a local plain
                          // HTTP server on an arbitrary port.
    std::string target;  // path + query, e.g. "/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
};

}  // namespace bobby::hermeneutic::ingestion

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
// (server_main.cpp, once it wires up a second venue) would otherwise
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

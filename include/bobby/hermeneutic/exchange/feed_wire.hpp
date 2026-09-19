#pragma once

#include <simdjson.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/core/fixed_point.hpp"
#include "bobby/hermeneutic/core/notional.hpp"

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
// A malformed string throws simdjson::simdjson_error so every parse
// failure in a Feed header that calls this funnels through the same catch
// site as every other simdjson error there.
//
// Shared here, not defined once per Feed header: VenueSession consumes
// each Feed's parse_message()/parse_snapshot_response() duck-typed, so
// nothing requires these helpers to live in one place - but a single
// translation unit that includes more than one Feed header needing them
// (server_main.cpp, once it wires up a second venue) would otherwise
// get a duplicate-definition (ODR) error from two identical `namespace
// bobby::hermeneutic::ingestion::detail { ... }` blocks.
//
// Parses a decimal string directly into FixedPointType's raw integer
// scale, entirely in integer arithmetic. Replaces a previous
// std::from_chars (string -> double) + FixedPointType(double)
// (double -> raw int64, via `value * scale + 0.5`) two-step path: a
// 2026-09-19 measurement found the first step safe below 15 total
// significant digits, unsafe above it (see this file's own git history
// for the brute-force numbers) - but that measurement covered only the
// first step. The second step - BasicFixedPoint(double)'s own
// `value * scale + 0.5` - is an independent double-precision multiply
// with its own rounding error, unmeasured by that first pass and not
// fixed by improving the first step alone. Removing the double
// intermediate entirely, not just narrowing one of its two uses, is the
// only way to close both at once. No other production call site should
// route wire-parsed price/size text through FixedPointType(double) again -
// that constructor is still used elsewhere (e.g.
// apps/aggregator/client_main.cpp's compile-time volume-band threshold
// literals like Notional(1e6), which are exact in double and never came
// off a wire), just never again for text that did.
//
// Accepted grammar: an optional leading '-' (never '+' - verified
// std::from_chars<double> itself rejects a leading '+', so dropping it
// here isn't a narrowing versus the path this replaces), then digits,
// optionally followed by '.' and more digits, with at least one digit
// somewhere (rejects "", "-", ".", and "-."). Deliberately narrower than
// std::from_chars<double>'s own grammar in two ways, both confirmed
// safe: scientific notation ("1e5") is rejected as a format error -
// every Binance/Bybit/OKX test fixture in this repo was checked and none
// use it, so there is nothing real to support; and "nan"/"inf"/
// "infinity" (which std::from_chars<double> parses successfully) are
// rejected too - an incidental fix for a real, if never-observed-in-the-
// wild, gap in the path this replaces: a feed sending literal "NaN" text
// would previously parse into a double NaN, then into
// `static_cast<raw_type>(NaN * scale + 0.5)`, which is undefined
// behavior, not a caught parse error.
template <typename FixedPointType>
inline FixedPointType parse_decimal_string_to_fixed(std::string_view text) {
    using Raw = typename FixedPointType::raw_type;
    constexpr int kDecimals = FixedPointType::decimals;
    constexpr Raw kScale = FixedPointType::scale;
    constexpr __int128 kRawMax = static_cast<__int128>(std::numeric_limits<Raw>::max());

    auto fail = [] { throw simdjson::simdjson_error(simdjson::NUMBER_ERROR); };

    // Stripped before splitting on '.', and applied to the whole
    // magnitude at the very end - not derived from int_part's own sign -
    // so a value like "-0.5" (integer part "0") doesn't lose its sign.
    std::string_view rest = text;
    bool negative = false;
    if (!rest.empty() && rest.front() == '-') {
        negative = true;
        rest.remove_prefix(1);
    }

    auto dot = rest.find('.');
    std::string_view int_part = dot == std::string_view::npos ? rest : rest.substr(0, dot);
    std::string_view frac_part =
        dot == std::string_view::npos ? std::string_view{} : rest.substr(dot + 1);
    if (int_part.empty() && frac_part.empty()) fail();  // "", "-", ".", "-."

    auto all_digits = [](std::string_view s) {
        for (char c : s) {
            if (c < '0' || c > '9') return false;
        }
        return true;
    };
    // Rejects non-digit garbage in either half, and a second '.' (which
    // lands inside frac_part as a non-digit character) in one check - no
    // separate "count the dots" logic needed.
    if (!all_digits(int_part) || !all_digits(frac_part)) fail();

    // Integer part, accumulated in a 128-bit intermediate. Bailing as
    // soon as it exceeds Raw's own max (not Raw's max/scale - the final,
    // tight bound is applied after scaling below) is a looser check, but
    // it's what keeps this loop itself safe from ever overflowing
    // __int128 on a maliciously long digit string: __int128 has roughly
    // 20 more decimal digits of headroom than Raw::max, so this trips
    // long before that could happen, however many digits `int_part` has.
    unsigned __int128 int_value = 0;
    for (char c : int_part) {
        int_value = int_value * 10 + static_cast<unsigned>(c - '0');
        if (int_value > static_cast<unsigned __int128>(kRawMax)) fail();
    }

    // Fractional part, rescaled to exactly kDecimals digits. Only the
    // first kDecimals+1 characters of frac_part are ever read, however
    // long it is: under "round half up, ties away from zero", the single
    // digit immediately after the cut point already fully determines the
    // outcome (>=5 always rounds up regardless of what follows, since
    // further digits can only make the discarded remainder larger, never
    // pull it back under half; <=4 always rounds down for the same
    // reason in reverse) - so digits beyond that one can never change
    // the result.
    std::size_t take = std::min(frac_part.size(), static_cast<std::size_t>(kDecimals) + 1);
    __int128 frac_numerator = 0;
    for (std::size_t i = 0; i < take; ++i) {
        frac_numerator = frac_numerator * 10 + (frac_part[i] - '0');
    }

    __int128 frac_value;
    if (frac_part.size() > static_cast<std::size_t>(kDecimals)) {
        // Exactly kDecimals+1 digits were gathered - rescale by dividing
        // out the extra one, rounding ties away from zero: the same
        // helper/convention every other rescale in this project uses
        // (see notional.hpp).
        frac_value = bobby::hermeneutic::detail::round_div_nearest_away(frac_numerator, 10);
    } else {
        // kDecimals or fewer digits were present - `frac_numerator`
        // already holds exactly `take` digits with nothing discarded;
        // pad with zeros on the right to reach kDecimals digits.
        frac_value = frac_numerator;
        for (std::size_t i = take; i < static_cast<std::size_t>(kDecimals); ++i) frac_value *= 10;
    }

    // Rounding the fractional part up can carry into the integer part
    // (e.g. "0.999" at 2 decimals rounds to "1.00") - round_div_nearest_
    // away can push frac_value to exactly kScale (never beyond: the
    // largest possible frac_numerator, kDecimals+1 nines, rounds to
    // exactly 10^kDecimals), so a single carry is the only case to
    // handle.
    if (frac_value == static_cast<__int128>(kScale)) {
        int_value += 1;
        frac_value = 0;
    }
    // Re-checks the same loose bound as the accumulation loop above,
    // now including the carry - only load-bearing for a hypothetical
    // Decimals=0 type (where int_value IS the final magnitude, so a
    // carry landing exactly on kRawMax needs to be caught here). For
    // Price/Size (Decimals=9/6), int_value at this point is still many
    // orders of magnitude below kRawMax whenever the final result is
    // going to be valid at all - the real guard for those is the tight
    // post-scale check just below, not this one.
    if (int_value > static_cast<unsigned __int128>(kRawMax)) fail();

    // Final, tight bound: against Raw::max after scaling, the actual
    // overflow condition for the raw_type this returns (unlike the loose
    // checks above, which only protect the accumulation loop itself and
    // the Decimals=0 edge case). Applies the same bound to both signs
    // rather than letting a negative result use Raw::min's one-unit-
    // larger magnitude (two's complement) - real price/size data never
    // comes remotely close to either bound, so this asymmetry has no
    // practical effect, and treating both signs identically here is
    // simpler than the alternative.
    __int128 magnitude = static_cast<__int128>(int_value) * static_cast<__int128>(kScale) + frac_value;
    if (magnitude > kRawMax) fail();

    return FixedPointType::from_raw(static_cast<Raw>(negative ? -magnitude : magnitude));
}

inline std::pair<Price, Size> parse_level(simdjson::ondemand::array level) {
    auto it = level.begin();
    std::string_view price_text = (*it).get_string();
    ++it;
    std::string_view size_text = (*it).get_string();
    return {parse_decimal_string_to_fixed<Price>(price_text),
            parse_decimal_string_to_fixed<Size>(size_text)};
}

inline std::vector<std::pair<Price, Size>> parse_levels(simdjson::ondemand::array levels) {
    std::vector<std::pair<Price, Size>> result;
    for (auto level : levels) {
        result.push_back(parse_level(level.get_array()));
    }
    return result;
}

}  // namespace bobby::hermeneutic::ingestion::detail

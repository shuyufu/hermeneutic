#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <expected>
#include <functional>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "bobby/hermeneutic/book/price_bands.hpp"
#include "bobby/hermeneutic/core/notional.hpp"

// hermeneutic_aggregator_client's own band-threshold configuration and CLI
// flag parsing - split out of client_main.cpp's own anonymous namespace
// (same reasoning as client_book.hpp's own comment: this is intricate,
// overflow/precision-sensitive logic that needs a header a test target can
// include, not left untestable in client_main.cpp's anonymous namespace).
// client_main.cpp itself only wires this into argv handling, gRPC
// plumbing, and printing.
namespace bobby::hermeneutic::aggregator {

// This tool's own default thresholds, used whenever the corresponding
// --volume-thresholds=/--price-bps= flag is absent.
constexpr std::array<Notional, 5> kDefaultVolumeBandThresholds = {
    Notional(1e6), Notional(5e6), Notional(10e6), Notional(25e6), Notional(50e6),
};

// < kBpsDenominator (not just non-negative) even though only the bid
// side's own bps_thresholds_valid<Bid>() requires it: client_main.cpp's
// publish_l2_bands() passes this same list to both bid_price_band_depths()
// and ask_price_band_depths(), so a value that's valid for asks but not
// bids would fail on the bid side alone.
constexpr std::array<int, 5> kDefaultPriceBandBps = {50, 100, 200, 500, 1000};

// Whichever threshold list applies to a run, plus its display labels
// (built once via build_labels() below, not per-update): volume_
// thresholds/volume_labels for volume-bands, price_bps/price_labels for
// price-bands.
struct BandConfig {
    std::vector<Notional> volume_thresholds;
    std::vector<std::string> volume_labels;
    std::vector<int> price_bps;
    std::vector<std::string> price_labels;
};

// BasicFixedPoint's own operator<< streams to_double() through ostream's
// default precision (6 significant digits) - fine for a Price/Size around
// 80000.5, but a Notional in this tool's 1M-50M+ band range overflows
// that into scientific notation. Precision is FixedPoint::decimals
// (Price/Notional=9, Size=6), not a fixed "2": that's the exact number of
// decimal digits the type's raw scale stores, so a small value (e.g. a
// sub-cent VWAP) still prints losslessly while std::fixed keeps a large
// Notional out of scientific notation.
template <typename FixedPoint>
std::string format_fixed(FixedPoint value) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(FixedPoint::decimals) << value.to_double();
    return out.str();
}

// format_fixed() always prints FixedPoint::decimals digits after the
// point (see its own comment) - fine for the actual band values, but a
// label built from a Notional (e.g. "1M") would otherwise read
// "1.000000000M". Trims those unneeded trailing zeros, and the bare "."
// left behind if every fractional digit was zero.
inline std::string trim_trailing_zeros(std::string decimal) {
    auto dot = decimal.find('.');
    if (dot == std::string::npos) return decimal;
    std::size_t last_significant = decimal.find_last_not_of('0');
    if (last_significant == dot) --last_significant;
    return decimal.substr(0, last_significant + 1);
}

// Renders a notional threshold the way this tool's original hardcoded
// labels did ("1M", "5M", "50M", ...): divide by the largest of 1e9/1e6/1e3
// that divides it evenly and suffix accordingly, falling back to a plain
// integer string for a value that isn't a round K/M/B.
//
// Operates on value.raw() directly, in pure int64 arithmetic - not
// value.to_double(): to_double() casts raw() through a double, which only
// represents integers exactly up to 2^53 (~9e15), well within the range a
// caller-supplied --volume-thresholds= value can now reach (Notional's own
// representable range goes up to raw_type::max(), ~9.2e18). Every
// threshold this function actually receives is a whole notional unit -
// parse_volume_thresholds() rejects decimals, and the built-in defaults
// are all round integers too - so raw() is always an exact multiple of
// Notional::scale, and dividing by it is exact integer division, never a
// lossy double round-trip. The `raw % scale != 0` branch is defensive:
// nothing in this tool can actually produce a fractional Notional here
// today, but the fallback keeps this function correct (if imprecise, same
// as before) rather than asserting on one if that ever changes.
inline std::string format_notional_label(Notional value) {
    Notional::raw_type raw = value.raw();
    if (raw % Notional::scale != 0) return trim_trailing_zeros(format_fixed(value));

    Notional::raw_type units = raw / Notional::scale;
    Notional::raw_type scaled = units;
    char suffix = '\0';
    if (units != 0 && units % 1'000'000'000 == 0) {
        scaled = units / 1'000'000'000;
        suffix = 'B';
    } else if (units != 0 && units % 1'000'000 == 0) {
        scaled = units / 1'000'000;
        suffix = 'M';
    } else if (units != 0 && units % 1'000 == 0) {
        scaled = units / 1'000;
        suffix = 'K';
    }
    std::string numeral = std::to_string(scaled);
    return suffix ? numeral + suffix : numeral;
}

inline std::string format_bps_label(int bps) { return std::to_string(bps) + "bps"; }

// Builds the "1M"/"50bps"-style labels lined up index-for-index with
// `thresholds`, appending '+' to the last one - this tool's convention
// (inherited from its original hardcoded labels) for marking the highest
// band as open-ended, whether the list came from a --volume-thresholds=/
// --price-bps= flag or this tool's own defaults.
template <typename T, typename LabelFn>
std::vector<std::string> build_labels(const std::vector<T>& thresholds, LabelFn label_fn) {
    std::vector<std::string> labels;
    labels.reserve(thresholds.size());
    for (std::size_t i = 0; i < thresholds.size(); ++i) {
        std::string label = label_fn(thresholds[i]);
        if (i + 1 == thresholds.size()) label += '+';
        labels.push_back(std::move(label));
    }
    return labels;
}

// Splits `csv` on ',' into raw tokens, always producing exactly one more
// token than there are commas - shared by parse_volume_thresholds()/
// parse_price_bps() below, which each convert and validate the tokens for
// their own value type. A leading, trailing, or doubled comma therefore
// always yields an empty token (e.g. "1000000,5000000," ->
// {"1000000", "5000000", ""}), which the per-type parsers below reject
// explicitly, rather than a std::getline-based split silently dropping a
// *trailing* empty token (getline hits eof with nothing left to read past
// the final comma, so it never sees that last, empty field).
inline std::vector<std::string> split_csv(const std::string& csv) {
    std::vector<std::string> tokens;
    std::size_t start = 0;
    while (true) {
        auto comma = csv.find(',', start);
        if (comma == std::string::npos) {
            tokens.push_back(csv.substr(start));
            break;
        }
        tokens.push_back(csv.substr(start, comma - start));
        start = comma + 1;
    }
    return tokens;
}

// Shared shape behind parse_volume_thresholds()/parse_price_bps() below:
// split on ',', convert+validate each token via `parse_token` (returning
// std::nullopt to reject that token outright), then require the resulting
// list to be non-empty, *strictly* increasing (not just non-decreasing -
// a duplicate/equal adjacent pair, e.g. "1000000,1000000,5000000", is
// almost certainly a copy-paste mistake, not an intentional two identical
// bands), and pass `extra_valid` (e.g. a range check `parse_token` itself
// can't express as a per-token parse failure).
template <typename T, typename TokenParser, typename ExtraValid>
std::optional<std::vector<T>> parse_ascending_list(const std::string& csv, TokenParser parse_token,
                                                    ExtraValid extra_valid) {
    std::vector<T> result;
    for (const auto& token : split_csv(csv)) {
        if (token.empty()) return std::nullopt;
        auto value = parse_token(token);
        if (!value) return std::nullopt;
        result.push_back(*value);
    }
    if (result.empty()) return std::nullopt;
    if (std::ranges::adjacent_find(result, std::ranges::greater_equal{}) != result.end()) return std::nullopt;
    if (!std::ranges::all_of(result, extra_valid)) return std::nullopt;
    return result;
}

// Parses a --volume-thresholds= value into the same shape volume_band_
// prices() itself requires (sorted ascending, strictly positive - see
// volume_bands.hpp) - checked here, at this program's own input boundary,
// so a malformed --volume-thresholds= flag is a clear startup error rather
// than a silent "ERROR" on every printed line once streaming starts.
//
// Deliberately integers only - no decimals ("1000000.5"), no scientific
// notation ("1e6"): every real threshold this tool cares about is a whole
// notional amount, so supporting anything richer would only be extra
// grammar to parse and validate for inputs nobody needs. A token
// containing '.' is rejected outright (from_decimal_string() alone
// wouldn't reject it, since it accepts a fractional part); every other
// token is handed to Notional::from_decimal_string() - exact, integer-only
// parsing straight into Notional's raw units, already bounds-checked
// internally (result_out_of_range for a threshold too large to represent)
// - rather than a hand-rolled std::from_chars-plus-manual-scaling
// duplicate of that same logic.
inline std::optional<std::vector<Notional>> parse_volume_thresholds(const std::string& csv) {
    return parse_ascending_list<Notional>(
        csv,
        [](const std::string& token) -> std::optional<Notional> {
            if (token.find('.') != std::string::npos) return std::nullopt;
            auto value = Notional::from_decimal_string(token);
            if (!value) return std::nullopt;
            return *value;
        },
        [](Notional n) { return n.raw() > 0; });
}

// Parses a --price-bps= value into the same shape price_band_depth()
// itself requires (sorted ascending, non-negative). Also rejects >=
// kBpsDenominator here even though that bound is only bps_thresholds_
// valid<Bid>()'s own requirement (see kDefaultPriceBandBps's comment):
// this list is shared between both sides, so a value the ask side would
// accept but the bid side wouldn't be usable here anyway.
//
// Parsed with std::from_chars, not std::stoi: stoi tolerates leading
// whitespace and a leading '+' (e.g. " 50" or "+50") since it wraps the
// C locale-aware strtol, which is a looser grammar than parse_volume_
// thresholds()'s from_decimal_string() accepts for the same class of
// input - from_chars enforces the same strict, locale-independent digits-
// only grammar for both flags.
inline std::optional<std::vector<int>> parse_price_bps(const std::string& csv) {
    return parse_ascending_list<int>(
        csv,
        [](const std::string& token) -> std::optional<int> {
            int value = 0;
            auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), value);
            if (ec != std::errc{} || ptr != token.data() + token.size()) return std::nullopt;
            return value;
        },
        [](int bps) { return bps >= 0 && bps < kBpsDenominator; });
}

// --volume-thresholds=/--price-bps=, pulled out of argv by extract_flags()
// below.
struct CliFlags {
    std::optional<std::string> volume_thresholds_csv;
    std::optional<std::string> price_bps_csv;
};

// Pulls --volume-thresholds=/--price-bps= out of `args` in place (removing
// the matched tokens), so the remaining positional arguments - address,
// mode, duration, books - parse exactly as they would without these flags,
// regardless of where among them the flags appeared. Two flags, not a
// general getopt-style parser: this tool only ever needs to override these
// two arrays.
//
// Rejects a flag that appears more than once (std::unexpected, rather than
// letting the last occurrence silently win with no diagnostic) - a repeat
// is far more likely to be a copy-paste mistake than an intentional
// override of an earlier value.
inline std::expected<CliFlags, std::string> extract_flags(std::vector<std::string>& args) {
    constexpr std::string_view kVolumeFlag = "--volume-thresholds=";
    constexpr std::string_view kPriceFlag = "--price-bps=";
    CliFlags flags;
    for (auto it = args.begin(); it != args.end();) {
        if (it->starts_with(kVolumeFlag)) {
            if (flags.volume_thresholds_csv) {
                return std::unexpected("--volume-thresholds= given more than once");
            }
            flags.volume_thresholds_csv = it->substr(kVolumeFlag.size());
            it = args.erase(it);
        } else if (it->starts_with(kPriceFlag)) {
            if (flags.price_bps_csv) {
                return std::unexpected("--price-bps= given more than once");
            }
            flags.price_bps_csv = it->substr(kPriceFlag.size());
            it = args.erase(it);
        } else {
            ++it;
        }
    }
    return flags;
}

}  // namespace bobby::hermeneutic::aggregator

#pragma once

#include <cctype>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace bobby::hermeneutic::ingestion {

// Which market a book covers - the suffix on a canonical book key
// ("BTCUSDT.SPOT"/"BTCUSDT.PERP") and, transitively, which of a venue's
// feeds native_symbol() below resolves to.
enum class BookType { Spot, Perp };

// Exchanges this project can source liquidity from. Deliberately just an
// enum, never a Feed type: this header (and the subscription config it
// parses) stays usable - and unit-testable - without linking simdjson or
// gRPC at all. Adding a venue means adding a case to native_symbol()/
// venue_id()/parse_venue() below, and separately wiring its Feed/Policy
// into apps/aggregator/main.cpp's own dispatch.
enum class Venue { Binance, Bybit, Okx };

inline std::optional<Venue> parse_venue(std::string_view token) {
    if (token == "BINANCE") return Venue::Binance;
    if (token == "BYBIT") return Venue::Bybit;
    if (token == "OKX") return Venue::Okx;
    return std::nullopt;
}

inline std::string_view venue_name(Venue venue) {
    switch (venue) {
        case Venue::Binance: return "BINANCE";
        case Venue::Bybit: return "BYBIT";
        case Venue::Okx: return "OKX";
    }
    return "";  // unreachable - silences -Wreturn-type on an exhaustive switch
}

// A base/quote pair split out of the subscription config's own "BASE_QUOTE"
// spelling - e.g. "BTC_USDT" -> {"BTC", "USDT"}. This underscore is this
// project's own naming rule, used only by the subscription config below
// (never by the book key it produces - see book_key()): splitting a
// concatenated ticker like "BTCUSDT" back into base/quote is fundamentally
// ambiguous without a maintained quote-asset dictionary (okx_feed.hpp's
// okx_canonical() hits exactly this wall going the other direction and
// deliberately declines to guess), so this pushes that boundary onto
// whoever writes the config instead of guessing at it.
struct BaseQuote {
    std::string base;
    std::string quote;
};

// nullopt unless `token` is exactly one non-empty base and one non-empty
// quote separated by a single '_'.
inline std::optional<BaseQuote> split_base_quote(std::string_view token) {
    auto underscore = token.find('_');
    if (underscore == std::string_view::npos || token.find('_', underscore + 1) != std::string_view::npos) {
        return std::nullopt;
    }
    std::string_view base = token.substr(0, underscore);
    std::string_view quote = token.substr(underscore + 1);
    if (base.empty() || quote.empty()) return std::nullopt;
    return BaseQuote{std::string(base), std::string(quote)};
}

// The canonical, venue-neutral SymbolBook key: concatenated, no
// underscore, exactly what AggregatorService::book()/apps/aggregator/
// main.cpp's book_symbols have always used ("BTCUSDT.SPOT"/"BTCUSDT.PERP" -
// see okx_canonical()'s doc comment for the existing convention this
// matches). Unaffected by this file's own "BASE_QUOTE" input spelling: a
// gRPC client already subscribed to "BTCUSDT.PERP" keeps working with it
// unchanged regardless of how the subscription config below is written.
inline std::string book_key(const BaseQuote& symbol, BookType type) {
    return symbol.base + symbol.quote + (type == BookType::Spot ? ".SPOT" : ".PERP");
}

// The VenueId string a wired-up VenueSession is tagged with - venue-native
// vocabulary ("futures"/"linear"/"swap", each exchange's own term for its
// derivatives market), not the venue-neutral "SPOT"/"PERP" book_key() uses.
// Deliberately not unified with book_key()'s vocabulary: a log line or
// SymbolBook::apply_batch's per-VenueId bookkeeping should still read the
// way each exchange's own docs do, and book_key() only gets to stay
// venue-agnostic because it doesn't have to pick one exchange's term as
// "the" answer.
inline std::string venue_id(Venue venue, BookType type) {
    switch (venue) {
        case Venue::Binance: return type == BookType::Spot ? "binance_spot" : "binance_futures";
        case Venue::Bybit: return type == BookType::Spot ? "bybit_spot" : "bybit_linear";
        case Venue::Okx: return type == BookType::Spot ? "okx_spot" : "okx_swap";
    }
    return "";  // unreachable - silences -Wreturn-type on an exhaustive switch
}

// The wire-format symbol a venue's own Feed subscribes with. Binance/Bybit
// use the same concatenated spelling for both spot and their derivatives
// market ("BTCUSDT" either way - see apps/aggregator/main.cpp's existing
// registry-building loop, which already relies on this); OKX separates
// base/quote with its own dash and tags a swap with "-SWAP" (see
// okx_feed.hpp's is_swap()/okx_canonical()). This is that OKX mapping's
// other direction, made unambiguous by construction - `symbol` already
// carries the base/quote boundary the config token supplied, rather than
// this having to recover it from a concatenated string the way
// okx_canonical() explicitly declines to.
inline std::string native_symbol(Venue venue, const BaseQuote& symbol, BookType type) {
    switch (venue) {
        case Venue::Binance:
        case Venue::Bybit:
            return symbol.base + symbol.quote;
        case Venue::Okx:
            return symbol.base + "-" + symbol.quote + (type == BookType::Perp ? "-SWAP" : "");
    }
    return "";  // unreachable - silences -Wreturn-type on an exhaustive switch
}

// One venue's contribution to one book: everything apps/aggregator/main.cpp
// needs to add this (venue, symbol, type) triple to the right
// SymbolRegistry and, grouped by (venue, type), to IngestionRunner.
struct VenueSubscription {
    Venue venue;
    BookType type;
    std::string book_key;      // e.g. "BTCUSDT.PERP" - AggregatorService::book()'s key
    std::string native_symbol; // e.g. "BTC-USDT-SWAP" - what this venue's Feed subscribes with
};

namespace detail {

inline std::string_view trim(std::string_view s) {
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.front()))) s.remove_prefix(1);
    while (!s.empty() && std::isspace(static_cast<unsigned char>(s.back()))) s.remove_suffix(1);
    return s;
}

}  // namespace detail

// Parses a ';'-separated list of "BASE_QUOTE.TYPE=[VENUE,VENUE,...]"
// entries (e.g. "BTC_USDT.SPOT=[BINANCE,OKX,BYBIT];BTC_USDT.PERP=[BINANCE,OKX]")
// into the flat list of (venue, book) pairs main() should wire up.
//
// This is a real system boundary - operator-supplied command-line input -
// so every mistake fails loud with the offending entry named, rather than
// silently dropping a venue or guessing at one: an unknown venue token, a
// malformed entry, a book listed twice (which would violate
// AggregatorService's own unique-symbols precondition), an empty venue
// list, or the same venue listed twice for one book are all rejected here
// rather than left for an operator to notice later as missing liquidity.
//
// Leading/trailing whitespace around any token is tolerated (operators
// format these by hand); a trailing ';' (an empty final entry) is dropped
// the same way split_symbols() already drops a trailing ','. The
// separators themselves ('_', '.', '=', '[', ']', ',', ';') and the
// uppercase spelling of TYPE/VENUE are not negotiable - this is a fixed
// grammar, not a free-form format.
inline std::expected<std::vector<VenueSubscription>, std::string> parse_book_subscriptions(
    std::string_view config) {
    std::vector<VenueSubscription> result;
    std::unordered_set<std::string> seen_book_keys;

    std::size_t pos = 0;
    while (pos <= config.size()) {
        auto semicolon = config.find(';', pos);
        std::string_view raw_entry = semicolon == std::string_view::npos
                                          ? config.substr(pos)
                                          : config.substr(pos, semicolon - pos);
        pos = semicolon == std::string_view::npos ? config.size() + 1 : semicolon + 1;

        std::string_view entry = detail::trim(raw_entry);
        if (entry.empty()) continue;

        auto eq = entry.find('=');
        if (eq == std::string_view::npos) {
            return std::unexpected("malformed subscription entry (missing '='): \"" + std::string(entry) + "\"");
        }

        std::string_view left = detail::trim(entry.substr(0, eq));
        std::string_view right = detail::trim(entry.substr(eq + 1));

        auto dot = left.find('.');
        if (dot == std::string_view::npos || left.find('.', dot + 1) != std::string_view::npos) {
            return std::unexpected(
                "malformed book key (expected \"BASE_QUOTE.SPOT\" or \"BASE_QUOTE.PERP\"): \"" +
                std::string(left) + "\"");
        }
        std::string_view symbol_part = left.substr(0, dot);
        std::string_view type_part = left.substr(dot + 1);

        BookType type;
        if (type_part == "SPOT") {
            type = BookType::Spot;
        } else if (type_part == "PERP") {
            type = BookType::Perp;
        } else {
            return std::unexpected("unknown book type \"" + std::string(type_part) +
                                    "\" (expected SPOT or PERP) in \"" + std::string(entry) + "\"");
        }

        auto symbol = split_base_quote(symbol_part);
        if (!symbol) {
            return std::unexpected("malformed symbol (expected \"BASE_QUOTE\", e.g. \"BTC_USDT\"): \"" +
                                    std::string(symbol_part) + "\"");
        }

        std::string key = book_key(*symbol, type);
        if (!seen_book_keys.insert(key).second) {
            return std::unexpected("book \"" + key + "\" is configured more than once");
        }

        if (right.size() < 2 || right.front() != '[' || right.back() != ']') {
            return std::unexpected("malformed venue list (expected \"[VENUE,...]\") for \"" + key + "\": \"" +
                                    std::string(right) + "\"");
        }
        std::string_view inner = detail::trim(right.substr(1, right.size() - 2));
        if (inner.empty()) {
            return std::unexpected("empty venue list for \"" + key + "\"");
        }

        std::unordered_set<Venue> seen_venues;
        std::size_t vpos = 0;
        while (vpos <= inner.size()) {
            auto comma = inner.find(',', vpos);
            std::string_view raw_token = comma == std::string_view::npos ? inner.substr(vpos)
                                                                          : inner.substr(vpos, comma - vpos);
            vpos = comma == std::string_view::npos ? inner.size() + 1 : comma + 1;

            std::string_view token = detail::trim(raw_token);
            if (token.empty()) {
                return std::unexpected("empty venue token in venue list for \"" + key + "\": \"" +
                                        std::string(inner) + "\"");
            }

            auto venue = parse_venue(token);
            if (!venue) {
                return std::unexpected("unknown venue \"" + std::string(token) + "\" for \"" + key + "\"");
            }
            if (!seen_venues.insert(*venue).second) {
                return std::unexpected("venue " + std::string(venue_name(*venue)) + " listed twice for \"" + key +
                                        "\"");
            }

            result.push_back(
                VenueSubscription{*venue, type, key, native_symbol(*venue, *symbol, type)});
        }
    }

    if (result.empty()) {
        return std::unexpected(std::string("no book subscriptions configured"));
    }
    return result;
}

}  // namespace bobby::hermeneutic::ingestion

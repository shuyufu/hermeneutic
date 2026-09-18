#pragma once

#include <cctype>
#include <expected>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic::ingestion {

using bobby::hermeneutic::symbol::BookType;
using bobby::hermeneutic::symbol::Venue;

// One venue's contribution to one book: everything apps/aggregator/main.cpp
// needs to add this (venue, symbol, type) triple to the right
// SymbolRegistry and, grouped by (venue, type), to IngestionRunner. This is
// a parsing *result*, not part of the symbol domain model itself (see
// bobby/hermeneutic/symbol/symbol.hpp) - it exists because this ingestion
// config, specifically, resolves a venue list into concrete subscriptions.
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
// into the flat list of (venue, book) pairs main() should wire up. The
// "BASE_QUOTE"/TYPE/VENUE vocabulary itself is
// bobby::hermeneutic::symbol's - this function only owns the CLI grammar
// around it (the ';'/'='/'['/']'/',' punctuation), not what a symbol or a
// venue is.
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

        auto symbol = bobby::hermeneutic::symbol::split_base_quote(symbol_part);
        if (!symbol) {
            return std::unexpected("malformed symbol (expected \"BASE_QUOTE\", e.g. \"BTC_USDT\"): \"" +
                                    std::string(symbol_part) + "\"");
        }

        std::string key = bobby::hermeneutic::symbol::book_key(*symbol, type);
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

            auto venue = bobby::hermeneutic::symbol::parse_venue(token);
            if (!venue) {
                return std::unexpected("unknown venue \"" + std::string(token) + "\" for \"" + key + "\"");
            }
            if (!seen_venues.insert(*venue).second) {
                return std::unexpected("venue " +
                                        std::string(bobby::hermeneutic::symbol::venue_name(*venue)) +
                                        " listed twice for \"" + key + "\"");
            }

            result.push_back(VenueSubscription{
                *venue, type, key, bobby::hermeneutic::symbol::native_symbol(*venue, *symbol, type)});
        }
    }

    if (result.empty()) {
        return std::unexpected(std::string("no book subscriptions configured"));
    }
    return result;
}

}  // namespace bobby::hermeneutic::ingestion

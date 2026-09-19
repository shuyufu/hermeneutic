#pragma once

#include <simdjson.h>

#include <expected>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic::ingestion {

using bobby::hermeneutic::symbol::BookId;
using bobby::hermeneutic::symbol::MarketType;
using bobby::hermeneutic::symbol::Venue;

// One venue's contribution to one book: everything apps/aggregator/server_main.cpp
// needs to add this (venue, symbol, type) triple to the right
// SymbolRegistry and, grouped by (venue, type), to IngestionRunner. This is
// a parsing *result*, not part of the symbol domain model itself (see
// bobby/hermeneutic/symbol/symbol.hpp) - it exists because this ingestion
// config, specifically, resolves a venue list into concrete subscriptions.
struct VenueSubscription {
    Venue venue;
    MarketType type;
    BookId book_id;              // AggregatorService::book()'s key
    std::string native_symbol;   // e.g. "BTC-USDT-SWAP" - what this venue's Feed subscribes with
};

// Parses a subscription config document, e.g.:
//   {
//     "books": [
//       {"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE", "OKX", "BYBIT"]},
//       {"symbol": "BTC_USDT", "type": "PERP", "venues": ["BINANCE", "OKX"]}
//     ]
//   }
// into the flat list of (venue, book) pairs main() should wire up.
// "symbol" is this project's own "BASE_QUOTE" spelling (see
// bobby::hermeneutic::symbol::split_base_quote) - splitting a concatenated
// ticker like "BTCUSDT" back into base/quote is ambiguous without a
// maintained quote-asset dictionary, so the boundary is still on whoever
// writes the config, same as before this moved from a CLI string to a file.
//
// This is a real system boundary - an operator-maintained config - so
// every mistake fails loud with enough context to fix it: malformed JSON,
// a missing/wrong-typed field, an unknown venue or type, a book listed
// twice (would violate AggregatorService's own unique-symbols
// precondition), an empty venue list, or the same venue listed twice for
// one book are all rejected here rather than left for an operator to
// notice later as missing liquidity.
//
// Takes the JSON text itself, not a path - see load_book_subscriptions()
// for the file-reading wrapper - so this stays a pure function callers
// (tests, in particular) can exercise without touching the filesystem.
inline std::expected<std::vector<VenueSubscription>, std::string> parse_book_subscriptions(
    std::string_view json_text) {
    std::vector<VenueSubscription> result;
    std::unordered_set<BookId> seen_book_ids;

    try {
        simdjson::padded_string padded(json_text);
        simdjson::ondemand::parser parser;
        simdjson::ondemand::document doc = parser.iterate(padded);
        simdjson::ondemand::array books = doc["books"].get_array();

        for (auto book_value : books) {
            simdjson::ondemand::object book = book_value.get_object();

            std::string_view symbol_field;
            std::string_view type_field;
            simdjson::ondemand::array venues_field;
            try {
                symbol_field = book["symbol"].get_string();
                type_field = book["type"].get_string();
                venues_field = book["venues"].get_array();
            } catch (const simdjson::simdjson_error&) {
                return std::unexpected(
                    "each entry in \"books\" needs a \"symbol\" (string), \"type\" (string), and "
                    "\"venues\" (array of strings)");
            }

            MarketType type;
            if (type_field == "SPOT") {
                type = MarketType::Spot;
            } else if (type_field == "PERP") {
                type = MarketType::Perp;
            } else {
                return std::unexpected("unknown book type \"" + std::string(type_field) +
                                        "\" (expected SPOT or PERP) for symbol \"" + std::string(symbol_field) +
                                        "\"");
            }

            auto symbol = bobby::hermeneutic::symbol::split_base_quote(symbol_field);
            if (!symbol) {
                return std::unexpected("malformed symbol (expected \"BASE_QUOTE\", e.g. \"BTC_USDT\"): \"" +
                                        std::string(symbol_field) + "\"");
            }

            BookId id{*symbol, type};
            std::string key_str = bobby::hermeneutic::symbol::to_string(id);
            if (!seen_book_ids.insert(id).second) {
                return std::unexpected("book \"" + key_str + "\" is configured more than once");
            }

            std::unordered_set<Venue> seen_venues;
            bool any_venue = false;
            for (auto venue_value : venues_field) {
                std::string_view venue_token;
                try {
                    venue_token = venue_value.get_string();
                } catch (const simdjson::simdjson_error&) {
                    return std::unexpected("\"venues\" for \"" + key_str + "\" must be an array of strings");
                }

                auto venue = bobby::hermeneutic::symbol::parse_venue(venue_token);
                if (!venue) {
                    return std::unexpected("unknown venue \"" + std::string(venue_token) + "\" for \"" + key_str +
                                            "\"");
                }
                if (!seen_venues.insert(*venue).second) {
                    return std::unexpected("venue " + std::string(bobby::hermeneutic::symbol::venue_name(*venue)) +
                                            " listed twice for \"" + key_str + "\"");
                }

                any_venue = true;
                result.push_back(VenueSubscription{
                    *venue, type, id, bobby::hermeneutic::symbol::native_symbol(*venue, *symbol, type)});
            }

            if (!any_venue) {
                return std::unexpected("empty venue list for \"" + key_str + "\"");
            }
        }
    } catch (const simdjson::simdjson_error& e) {
        return std::unexpected("malformed subscription config: " + std::string(e.what()));
    }

    if (result.empty()) {
        return std::unexpected(std::string("no book subscriptions configured"));
    }
    return result;
}

// Reads `path` and parses it as a subscription config - see
// parse_book_subscriptions() above for the document shape and every
// validation this applies. A missing or unreadable file is reported the
// same way as any other configuration mistake (startup error naming the
// path), not a crash or an empty-config fallback.
inline std::expected<std::vector<VenueSubscription>, std::string> load_book_subscriptions(
    std::string_view path) {
    simdjson::padded_string json;
    auto error = simdjson::padded_string::load(path).get(json);
    if (error) {
        return std::unexpected("failed to read subscription config \"" + std::string(path) +
                                "\": " + std::string(simdjson::error_message(error)));
    }
    return parse_book_subscriptions(std::string_view(json));
}

}  // namespace bobby::hermeneutic::ingestion

#pragma once

#include <simdjson.h>

#include <chrono>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "bobby/hermeneutic/ingestion/idle_timeout.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic::ingestion {

using bobby::hermeneutic::symbol::BookId;
using bobby::hermeneutic::symbol::Exchange;
using bobby::hermeneutic::symbol::MarketType;
using bobby::hermeneutic::symbol::NativeSymbol;
using bobby::hermeneutic::symbol::VenueId;

// One venue's contribution to one book: everything apps/aggregator/server_main.cpp
// needs to add this (venue, symbol, type) triple to the right
// SymbolRegistry and, grouped by venue_id, to IngestionRunner. This is
// a parsing *result*, not part of the symbol domain model itself (see
// bobby/hermeneutic/symbol/symbol.hpp) - it exists because this ingestion
// config, specifically, resolves a venue list into concrete subscriptions.
//
// `venue_id` (not separate `Exchange exchange; MarketType type;` fields):
// the pairing IS what VenueId exists to own - see symbol.hpp's own
// comment - so spelling it out as two fields here would just be a
// second, independent place for the exact same pairing to drift out of
// sync.
struct VenueSubscription {
    VenueId venue_id;
    BookId book_id;              // AggregatorService::book()'s key
    NativeSymbol native_symbol;  // e.g. "BTC-USDT-SWAP" - what this venue's Feed subscribes with
};

// Shared by parse_book_subscriptions()'s "type" field and
// parse_idle_timeout_config()'s "type" field below - both this project's
// own venue-neutral vocabulary (see symbol::MarketType's own comment), not
// a wire-format string worth two independent if/else ladders. Returns
// nullopt rather than an error string: each caller's own error message
// names its own field/entry, which this function has no business guessing.
inline std::optional<MarketType> parse_market_type(std::string_view type_field) {
    if (type_field == "SPOT") return MarketType::Spot;
    if (type_field == "PERP") return MarketType::Perp;
    return std::nullopt;
}

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

            auto parsed_type = parse_market_type(type_field);
            if (!parsed_type) {
                return std::unexpected("unknown book type \"" + std::string(type_field) +
                                        "\" (expected SPOT or PERP) for symbol \"" + std::string(symbol_field) +
                                        "\"");
            }
            MarketType type = *parsed_type;

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

            std::unordered_set<Exchange> seen_exchanges;
            bool any_venue = false;
            for (auto venue_value : venues_field) {
                std::string_view venue_token;
                try {
                    venue_token = venue_value.get_string();
                } catch (const simdjson::simdjson_error&) {
                    return std::unexpected("\"venues\" for \"" + key_str + "\" must be an array of strings");
                }

                auto exchange = bobby::hermeneutic::symbol::parse_exchange(venue_token);
                if (!exchange) {
                    return std::unexpected("unknown venue \"" + std::string(venue_token) + "\" for \"" + key_str +
                                            "\"");
                }
                if (!seen_exchanges.insert(*exchange).second) {
                    return std::unexpected("venue " +
                                            std::string(bobby::hermeneutic::symbol::exchange_name(*exchange)) +
                                            " listed twice for \"" + key_str + "\"");
                }

                any_venue = true;
                result.push_back(VenueSubscription{VenueId{*exchange, type}, id,
                                                     bobby::hermeneutic::symbol::native_symbol(*exchange, *symbol, type)});
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

// The WebSocket idle-read timeout (see WebSocketConnection::connect()'s
// own comment for what this guards against - transport responsiveness,
// via Boost.Beast's own keep_alive_pings, not application-data freshness)
// a VenueSession should use, per VenueId. Configurable per venue rather
// than a single fixed constant mainly as a safety knob: this measures how
// long we tolerate a venue's WS gateway not answering our own idle ping,
// which hasn't been live-verified per venue (a healthy gateway should
// answer in well under a second, but nothing here has actually confirmed
// that per-venue yet - see docs/ingestion_design.md 第10節第15項).
struct IdleTimeoutConfig {
    std::chrono::seconds default_timeout = kDefaultIdleTimeout;
    std::unordered_map<VenueId, std::chrono::seconds> overrides;

    std::chrono::seconds for_venue(const VenueId& venue_id) const {
        auto it = overrides.find(venue_id);
        return it != overrides.end() ? it->second : default_timeout;
    }
};

// Parses the same config document's optional idle-timeout section, e.g.:
//   {
//     "books": [...],
//     "idle_timeout_seconds": 30,
//     "venue_idle_timeout_overrides": [
//       {"venue": "OKX", "type": "SPOT", "seconds": 120},
//       {"venue": "OKX", "type": "PERP", "seconds": 120}
//     ]
//   }
// Both fields are optional; an absent "idle_timeout_seconds" keeps
// kDefaultIdleTimeout, and an absent/empty "venue_idle_timeout_overrides"
// leaves every venue on the default. Deliberately a separate parse of the
// same document rather than folded into parse_book_subscriptions() above:
// that function's return type is a system boundary its own callers
// (including every existing test) depend on, and this section is
// orthogonal to what it validates - VenueSubscription's own (venue, book)
// pairing.
inline std::expected<IdleTimeoutConfig, std::string> parse_idle_timeout_config(std::string_view json_text) {
    IdleTimeoutConfig config;

    try {
        simdjson::padded_string padded(json_text);
        simdjson::ondemand::parser parser;
        simdjson::ondemand::document doc = parser.iterate(padded);

        auto idle_timeout_seconds_range_error = [] {
            return std::unexpected("\"idle_timeout_seconds\" must be a positive number of seconds, no more than " +
                                    std::to_string(kMaxIdleTimeout.count()) + " (24 hours)");
        };

        std::optional<int64_t> default_seconds;
        try {
            default_seconds = doc["idle_timeout_seconds"].get_int64();
        } catch (const simdjson::simdjson_error& e) {
            // NO_SUCH_FIELD alone means "absent, keep the default".
            // INCORRECT_TYPE (present but not a number, e.g. a string)
            // gets the same targeted message as an out-of-range value
            // below, matching how the override-entry parser already
            // handles this same class of mistake for its own fields - a
            // /code-review pass caught this catch only ever recognizing
            // NO_SUCH_FIELD, so a wrong-typed value fell through to the
            // generic "malformed subscription config" message instead of
            // naming the field. Anything else really is document
            // corruption, rethrown to the outer catch below (the same
            // reasoning ParseIdleTimeoutConfig.RejectsMalformedJson
            // caught this function getting wrong the first time).
            if (e.error() == simdjson::INCORRECT_TYPE) return idle_timeout_seconds_range_error();
            if (e.error() != simdjson::NO_SUCH_FIELD) throw;
        }
        if (default_seconds) {
            if (*default_seconds <= 0 || *default_seconds > kMaxIdleTimeout.count()) {
                return idle_timeout_seconds_range_error();
            }
            config.default_timeout = std::chrono::seconds(*default_seconds);
        }

        simdjson::ondemand::array overrides_field;
        bool has_overrides = true;
        try {
            overrides_field = doc["venue_idle_timeout_overrides"].get_array();
        } catch (const simdjson::simdjson_error& e) {
            // Same reasoning as the idle_timeout_seconds try/catch above.
            if (e.error() != simdjson::NO_SUCH_FIELD) throw;
            has_overrides = false;
        }

        if (has_overrides) {
            for (auto entry_value : overrides_field) {
                std::string_view venue_token;
                std::string_view type_field;
                int64_t seconds;
                try {
                    // entry_value.get_object() must be inside this same
                    // try/catch, not before it: a non-object entry (e.g.
                    // "OKX" instead of {...}) throws INCORRECT_TYPE right
                    // here, and if that throw were outside this block it
                    // would escape to the outer "malformed subscription
                    // config" catch below instead of this entry's own
                    // field-list error - a real bug this exact scenario
                    // caught (empirically verified by compiling this
                    // header standalone).
                    simdjson::ondemand::object entry = entry_value.get_object();
                    venue_token = entry["venue"].get_string();
                    type_field = entry["type"].get_string();
                    seconds = entry["seconds"].get_int64();
                } catch (const simdjson::simdjson_error& e) {
                    // Only a missing key or a right-key-wrong-type value is
                    // actually this entry's own fault; anything else (the
                    // document being malformed somewhere simdjson only
                    // notices while skip-parsing this entry) is rethrown to
                    // the outer catch below, same reasoning as the two
                    // top-level try/catches above - otherwise an operator
                    // chasing real document corruption gets sent looking
                    // for a nonexistent field-naming bug in this entry
                    // instead.
                    if (e.error() != simdjson::NO_SUCH_FIELD && e.error() != simdjson::INCORRECT_TYPE) throw;
                    return std::unexpected(
                        "each entry in \"venue_idle_timeout_overrides\" needs \"venue\" (string), "
                        "\"type\" (string), and \"seconds\" (positive integer)");
                }
                if (seconds <= 0 || seconds > kMaxIdleTimeout.count()) {
                    return std::unexpected("idle timeout override for \"" + std::string(venue_token) +
                                            "\" must be a positive number of seconds, no more than " +
                                            std::to_string(kMaxIdleTimeout.count()) + " (24 hours)");
                }

                auto exchange = bobby::hermeneutic::symbol::parse_exchange(venue_token);
                if (!exchange) {
                    return std::unexpected("unknown venue \"" + std::string(venue_token) +
                                            "\" in \"venue_idle_timeout_overrides\"");
                }
                auto parsed_type = parse_market_type(type_field);
                if (!parsed_type) {
                    return std::unexpected("unknown type \"" + std::string(type_field) +
                                            "\" in \"venue_idle_timeout_overrides\" (expected SPOT or PERP)");
                }
                MarketType type = *parsed_type;

                VenueId venue_id{*exchange, type};
                if (!config.overrides.try_emplace(venue_id, std::chrono::seconds(seconds)).second) {
                    return std::unexpected("idle timeout override for \"" + std::string(venue_token) + "_" +
                                            std::string(type_field) + "\" is configured more than once");
                }
            }
        }
    } catch (const simdjson::simdjson_error& e) {
        return std::unexpected("malformed subscription config: " + std::string(e.what()));
    }

    return config;
}

// Reads `path` and parses its optional idle-timeout section - see
// parse_idle_timeout_config() above for the document shape. Mirrors
// load_book_subscriptions()'s own error handling for a missing/unreadable
// file.
inline std::expected<IdleTimeoutConfig, std::string> load_idle_timeout_config(std::string_view path) {
    simdjson::padded_string json;
    auto error = simdjson::padded_string::load(path).get(json);
    if (error) {
        return std::unexpected("failed to read subscription config \"" + std::string(path) +
                                "\": " + std::string(simdjson::error_message(error)));
    }
    return parse_idle_timeout_config(std::string_view(json));
}

}  // namespace bobby::hermeneutic::ingestion

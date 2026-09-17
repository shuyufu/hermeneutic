#pragma once

#include <simdjson.h>

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>

#include "bobby/hermeneutic/fixed_point.hpp"
#include "bobby/hermeneutic/symbol_sync.hpp"

namespace bobby::hermeneutic::ingestion {

// A REST request this feed wants made, pure data -- no I/O happens here.
// The driver (VenueSession, not yet built) is what actually performs it.
struct HttpRequestSpec {
    std::string host;    // e.g. "fapi.binance.com"
    std::string target;  // path + query, e.g. "/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
};

using ParsedMessage = std::optional<std::variant<SnapshotMessage, DepthUpdate>>;

namespace detail {

// Binance sends price/quantity as JSON strings (not numbers), to avoid any
// floating-point ambiguity in the wire format itself. std::from_chars
// (locale-independent, no exceptions of its own) parses the decimal text;
// a malformed string throws simdjson::simdjson_error so every parse
// failure in this file funnels through the same catch site.
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

// VenueFeed for Binance USDS-M Futures: parse/encode only, no I/O. See
// docs/ingestion_design.md for the design this implements and the primary
// sources (developers.binance.com) the wire format and resync semantics
// were verified against.
class BinanceFuturesFeed {
  public:
    // Snapshot comes from a REST call (the RequestSnapshot action triggers
    // an actual HTTP GET), not pushed over the WebSocket -- unlike some
    // other venues (see docs/ingestion_design.md's TrustConnectionOrderPolicy
    // note).
    static constexpr bool kSnapshotViaRest = true;

    // Pure function: the JSON to send right after connecting, to subscribe
    // every symbol's depth diff stream. Stream name pattern is
    // `{symbol}@depth@{update_speed}` (symbol lowercased), matching one of
    // Binance's documented update_speed options ("100ms"/"500ms").
    std::string subscribe_message(std::span<const SymbolId> symbols,
                                   std::string_view update_speed = "100ms") const {
        std::string params;
        for (std::size_t i = 0; i < symbols.size(); ++i) {
            if (i > 0) params += ',';
            std::string lowered = symbols[i];
            std::ranges::transform(lowered, lowered.begin(),
                                    [](unsigned char c) { return std::tolower(c); });
            params += '"';
            params += lowered;
            params += "@depth@";
            params += update_speed;
            params += '"';
        }
        return "{\"method\":\"SUBSCRIBE\",\"params\":[" + params + "],\"id\":1}";
    }

    // Parses one WebSocket text message. std::nullopt (not an error) means
    // this message is recognized but irrelevant to book state -- e.g. the
    // SUBSCRIBE acknowledgement Binance sends back (`{"result":null,"id":1}`),
    // which has no "e" field at all. std::errc::bad_message means the
    // message looked like a depth event (had "e":"depthUpdate") but was
    // otherwise malformed.
    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const {
        try {
            simdjson::ondemand::parser parser;
            simdjson::padded_string padded(text);
            simdjson::ondemand::document doc = parser.iterate(padded);
            simdjson::ondemand::object root = doc.get_object();

            std::string_view event_type;
            try {
                event_type = root["e"].get_string();
            } catch (const simdjson::simdjson_error&) {
                return ParsedMessage{std::nullopt};  // no "e" field, e.g. a SUBSCRIBE ack
            }
            if (event_type != "depthUpdate") return ParsedMessage{std::nullopt};

            DepthUpdate update;
            update.symbol.assign(std::string_view(root["s"].get_string()));
            update.first_id = static_cast<std::uint64_t>(root["U"].get_uint64());
            update.final_id = static_cast<std::uint64_t>(root["u"].get_uint64());
            update.prev_final_id = static_cast<std::uint64_t>(root["pu"].get_uint64());
            update.bids = detail::parse_levels(root["b"].get_array());
            update.asks = detail::parse_levels(root["a"].get_array());

            return ParsedMessage{std::in_place, std::move(update)};
        } catch (const simdjson::simdjson_error&) {
            return std::unexpected(std::errc::bad_message);
        }
    }

    // GET /fapi/v1/depth?symbol=<symbol>&limit=1000 -- see
    // developers.binance.com's Order Book REST endpoint doc.
    HttpRequestSpec snapshot_request(const SymbolId& symbol) const {
        return HttpRequestSpec{"fapi.binance.com", "/fapi/v1/depth?symbol=" + symbol + "&limit=1000"};
    }

    // `symbol` is supplied by the caller (the request it made), not read
    // from the response body -- the REST response itself carries no symbol
    // field (see the doc's Sources for the exact shape).
    std::expected<SnapshotMessage, std::errc> parse_snapshot_response(SymbolId symbol,
                                                                       std::string_view body) const {
        try {
            simdjson::ondemand::parser parser;
            simdjson::padded_string padded(body);
            simdjson::ondemand::document doc = parser.iterate(padded);
            simdjson::ondemand::object root = doc.get_object();

            SnapshotMessage snapshot;
            snapshot.symbol = std::move(symbol);
            snapshot.last_update_id = static_cast<std::uint64_t>(root["lastUpdateId"].get_uint64());
            snapshot.bids = detail::parse_levels(root["bids"].get_array());
            snapshot.asks = detail::parse_levels(root["asks"].get_array());
            return snapshot;
        } catch (const simdjson::simdjson_error&) {
            return std::unexpected(std::errc::bad_message);
        }
    }
};

}  // namespace bobby::hermeneutic::ingestion

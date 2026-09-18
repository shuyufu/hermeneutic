#pragma once

#include <simdjson.h>

#include <algorithm>
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

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/core/fixed_point.hpp"
#include "bobby/hermeneutic/exchange/binance/binance_wire.hpp"
#include "bobby/hermeneutic/exchange/json_wire.hpp"

namespace bobby::hermeneutic::ingestion {

using ParsedMessage = std::optional<std::variant<SnapshotMessage, DepthUpdate>>;

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

    // Combined-stream endpoint used with the SUBSCRIBE message below -
    // depth events arrive unwrapped (no {"stream":...,"data":...} envelope),
    // matching what parse_message() expects.
    std::string_view ws_host() const { return "fstream.binance.com"; }
    std::string_view ws_port() const { return "443"; }
    std::string_view ws_target() const { return "/ws"; }

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
        return HttpRequestSpec{"fapi.binance.com", "443", "/fapi/v1/depth?symbol=" + symbol + "&limit=1000"};
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

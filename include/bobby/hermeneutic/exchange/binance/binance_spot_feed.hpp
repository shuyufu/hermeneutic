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
#include <vector>

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/core/fixed_point.hpp"
#include "bobby/hermeneutic/exchange/feed_wire.hpp"

namespace bobby::hermeneutic::ingestion {

// VenueFeed for Binance Spot: parse/encode only, no I/O. Spot's
// depthUpdate has no `pu` field (unlike Futures'), so
// BinanceSpotSequencePolicy (symbol_sync.hpp) validates continuity via
// `first_id == last_applied_final_id + 1` instead, and
// DepthUpdate::prev_final_id is simply left at 0 below.
class BinanceSpotFeed {
  public:
    // Same as Futures: snapshot comes from a REST call, not pushed over
    // the WebSocket.
    static constexpr bool kSnapshotViaRest = true;

    // Bare /ws (not /stream, the combined-stream endpoint that wraps
    // payloads as {"stream":...,"data":...}) accepts the same dynamic
    // SUBSCRIBE method Futures uses and delivers unwrapped depthUpdate
    // events, matching what parse_message() expects. Different port from
    // Futures: Spot's plaintext WS port is 9443, not 443.
    std::string_view ws_host() const { return "stream.binance.com"; }
    std::string_view ws_port() const { return "9443"; }
    std::string_view ws_target() const { return "/ws"; }

    // Pure function: the JSON to send right after connecting, to subscribe
    // every symbol's depth diff stream. Stream name pattern is
    // `{symbol}@depth@{update_speed}` (symbol lowercased). Spot's
    // documented update_speed options are "100ms" and the default 1000ms
    // (spelled out explicitly, not "500ms" like Futures) - default kept at
    // "100ms" here for parity with BinanceFuturesFeed's default, not
    // because Spot's own default differs.
    std::string subscribe_message(std::span<const NativeSymbol> symbols,
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
            // No `pu` field on Spot's depthUpdate - BinanceSpotSequencePolicy
            // never reads this, but it must still be set to something
            // deterministic (see symbol_sync.hpp's DepthUpdate: no default
            // member initializers, so an unset field is indeterminate, not
            // zero).
            update.prev_final_id = 0;
            update.bids = detail::parse_levels(root["b"].get_array());
            update.asks = detail::parse_levels(root["a"].get_array());

            return ParsedMessage{std::in_place, std::move(update)};
        } catch (const simdjson::simdjson_error&) {
            return std::unexpected(std::errc::bad_message);
        }
    }

    // GET /api/v3/depth?symbol=<symbol>&limit=5000. limit=5000 is the
    // maximum Spot allows (Futures caps at 1000); a full-depth snapshot
    // minimizes how often on_snapshot() has to retry when Spot's <= drop
    // rule (BinanceSpotSequencePolicy, unlike Futures' strict <) empties
    // the buffer on a quiet symbol.
    HttpRequestSpec snapshot_request(const NativeSymbol& symbol) const {
        return HttpRequestSpec{"api.binance.com", "443", "/api/v3/depth?symbol=" + symbol + "&limit=5000"};
    }

    // `symbol` is supplied by the caller (the request it made), not read
    // from the response body -- the REST response itself carries no symbol
    // field (see the doc's Sources for the exact shape). Same shape as
    // Futures' response minus the `E`/`T` fields Futures has and this code
    // doesn't parse from either.
    std::expected<SnapshotMessage, std::errc> parse_snapshot_response(NativeSymbol symbol,
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

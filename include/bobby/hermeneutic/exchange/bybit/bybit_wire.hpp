#pragma once

#include <simdjson.h>

#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/exchange/feed_wire.hpp"

namespace bobby::hermeneutic::ingestion {

// ParsedMessage is defined once in feed_wire.hpp - see its own comment.

namespace detail {

// Shared by every Bybit v5 public orderbook Feed (linear, spot - verified
// live against both wss://stream.bybit.com/v5/public/linear and .../spot,
// 2026-09-18: identical envelope shape, identical single-update-id `u`
// semantics on every delta). The only thing that actually differs between
// Bybit's market segments is which WS path a Feed connects to
// (BybitLinearFeed/BybitSpotFeed's own ws_target()) - not documented by
// Bybit as a formal guarantee that will hold for every future product
// (inverse/option aren't verified at all), so this is deliberately kept
// separate from a given Feed's endpoint constants rather than assumed to
// extend automatically to a market segment nobody has actually checked.
//
// Pure function: the JSON to send right after connecting, to subscribe
// every symbol's orderbook diff topic. Topic name pattern is
// `orderbook.{depth}.{symbol}` (symbol uppercased, matching what Bybit both
// expects in the subscribe request and echoes back in `data.s`); `depth`
// must be one of Bybit's documented values for the calling Feed's market
// segment - not validated here, a caller-level concern.
inline std::string bybit_orderbook_subscribe_message(std::span<const NativeSymbol> symbols, int depth) {
    std::string args;
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i > 0) args += ',';
        args += "\"orderbook.";
        args += std::to_string(depth);
        args += '.';
        args += symbols[i];
        args += '"';
    }
    return "{\"op\":\"subscribe\",\"args\":[" + args + "]}";
}

// Parses one WebSocket text message. std::nullopt (not an error) means this
// message is recognized but irrelevant to book state - e.g. the subscribe
// acknowledgement Bybit sends back
// (`{"success":true,"ret_msg":"","conn_id":"...","req_id":"","op":"subscribe"}`),
// which has no "topic" field at all - or a "type" this feed doesn't
// recognize, to stay forward-compatible with a future message shape rather
// than treating it as malformed. std::errc::bad_message means the message
// looked like an orderbook event (had "topic" and a recognized "type") but
// was otherwise malformed.
inline std::expected<ParsedMessage, std::errc> parse_bybit_orderbook_message(std::string_view text) {
    try {
        simdjson::ondemand::parser parser;
        simdjson::padded_string padded(text);
        simdjson::ondemand::document doc = parser.iterate(padded);
        simdjson::ondemand::object root = doc.get_object();

        try {
            // simdjson's on-demand API is lazy: get_string() itself doesn't
            // perform the field lookup that can throw - only consuming the
            // returned simdjson_result (here, converting it to
            // std::string_view) does. A discarded `root["topic"].get_string();`
            // statement would silently never throw NO_SUCH_FIELD at all,
            // defeating this whole check - caught the hard way, by a test
            // (SubscribeAckHasNoTopicFieldAndIsIgnored) that failed until
            // this value was actually bound to something.
            std::string_view topic = root["topic"].get_string();
            (void)topic;
        } catch (const simdjson::simdjson_error&) {
            return ParsedMessage{std::nullopt};  // no "topic", e.g. a subscribe ack
        }

        std::string_view type = root["type"].get_string();
        simdjson::ondemand::object data = root["data"].get_object();

        if (type == "snapshot") {
            SnapshotMessage snapshot;
            snapshot.symbol.assign(std::string_view(data["s"].get_string()));
            snapshot.bids = detail::parse_levels(data["b"].get_array());
            snapshot.asks = detail::parse_levels(data["a"].get_array());
            snapshot.last_update_id = static_cast<std::uint64_t>(data["u"].get_uint64());
            return ParsedMessage{std::in_place, std::move(snapshot)};
        } else if (type == "delta") {
            DepthUpdate update;
            update.symbol.assign(std::string_view(data["s"].get_string()));
            update.bids = detail::parse_levels(data["b"].get_array());
            update.asks = detail::parse_levels(data["a"].get_array());
            // Bybit carries one update id per message (`u`), not Binance's
            // first_id/final_id range - both fields get the same value so
            // BybitSequencePolicy's == comparisons read consistently
            // regardless of which one it happens to use. prev_final_id has
            // no Bybit equivalent (no `pu`-style back-pointer) - set
            // explicitly to 0 rather than left default-constructed, so a
            // future policy/feed mismatch reads a deterministic value
            // instead of whatever garbage DepthUpdate's default state
            // happens to be.
            std::uint64_t update_id = static_cast<std::uint64_t>(data["u"].get_uint64());
            update.first_id = update_id;
            update.final_id = update_id;
            update.prev_final_id = 0;
            return ParsedMessage{std::in_place, std::move(update)};
        }
        return ParsedMessage{std::nullopt};  // recognized envelope, unrecognized type
    } catch (const simdjson::simdjson_error&) {
        return std::unexpected(std::errc::bad_message);
    }
}

}  // namespace detail

}  // namespace bobby::hermeneutic::ingestion

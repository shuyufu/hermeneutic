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
#include <variant>

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/exchange/json_wire.hpp"

namespace bobby::hermeneutic::ingestion {

using ParsedMessage = std::optional<std::variant<SnapshotMessage, DepthUpdate>>;

// OKX's own instId format tells the caller unambiguously whether an
// instrument is a perpetual swap ("BTC-USDT-SWAP") - anything else with
// exactly one dash is treated as spot ("BTC-USDT"). Dated futures/options
// (an instId with a different shape entirely, e.g. an expiry-date or
// strike/option-type suffix) aren't modeled - not supported this pass.
inline bool is_swap(std::string_view instid) { return instid.ends_with("-SWAP"); }

// Maps an OKX instId to the canonical, venue-neutral symbol key used across
// this project's SymbolBooks (see docs/ingestion_design.md's OKX section):
// "BTC-USDT-SWAP" -> "BTCUSDT.PERP" (shares Binance/Bybit's perpetual book),
// "BTC-USDT" -> "BTCUSDT.SPOT" (shares their spot book). Only this direction
// (OKX-native -> canonical) is implemented: it's an unambiguous string
// operation (removing characters OKX itself put there). The reverse
// (canonical -> OKX-native) would require guessing where the quote asset
// starts (USDT/USDC/BUSD/...), which this project deliberately doesn't
// attempt - callers supply OKX-native instId strings directly.
// std::nullopt means an instId shape this function doesn't recognize
// (dated futures, options, or anything malformed) - not supported this pass.
inline std::optional<std::string> okx_canonical(std::string_view instid) {
    bool swap = is_swap(instid);
    if (swap) instid.remove_suffix(std::string_view("-SWAP").size());

    auto dash = instid.find('-');
    if (dash == std::string_view::npos || instid.find('-', dash + 1) != std::string_view::npos) {
        return std::nullopt;  // not a plain BASE-QUOTE shape after stripping -SWAP
    }

    std::string canonical;
    canonical.reserve(instid.size());
    for (char c : instid) {
        if (c != '-') canonical += c;
    }
    canonical += swap ? ".PERP" : ".SPOT";
    return canonical;
}

// VenueFeed for OKX's v5 public `books` channel: parse/encode only, no I/O.
// Serves both spot and perpetual-swap instIds - OKX's `books` channel is
// identical at the protocol level for both (same endpoint, same channel
// name, same message shape); only the instId string subscribed differs.
// See docs/ingestion_design.md for the design this implements and OKX's
// own documented order-book-maintenance semantics (quoted directly by the
// user, not assumed from training data - see OkxSequencePolicy's comment
// in symbol_sync.hpp for the verification).
//
// No client-initiated keepalive is implemented (no periodic "ping" text,
// no VenueSession changes) - verified live rather than assumed, given
// general OKX docs describe a recommended client ping for connection
// health: a live subscription to wss://ws.okx.com:8443/ws/v5/public
// books/BTC-USDT ran 40s, and books/TRX-USDT ran 80s (past the documented
// ~60s "extended idle period" heartbeat threshold), with the client
// sending nothing beyond the initial subscribe request in either case
// (2026-09-18). Both connections stayed open throughout, continuously
// receiving real updates or OKX's own documented idle-heartbeat message
// (empty bids/asks) - the server's own outgoing traffic appears to be
// what keeps the connection alive on OKX's side, not anything the client
// sends. VenueSession is always subscribed to at least one instId once
// connected, so this project's actual usage never hits a scenario this
// evidence doesn't cover; parse_message()'s "pong" check above is kept
// as a harmless no-op in case that assessment ever needs revisiting.
class OkxFeed {
  public:
    // OKX pushes its own snapshot as the first message on a fresh
    // (re)subscribe, ahead of every update, on the same WebSocket
    // connection - unlike Binance, which races a separate REST call
    // against the already-flowing diff stream. RequestSnapshot is
    // therefore a no-op for this feed (see venue_session.hpp's
    // handle_request_snapshot()).
    static constexpr bool kSnapshotViaRest = false;

    std::string_view ws_host() const { return "ws.okx.com"; }
    std::string_view ws_port() const { return "8443"; }
    std::string_view ws_target() const { return "/ws/v5/public"; }

    // Pure function: the JSON to send right after connecting, to subscribe
    // every symbol's `books` channel. `symbols` are OKX-native instId
    // strings (e.g. "BTC-USDT", "BTC-USDT-SWAP") - used verbatim, no
    // transformation - matching what OkxFeed::parse_message() reads back
    // out of `arg.instId` and what okx_canonical() expects as input.
    std::string subscribe_message(std::span<const SymbolId> symbols) const {
        std::string args;
        for (std::size_t i = 0; i < symbols.size(); ++i) {
            if (i > 0) args += ',';
            args += R"({"channel":"books","instId":")";
            args += symbols[i];
            args += "\"}";
        }
        return R"({"op":"subscribe","args":[)" + args + "]}";
    }

    // Parses one WebSocket text message. std::nullopt (not an error) means
    // this message is recognized but irrelevant to book state:
    //   - the literal string "pong" (this feed's keepalive reply - not
    //     JSON at all, so it must be checked before attempting to parse
    //     one, or every keepalive tick would otherwise show up as a
    //     std::errc::bad_message)
    //   - a subscribe ack (`{"event":"subscribe",...}`) or error
    //     (`{"event":"error",...}`) - no "data" field at all
    // std::errc::bad_message means the message had a "data" field but was
    // otherwise malformed.
    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const {
        if (text == "pong") return ParsedMessage{std::nullopt};

        try {
            simdjson::ondemand::parser parser;
            simdjson::padded_string padded(text);
            simdjson::ondemand::document doc = parser.iterate(padded);
            simdjson::ondemand::object root = doc.get_object();

            // simdjson's on-demand object access is forward-only within one
            // document - fields must be read in the order they actually
            // appear on the wire ("arg", then "action", then "data" for
            // every real OKX push message), or a later lookup silently
            // can't find an earlier field it already scanned past. "arg"
            // and "action" are read together, in one try/catch: either one
            // missing (a subscribe ack has no "action"; some error events
            // have no "arg") means "ignorable", not "malformed".
            std::string_view instid;
            std::string_view action;
            try {
                instid = root["arg"]["instId"].get_string();
                action = root["action"].get_string();
            } catch (const simdjson::simdjson_error&) {
                return ParsedMessage{std::nullopt};  // e.g. a subscribe ack or error event
            }

            simdjson::ondemand::array data = root["data"].get_array();
            auto it = data.begin();
            // OKX's documented `books` message shape always carries exactly
            // one entry in "data" - an empty array is malformed, not just
            // "recognized but irrelevant". Checked explicitly rather than
            // left to fall through to *it below: dereferencing an on-demand
            // array's end() iterator is not a simdjson_error (it trips a
            // hard assert in a debug build, or is undefined behavior under
            // NDEBUG - verified empirically), so the catch around this
            // function would never see it and this could abort or corrupt
            // the whole ingestion process on a single malformed message.
            if (it == data.end()) return std::unexpected(std::errc::bad_message);
            simdjson::ondemand::object entry = (*it).get_object();

            if (action == "snapshot") {
                SnapshotMessage snapshot;
                snapshot.symbol.assign(instid);
                snapshot.bids = detail::parse_levels(entry["bids"].get_array());
                snapshot.asks = detail::parse_levels(entry["asks"].get_array());
                snapshot.last_update_id = static_cast<std::uint64_t>(entry["seqId"].get_int64());
                return ParsedMessage{std::in_place, std::move(snapshot)};
            } else if (action == "update") {
                DepthUpdate update;
                update.symbol.assign(instid);
                update.bids = detail::parse_levels(entry["bids"].get_array());
                update.asks = detail::parse_levels(entry["asks"].get_array());
                // seqId is signed (-1 only ever appears in a snapshot's
                // prevSeqId, never here, but the field itself is declared
                // signed by OKX) - read via get_int64(), not get_uint64().
                std::uint64_t seq_id = static_cast<std::uint64_t>(entry["seqId"].get_int64());
                update.first_id = seq_id;  // OKX has no distinct range start - every message carries one seqId
                update.final_id = seq_id;
                update.prev_final_id = static_cast<std::uint64_t>(entry["prevSeqId"].get_int64());
                return ParsedMessage{std::in_place, std::move(update)};
            }
            return ParsedMessage{std::nullopt};  // recognized envelope, unrecognized action
        } catch (const simdjson::simdjson_error&) {
            return std::unexpected(std::errc::bad_message);
        }
    }
};

}  // namespace bobby::hermeneutic::ingestion

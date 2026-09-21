#pragma once

#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

#include "bobby/hermeneutic/book/symbol_sync.hpp"
#include "bobby/hermeneutic/exchange/bybit/bybit_wire.hpp"

namespace bobby::hermeneutic::ingestion {

// VenueFeed for Bybit's v5 public orderbook stream, linear market segment
// (i.e. USDT perpetuals):
// bybit-exchange.github.io/docs/v5/websocket/public/orderbook. Parsing
// itself is shared with BybitSpotFeed via bybit_wire.hpp - this class
// only supplies the linear endpoint. Kept in
// the same file as BybitSpotFeed (unlike Binance's Futures/Spot, which are
// genuinely different wire semantics and stay in separate files): the two
// Bybit classes differ only in ws_target() and their doc comments,
// otherwise delegating 100% to the same detail:: functions below.
class BybitLinearFeed {
  public:
    // Bybit pushes its own snapshot as the first message on a topic,
    // ordered ahead of every delta on that same WebSocket connection -
    // unlike Binance, which requires a separate REST call raced against the
    // already-flowing diff stream. RequestSnapshot is therefore a no-op for
    // this feed (see venue_session.hpp's handle_request_snapshot()); the
    // first ParsedMessage this feed ever emits for a topic is always the
    // SnapshotMessage.
    static constexpr bool kSnapshotViaRest = false;

    std::string_view ws_host() const { return "stream.bybit.com"; }
    std::string_view ws_port() const { return "443"; }
    std::string_view ws_target() const { return "/v5/public/linear"; }

    std::string subscribe_message(std::span<const NativeSymbol> symbols, int depth = 50) const {
        return detail::bybit_orderbook_subscribe_message(symbols, depth);
    }

    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const {
        return detail::parse_bybit_orderbook_message(text);
    }
};

// VenueFeed for Bybit's v5 public orderbook stream, spot market segment:
// identical envelope shape and single-update-id `u` semantics to
// BybitLinearFeed's own linear market (see bybit_wire.hpp's
// shared-parsing comment for why that isn't assumed to carry over from
// linear without checking). Parsing itself is shared with BybitLinearFeed
// via bybit_wire.hpp - this class only supplies the spot endpoint.
class BybitSpotFeed {
  public:
    // Same reasoning as BybitLinearFeed::kSnapshotViaRest: Bybit pushes its
    // own snapshot as the first message on a topic, ahead of every diff on
    // the same connection, for spot too.
    static constexpr bool kSnapshotViaRest = false;

    std::string_view ws_host() const { return "stream.bybit.com"; }
    std::string_view ws_port() const { return "443"; }
    std::string_view ws_target() const { return "/v5/public/spot"; }

    std::string subscribe_message(std::span<const NativeSymbol> symbols, int depth = 50) const {
        return detail::bybit_orderbook_subscribe_message(symbols, depth);
    }

    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const {
        return detail::parse_bybit_orderbook_message(text);
    }
};

}  // namespace bobby::hermeneutic::ingestion

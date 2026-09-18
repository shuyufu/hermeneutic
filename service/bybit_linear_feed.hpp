#pragma once

#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

#include "bobby/hermeneutic/symbol_sync.hpp"
#include "bybit_wire.hpp"

namespace bobby::hermeneutic::ingestion {

// VenueFeed for Bybit's v5 public orderbook stream, linear market segment
// (i.e. USDT perpetuals) - see docs/ingestion_design.md for the design this
// implements and bybit-exchange.github.io/docs/v5/websocket/public/orderbook,
// the primary source the wire format and resync semantics were verified
// against (both by reading the doc and by a live probe, see there for why
// both were needed). Parsing itself is shared with BybitSpotFeed via
// bybit_wire.hpp - this class only supplies the linear endpoint.
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

    std::string subscribe_message(std::span<const SymbolId> symbols, int depth = 50) const {
        return detail::bybit_orderbook_subscribe_message(symbols, depth);
    }

    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const {
        return detail::parse_bybit_orderbook_message(text);
    }
};

}  // namespace bobby::hermeneutic::ingestion

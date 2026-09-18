#pragma once

#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <system_error>

#include "bobby/hermeneutic/symbol_sync.hpp"
#include "bybit_wire.hpp"

namespace bobby::hermeneutic::ingestion {

// VenueFeed for Bybit's v5 public orderbook stream, spot market segment -
// see docs/ingestion_design.md for the design this implements. Wire format
// and resync semantics verified live against
// wss://stream.bybit.com/v5/public/spot (orderbook.50.BTCUSDT, 2026-09-18):
// identical envelope shape and single-update-id `u` semantics to
// BybitLinearFeed's own linear-market verification - not assumed to carry
// over from linear without checking, since Bybit doesn't document this as a
// formal cross-product guarantee (see bybit_wire.hpp's shared-parsing
// comment). Parsing itself is shared with BybitLinearFeed via
// bybit_wire.hpp - this class only supplies the spot endpoint.
class BybitSpotFeed {
  public:
    // Same reasoning as BybitLinearFeed::kSnapshotViaRest: Bybit pushes its
    // own snapshot as the first message on a topic, ahead of every diff on
    // the same connection, for spot too.
    static constexpr bool kSnapshotViaRest = false;

    std::string_view ws_host() const { return "stream.bybit.com"; }
    std::string_view ws_port() const { return "443"; }
    std::string_view ws_target() const { return "/v5/public/spot"; }

    std::string subscribe_message(std::span<const SymbolId> symbols, int depth = 50) const {
        return detail::bybit_orderbook_subscribe_message(symbols, depth);
    }

    std::expected<ParsedMessage, std::errc> parse_message(std::string_view text) const {
        return detail::parse_bybit_orderbook_message(text);
    }
};

}  // namespace bobby::hermeneutic::ingestion

#pragma once

#include <string>

#include "bobby/hermeneutic/exchange/json_wire.hpp"

namespace bobby::hermeneutic::ingestion {

// A REST request this feed wants made, pure data -- no I/O happens here.
// The driver (VenueSession) is what actually performs it. Shared across
// Binance feeds (Futures, Spot): VenueSession consumes this duck-typed
// (`auto spec = feed.snapshot_request(symbol)`, then `.host`/`.port`/
// `.target`), but a single translation unit including more than one
// Binance feed header needs one definition, not a redefinition per feed.
struct HttpRequestSpec {
    std::string host;    // e.g. "fapi.binance.com"
    std::string port;    // e.g. "443" -- kept separate from "is this TLS"
                          // so a test double can point at a local plain
                          // HTTP server on an arbitrary port.
    std::string target;  // path + query, e.g. "/fapi/v1/depth?symbol=BTCUSDT&limit=1000"
};

// detail::parse_decimal_string/parse_level/parse_levels used to live here,
// duplicating the identical helpers Bybit's ingestion needed (both venues
// happen to send price/quantity as JSON strings) - two copies in the same
// `bobby::hermeneutic::ingestion::detail` namespace, pulled into the same
// translation unit (aggregator_main.cpp includes every Feed header), is an
// ODR violation. Moved to the venue-neutral service/json_wire.hpp; this
// file now only holds what's genuinely Binance-specific (HttpRequestSpec,
// for its REST snapshot endpoint - Bybit has no REST snapshot at all, see
// BybitLinearFeed/BybitSpotFeed's kSnapshotViaRest == false).

}  // namespace bobby::hermeneutic::ingestion

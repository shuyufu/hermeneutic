#pragma once

#include <optional>

#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"
#include "bobby/hermeneutic/symbol/symbol.hpp"

// The thin, deliberately app-private conversion between the wire BookId
// (aggregator.proto) and this project's own symbol::BookId
// (symbol/symbol.hpp). symbol.hpp itself stays proto/gRPC-free (see its own
// class comment) - this seam is where that boundary is crossed, once, for
// the two places that actually need to (apps/aggregator/aggregator_service.hpp
// on the server side, apps/aggregator/client_main.cpp on the client side).
//
// Below, an unqualified `MarketType` is always the proto enum
// (aggregator.grpc.pb.h, brought into this namespace); `symbol::MarketType`
// is this project's own C++ enum (symbol.hpp) - same name by design (see
// symbol.hpp's own comment on MarketType), but two distinct types that only
// ever appear qualified vs. unqualified side by side, never ambiguous.
namespace bobby::hermeneutic::aggregator {

// nullopt for anything a well-formed request could never produce:
// MARKET_TYPE_UNSPECIFIED (proto3's implicit zero value on an unset
// `market` field) or an empty base/quote. Distinct from "well-formed but
// not one of this server's books" (symbol::BookId is a real, total value
// there - see AggregatorService::book()) - callers are expected to report
// this case as INVALID_ARGUMENT and that one as NOT_FOUND.
inline std::optional<symbol::BookId> to_symbol_book_id(const BookId& wire) {
    if (wire.base().empty() || wire.quote().empty()) return std::nullopt;
    symbol::MarketType type;
    switch (wire.market()) {
        case MarketType::SPOT: type = symbol::MarketType::Spot; break;
        case MarketType::PERP: type = symbol::MarketType::Perp; break;
        // Deliberately not the exhaustive, compiler-enforced switch pattern
        // symbol.hpp's own to_string(VenueId)/native_symbol() use (see this
        // project's docs/ingestion_design.md 第10節第10項): proto3 enums
        // are open on the wire - a newer client can send a MarketType this
        // server's generated code doesn't even know the name of yet - so a
        // catch-all is required here, not just permitted. MARKET_TYPE_UNSPECIFIED
        // falls in here too, alongside any value this protoc-generated
        // enum has no case for at all.
        case MarketType::MARKET_TYPE_UNSPECIFIED:
        default: return std::nullopt;
    }
    return symbol::BookId{symbol::BaseQuote{symbol::Asset{wire.base()}, symbol::Asset{wire.quote()}}, type};
}

// The inverse direction - fills `wire` from a symbol::BookId this project
// already knows is well-formed (e.g. client_main.cpp's own argv parsing via
// symbol::parse_book_id), so there's nothing to validate here.
inline void fill_wire_book_id(BookId* wire, const symbol::BookId& id) {
    wire->set_base(id.symbol.base.code);
    wire->set_quote(id.symbol.quote.code);
    wire->set_market(id.type == symbol::MarketType::Spot ? MarketType::SPOT : MarketType::PERP);
}

}  // namespace bobby::hermeneutic::aggregator

#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

// This project's own symbol domain model: what a book is (a base/quote
// pair plus which market it covers) and how that identity maps to each
// venue's own wire format. Zero dependency beyond the standard library -
// deliberately its own tier (like core/book), not folded into ingestion/,
// so anything that needs to talk about symbols/venues depends on this
// alone rather than pulling in gRPC/simdjson/Boost.
namespace bobby::hermeneutic::symbol {

// Which market a book covers - the suffix on a BookId's human-readable form
// ("BTC_USDT.SPOT"/"BTC_USDT.PERP", see to_string() below) and, transitively,
// which of a venue's feeds native_symbol() below resolves to. Named to match
// aggregator.proto's own MarketType enum (BookId.market); the two are
// distinct types in distinct namespaces (this one never depends on proto;
// apps/aggregator/book_id.hpp is the seam that converts between them).
//
// No Unspecified/Invalid member: a symbol::MarketType value is only ever
// produced by something that already knows Spot or Perp, unlike
// aggregator.proto's MarketType, which needs MARKET_TYPE_UNSPECIFIED
// because proto3 gives every unset enum field an implicit zero value on
// the wire. "Unknown/malformed" is represented by the ABSENCE of a
// MarketType (std::optional/std::expected returning nullopt at the parse
// boundary - see book_id.hpp's to_symbol_book_id()), the same pattern
// Asset/BaseQuote/BookId/Exchange all use - never an extra enumerator
// inside the type itself.
//
// Adding a market type means adding a case to every switch on this type
// (to_string(BookId), to_string(VenueId), native_symbol(), book_id.hpp's
// fill_wire_book_id()) - each is a real exhaustive switch (no default:),
// not a binary ternary, so a missed case is a compiler warning rather
// than a silent misclassification.
enum class MarketType { Spot, Perp };

// Exchanges this project can source liquidity from. Deliberately just an
// enum, never a Feed type: this header stays usable - and unit-testable -
// without linking simdjson or gRPC at all. Adding an exchange means adding
// a case to native_symbol()/venue_naming()/parse_exchange(), and
// separately wiring its Feed/Policy into server_main.cpp's own dispatch.
enum class Exchange { Binance, Bybit, Okx };

inline std::optional<Exchange> parse_exchange(std::string_view token) {
    if (token == "BINANCE") return Exchange::Binance;
    if (token == "BYBIT") return Exchange::Bybit;
    if (token == "OKX") return Exchange::Okx;
    return std::nullopt;
}

inline std::string_view exchange_name(Exchange exchange) {
    switch (exchange) {
        case Exchange::Binance: return "BINANCE";
        case Exchange::Bybit: return "BYBIT";
        case Exchange::Okx: return "OKX";
    }
    return "";  // unreachable - silences -Wreturn-type on an exhaustive switch
}

// A single currency/coin identity ("BTC", "USDT", ...) - a strong typedef
// over the bare code, not a validated/closed set: this project doesn't
// maintain (and, per split_base_quote()'s own comment, deliberately
// avoids needing) a registry of "known" assets, so any non-empty code is
// a valid Asset. The type exists to stop base/quote from being
// interchangeable with an arbitrary std::string at a call site, and to
// give a future need (display precision, per-asset metadata) a single
// place to land without touching every BaseQuote consumer.
// Equality only, not a full ordering: "less than" has no meaning for a
// currency (any order over asset codes would just be alphabetical, an
// implementation artifact, not a fact about the assets) and nothing here
// puts Asset in an ordered container. Add operator<=> if that changes;
// there's no cost to deferring it until something actually needs it.
struct Asset {
    std::string code;

    bool operator==(const Asset&) const = default;
};

// A base/quote pair split out of a "BASE_QUOTE" spelling - e.g.
// "BTC_USDT" -> {"BTC", "USDT"}. This underscore is this project's own
// naming rule for writing a symbol unambiguously: splitting a concatenated
// ticker like "BTCUSDT" back into base/quote is fundamentally ambiguous
// without a maintained quote-asset dictionary (recovering OKX's own
// dash-separated instId from one, e.g., would require guessing where
// "BTCUSDT" splits into "BTC"/"USDT" without OKX itself having marked the
// boundary), so this pushes that boundary onto whoever writes the symbol
// instead of guessing at it.
struct BaseQuote {
    Asset base;
    Asset quote;

    bool operator==(const BaseQuote&) const = default;
};

// nullopt unless `token` is exactly one non-empty base and one non-empty
// quote separated by a single '_'.
inline std::optional<BaseQuote> split_base_quote(std::string_view token) {
    auto underscore = token.find('_');
    if (underscore == std::string_view::npos || token.find('_', underscore + 1) != std::string_view::npos) {
        return std::nullopt;
    }
    std::string_view base = token.substr(0, underscore);
    std::string_view quote = token.substr(underscore + 1);
    if (base.empty() || quote.empty()) return std::nullopt;
    return BaseQuote{Asset{std::string(base)}, Asset{std::string(quote)}};
}

// A book's identity as a structured value - the type this project uses
// everywhere a book is looked up, keyed, or compared (AggregatorService's
// map, book_subscription.hpp's dedup, the gRPC BookId message a client
// subscribes with - see aggregator.proto). A concatenated string key
// ("BTCUSDT.SPOT") can't be parsed back into (base, quote) without a
// maintained quote-asset dictionary (the same ambiguity split_base_quote()
// describes), so this keeps the base/quote boundary in the type instead.
//
// Equality/hash only, not ordered (same rationale as Asset's own comment):
// nothing here needs to sort books, only to use one as a map/set key.
struct BookId {
    BaseQuote symbol;
    MarketType type;

    bool operator==(const BookId&) const = default;
};

// The human-readable form of a BookId - underscore-separated base/quote,
// same "BASE_QUOTE" spelling split_base_quote() consumes, plus the
// ".SPOT"/".PERP" suffix: "BTC_USDT.SPOT"/"BTC_USDT.PERP". For logs, CLI
// arguments, and error messages only - nothing in this project parses this
// back out of an aggregate string on a hot path any more (see BookId's own
// comment); parse_book_id() below exists purely to accept this same
// spelling back at an input boundary (a CLI argument, a config value),
// exactly once, not to round-trip internal state.
inline std::string to_string(const BookId& id) {
    // Exhaustive switch (see MarketType's own comment on extensibility),
    // not `id.type == MarketType::Spot ? ".SPOT" : ".PERP"` - that ternary
    // would silently mismatch a future third MarketType value instead of
    // getting an unhandled-enumerator warning at the switch itself.
    std::string_view suffix = "";
    switch (id.type) {
        case MarketType::Spot: suffix = ".SPOT"; break;
        case MarketType::Perp: suffix = ".PERP"; break;
    }
    return id.symbol.base.code + "_" + id.symbol.quote.code + std::string(suffix);
}

// The inverse of to_string() above for any BookId whose base/quote codes
// don't themselves contain '_' (Asset's own comment allows any non-empty
// code, but split_base_quote() below only recognizes a single separating
// underscore - a code containing one is already unrepresentable in the
// "BASE_QUOTE" input spelling this project uses everywhere else). Within
// that domain: nullopt unless `token` is "BASE_QUOTE.SPOT" or
// "BASE_QUOTE.PERP" with a valid split_base_quote() base/quote.
inline std::optional<BookId> parse_book_id(std::string_view token) {
    std::string_view suffix = ".SPOT";
    MarketType type = MarketType::Spot;
    if (!token.ends_with(suffix)) {
        suffix = ".PERP";
        type = MarketType::Perp;
        if (!token.ends_with(suffix)) return std::nullopt;
    }
    auto symbol = split_base_quote(token.substr(0, token.size() - suffix.size()));
    if (!symbol) return std::nullopt;
    return BookId{*symbol, type};
}

// A venue's identity as a structured value - which VenueSession/
// AggregateOrderBook actually key their per-venue bookkeeping by (see
// aggregate_order_book.hpp's apply_snapshot()/apply_batch()/
// invalidate_venue()). This is the real "venue" concept this project
// means everywhere else it says "venue": an Exchange scoped to a market
// (e.g. "binance_spot" and "binance_futures" are two different venues
// even though the same company runs both).
//
// Equality/hash only, not ordered (same rationale as BookId's own comment).
struct VenueId {
    Exchange exchange;
    MarketType type;

    bool operator==(const VenueId&) const = default;
};

// The human-readable form of a VenueId - venue-native vocabulary
// ("futures"/"linear"/"swap", each exchange's own term for its derivatives
// market), not the venue-neutral "SPOT"/"PERP" BookId's own to_string()
// uses: a log line should still read the way each exchange's own docs do.
// For display only - nothing constructs a VenueId from a string.
// `exchange_part` is the lowercase log-facing exchange name (distinct from
// exchange_name()'s uppercase config-facing spelling); `spot_suffix`/
// `perp_suffix` are that exchange's own term for each market segment.
//
// Factored out of to_string(VenueId) itself so that function needs only
// one flat switch(MarketType), not a switch(Exchange) nested inside every
// one of its cases. Both switches stay real exhaustive switches (no
// default:) - a future third value of either enum should be a compiler
// warning here.
//
// Declared at namespace scope, not as a function-local struct returned
// via `auto`: a function-local type's name isn't visible to its own
// caller, so `to_string(VenueId)` would be stuck spelling its result as
// `auto` forever, and no other function could ever hold one by name.
struct VenueNaming {
    std::string_view exchange_part;
    std::string_view spot_suffix;
    std::string_view perp_suffix;
};

constexpr VenueNaming venue_naming(Exchange exchange) {
    switch (exchange) {
        case Exchange::Binance: return VenueNaming{"binance", "spot", "futures"};
        case Exchange::Bybit: return VenueNaming{"bybit", "spot", "linear"};
        case Exchange::Okx: return VenueNaming{"okx", "spot", "swap"};
    }
    return VenueNaming{"", "", ""};  // unreachable - silences -Wreturn-type
}

inline std::string to_string(const VenueId& id) {
    VenueNaming naming = venue_naming(id.exchange);
    std::string_view market_part;
    switch (id.type) {
        case MarketType::Spot: market_part = naming.spot_suffix; break;
        case MarketType::Perp: market_part = naming.perp_suffix; break;
    }
    return std::string(naming.exchange_part) + "_" + std::string(market_part);
}

// The wire-format symbol a venue's own Feed subscribes with - an opaque,
// venue-scoped string ("BTCUSDT", "BTC-USDT-SWAP"), not a cross-venue
// identity the way BookId/BaseQuote are. Every consumer of this type
// (exchange/*_feed.hpp's DepthUpdate/SnapshotMessage, VenueSession's
// SymbolRegistry/symbol_syncs_ maps) is already scoped to one VenueId, so
// there's no need for a strong type here to keep two venues' symbols from
// colliding in the same container - a plain alias is the right altitude.
using NativeSymbol = std::string;

// Computes that wire-format spelling for one (exchange, symbol, type)
// triple. Binance/Bybit use the same concatenated spelling for both spot
// and their derivatives market ("BTCUSDT" either way - server_main.cpp's
// registry-building loop relies on this); OKX separates base/quote with
// its own dash and tags a swap with "-SWAP" (matching the instId shape
// OkxFeed::parse_message() reads back verbatim).
inline NativeSymbol native_symbol(Exchange exchange, const BaseQuote& symbol, MarketType type) {
    switch (exchange) {
        case Exchange::Binance:
        case Exchange::Bybit:
            return symbol.base.code + symbol.quote.code;
        case Exchange::Okx: {
            // Exhaustive switch, not `type == MarketType::Perp ? "-SWAP" :
            // ""` - see MarketType's own comment on why every switch over
            // it (not a ternary) is what makes a missed future value a
            // compiler warning instead of a silent wrong suffix.
            std::string_view suffix = "";
            switch (type) {
                case MarketType::Spot: suffix = ""; break;
                case MarketType::Perp: suffix = "-SWAP"; break;
            }
            return symbol.base.code + "-" + symbol.quote.code + std::string(suffix);
        }
    }
    return "";  // unreachable - silences -Wreturn-type on an exhaustive switch
}

}  // namespace bobby::hermeneutic::symbol

// Specialized so BookId can be an std::unordered_map/unordered_set key
// directly (AggregatorService's book registry, book_subscription.hpp's
// dedup set) without every call site supplying its own hasher. Combines the
// three fields with the same boost::hash_combine-style mixing this project
// has no existing helper for; a plain XOR here would collide easily (e.g.
// two Assets with the same code hashing identically, or a swapped
// base/quote producing the same combined hash via XOR's commutativity).
template <>
struct std::hash<bobby::hermeneutic::symbol::BookId> {
    std::size_t operator()(const bobby::hermeneutic::symbol::BookId& id) const noexcept {
        std::size_t seed = std::hash<std::string>{}(id.symbol.base.code);
        auto combine = [&seed](std::size_t value) {
            seed ^= value + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        };
        combine(std::hash<std::string>{}(id.symbol.quote.code));
        combine(std::hash<int>{}(static_cast<int>(id.type)));
        return seed;
    }
};

// Specialized so VenueId can be an std::unordered_map key directly
// (AggregateOrderBook's per-venue book map) without every call site
// supplying its own hasher - same reasoning as std::hash<BookId> above,
// just combining two enums instead of two strings + an enum.
template <>
struct std::hash<bobby::hermeneutic::symbol::VenueId> {
    std::size_t operator()(const bobby::hermeneutic::symbol::VenueId& id) const noexcept {
        std::size_t seed = std::hash<int>{}(static_cast<int>(id.exchange));
        seed ^= std::hash<int>{}(static_cast<int>(id.type)) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        return seed;
    }
};

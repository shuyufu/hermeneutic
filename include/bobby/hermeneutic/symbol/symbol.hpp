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
// so anything that needs to talk about symbols/venues (a CLI config
// parser today, a future client-side config or REST surface tomorrow)
// depends on this alone rather than pulling in gRPC/simdjson/Boost. See
// docs/ingestion_design.md's OKX section for the naming-ambiguity problem
// this exists to solve.
namespace bobby::hermeneutic::symbol {

// Which market a book covers - the suffix on a BookId's human-readable form
// ("BTC_USDT.SPOT"/"BTC_USDT.PERP", see to_string() below) and, transitively,
// which of a venue's feeds native_symbol() below resolves to. Named to match
// aggregator.proto's own MarketType enum (BookId.market) - the two are
// distinct types in distinct namespaces (this one never depends on proto,
// see this file's own header comment; apps/aggregator/book_id.hpp is the
// seam that converts between them), but book_id.hpp already treats them as
// the same concept 1:1, so there's no reason for them to spell it two ways.
enum class MarketType { Spot, Perp };

// Exchanges this project can source liquidity from. Deliberately just an
// enum, never a Feed type: this header (and anything that parses a
// venue list against it, e.g. bobby::hermeneutic::ingestion's subscription
// config) stays usable - and unit-testable - without linking simdjson or
// gRPC at all. Adding a venue means adding a case to native_symbol()/
// venue_id()/parse_venue() below, and separately wiring its Feed/Policy
// into apps/aggregator/server_main.cpp's own dispatch.
enum class Venue { Binance, Bybit, Okx };

inline std::optional<Venue> parse_venue(std::string_view token) {
    if (token == "BINANCE") return Venue::Binance;
    if (token == "BYBIT") return Venue::Bybit;
    if (token == "OKX") return Venue::Okx;
    return std::nullopt;
}

inline std::string_view venue_name(Venue venue) {
    switch (venue) {
        case Venue::Binance: return "BINANCE";
        case Venue::Bybit: return "BYBIT";
        case Venue::Okx: return "OKX";
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
// actually subscribes with - see aggregator.proto). Replaces the earlier
// concatenated string key ("BTCUSDT.SPOT"): that format could not be parsed
// back into (base, quote) without a maintained quote-asset dictionary (the
// same ambiguity split_base_quote()'s own comment describes, and the same
// reason OKX's own instId is never guessed at from one), so an invalid book
// was only ever a runtime NOT_FOUND, never something the type system ruled
// out. See docs/ingestion_design.md's 第10節第10項 for the full history of
// why this replaced book_key().
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
    return id.symbol.base.code + "_" + id.symbol.quote.code + (id.type == MarketType::Spot ? ".SPOT" : ".PERP");
}

// The inverse of to_string() above for any BookId whose base/quote codes
// don't themselves contain '_' (Asset's own comment allows any non-empty
// code, but split_base_quote() below only recognizes a single separating
// underscore - a code containing one is already unrepresentable in the
// "BASE_QUOTE" input spelling this project uses everywhere else, not a new
// limitation this function introduces). Within that domain: nullopt unless
// `token` is "BASE_QUOTE.SPOT" or "BASE_QUOTE.PERP" with a valid
// split_base_quote() base/quote. Unlike the old book_key() this replaces,
// this direction is well-defined precisely because to_string() never
// discards the base/quote boundary the way concatenation did - there is no
// ambiguity left to guess at.
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

// The VenueId string a wired-up VenueSession is tagged with - venue-native
// vocabulary ("futures"/"linear"/"swap", each exchange's own term for its
// derivatives market), not the venue-neutral "SPOT"/"PERP" BookId uses.
// Deliberately not unified with BookId's vocabulary: a log line or
// SymbolBook::apply_batch's per-VenueId bookkeeping should still read the
// way each exchange's own docs do, and BookId only gets to stay
// venue-agnostic because it doesn't have to pick one exchange's term as
// "the" answer.
inline std::string venue_id(Venue venue, MarketType type) {
    switch (venue) {
        case Venue::Binance: return type == MarketType::Spot ? "binance_spot" : "binance_futures";
        case Venue::Bybit: return type == MarketType::Spot ? "bybit_spot" : "bybit_linear";
        case Venue::Okx: return type == MarketType::Spot ? "okx_spot" : "okx_swap";
    }
    return "";  // unreachable - silences -Wreturn-type on an exhaustive switch
}

// The wire-format symbol a venue's own Feed subscribes with. Binance/Bybit
// use the same concatenated spelling for both spot and their derivatives
// market ("BTCUSDT" either way - see apps/aggregator/server_main.cpp's registry-
// building loop, which relies on this); OKX separates base/quote with its
// own dash and tags a swap with "-SWAP" (matching the instId shape
// OkxFeed::parse_message() reads back verbatim - see okx_feed.hpp).
// Unambiguous by construction - `symbol` already carries the base/quote
// boundary the caller supplied, rather than this having to recover it
// from a concatenated string the way split_base_quote()'s own comment
// explains this project avoids.
inline std::string native_symbol(Venue venue, const BaseQuote& symbol, MarketType type) {
    switch (venue) {
        case Venue::Binance:
        case Venue::Bybit:
            return symbol.base.code + symbol.quote.code;
        case Venue::Okx:
            return symbol.base.code + "-" + symbol.quote.code + (type == MarketType::Perp ? "-SWAP" : "");
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

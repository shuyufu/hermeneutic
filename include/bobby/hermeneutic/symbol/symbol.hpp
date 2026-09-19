#pragma once

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

// Which market a book covers - the suffix on a canonical book key
// ("BTCUSDT.SPOT"/"BTCUSDT.PERP") and, transitively, which of a venue's
// feeds native_symbol() below resolves to.
enum class BookType { Spot, Perp };

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
// naming rule for writing a symbol unambiguously (never the canonical
// book key it produces below - see book_key()): splitting a concatenated
// ticker like "BTCUSDT" back into base/quote is fundamentally ambiguous
// without a maintained quote-asset dictionary (recovering OKX's own
// dash-separated instId from one, e.g., would require guessing where
// "BTCUSDT" splits into "BTC"/"USDT" without OKX itself having marked the
// boundary), so this pushes that boundary onto whoever writes the symbol
// instead of guessing at it.
struct BaseQuote {
    Asset base;
    Asset quote;
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

// The canonical, venue-neutral SymbolBook key: concatenated, no
// underscore, exactly what AggregatorService::book()/apps/aggregator/
// main.cpp's book_symbols have always used ("BTCUSDT.SPOT"/"BTCUSDT.PERP").
// Unaffected by the "BASE_QUOTE" input spelling any particular
// caller (e.g. bobby::hermeneutic::ingestion's subscription config) uses:
// a gRPC client already subscribed to "BTCUSDT.PERP" keeps working with it
// unchanged no matter how a symbol was written on the way in.
inline std::string book_key(const BaseQuote& symbol, BookType type) {
    return symbol.base.code + symbol.quote.code + (type == BookType::Spot ? ".SPOT" : ".PERP");
}

// The VenueId string a wired-up VenueSession is tagged with - venue-native
// vocabulary ("futures"/"linear"/"swap", each exchange's own term for its
// derivatives market), not the venue-neutral "SPOT"/"PERP" book_key() uses.
// Deliberately not unified with book_key()'s vocabulary: a log line or
// SymbolBook::apply_batch's per-VenueId bookkeeping should still read the
// way each exchange's own docs do, and book_key() only gets to stay
// venue-agnostic because it doesn't have to pick one exchange's term as
// "the" answer.
inline std::string venue_id(Venue venue, BookType type) {
    switch (venue) {
        case Venue::Binance: return type == BookType::Spot ? "binance_spot" : "binance_futures";
        case Venue::Bybit: return type == BookType::Spot ? "bybit_spot" : "bybit_linear";
        case Venue::Okx: return type == BookType::Spot ? "okx_spot" : "okx_swap";
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
inline std::string native_symbol(Venue venue, const BaseQuote& symbol, BookType type) {
    switch (venue) {
        case Venue::Binance:
        case Venue::Bybit:
            return symbol.base.code + symbol.quote.code;
        case Venue::Okx:
            return symbol.base.code + "-" + symbol.quote.code + (type == BookType::Perp ? "-SWAP" : "");
    }
    return "";  // unreachable - silences -Wreturn-type on an exhaustive switch
}

}  // namespace bobby::hermeneutic::symbol

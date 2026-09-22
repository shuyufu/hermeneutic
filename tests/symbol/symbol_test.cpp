#include "bobby/hermeneutic/symbol/symbol.hpp"

#include <gtest/gtest.h>

#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace bobby::hermeneutic::symbol {
namespace {

TEST(Asset, EqualityIsByCode) {
    EXPECT_EQ(Asset{"BTC"}, Asset{"BTC"});
    EXPECT_NE(Asset{"BTC"}, Asset{"ETH"});
}

TEST(SplitBaseQuote, SplitsOnSingleUnderscore) {
    auto result = split_base_quote("BTC_USDT");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->base.code, "BTC");
    EXPECT_EQ(result->quote.code, "USDT");
}

TEST(SplitBaseQuote, RejectsMissingUnderscore) {
    EXPECT_FALSE(split_base_quote("BTCUSDT").has_value());
}

TEST(SplitBaseQuote, RejectsMultipleUnderscores) {
    EXPECT_FALSE(split_base_quote("BTC_USD_T").has_value());
}

TEST(SplitBaseQuote, RejectsEmptyBaseOrQuote) {
    EXPECT_FALSE(split_base_quote("_USDT").has_value());
    EXPECT_FALSE(split_base_quote("BTC_").has_value());
}

TEST(ParseExchange, RecognizesKnownExchanges) {
    EXPECT_EQ(parse_exchange("BINANCE"), Exchange::Binance);
    EXPECT_EQ(parse_exchange("BYBIT"), Exchange::Bybit);
    EXPECT_EQ(parse_exchange("OKX"), Exchange::Okx);
}

TEST(ParseExchange, RejectsUnknownOrWrongCase) {
    EXPECT_FALSE(parse_exchange("BINANCEX").has_value());
    EXPECT_FALSE(parse_exchange("binance").has_value());
    EXPECT_FALSE(parse_exchange("").has_value());
}

TEST(ExchangeName, RoundTripsThroughParseExchange) {
    EXPECT_EQ(exchange_name(Exchange::Binance), "BINANCE");
    EXPECT_EQ(exchange_name(Exchange::Bybit), "BYBIT");
    EXPECT_EQ(exchange_name(Exchange::Okx), "OKX");
}

TEST(BookIdToString, IsUnderscoreSeparatedWithTypeSuffix) {
    BookId btc_usdt_spot{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot};
    BookId btc_usdt_perp{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Perp};
    EXPECT_EQ(to_string(btc_usdt_spot), "BTC_USDT.SPOT");
    EXPECT_EQ(to_string(btc_usdt_perp), "BTC_USDT.PERP");
}

TEST(ParseBookId, ParsesSpotAndPerp) {
    auto spot = parse_book_id("BTC_USDT.SPOT");
    ASSERT_TRUE(spot.has_value());
    EXPECT_EQ(spot->symbol.base.code, "BTC");
    EXPECT_EQ(spot->symbol.quote.code, "USDT");
    EXPECT_EQ(spot->type, MarketType::Spot);

    auto perp = parse_book_id("BTC_USDT.PERP");
    ASSERT_TRUE(perp.has_value());
    EXPECT_EQ(perp->type, MarketType::Perp);
}

TEST(ParseBookId, RejectsMissingOrUnknownSuffix) {
    EXPECT_FALSE(parse_book_id("BTC_USDT").has_value());
    EXPECT_FALSE(parse_book_id("BTC_USDT.FUTURES").has_value());
}

TEST(ParseBookId, RejectsMalformedBaseQuote) {
    EXPECT_FALSE(parse_book_id("BTCUSDT.SPOT").has_value());  // no underscore
    EXPECT_FALSE(parse_book_id("_USDT.SPOT").has_value());    // empty base
}

TEST(ParseBookId, IsTheExactInverseOfToString) {
    for (BookId id : {BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot},
                       BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Perp},
                       BookId{BaseQuote{{"ETH"}, {"USDC"}}, MarketType::Perp}}) {
        EXPECT_EQ(parse_book_id(to_string(id)), id);
    }
}

TEST(BookIdEquality, ComparesBothBaseQuoteAndType) {
    BookId btc_usdt_spot{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot};
    EXPECT_EQ(btc_usdt_spot, (BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot}));
    EXPECT_NE(btc_usdt_spot, (BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Perp}));
    EXPECT_NE(btc_usdt_spot, (BookId{BaseQuote{{"ETH"}, {"USDT"}}, MarketType::Spot}));
}

// Exercises usability as an unordered_set key (compiles/links only if
// std::hash<BookId> exists) and correct membership. unordered_set falls
// back to operator== on a hash collision, so this does not by itself prove
// anything about std::hash<BookId>'s collision rate - it proves the type is
// usable as a key at all, which is the actual requirement here.
TEST(BookIdHash, UsableAsUnorderedSetKey) {
    std::unordered_set<BookId> seen;
    EXPECT_TRUE(seen.insert(BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot}).second);
    EXPECT_TRUE(seen.insert(BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Perp}).second);
    EXPECT_TRUE(seen.insert(BookId{BaseQuote{{"BT"}, {"CUSDT"}}, MarketType::Spot}).second);
    EXPECT_FALSE(seen.insert(BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot}).second);
    EXPECT_EQ(seen.size(), 3u);
}

TEST(VenueIdString, UsesEachExchangesOwnDerivativesTerm) {
    EXPECT_EQ(to_string(VenueId{Exchange::Binance, MarketType::Spot}), "binance_spot");
    EXPECT_EQ(to_string(VenueId{Exchange::Binance, MarketType::Perp}), "binance_futures");
    EXPECT_EQ(to_string(VenueId{Exchange::Bybit, MarketType::Spot}), "bybit_spot");
    EXPECT_EQ(to_string(VenueId{Exchange::Bybit, MarketType::Perp}), "bybit_linear");
    EXPECT_EQ(to_string(VenueId{Exchange::Okx, MarketType::Spot}), "okx_spot");
    EXPECT_EQ(to_string(VenueId{Exchange::Okx, MarketType::Perp}), "okx_swap");
}

// Same rationale as BookIdHash.UsableAsUnorderedSetKey above - proves
// std::hash<VenueId> makes VenueId usable as a hash-container key at all,
// which is exactly what AggregateOrderBook's venues_ (std::unordered_map<
// VenueId, L2OrderBook>) now requires.
TEST(VenueIdHash, UsableAsUnorderedSetKey) {
    std::unordered_set<VenueId> seen;
    EXPECT_TRUE(seen.insert(VenueId{Exchange::Binance, MarketType::Spot}).second);
    EXPECT_TRUE(seen.insert(VenueId{Exchange::Binance, MarketType::Perp}).second);
    EXPECT_TRUE(seen.insert(VenueId{Exchange::Okx, MarketType::Spot}).second);
    EXPECT_FALSE(seen.insert(VenueId{Exchange::Binance, MarketType::Spot}).second);
    EXPECT_EQ(seen.size(), 3u);
}

TEST(NativeSymbol, BinanceAndBybitAreConcatenatedRegardlessOfType) {
    BaseQuote btc_usdt{{"BTC"}, {"USDT"}};
    EXPECT_EQ(native_symbol(Exchange::Binance, btc_usdt, MarketType::Spot), "BTCUSDT");
    EXPECT_EQ(native_symbol(Exchange::Binance, btc_usdt, MarketType::Perp), "BTCUSDT");
    EXPECT_EQ(native_symbol(Exchange::Bybit, btc_usdt, MarketType::Spot), "BTCUSDT");
    EXPECT_EQ(native_symbol(Exchange::Bybit, btc_usdt, MarketType::Perp), "BTCUSDT");
}

TEST(NativeSymbol, OkxUsesDashAndSwapSuffix) {
    BaseQuote btc_usdt{{"BTC"}, {"USDT"}};
    EXPECT_EQ(native_symbol(Exchange::Okx, btc_usdt, MarketType::Spot), "BTC-USDT");
    EXPECT_EQ(native_symbol(Exchange::Okx, btc_usdt, MarketType::Perp), "BTC-USDT-SWAP");
}

TEST(VenuesMissingFrom, EmptyWhenEveryCandidateIsWired) {
    VenueId binance_spot{Exchange::Binance, MarketType::Spot};
    VenueId okx_perp{Exchange::Okx, MarketType::Perp};
    EXPECT_TRUE(venues_missing_from(std::vector<VenueId>{binance_spot, okx_perp}, {binance_spot, okx_perp}).empty());
}

TEST(VenuesMissingFrom, ReportsCandidatesAbsentFromWired) {
    VenueId binance_spot{Exchange::Binance, MarketType::Spot};
    VenueId okx_perp{Exchange::Okx, MarketType::Perp};
    auto missing = venues_missing_from(std::vector<VenueId>{binance_spot, okx_perp}, {binance_spot});
    ASSERT_EQ(missing.size(), 1u);
    EXPECT_EQ(missing[0], to_string(okx_perp));
}

TEST(VenuesMissingFrom, EmptyWhenCandidatesAreEmpty) {
    VenueId binance_spot{Exchange::Binance, MarketType::Spot};
    EXPECT_TRUE(venues_missing_from(std::vector<VenueId>{}, {binance_spot}).empty());
}

// Confirms the templated venues_missing_from() actually accepts a lazy
// range like std::views::keys(), not just a materialized std::vector -
// server_main.cpp's whole reason for taking a range instead of requiring
// each call site to first copy a map's keys into one.
TEST(VenuesMissingFrom, AcceptsAKeysViewDirectly) {
    VenueId binance_spot{Exchange::Binance, MarketType::Spot};
    VenueId okx_perp{Exchange::Okx, MarketType::Perp};
    std::unordered_map<VenueId, int> configured{{binance_spot, 1}, {okx_perp, 2}};
    auto missing = venues_missing_from(std::views::keys(configured), {binance_spot});
    ASSERT_EQ(missing.size(), 1u);
    EXPECT_EQ(missing[0], to_string(okx_perp));
}

}  // namespace
}  // namespace bobby::hermeneutic::symbol

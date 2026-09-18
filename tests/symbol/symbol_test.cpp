#include "bobby/hermeneutic/symbol/symbol.hpp"

#include <gtest/gtest.h>

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

TEST(ParseVenue, RecognizesKnownVenues) {
    EXPECT_EQ(parse_venue("BINANCE"), Venue::Binance);
    EXPECT_EQ(parse_venue("BYBIT"), Venue::Bybit);
    EXPECT_EQ(parse_venue("OKX"), Venue::Okx);
}

TEST(ParseVenue, RejectsUnknownOrWrongCase) {
    EXPECT_FALSE(parse_venue("BINANCEX").has_value());
    EXPECT_FALSE(parse_venue("binance").has_value());
    EXPECT_FALSE(parse_venue("").has_value());
}

TEST(VenueName, RoundTripsThroughParseVenue) {
    EXPECT_EQ(venue_name(Venue::Binance), "BINANCE");
    EXPECT_EQ(venue_name(Venue::Bybit), "BYBIT");
    EXPECT_EQ(venue_name(Venue::Okx), "OKX");
}

TEST(BookKey, IsConcatenatedWithTypeSuffix) {
    BaseQuote btc_usdt{"BTC", "USDT"};
    EXPECT_EQ(book_key(btc_usdt, BookType::Spot), "BTCUSDT.SPOT");
    EXPECT_EQ(book_key(btc_usdt, BookType::Perp), "BTCUSDT.PERP");
}

TEST(VenueIdString, UsesEachExchangesOwnDerivativesTerm) {
    EXPECT_EQ(venue_id(Venue::Binance, BookType::Spot), "binance_spot");
    EXPECT_EQ(venue_id(Venue::Binance, BookType::Perp), "binance_futures");
    EXPECT_EQ(venue_id(Venue::Bybit, BookType::Spot), "bybit_spot");
    EXPECT_EQ(venue_id(Venue::Bybit, BookType::Perp), "bybit_linear");
    EXPECT_EQ(venue_id(Venue::Okx, BookType::Spot), "okx_spot");
    EXPECT_EQ(venue_id(Venue::Okx, BookType::Perp), "okx_swap");
}

TEST(NativeSymbol, BinanceAndBybitAreConcatenatedRegardlessOfType) {
    BaseQuote btc_usdt{"BTC", "USDT"};
    EXPECT_EQ(native_symbol(Venue::Binance, btc_usdt, BookType::Spot), "BTCUSDT");
    EXPECT_EQ(native_symbol(Venue::Binance, btc_usdt, BookType::Perp), "BTCUSDT");
    EXPECT_EQ(native_symbol(Venue::Bybit, btc_usdt, BookType::Spot), "BTCUSDT");
    EXPECT_EQ(native_symbol(Venue::Bybit, btc_usdt, BookType::Perp), "BTCUSDT");
}

TEST(NativeSymbol, OkxUsesDashAndSwapSuffix) {
    BaseQuote btc_usdt{"BTC", "USDT"};
    EXPECT_EQ(native_symbol(Venue::Okx, btc_usdt, BookType::Spot), "BTC-USDT");
    EXPECT_EQ(native_symbol(Venue::Okx, btc_usdt, BookType::Perp), "BTC-USDT-SWAP");
}

}  // namespace
}  // namespace bobby::hermeneutic::symbol

#include "bobby/hermeneutic/ingestion/book_subscription.hpp"

#include <gtest/gtest.h>

namespace bobby::hermeneutic::ingestion {
namespace {

TEST(SplitBaseQuote, SplitsOnSingleUnderscore) {
    auto result = split_base_quote("BTC_USDT");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->base, "BTC");
    EXPECT_EQ(result->quote, "USDT");
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

TEST(ParseBookSubscriptions, ParsesTheMotivatingExample) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[BINANCE,OKX,BYBIT];BTC_USDT.PERP=[BINANCE,OKX]");
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->size(), 5u);

    EXPECT_EQ((*result)[0].venue, Venue::Binance);
    EXPECT_EQ((*result)[0].type, BookType::Spot);
    EXPECT_EQ((*result)[0].book_key, "BTCUSDT.SPOT");
    EXPECT_EQ((*result)[0].native_symbol, "BTCUSDT");

    EXPECT_EQ((*result)[1].venue, Venue::Okx);
    EXPECT_EQ((*result)[1].type, BookType::Spot);
    EXPECT_EQ((*result)[1].book_key, "BTCUSDT.SPOT");
    EXPECT_EQ((*result)[1].native_symbol, "BTC-USDT");

    EXPECT_EQ((*result)[2].venue, Venue::Bybit);
    EXPECT_EQ((*result)[2].book_key, "BTCUSDT.SPOT");
    EXPECT_EQ((*result)[2].native_symbol, "BTCUSDT");

    EXPECT_EQ((*result)[3].venue, Venue::Binance);
    EXPECT_EQ((*result)[3].type, BookType::Perp);
    EXPECT_EQ((*result)[3].book_key, "BTCUSDT.PERP");
    EXPECT_EQ((*result)[3].native_symbol, "BTCUSDT");

    EXPECT_EQ((*result)[4].venue, Venue::Okx);
    EXPECT_EQ((*result)[4].type, BookType::Perp);
    EXPECT_EQ((*result)[4].book_key, "BTCUSDT.PERP");
    EXPECT_EQ((*result)[4].native_symbol, "BTC-USDT-SWAP");
}

TEST(ParseBookSubscriptions, HandlesMultipleSymbols) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[BINANCE];ETH_USDT.PERP=[BYBIT,OKX]");
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->size(), 3u);
    EXPECT_EQ((*result)[0].book_key, "BTCUSDT.SPOT");
    EXPECT_EQ((*result)[1].book_key, "ETHUSDT.PERP");
    EXPECT_EQ((*result)[2].book_key, "ETHUSDT.PERP");
}

TEST(ParseBookSubscriptions, TrimsWhitespaceAroundEveryToken) {
    auto result = parse_book_subscriptions("  BTC_USDT.SPOT = [ BINANCE , OKX ]  ");
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->size(), 2u);
    EXPECT_EQ((*result)[0].venue, Venue::Binance);
    EXPECT_EQ((*result)[1].venue, Venue::Okx);
}

TEST(ParseBookSubscriptions, DropsATrailingSemicolonLikeATrailingComma) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[BINANCE];");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->size(), 1u);
}

TEST(ParseBookSubscriptions, RejectsEmptyConfig) {
    EXPECT_FALSE(parse_book_subscriptions("").has_value());
    EXPECT_FALSE(parse_book_subscriptions("   ").has_value());
}

TEST(ParseBookSubscriptions, RejectsMissingEquals) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT[BINANCE]");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("missing '='"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsUnknownBookType) {
    auto result = parse_book_subscriptions("BTC_USDT.FUTURES=[BINANCE]");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("unknown book type"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsSymbolWithoutUnderscore) {
    auto result = parse_book_subscriptions("BTCUSDT.SPOT=[BINANCE]");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("malformed symbol"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsDuplicateBookKey) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[BINANCE];BTC_USDT.SPOT=[OKX]");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("configured more than once"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsMissingBrackets) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=BINANCE");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("malformed venue list"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsEmptyVenueList) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[]");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("empty venue list"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsUnknownVenue) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[BINANCE,DERIBIT]");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("unknown venue \"DERIBIT\""), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsDuplicateVenueWithinOneBook) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[BINANCE,BINANCE]");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("listed twice"), std::string::npos);
}

// The same venue may legitimately serve both a symbol's spot book and its
// perp book - that's two different (venue, type) pairs, not a duplicate.
TEST(ParseBookSubscriptions, SameVenueAcrossDifferentBooksIsNotADuplicate) {
    auto result = parse_book_subscriptions("BTC_USDT.SPOT=[BINANCE];BTC_USDT.PERP=[BINANCE]");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->size(), 2u);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

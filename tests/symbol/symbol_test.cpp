#include "bobby/hermeneutic/symbol/symbol.hpp"

#include <gtest/gtest.h>

#include <unordered_set>

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

TEST(BookIdToString, IsUnderscoreSeparatedWithTypeSuffix) {
    BookId btc_usdt_spot{BaseQuote{"BTC", "USDT"}, MarketType::Spot};
    BookId btc_usdt_perp{BaseQuote{"BTC", "USDT"}, MarketType::Perp};
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
    for (BookId id : {BookId{BaseQuote{"BTC", "USDT"}, MarketType::Spot},
                       BookId{BaseQuote{"BTC", "USDT"}, MarketType::Perp},
                       BookId{BaseQuote{"ETH", "USDC"}, MarketType::Perp}}) {
        EXPECT_EQ(parse_book_id(to_string(id)), id);
    }
}

TEST(BookIdEquality, ComparesBothBaseQuoteAndType) {
    BookId btc_usdt_spot{BaseQuote{"BTC", "USDT"}, MarketType::Spot};
    EXPECT_EQ(btc_usdt_spot, (BookId{BaseQuote{"BTC", "USDT"}, MarketType::Spot}));
    EXPECT_NE(btc_usdt_spot, (BookId{BaseQuote{"BTC", "USDT"}, MarketType::Perp}));
    EXPECT_NE(btc_usdt_spot, (BookId{BaseQuote{"ETH", "USDT"}, MarketType::Spot}));
}

// Exercises usability as an unordered_set key (compiles/links only if
// std::hash<BookId> exists) and correct membership. unordered_set falls
// back to operator== on a hash collision, so this does not by itself prove
// anything about std::hash<BookId>'s collision rate - it proves the type is
// usable as a key at all, which is the actual requirement here.
TEST(BookIdHash, UsableAsUnorderedSetKey) {
    std::unordered_set<BookId> seen;
    EXPECT_TRUE(seen.insert(BookId{BaseQuote{"BTC", "USDT"}, MarketType::Spot}).second);
    EXPECT_TRUE(seen.insert(BookId{BaseQuote{"BTC", "USDT"}, MarketType::Perp}).second);
    EXPECT_TRUE(seen.insert(BookId{BaseQuote{"BT", "CUSDT"}, MarketType::Spot}).second);
    EXPECT_FALSE(seen.insert(BookId{BaseQuote{"BTC", "USDT"}, MarketType::Spot}).second);
    EXPECT_EQ(seen.size(), 3u);
}

TEST(VenueIdString, UsesEachExchangesOwnDerivativesTerm) {
    EXPECT_EQ(venue_id(Venue::Binance, MarketType::Spot), "binance_spot");
    EXPECT_EQ(venue_id(Venue::Binance, MarketType::Perp), "binance_futures");
    EXPECT_EQ(venue_id(Venue::Bybit, MarketType::Spot), "bybit_spot");
    EXPECT_EQ(venue_id(Venue::Bybit, MarketType::Perp), "bybit_linear");
    EXPECT_EQ(venue_id(Venue::Okx, MarketType::Spot), "okx_spot");
    EXPECT_EQ(venue_id(Venue::Okx, MarketType::Perp), "okx_swap");
}

TEST(NativeSymbol, BinanceAndBybitAreConcatenatedRegardlessOfType) {
    BaseQuote btc_usdt{"BTC", "USDT"};
    EXPECT_EQ(native_symbol(Venue::Binance, btc_usdt, MarketType::Spot), "BTCUSDT");
    EXPECT_EQ(native_symbol(Venue::Binance, btc_usdt, MarketType::Perp), "BTCUSDT");
    EXPECT_EQ(native_symbol(Venue::Bybit, btc_usdt, MarketType::Spot), "BTCUSDT");
    EXPECT_EQ(native_symbol(Venue::Bybit, btc_usdt, MarketType::Perp), "BTCUSDT");
}

TEST(NativeSymbol, OkxUsesDashAndSwapSuffix) {
    BaseQuote btc_usdt{"BTC", "USDT"};
    EXPECT_EQ(native_symbol(Venue::Okx, btc_usdt, MarketType::Spot), "BTC-USDT");
    EXPECT_EQ(native_symbol(Venue::Okx, btc_usdt, MarketType::Perp), "BTC-USDT-SWAP");
}

}  // namespace
}  // namespace bobby::hermeneutic::symbol

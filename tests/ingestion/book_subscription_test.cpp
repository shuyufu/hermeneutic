#include "bobby/hermeneutic/ingestion/book_subscription.hpp"

#include <gtest/gtest.h>

#include <fstream>

namespace bobby::hermeneutic::ingestion {
namespace {

using bobby::hermeneutic::symbol::BookType;
using bobby::hermeneutic::symbol::Venue;

TEST(ParseBookSubscriptions, ParsesTheMotivatingExample) {
    auto result = parse_book_subscriptions(R"({
        "books": [
            {"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE", "OKX", "BYBIT"]},
            {"symbol": "BTC_USDT", "type": "PERP", "venues": ["BINANCE", "OKX"]}
        ]
    })");
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
    auto result = parse_book_subscriptions(R"({
        "books": [
            {"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE"]},
            {"symbol": "ETH_USDT", "type": "PERP", "venues": ["BYBIT", "OKX"]}
        ]
    })");
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->size(), 3u);
    EXPECT_EQ((*result)[0].book_key, "BTCUSDT.SPOT");
    EXPECT_EQ((*result)[1].book_key, "ETHUSDT.PERP");
    EXPECT_EQ((*result)[2].book_key, "ETHUSDT.PERP");
}

TEST(ParseBookSubscriptions, RejectsMalformedJson) {
    auto result = parse_book_subscriptions("{not json");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("malformed subscription config"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsMissingBooksKey) {
    auto result = parse_book_subscriptions(R"({"symbols": []})");
    ASSERT_FALSE(result.has_value());
}

TEST(ParseBookSubscriptions, RejectsEntryMissingFields) {
    auto result = parse_book_subscriptions(R"({"books": [{"symbol": "BTC_USDT"}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("needs a \"symbol\""), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsUnknownBookType) {
    auto result = parse_book_subscriptions(
        R"({"books": [{"symbol": "BTC_USDT", "type": "FUTURES", "venues": ["BINANCE"]}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("unknown book type"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsSymbolWithoutUnderscore) {
    auto result =
        parse_book_subscriptions(R"({"books": [{"symbol": "BTCUSDT", "type": "SPOT", "venues": ["BINANCE"]}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("malformed symbol"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsDuplicateBookKey) {
    auto result = parse_book_subscriptions(R"({
        "books": [
            {"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE"]},
            {"symbol": "BTC_USDT", "type": "SPOT", "venues": ["OKX"]}
        ]
    })");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("configured more than once"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsEmptyVenueList) {
    auto result =
        parse_book_subscriptions(R"({"books": [{"symbol": "BTC_USDT", "type": "SPOT", "venues": []}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("empty venue list"), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsUnknownVenue) {
    auto result = parse_book_subscriptions(
        R"({"books": [{"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE", "DERIBIT"]}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("unknown venue \"DERIBIT\""), std::string::npos);
}

TEST(ParseBookSubscriptions, RejectsDuplicateVenueWithinOneBook) {
    auto result = parse_book_subscriptions(
        R"({"books": [{"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE", "BINANCE"]}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("listed twice"), std::string::npos);
}

// The same venue may legitimately serve both a symbol's spot book and its
// perp book - that's two different (venue, type) pairs, not a duplicate.
TEST(ParseBookSubscriptions, SameVenueAcrossDifferentBooksIsNotADuplicate) {
    auto result = parse_book_subscriptions(R"({
        "books": [
            {"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE"]},
            {"symbol": "BTC_USDT", "type": "PERP", "venues": ["BINANCE"]}
        ]
    })");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->size(), 2u);
}

TEST(ParseBookSubscriptions, RejectsEmptyBooksList) {
    auto result = parse_book_subscriptions(R"({"books": []})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("no book subscriptions configured"), std::string::npos);
}

TEST(LoadBookSubscriptions, ReadsAndParsesARealFile) {
    std::string path = testing::TempDir() + "book_subscription_test_valid.json";
    {
        std::ofstream file(path);
        file << R"({"books": [{"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE"]}]})";
    }

    auto result = load_book_subscriptions(path);
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->size(), 1u);
    EXPECT_EQ((*result)[0].book_key, "BTCUSDT.SPOT");
}

TEST(LoadBookSubscriptions, RejectsAMissingFile) {
    auto result = load_book_subscriptions(testing::TempDir() + "book_subscription_test_does_not_exist.json");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("failed to read subscription config"), std::string::npos);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

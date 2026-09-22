#include "bobby/hermeneutic/ingestion/book_subscription.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <fstream>

namespace bobby::hermeneutic::ingestion {
namespace {

using bobby::hermeneutic::symbol::Exchange;
using bobby::hermeneutic::symbol::MarketType;
using bobby::hermeneutic::symbol::to_string;

TEST(ParseBookSubscriptions, ParsesTheMotivatingExample) {
    auto result = parse_book_subscriptions(R"({
        "books": [
            {"symbol": "BTC_USDT", "type": "SPOT", "venues": ["BINANCE", "OKX", "BYBIT"]},
            {"symbol": "BTC_USDT", "type": "PERP", "venues": ["BINANCE", "OKX"]}
        ]
    })");
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->size(), 5u);

    EXPECT_EQ((*result)[0].venue_id.exchange, Exchange::Binance);
    EXPECT_EQ((*result)[0].venue_id.type, MarketType::Spot);
    EXPECT_EQ(to_string((*result)[0].book_id), "BTC_USDT.SPOT");
    EXPECT_EQ((*result)[0].native_symbol, "BTCUSDT");

    EXPECT_EQ((*result)[1].venue_id.exchange, Exchange::Okx);
    EXPECT_EQ((*result)[1].venue_id.type, MarketType::Spot);
    EXPECT_EQ(to_string((*result)[1].book_id), "BTC_USDT.SPOT");
    EXPECT_EQ((*result)[1].native_symbol, "BTC-USDT");

    EXPECT_EQ((*result)[2].venue_id.exchange, Exchange::Bybit);
    EXPECT_EQ(to_string((*result)[2].book_id), "BTC_USDT.SPOT");
    EXPECT_EQ((*result)[2].native_symbol, "BTCUSDT");

    EXPECT_EQ((*result)[3].venue_id.exchange, Exchange::Binance);
    EXPECT_EQ((*result)[3].venue_id.type, MarketType::Perp);
    EXPECT_EQ(to_string((*result)[3].book_id), "BTC_USDT.PERP");
    EXPECT_EQ((*result)[3].native_symbol, "BTCUSDT");

    EXPECT_EQ((*result)[4].venue_id.exchange, Exchange::Okx);
    EXPECT_EQ((*result)[4].venue_id.type, MarketType::Perp);
    EXPECT_EQ(to_string((*result)[4].book_id), "BTC_USDT.PERP");
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
    EXPECT_EQ(to_string((*result)[0].book_id), "BTC_USDT.SPOT");
    EXPECT_EQ(to_string((*result)[1].book_id), "ETH_USDT.PERP");
    EXPECT_EQ(to_string((*result)[2].book_id), "ETH_USDT.PERP");
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

TEST(ParseBookSubscriptions, RejectsUnknownMarketType) {
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
    EXPECT_EQ(to_string((*result)[0].book_id), "BTC_USDT.SPOT");
}

TEST(LoadBookSubscriptions, RejectsAMissingFile) {
    auto result = load_book_subscriptions(testing::TempDir() + "book_subscription_test_does_not_exist.json");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("failed to read subscription config"), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, DefaultsWhenFieldsAreAbsent) {
    auto result = parse_idle_timeout_config(R"({"books": []})");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->default_timeout, kDefaultIdleTimeout);
    EXPECT_TRUE(result->overrides.empty());
    EXPECT_EQ(result->for_venue(VenueId{Exchange::Okx, MarketType::Spot}), kDefaultIdleTimeout);
}

TEST(ParseIdleTimeoutConfig, ParsesCustomDefault) {
    auto result = parse_idle_timeout_config(R"({"idle_timeout_seconds": 45})");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->default_timeout, std::chrono::seconds(45));
}

TEST(ParseIdleTimeoutConfig, RejectsZeroOrNegativeDefault) {
    for (auto* json : {R"({"idle_timeout_seconds": 0})", R"({"idle_timeout_seconds": -5})"}) {
        auto result = parse_idle_timeout_config(json);
        ASSERT_FALSE(result.has_value()) << json;
        EXPECT_NE(result.error().find("positive"), std::string::npos) << json;
    }
}

// A wrong-typed (not absent, but present-and-wrong) value must name
// "idle_timeout_seconds" specifically, not fall through to the generic
// "malformed subscription config" message.
TEST(ParseIdleTimeoutConfig, RejectsWrongTypedDefault) {
    auto result = parse_idle_timeout_config(R"({"idle_timeout_seconds": "30"})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("\"idle_timeout_seconds\""), std::string::npos);
}

// Guards against silently overflowing when this value is later converted
// to websocket::stream_base::timeout::duration (a nanosecond-resolution,
// int64 std::chrono::steady_clock::duration) - see idle_timeout.hpp's own
// comment on kMaxIdleTimeout for why an unbounded value here would be a
// real bug, not just an unreasonable one.
TEST(ParseIdleTimeoutConfig, RejectsDefaultAboveMax) {
    auto result = parse_idle_timeout_config(R"({"idle_timeout_seconds": 999999999999})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("no more than"), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, AcceptsDefaultExactlyAtMax) {
    auto result = parse_idle_timeout_config(R"({"idle_timeout_seconds": 86400})");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->default_timeout, kMaxIdleTimeout);
}

TEST(ParseIdleTimeoutConfig, ParsesOverridesAndFeedsForVenue) {
    auto result = parse_idle_timeout_config(R"({
        "idle_timeout_seconds": 30,
        "venue_idle_timeout_overrides": [
            {"venue": "OKX", "type": "SPOT", "seconds": 120},
            {"venue": "OKX", "type": "PERP", "seconds": 90}
        ]
    })");
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_EQ(result->overrides.size(), 2u);
    EXPECT_EQ(result->for_venue(VenueId{Exchange::Okx, MarketType::Spot}), std::chrono::seconds(120));
    EXPECT_EQ(result->for_venue(VenueId{Exchange::Okx, MarketType::Perp}), std::chrono::seconds(90));
    // A venue with no override still falls back to the configured default.
    EXPECT_EQ(result->for_venue(VenueId{Exchange::Binance, MarketType::Spot}), std::chrono::seconds(30));
}

TEST(ParseIdleTimeoutConfig, RejectsMalformedOverrideEntry) {
    auto result = parse_idle_timeout_config(R"({"venue_idle_timeout_overrides": [{"venue": "OKX"}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("each entry in \"venue_idle_timeout_overrides\""), std::string::npos);
}

// A non-object entry (get_object() itself throws INCORRECT_TYPE) must be
// reported the same way as any other malformed entry, not escape to the
// generic "malformed subscription config" message - see
// parse_idle_timeout_config()'s own comment on why get_object() has to be
// inside this entry's try/catch, not before it.
TEST(ParseIdleTimeoutConfig, RejectsNonObjectOverrideEntry) {
    auto result = parse_idle_timeout_config(R"({"venue_idle_timeout_overrides": ["OKX"]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("each entry in \"venue_idle_timeout_overrides\""), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, RejectsZeroOrNegativeOverrideSeconds) {
    auto result = parse_idle_timeout_config(
        R"({"venue_idle_timeout_overrides": [{"venue": "OKX", "type": "SPOT", "seconds": 0}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("positive"), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, RejectsOverrideSecondsAboveMax) {
    auto result = parse_idle_timeout_config(
        R"({"venue_idle_timeout_overrides": [{"venue": "OKX", "type": "SPOT", "seconds": 999999999999}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("no more than"), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, RejectsUnknownVenueInOverride) {
    auto result = parse_idle_timeout_config(
        R"({"venue_idle_timeout_overrides": [{"venue": "DERIBIT", "type": "SPOT", "seconds": 60}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("unknown venue \"DERIBIT\""), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, RejectsUnknownTypeInOverride) {
    auto result = parse_idle_timeout_config(
        R"({"venue_idle_timeout_overrides": [{"venue": "OKX", "type": "FUTURES", "seconds": 60}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("unknown type \"FUTURES\""), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, RejectsDuplicateOverrideForTheSameVenue) {
    auto result = parse_idle_timeout_config(R"({
        "venue_idle_timeout_overrides": [
            {"venue": "OKX", "type": "SPOT", "seconds": 60},
            {"venue": "OKX", "type": "SPOT", "seconds": 90}
        ]
    })");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("more than once"), std::string::npos);
}

TEST(ParseIdleTimeoutConfig, RejectsMalformedJson) {
    auto result = parse_idle_timeout_config("{not json");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("malformed subscription config"), std::string::npos);
}

TEST(LoadIdleTimeoutConfig, ReadsAndParsesARealFile) {
    std::string path = testing::TempDir() + "idle_timeout_config_test_valid.json";
    {
        std::ofstream file(path);
        file << R"({"idle_timeout_seconds": 45})";
    }

    auto result = load_idle_timeout_config(path);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->default_timeout, std::chrono::seconds(45));
}

TEST(LoadIdleTimeoutConfig, RejectsAMissingFile) {
    auto result = load_idle_timeout_config(testing::TempDir() + "idle_timeout_config_test_does_not_exist.json");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("failed to read subscription config"), std::string::npos);
}

TEST(ParseIoThreads, DefaultsToOneWhenAbsent) {
    auto result = parse_io_threads(R"({"books": []})");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(*result, 1);
}

TEST(ParseIoThreads, ParsesCustomValue) {
    auto result = parse_io_threads(R"({"io_threads": 4})");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(*result, 4);
}

TEST(ParseIoThreads, RejectsZeroOrNegative) {
    for (auto* json : {R"({"io_threads": 0})", R"({"io_threads": -3})"}) {
        auto result = parse_io_threads(json);
        ASSERT_FALSE(result.has_value()) << json;
        EXPECT_NE(result.error().find("positive"), std::string::npos) << json;
    }
}

TEST(ParseIoThreads, RejectsAboveMax) {
    auto result = parse_io_threads(R"({"io_threads": 65})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("no more than"), std::string::npos);
}

TEST(ParseIoThreads, AcceptsExactlyAtMax) {
    auto result = parse_io_threads(R"({"io_threads": 64})");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(*result, 64);
}

// A wrong-typed (not absent, but present-and-wrong) value must name
// "io_threads" specifically, not fall through to the generic "malformed
// subscription config" message - same reasoning as
// ParseIdleTimeoutConfig.RejectsWrongTypedDefault above.
TEST(ParseIoThreads, RejectsWrongTypedValue) {
    auto result = parse_io_threads(R"({"io_threads": "4"})");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("\"io_threads\""), std::string::npos);
}

TEST(ParseIoThreads, RejectsMalformedJson) {
    auto result = parse_io_threads("{not json");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("malformed subscription config"), std::string::npos);
}

TEST(LoadIoThreads, ReadsAndParsesARealFile) {
    std::string path = testing::TempDir() + "io_threads_test_valid.json";
    {
        std::ofstream file(path);
        file << R"({"io_threads": 3})";
    }

    auto result = load_io_threads(path);
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(*result, 3);
}

TEST(LoadIoThreads, RejectsAMissingFile) {
    auto result = load_io_threads(testing::TempDir() + "io_threads_test_does_not_exist.json");
    ASSERT_FALSE(result.has_value());
    EXPECT_NE(result.error().find("failed to read subscription config"), std::string::npos);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

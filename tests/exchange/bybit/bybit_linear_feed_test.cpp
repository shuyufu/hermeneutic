#include "bobby/hermeneutic/exchange/bybit/bybit_feed.hpp"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace bobby::hermeneutic::ingestion {
namespace {

TEST(BybitLinearFeedTest, ParsesRealSnapshotMessageExample) {
    // Captured live from wss://stream.bybit.com/v5/public/linear,
    // orderbook.50.BTCUSDT (2026-09-18), truncated to a few levels.
    constexpr std::string_view kText =
        R"({"topic":"orderbook.50.BTCUSDT","type":"snapshot","ts":1789716283128,)"
        R"("data":{"s":"BTCUSDT","b":[["77796.10","3.875"],["77796.00","0.028"]],)"
        R"("a":[["77796.20","1.073"],["77798.10","0.287"]],"u":158434480,"seq":811957851583},)"
        R"("cts":1789716283126})";

    BybitLinearFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<SnapshotMessage>(**result));

    const SnapshotMessage& snapshot = std::get<SnapshotMessage>(**result);
    EXPECT_EQ(snapshot.symbol, "BTCUSDT");
    EXPECT_EQ(snapshot.last_update_id, 158434480u);
    ASSERT_EQ(snapshot.bids.size(), 2u);
    EXPECT_EQ(snapshot.bids[0].first, Price(77796.10));
    EXPECT_EQ(snapshot.bids[0].second, Size(3.875));
    ASSERT_EQ(snapshot.asks.size(), 2u);
    EXPECT_EQ(snapshot.asks[0].first, Price(77796.20));
    EXPECT_EQ(snapshot.asks[0].second, Size(1.073));
}

TEST(BybitLinearFeedTest, ParsesRealDeltaMessageExampleWithSingleUpdateId) {
    // Captured live from the same session, the message right after the
    // snapshot above (u = 158434481, exactly the snapshot's u + 1).
    constexpr std::string_view kText =
        R"({"topic":"orderbook.50.BTCUSDT","type":"delta","ts":1789716121648,)"
        R"("data":{"s":"BTCUSDT","b":[],"a":[["77805.60","0.372"],["77808.90","0"],)"
        R"(["77809.60","0.025"],["77810.00","0.008"]],"u":158434481,"seq":811957851647},)"
        R"("cts":1789716121644})";

    BybitLinearFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<DepthUpdate>(**result));

    const DepthUpdate& update = std::get<DepthUpdate>(**result);
    EXPECT_EQ(update.symbol, "BTCUSDT");
    // Bybit carries one update id, not a first_id/final_id range - both
    // fields get the same value (see BybitLinearFeed::parse_message).
    EXPECT_EQ(update.first_id, 158434481u);
    EXPECT_EQ(update.final_id, 158434481u);
    EXPECT_EQ(update.prev_final_id, 0u);  // no `pu`-equivalent on Bybit - deterministically 0
    EXPECT_TRUE(update.bids.empty());
    ASSERT_EQ(update.asks.size(), 4u);
    EXPECT_EQ(update.asks[0].first, Price(77805.60));
    EXPECT_EQ(update.asks[0].second, Size(0.372));
    // size "0" means delete - parsed through as-is (SymbolBook interprets
    // the zero, same convention Binance's feed already relies on).
    EXPECT_EQ(update.asks[1].first, Price(77808.90));
    EXPECT_EQ(update.asks[1].second, Size(0.0));
}

TEST(BybitLinearFeedTest, SubscribeAckHasNoTopicFieldAndIsIgnored) {
    BybitLinearFeed feed;
    auto result = feed.parse_message(
        R"({"success":true,"ret_msg":"","conn_id":"abc123","req_id":"","op":"subscribe"})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());  // recognized, nothing to do - not an error
}

TEST(BybitLinearFeedTest, UnrecognizedTypeUnderARealTopicIsIgnored) {
    BybitLinearFeed feed;
    auto result = feed.parse_message(
        R"({"topic":"orderbook.50.BTCUSDT","type":"someFutureType","data":{},"ts":1,"cts":1})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(BybitLinearFeedTest, MalformedJsonFailsWithBadMessage) {
    BybitLinearFeed feed;
    auto result = feed.parse_message(R"({"topic":"orderbook.50.BTCUSDT", not valid json)");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BybitLinearFeedTest, DeltaMissingRequiredFieldFailsWithBadMessage) {
    BybitLinearFeed feed;
    // Has a recognized topic/type but "data" has no "u" - genuinely
    // malformed, not just a message type this feed doesn't care about.
    auto result = feed.parse_message(
        R"({"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[],"a":[]}})");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BybitLinearFeedTest, SubscribeMessageListsEachTopicWithDefaultDepth50) {
    BybitLinearFeed feed;
    std::array<NativeSymbol, 2> symbols{"BTCUSDT", "ETHUSDT"};
    EXPECT_EQ(feed.subscribe_message(symbols),
              R"({"op":"subscribe","args":["orderbook.50.BTCUSDT","orderbook.50.ETHUSDT"]})");
}

TEST(BybitLinearFeedTest, SubscribeMessageHonorsCustomDepth) {
    BybitLinearFeed feed;
    std::array<NativeSymbol, 1> symbols{"BTCUSDT"};
    EXPECT_EQ(feed.subscribe_message(symbols, 200),
              R"({"op":"subscribe","args":["orderbook.200.BTCUSDT"]})");
}

TEST(BybitLinearFeedTest, WebSocketEndpointMatchesDocumentedPublicLinearStream) {
    BybitLinearFeed feed;
    EXPECT_EQ(feed.ws_host(), "stream.bybit.com");
    EXPECT_EQ(feed.ws_port(), "443");
    EXPECT_EQ(feed.ws_target(), "/v5/public/linear");
}

TEST(BybitLinearFeedTest, SnapshotIsNotFetchedViaRest) {
    // Bybit pushes its own snapshot over the WebSocket - see the class
    // comment on kSnapshotViaRest.
    EXPECT_FALSE(BybitLinearFeed::kSnapshotViaRest);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

#include "bobby/hermeneutic/exchange/bybit/bybit_feed.hpp"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace bobby::hermeneutic::ingestion {
namespace {

TEST(BybitSpotFeedTest, ParsesRealSnapshotMessageExample) {
    // Captured live from wss://stream.bybit.com/v5/public/spot,
    // orderbook.50.BTCUSDT (2026-09-18), truncated to a few levels. Note
    // the field order differs from linear's (ts before type, not after) -
    // parse_message() must not assume a fixed field order.
    constexpr std::string_view kText =
        R"({"topic":"orderbook.50.BTCUSDT","ts":1789717872456,"type":"snapshot",)"
        R"("data":{"s":"BTCUSDT","b":[["77804","0.293315"],["77803.9","0.002"]],)"
        R"("a":[["77804.1","0.271735"],["77804.2","0.1"]],"u":271249961,"seq":114178894363},)"
        R"("cts":1789717872442})";

    BybitSpotFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<SnapshotMessage>(**result));

    const SnapshotMessage& snapshot = std::get<SnapshotMessage>(**result);
    EXPECT_EQ(snapshot.symbol, "BTCUSDT");
    EXPECT_EQ(snapshot.last_update_id, 271249961u);
    ASSERT_EQ(snapshot.bids.size(), 2u);
    EXPECT_EQ(snapshot.bids[0].first, Price(77804.0));
    EXPECT_EQ(snapshot.bids[0].second, Size(0.293315));
    ASSERT_EQ(snapshot.asks.size(), 2u);
    EXPECT_EQ(snapshot.asks[0].first, Price(77804.1));
    EXPECT_EQ(snapshot.asks[0].second, Size(0.271735));
}

TEST(BybitSpotFeedTest, ParsesRealDeltaMessageExampleWithSingleUpdateId) {
    // Captured live from the same session, the message right after the
    // snapshot above (u = 271249962, exactly the snapshot's u + 1).
    constexpr std::string_view kText =
        R"({"topic":"orderbook.50.BTCUSDT","ts":1789717872496,"type":"delta",)"
        R"("data":{"s":"BTCUSDT","b":[["77787.8","0.000097"],["77785.2","0.083296"],)"
        R"(["77769.5","0"]],"a":[["77804.1","0.271735"]],"u":271249962,"seq":114178894371},)"
        R"("cts":1789717872495})";

    BybitSpotFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<DepthUpdate>(**result));

    const DepthUpdate& update = std::get<DepthUpdate>(**result);
    EXPECT_EQ(update.symbol, "BTCUSDT");
    EXPECT_EQ(update.first_id, 271249962u);
    EXPECT_EQ(update.final_id, 271249962u);
    EXPECT_EQ(update.prev_final_id, 0u);  // no `pu`-equivalent on Bybit - deterministically 0
    ASSERT_EQ(update.bids.size(), 3u);
    EXPECT_EQ(update.bids[0].first, Price(77787.8));
    EXPECT_EQ(update.bids[0].second, Size(0.000097));
    // size "0" means delete - parsed through as-is.
    EXPECT_EQ(update.bids[2].first, Price(77769.5));
    EXPECT_EQ(update.bids[2].second, Size(0.0));
    ASSERT_EQ(update.asks.size(), 1u);
}

TEST(BybitSpotFeedTest, SubscribeAckHasNoTopicFieldAndIsIgnored) {
    BybitSpotFeed feed;
    auto result = feed.parse_message(
        R"({"success":true,"ret_msg":"subscribe","conn_id":"abc123","op":"subscribe"})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());  // recognized, nothing to do - not an error
}

TEST(BybitSpotFeedTest, UnrecognizedTypeUnderARealTopicIsIgnored) {
    BybitSpotFeed feed;
    auto result = feed.parse_message(
        R"({"topic":"orderbook.50.BTCUSDT","type":"someFutureType","data":{},"ts":1,"cts":1})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(BybitSpotFeedTest, MalformedJsonFailsWithBadMessage) {
    BybitSpotFeed feed;
    auto result = feed.parse_message(R"({"topic":"orderbook.50.BTCUSDT", not valid json)");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BybitSpotFeedTest, DeltaMissingRequiredFieldFailsWithBadMessage) {
    BybitSpotFeed feed;
    // Has a recognized topic/type but "data" has no "u" - genuinely
    // malformed, not just a message type this feed doesn't care about.
    auto result = feed.parse_message(
        R"({"topic":"orderbook.50.BTCUSDT","type":"delta","data":{"s":"BTCUSDT","b":[],"a":[]}})");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BybitSpotFeedTest, SubscribeMessageListsEachTopicWithDefaultDepth50) {
    BybitSpotFeed feed;
    std::array<NativeSymbol, 2> symbols{"BTCUSDT", "ETHUSDT"};
    EXPECT_EQ(feed.subscribe_message(symbols),
              R"({"op":"subscribe","args":["orderbook.50.BTCUSDT","orderbook.50.ETHUSDT"]})");
}

TEST(BybitSpotFeedTest, WebSocketEndpointMatchesDocumentedPublicSpotStream) {
    BybitSpotFeed feed;
    EXPECT_EQ(feed.ws_host(), "stream.bybit.com");
    EXPECT_EQ(feed.ws_port(), "443");
    EXPECT_EQ(feed.ws_target(), "/v5/public/spot");
}

TEST(BybitSpotFeedTest, SnapshotIsNotFetchedViaRest) {
    EXPECT_FALSE(BybitSpotFeed::kSnapshotViaRest);
}

TEST(BybitSpotFeedTest, DiffersFromLinearFeedOnlyByWsTarget) {
    // The two Feeds share their parsing entirely (bybit_wire.hpp) - the
    // endpoint is the one real difference between them.
    BybitSpotFeed spot;
    BybitLinearFeed linear;
    EXPECT_EQ(spot.ws_host(), linear.ws_host());
    EXPECT_EQ(spot.ws_port(), linear.ws_port());
    EXPECT_NE(spot.ws_target(), linear.ws_target());
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

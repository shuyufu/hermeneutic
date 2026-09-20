#include "bobby/hermeneutic/exchange/binance/binance_futures_feed.hpp"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace bobby::hermeneutic::ingestion {
namespace {

TEST(BinanceFuturesFeedTest, ParsesRealDepthUpdateExample) {
    // Verbatim from developers.binance.com's Diff. Book Depth Streams doc.
    constexpr std::string_view kText =
        R"({"e":"depthUpdate","E":123456789,"T":123456788,"s":"BNBUSDT",)"
        R"("U":157,"u":160,"pu":149,"b":[["0.0024","10"]],"a":[["0.0026","100"]],)"
        R"("ps":"BTCUSDT","st":1})";

    BinanceFuturesFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<DepthUpdate>(**result));

    const DepthUpdate& update = std::get<DepthUpdate>(**result);
    EXPECT_EQ(update.symbol, "BNBUSDT");
    EXPECT_EQ(update.first_id, 157u);
    EXPECT_EQ(update.final_id, 160u);
    EXPECT_EQ(update.prev_final_id, 149u);
    ASSERT_EQ(update.bids.size(), 1u);
    EXPECT_EQ(update.bids[0].first, Price(0.0024));
    EXPECT_EQ(update.bids[0].second, Size(10.0));
    ASSERT_EQ(update.asks.size(), 1u);
    EXPECT_EQ(update.asks[0].first, Price(0.0026));
    EXPECT_EQ(update.asks[0].second, Size(100.0));
}

TEST(BinanceFuturesFeedTest, SubscribeAckHasNoEventFieldAndIsIgnored) {
    BinanceFuturesFeed feed;
    auto result = feed.parse_message(R"({"result":null,"id":1})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());  // recognized, nothing to do - not an error
}

TEST(BinanceFuturesFeedTest, UnrelatedEventTypeIsIgnored) {
    BinanceFuturesFeed feed;
    auto result = feed.parse_message(R"({"e":"someOtherEvent","s":"BNBUSDT"})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(BinanceFuturesFeedTest, MalformedJsonFailsWithBadMessage) {
    BinanceFuturesFeed feed;
    auto result = feed.parse_message(R"({"e":"depthUpdate", not valid json)");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BinanceFuturesFeedTest, DepthUpdateMissingRequiredFieldFailsWithBadMessage) {
    BinanceFuturesFeed feed;
    // Has "e":"depthUpdate" but no "u" - genuinely malformed, not just a
    // message type this feed doesn't care about.
    auto result = feed.parse_message(R"({"e":"depthUpdate","s":"BNBUSDT","U":157})");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BinanceFuturesFeedTest, ParsesRealSnapshotResponseExample) {
    // Verbatim from developers.binance.com's Order Book REST endpoint doc.
    constexpr std::string_view kBody =
        R"({"lastUpdateId":1027024,"E":1589436922972,"T":1589436922959,)"
        R"("bids":[["4.00000000","431.00000000"]],)"
        R"("asks":[["4.00000200","12.00000000"]]})";

    BinanceFuturesFeed feed;
    auto result = feed.parse_snapshot_response("BNBUSDT", kBody);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->symbol, "BNBUSDT");  // supplied by the caller, not in the body
    EXPECT_EQ(result->last_update_id, 1027024u);
    ASSERT_EQ(result->bids.size(), 1u);
    EXPECT_EQ(result->bids[0].first, Price(4.0));
    EXPECT_EQ(result->bids[0].second, Size(431.0));
    ASSERT_EQ(result->asks.size(), 1u);
    EXPECT_EQ(result->asks[0].first, Price(4.000002));
    EXPECT_EQ(result->asks[0].second, Size(12.0));
}

TEST(BinanceFuturesFeedTest, MalformedSnapshotResponseFailsWithBadMessage) {
    BinanceFuturesFeed feed;
    auto result = feed.parse_snapshot_response("BNBUSDT", "not json at all");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BinanceFuturesFeedTest, SubscribeMessageListsEachSymbolLowercasedWithStreamSuffix) {
    BinanceFuturesFeed feed;
    std::array<NativeSymbol, 2> symbols{"BTCUSDT", "ETHUSDT"};
    EXPECT_EQ(feed.subscribe_message(symbols),
              R"({"method":"SUBSCRIBE","params":["btcusdt@depth@100ms","ethusdt@depth@100ms"],"id":1})");
}

TEST(BinanceFuturesFeedTest, SubscribeMessageHonorsCustomUpdateSpeed) {
    BinanceFuturesFeed feed;
    std::array<NativeSymbol, 1> symbols{"BTCUSDT"};
    EXPECT_EQ(feed.subscribe_message(symbols, "500ms"),
              R"({"method":"SUBSCRIBE","params":["btcusdt@depth@500ms"],"id":1})");
}

TEST(BinanceFuturesFeedTest, SnapshotRequestBuildsDocumentedRestEndpoint) {
    BinanceFuturesFeed feed;
    HttpRequestSpec spec = feed.snapshot_request("BTCUSDT");
    EXPECT_EQ(spec.host, "fapi.binance.com");
    EXPECT_EQ(spec.port, "443");
    EXPECT_EQ(spec.target, "/fapi/v1/depth?symbol=BTCUSDT&limit=1000");
}

TEST(BinanceFuturesFeedTest, WebSocketEndpointMatchesDocumentedCombinedStream) {
    BinanceFuturesFeed feed;
    EXPECT_EQ(feed.ws_host(), "fstream.binance.com");
    EXPECT_EQ(feed.ws_port(), "443");
    EXPECT_EQ(feed.ws_target(), "/ws");
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

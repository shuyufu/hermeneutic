#include "bobby/hermeneutic/exchange/binance/binance_spot_feed.hpp"

#include <gtest/gtest.h>

#include <array>
#include <string>

#include "bobby/hermeneutic/exchange/binance/binance_futures_feed.hpp"

namespace bobby::hermeneutic::ingestion {
namespace {

TEST(BinanceSpotFeedTest, ParsesRealDepthUpdateExampleWithNoPuField) {
    // Verbatim from developers.binance.com's Spot Diff. Depth Stream doc.
    // Note: no "pu" field at all - unlike Futures' depthUpdate.
    constexpr std::string_view kText =
        R"({ "e": "depthUpdate", "E": 1672515782136, "s": "BNBBTC", "U": 157, "u": 160, )"
        R"("b": [ [ "0.0024", "10" ] ], "a": [ [ "0.0026", "100" ] ] })";

    BinanceSpotFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<DepthUpdate>(**result));

    const DepthUpdate& update = std::get<DepthUpdate>(**result);
    EXPECT_EQ(update.symbol, "BNBBTC");
    EXPECT_EQ(update.first_id, 157u);
    EXPECT_EQ(update.final_id, 160u);
    EXPECT_EQ(update.prev_final_id, 0u);  // no `pu` on Spot - deterministically 0, not garbage
    ASSERT_EQ(update.bids.size(), 1u);
    EXPECT_EQ(update.bids[0].first, Price(0.0024));
    EXPECT_EQ(update.bids[0].second, Size(10.0));
    ASSERT_EQ(update.asks.size(), 1u);
    EXPECT_EQ(update.asks[0].first, Price(0.0026));
    EXPECT_EQ(update.asks[0].second, Size(100.0));
}

TEST(BinanceSpotFeedTest, SameMessageFailsUnderFuturesFeedProvingTheFieldIsGenuinelyMissing) {
    // The exact text BinanceSpotFeed parses successfully above must fail
    // under BinanceFuturesFeed, which requires "pu" - proving Spot's lack
    // of that field is a real wire-format difference, not just an untested
    // one.
    constexpr std::string_view kText =
        R"({ "e": "depthUpdate", "E": 1672515782136, "s": "BNBBTC", "U": 157, "u": 160, )"
        R"("b": [ [ "0.0024", "10" ] ], "a": [ [ "0.0026", "100" ] ] })";

    BinanceFuturesFeed futures_feed;
    auto result = futures_feed.parse_message(kText);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BinanceSpotFeedTest, SubscribeAckHasNoEventFieldAndIsIgnored) {
    BinanceSpotFeed feed;
    auto result = feed.parse_message(R"({"result":null,"id":1})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());  // recognized, nothing to do - not an error
}

TEST(BinanceSpotFeedTest, UnrelatedEventTypeIsIgnored) {
    BinanceSpotFeed feed;
    auto result = feed.parse_message(R"({"e":"someOtherEvent","s":"BNBBTC"})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(BinanceSpotFeedTest, MalformedJsonFailsWithBadMessage) {
    BinanceSpotFeed feed;
    auto result = feed.parse_message(R"({"e":"depthUpdate", not valid json)");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BinanceSpotFeedTest, DepthUpdateMissingRequiredFieldFailsWithBadMessage) {
    BinanceSpotFeed feed;
    // Has "e":"depthUpdate" but no "u" - genuinely malformed, not just a
    // message type this feed doesn't care about.
    auto result = feed.parse_message(R"({"e":"depthUpdate","s":"BNBBTC","U":157})");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BinanceSpotFeedTest, ParsesRealSnapshotResponseExample) {
    // Verbatim from developers.binance.com's Spot Order Book REST endpoint
    // doc. Same shape as Futures' response, minus the `E`/`T` fields
    // Futures has and neither feed parses.
    constexpr std::string_view kBody =
        R"({"lastUpdateId":1027024,)"
        R"("bids":[["4.00000000","431.00000000"]],)"
        R"("asks":[["4.00000200","12.00000000"]]})";

    BinanceSpotFeed feed;
    auto result = feed.parse_snapshot_response("BNBBTC", kBody);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->symbol, "BNBBTC");  // supplied by the caller, not in the body
    EXPECT_EQ(result->last_update_id, 1027024u);
    ASSERT_EQ(result->bids.size(), 1u);
    EXPECT_EQ(result->bids[0].first, Price(4.0));
    EXPECT_EQ(result->bids[0].second, Size(431.0));
    ASSERT_EQ(result->asks.size(), 1u);
    EXPECT_EQ(result->asks[0].first, Price(4.000002));
    EXPECT_EQ(result->asks[0].second, Size(12.0));
}

TEST(BinanceSpotFeedTest, MalformedSnapshotResponseFailsWithBadMessage) {
    BinanceSpotFeed feed;
    auto result = feed.parse_snapshot_response("BNBBTC", "not json at all");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(BinanceSpotFeedTest, SubscribeMessageListsEachSymbolLowercasedWithStreamSuffix) {
    BinanceSpotFeed feed;
    std::array<SymbolId, 2> symbols{"BTCUSDT", "ETHUSDT"};
    EXPECT_EQ(feed.subscribe_message(symbols),
              R"({"method":"SUBSCRIBE","params":["btcusdt@depth@100ms","ethusdt@depth@100ms"],"id":1})");
}

TEST(BinanceSpotFeedTest, SubscribeMessageHonorsCustomUpdateSpeed) {
    // 1000ms is Spot's documented default speed (not Futures' "500ms"
    // option, which Spot doesn't have).
    BinanceSpotFeed feed;
    std::array<SymbolId, 1> symbols{"BTCUSDT"};
    EXPECT_EQ(feed.subscribe_message(symbols, "1000ms"),
              R"({"method":"SUBSCRIBE","params":["btcusdt@depth@1000ms"],"id":1})");
}

TEST(BinanceSpotFeedTest, SnapshotRequestBuildsDocumentedRestEndpoint) {
    BinanceSpotFeed feed;
    HttpRequestSpec spec = feed.snapshot_request("BTCUSDT");
    EXPECT_EQ(spec.host, "api.binance.com");
    EXPECT_EQ(spec.port, "443");
    EXPECT_EQ(spec.target, "/api/v3/depth?symbol=BTCUSDT&limit=5000");
}

TEST(BinanceSpotFeedTest, WebSocketEndpointMatchesDocumentedRawStream) {
    BinanceSpotFeed feed;
    EXPECT_EQ(feed.ws_host(), "stream.binance.com");
    EXPECT_EQ(feed.ws_port(), "9443");
    EXPECT_EQ(feed.ws_target(), "/ws");
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

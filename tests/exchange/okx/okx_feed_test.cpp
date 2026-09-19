#include "bobby/hermeneutic/exchange/okx/okx_feed.hpp"

#include <gtest/gtest.h>

#include <array>
#include <string>

namespace bobby::hermeneutic::ingestion {
namespace {

// These example payloads are constructed directly from OKX's documented v5
// `books` channel field semantics (seqId/prevSeqId, action, arg.instId,
// 4-element level arrays) - not a live capture the way Bybit's feed test
// examples are. A short live smoke test against wss://ws.okx.com:8443
// before this ships to production is recommended (see docs/ingestion_design.md).

TEST(OkxFeedTest, ParsesSnapshotMessageExample) {
    constexpr std::string_view kText =
        R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot",)"
        R"("data":[{"asks":[["8476.98","415","0","13"],["8477.00","7","0","2"]],)"
        R"("bids":[["8476.97","256","0","12"],["8476.96","10","0","1"]],)"
        R"("ts":"1597026383085","checksum":-855196043,"prevSeqId":-1,"seqId":10}]})";

    OkxFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<SnapshotMessage>(**result));

    const SnapshotMessage& snapshot = std::get<SnapshotMessage>(**result);
    EXPECT_EQ(snapshot.symbol, "BTC-USDT");
    EXPECT_EQ(snapshot.last_update_id, 10u);
    ASSERT_EQ(snapshot.bids.size(), 2u);
    EXPECT_EQ(snapshot.bids[0].first, Price(8476.97));
    EXPECT_EQ(snapshot.bids[0].second, Size(256.0));
    ASSERT_EQ(snapshot.asks.size(), 2u);
    EXPECT_EQ(snapshot.asks[0].first, Price(8476.98));
    EXPECT_EQ(snapshot.asks[0].second, Size(415.0));
}

TEST(OkxFeedTest, ParsesUpdateMessageExample) {
    constexpr std::string_view kText =
        R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update",)"
        R"("data":[{"asks":[["8476.98","0","0","0"]],)"
        R"("bids":[["8476.99","500","0","3"]],)"
        R"("ts":"1597026383086","checksum":1234,"prevSeqId":10,"seqId":15}]})";

    OkxFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<DepthUpdate>(**result));

    const DepthUpdate& update = std::get<DepthUpdate>(**result);
    EXPECT_EQ(update.symbol, "BTC-USDT");
    // OKX carries one seqId, not a first_id/final_id range - both fields
    // get the same value (see OkxFeed::parse_message).
    EXPECT_EQ(update.first_id, 15u);
    EXPECT_EQ(update.final_id, 15u);
    EXPECT_EQ(update.prev_final_id, 10u);
    ASSERT_EQ(update.bids.size(), 1u);
    EXPECT_EQ(update.bids[0].first, Price(8476.99));
    EXPECT_EQ(update.bids[0].second, Size(500.0));
    // size "0" means delete - parsed through as-is.
    ASSERT_EQ(update.asks.size(), 1u);
    EXPECT_EQ(update.asks[0].first, Price(8476.98));
    EXPECT_EQ(update.asks[0].second, Size(0.0));
}

TEST(OkxFeedTest, IdleHeartbeatWithEmptyBidsAsksStillCarriesTheSeqChain) {
    // OKX's documented "no update for ~60s" keepalive: empty bids/asks,
    // prevSeqId == seqId (chains from itself). Must still parse as a normal
    // DepthUpdate - dropping it silently would break the prevSeqId chain
    // for the next real update (see OkxSequencePolicy's comment).
    constexpr std::string_view kText =
        R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update",)"
        R"("data":[{"asks":[],"bids":[],)"
        R"("ts":"1597026383090","checksum":1234,"prevSeqId":15,"seqId":15}]})";

    OkxFeed feed;
    auto result = feed.parse_message(kText);
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->has_value());
    ASSERT_TRUE(std::holds_alternative<DepthUpdate>(**result));

    const DepthUpdate& update = std::get<DepthUpdate>(**result);
    EXPECT_EQ(update.final_id, 15u);
    EXPECT_EQ(update.prev_final_id, 15u);
    EXPECT_TRUE(update.bids.empty());
    EXPECT_TRUE(update.asks.empty());
}

TEST(OkxFeedTest, PongLiteralIsIgnored) {
    // Not JSON at all - must be checked before attempting to parse one.
    OkxFeed feed;
    auto result = feed.parse_message("pong");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(OkxFeedTest, SubscribeAckHasNoDataFieldAndIsIgnored) {
    OkxFeed feed;
    auto result = feed.parse_message(
        R"({"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"},"connId":"a4d3ae55"})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(OkxFeedTest, ErrorEventHasNoDataFieldAndIsIgnored) {
    OkxFeed feed;
    auto result = feed.parse_message(
        R"({"event":"error","code":"60012","msg":"Invalid request","connId":"a4d3ae55"})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(OkxFeedTest, UnrecognizedActionUnderARealDataEnvelopeIsIgnored) {
    OkxFeed feed;
    auto result = feed.parse_message(
        R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"someFutureAction",)"
        R"("data":[{"asks":[],"bids":[],"seqId":1,"prevSeqId":0}]})");
    ASSERT_TRUE(result.has_value());
    EXPECT_FALSE(result->has_value());
}

TEST(OkxFeedTest, EmptyDataArrayFailsWithBadMessageRatherThanCrashing) {
    // A recognized envelope ("arg"/"action" present) but an empty "data"
    // array - dereferencing an on-demand array's end() iterator is not a
    // simdjson_error (it's a hard assert in debug, undefined behavior under
    // NDEBUG, verified empirically against this project's simdjson build),
    // so this must be checked explicitly rather than left to *data.begin().
    OkxFeed feed;
    auto result = feed.parse_message(
        R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(OkxFeedTest, MalformedJsonFailsWithBadMessage) {
    OkxFeed feed;
    auto result = feed.parse_message(R"({"arg":{"instId":"BTC-USDT"}, not valid json)");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(OkxFeedTest, UpdateMissingRequiredFieldFailsWithBadMessage) {
    // Has a recognized "data" envelope but the entry is missing "seqId" -
    // genuinely malformed, not just a message this feed doesn't care about.
    OkxFeed feed;
    auto result = feed.parse_message(
        R"({"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update",)"
        R"("data":[{"asks":[],"bids":[],"prevSeqId":10}]})");
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::bad_message);
}

TEST(OkxFeedTest, SubscribeMessageListsEachInstIdOnBooksChannel) {
    OkxFeed feed;
    std::array<SymbolId, 2> symbols{"BTC-USDT", "BTC-USDT-SWAP"};
    EXPECT_EQ(feed.subscribe_message(symbols),
              R"({"op":"subscribe","args":[{"channel":"books","instId":"BTC-USDT"},)"
              R"({"channel":"books","instId":"BTC-USDT-SWAP"}]})");
}

TEST(OkxFeedTest, WebSocketEndpointMatchesDocumentedPublicEndpoint) {
    OkxFeed feed;
    EXPECT_EQ(feed.ws_host(), "ws.okx.com");
    EXPECT_EQ(feed.ws_port(), "8443");
    EXPECT_EQ(feed.ws_target(), "/ws/v5/public");
}

TEST(OkxFeedTest, SnapshotIsNotFetchedViaRest) {
    // OKX pushes its own snapshot over the WebSocket - see the class
    // comment on kSnapshotViaRest.
    EXPECT_FALSE(OkxFeed::kSnapshotViaRest);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

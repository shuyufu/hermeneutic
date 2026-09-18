#include "bobby/hermeneutic/service/aggregator_service.hpp"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

namespace bobby::hermeneutic::aggregator {
namespace {

SubscribeL2DiffRequest subscribe_l2_diff_request(std::string_view symbol) {
    SubscribeL2DiffRequest request;
    request.set_symbol(std::string(symbol));
    return request;
}

SubscribeBboRequest subscribe_bbo_request(std::string_view symbol) {
    SubscribeBboRequest request;
    request.set_symbol(std::string(symbol));
    return request;
}

// Thread-safe queue the client-reader thread pushes into and the test
// thread polls, so assertions can wait for a specific message to arrive
// without racing the streaming thread.
template <typename T>
class UpdateQueue {
  public:
    void push(T update) {
        std::lock_guard lock(mutex_);
        queue_.push_back(std::move(update));
        cv_.notify_all();
    }

    // Blocks until at least `index + 1` updates have arrived (or times out),
    // then returns the update at `index`.
    T wait_for(std::size_t index) {
        std::unique_lock lock(mutex_);
        bool ok = cv_.wait_for(lock, std::chrono::seconds(5),
                                [&] { return queue_.size() > index; });
        if (!ok) {
            ADD_FAILURE() << "timed out waiting for update #" << index;
            return {};
        }
        return queue_[index];
    }

  private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<T> queue_;
};

// A BBO client reading in the background, mirroring the inline pattern used
// for extra L2 readers elsewhere in this file (e.g. SnapshotOrdering
// MatchesBookConvention's second_reader) but factored out since several BBO
// tests each need one of these. Heap-allocated (returned via unique_ptr) so
// `context`'s address stays stable for the reader thread's lambda.
struct BboSubscription {
    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientReaderInterface<BboUpdate>> reader;
    UpdateQueue<BboUpdate> updates;
    std::thread thread;

    ~BboSubscription() {
        context.TryCancel();
        if (thread.joinable()) thread.join();
    }
};

std::unique_ptr<BboSubscription> subscribe_bbo(Aggregator::Stub& stub, std::string_view symbol) {
    auto sub = std::make_unique<BboSubscription>();
    sub->reader = stub.SubscribeBbo(&sub->context, subscribe_bbo_request(symbol));
    sub->thread = std::thread([raw = sub.get()] {
        BboUpdate update;
        while (raw->reader->Read(&update)) raw->updates.push(update);
    });
    return sub;
}

TEST(SubscriberQueueTest, DrainsInFifoOrderAndReportsResultKind) {
    SubscriberQueue<L2Update> queue(4, OverflowPolicy::Close);

    std::vector<L2Update> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<L2Update>::DrainResult::TimedOut);
    EXPECT_TRUE(out.empty());

    L2Update first, second;
    first.mutable_heartbeat()->set_ts_ns(1);
    second.mutable_heartbeat()->set_ts_ns(2);
    EXPECT_TRUE(queue.push_or_close(first));
    EXPECT_TRUE(queue.push_or_close(second));

    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<L2Update>::DrainResult::Drained);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].heartbeat().ts_ns(), 1u);
    EXPECT_EQ(out[1].heartbeat().ts_ns(), 2u);
}

TEST(SubscriberQueueTest, CloseOverflowClosesAndDiscardsEverythingQueued) {
    SubscriberQueue<L2Update> queue(2, OverflowPolicy::Close);

    L2Update update;
    ASSERT_TRUE(queue.push_or_close(update));
    ASSERT_TRUE(queue.push_or_close(update));
    // Third push finds the queue already at capacity: closes it instead of
    // dropping the oldest entry, since a gap in an L2Diff stream leaves the
    // subscriber's book genuinely wrong - only a fresh snapshot recovers it,
    // so there's nothing worth keeping once it's fallen this far behind.
    EXPECT_FALSE(queue.push_or_close(update));
    // Pushes after closing are also rejected, not re-queued.
    EXPECT_FALSE(queue.push_or_close(update));

    std::vector<L2Update> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<L2Update>::DrainResult::Closed);
    EXPECT_TRUE(out.empty());  // the two successfully queued updates were discarded, not delivered
}

TEST(SubscriberQueueTest, DropOldestOverflowKeepsNewestWithoutClosing) {
    SubscriberQueue<BboUpdate> queue(2, OverflowPolicy::DropOldest);

    BboUpdate first, second, third;
    first.mutable_bbo()->set_book_seq(1);
    second.mutable_bbo()->set_book_seq(2);
    third.mutable_bbo()->set_book_seq(3);

    ASSERT_TRUE(queue.push_or_close(first));
    ASSERT_TRUE(queue.push_or_close(second));
    // Third push overflows capacity 2: drops the oldest (seq 1) instead of
    // closing, since a stale Bbo costs nothing to skip - each one is a
    // complete, self-contained state, not a delta.
    EXPECT_TRUE(queue.push_or_close(third));

    std::vector<BboUpdate> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<BboUpdate>::DrainResult::Drained);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].bbo().book_seq(), 2u);
    EXPECT_EQ(out[1].bbo().book_seq(), 3u);
}

class AggregatorServiceTest : public ::testing::Test {
  protected:
    static constexpr std::string_view kSymbol = "BTCUSDT";

    AggregatorServiceTest() : service_(std::vector<std::string>{std::string(kSymbol)}) {}

    // Starts a real server on an ephemeral port and a real client stub
    // against it, so this exercises the actual gRPC wire path rather than
    // calling AggregatorService's methods directly against each other.
    void SetUp() override {
        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(&service_);
        server_ = builder.BuildAndStart();
        ASSERT_NE(server_, nullptr);

        auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                            grpc::InsecureChannelCredentials());
        stub_ = Aggregator::NewStub(channel);

        reader_ = stub_->SubscribeL2Diff(&context_, subscribe_l2_diff_request(kSymbol));
        reader_thread_ = std::thread([this] {
            L2Update update;
            while (reader_->Read(&update)) {
                updates_.push(update);
            }
        });
    }

    void TearDown() override {
        context_.TryCancel();
        if (reader_thread_.joinable()) reader_thread_.join();
        server_->Shutdown();
    }

    // Ingestion for this fixture's tests goes through the symbol's own
    // SymbolBook directly, the same way a real ingestion dispatch layer
    // would - the fixture used to expose apply_delta/apply_snapshot/
    // invalidate_venue/send_heartbeat straight on AggregatorService, back
    // when it wrapped exactly one symbol.
    SymbolBook& book() { return *service_.book(kSymbol); }

    std::unique_ptr<BboSubscription> subscribe_bbo() { return ::bobby::hermeneutic::aggregator::subscribe_bbo(*stub_, kSymbol); }

    AggregatorService service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<Aggregator::Stub> stub_;
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientReaderInterface<L2Update>> reader_;
    std::thread reader_thread_;
    UpdateQueue<L2Update> updates_;
};

TEST_F(AggregatorServiceTest, SubscribingToEmptyBookYieldsEmptySnapshot) {
    L2Update first = updates_.wait_for(0);
    ASSERT_TRUE(first.has_snapshot());
    EXPECT_EQ(first.snapshot().book_seq(), 0u);
    EXPECT_EQ(first.snapshot().bids_size(), 0);
    EXPECT_EQ(first.snapshot().asks_size(), 0);
}

TEST_F(AggregatorServiceTest, ApplyDeltaAfterSubscribeProducesDiff) {
    updates_.wait_for(0);  // initial snapshot

    auto result = book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0));
    ASSERT_TRUE(result.has_value());

    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(1.0).raw());
    EXPECT_EQ(msg.diff().asks_size(), 0);
}

TEST_F(AggregatorServiceTest, MultipleVenuesAggregateAndPartialRemovalKeepsRemainder) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    EXPECT_EQ(updates_.wait_for(1).diff().bids(0).size_raw(), Size(1.0).raw());

    ASSERT_TRUE(book().apply_delta("okx", Side::Bid, Price(100.0), Size(2.0)).has_value());
    EXPECT_EQ(updates_.wait_for(2).diff().bids(0).size_raw(), Size(3.0).raw());

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(0.0)).has_value());
    L2Update msg = updates_.wait_for(3);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    // okx's remaining size, not a removal (binance's own contribution was
    // the only thing zeroed).
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(2.0).raw());
}

TEST_F(AggregatorServiceTest, InvalidateVenueRemovesOnlyItsExclusiveLevels) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(book().apply_delta("okx", Side::Bid, Price(100.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(101.0), Size(5.0)).has_value());
    updates_.wait_for(3);

    book().invalidate_venue("binance");
    L2Update msg = updates_.wait_for(4);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 4u);

    // Bid @100 still has okx's 2.0 left (not removed); ask @101 was
    // exclusively binance's and must be reported as removed (size_raw 0).
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(2.0).raw());
    ASSERT_EQ(msg.diff().asks_size(), 1);
    EXPECT_EQ(msg.diff().asks(0).size_raw(), 0);
}

TEST_F(AggregatorServiceTest, ApplySnapshotDiffsAgainstAggregateNotJustThatVenue) {
    updates_.wait_for(0);  // initial snapshot

    // binance holds two bid levels; okx also contributes at 100.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(103.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(book().apply_delta("okx", Side::Bid, Price(100.0), Size(5.0)).has_value());
    updates_.wait_for(3);

    // Replace binance's entire bid side: 100 changes size, 103 is dropped
    // (absent from the new levels), 102 is a brand new price.
    std::array<std::pair<Price, Size>, 2> levels{{
        {Price(100.0), Size(3.0)},
        {Price(102.0), Size(4.0)},
    }};
    auto result = book().apply_snapshot("binance", Side::Bid, levels);
    ASSERT_TRUE(result.has_value());

    L2Update msg = updates_.wait_for(4);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 4u);
    ASSERT_EQ(msg.diff().bids_size(), 3);
    // L2Diff.bids is guaranteed to come out in the same order as
    // L2Snapshot.bids (descending by price, matching the aggregate book's
    // own bid ordering): 103, 102, 100.
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(103.0).raw());
    EXPECT_EQ(msg.diff().bids(0).size_raw(), 0);  // binance dropped it, no one else held it
    EXPECT_EQ(msg.diff().bids(1).price_raw(), Price(102.0).raw());
    EXPECT_EQ(msg.diff().bids(1).size_raw(), Size(4.0).raw());  // new level, binance only
    EXPECT_EQ(msg.diff().bids(2).price_raw(), Price(100.0).raw());
    EXPECT_EQ(msg.diff().bids(2).size_raw(), Size(8.0).raw());  // okx's 5 + binance's new 3
    EXPECT_EQ(msg.diff().asks_size(), 0);
}

TEST_F(AggregatorServiceTest, ApplySnapshotOnAskSideProducesAscendingDiff) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(105.0), Size(2.0)).has_value());
    updates_.wait_for(2);

    // Replace binance's entire ask side: 101 changes size, 105 is dropped,
    // 103 is a brand new price.
    std::array<std::pair<Price, Size>, 2> levels{{
        {Price(101.0), Size(4.0)},
        {Price(103.0), Size(3.0)},
    }};
    ASSERT_TRUE(book().apply_snapshot("binance", Side::Ask, levels).has_value());

    L2Update msg = updates_.wait_for(3);
    ASSERT_TRUE(msg.has_diff());
    ASSERT_EQ(msg.diff().asks_size(), 3);
    // Ascending by price, matching L2Snapshot.asks: 101, 103, 105.
    EXPECT_EQ(msg.diff().asks(0).price_raw(), Price(101.0).raw());
    EXPECT_EQ(msg.diff().asks(0).size_raw(), Size(4.0).raw());
    EXPECT_EQ(msg.diff().asks(1).price_raw(), Price(103.0).raw());
    EXPECT_EQ(msg.diff().asks(1).size_raw(), Size(3.0).raw());
    EXPECT_EQ(msg.diff().asks(2).price_raw(), Price(105.0).raw());
    EXPECT_EQ(msg.diff().asks(2).size_raw(), 0);  // dropped, no one else held it
    EXPECT_EQ(msg.diff().bids_size(), 0);
}

TEST_F(AggregatorServiceTest, SnapshotOrderingMatchesBookConvention) {
    updates_.wait_for(0);  // initial (empty) snapshot for the fixture's own subscriber

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(102.0), Size(1.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(3);
    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(105.0), Size(1.0)).has_value());
    updates_.wait_for(4);
    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(103.0), Size(1.0)).has_value());
    updates_.wait_for(5);
    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(104.0), Size(1.0)).has_value());
    updates_.wait_for(6);

    // A second, independent subscriber joining now must see the book's own
    // ordering directly in its initial snapshot: bids descending, asks
    // ascending.
    grpc::ClientContext second_context;
    auto second_reader = stub_->SubscribeL2Diff(&second_context, subscribe_l2_diff_request(kSymbol));
    L2Update snapshot_msg;
    ASSERT_TRUE(second_reader->Read(&snapshot_msg));
    second_context.TryCancel();

    ASSERT_TRUE(snapshot_msg.has_snapshot());
    const auto& snapshot = snapshot_msg.snapshot();
    ASSERT_EQ(snapshot.bids_size(), 3);
    EXPECT_EQ(snapshot.bids(0).price_raw(), Price(102.0).raw());
    EXPECT_EQ(snapshot.bids(1).price_raw(), Price(101.0).raw());
    EXPECT_EQ(snapshot.bids(2).price_raw(), Price(100.0).raw());
    ASSERT_EQ(snapshot.asks_size(), 3);
    EXPECT_EQ(snapshot.asks(0).price_raw(), Price(103.0).raw());
    EXPECT_EQ(snapshot.asks(1).price_raw(), Price(104.0).raw());
    EXPECT_EQ(snapshot.asks(2).price_raw(), Price(105.0).raw());
}

TEST_F(AggregatorServiceTest, FailedApplyDoesNotBroadcast) {
    updates_.wait_for(0);  // initial snapshot

    auto bad = book().apply_delta("binance", Side::Bid, Price(100.0), Size(-1.0));
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), std::errc::invalid_argument);

    auto good = book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0));
    ASSERT_TRUE(good.has_value());

    // If the failed call had broadcast anything, this would be seq 2 / the
    // third message overall instead of seq 1 / the second.
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(1.0).raw());
}

TEST_F(AggregatorServiceTest, NoOpApplyDeltaDoesNotBroadcast) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);

    // Re-applying the exact same size changes nothing in the aggregate.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(2.0)).has_value());

    // If the no-op call had broadcast anything, this would be seq 3 / the
    // third message overall instead of seq 2 / the second.
    L2Update msg = updates_.wait_for(2);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 2u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(2.0).raw());
}

TEST_F(AggregatorServiceTest, HeartbeatIsDeliveredAndDoesNotAdvanceSeq) {
    updates_.wait_for(0);  // initial snapshot

    book().send_heartbeat();
    L2Update heartbeat_msg = updates_.wait_for(1);
    ASSERT_TRUE(heartbeat_msg.has_heartbeat());
    EXPECT_NE(heartbeat_msg.heartbeat().ts_ns(), 0u);

    // A real book change right after must still be seq 1 / the third
    // message overall - proof the heartbeat above didn't touch seq_.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    L2Update diff_msg = updates_.wait_for(2);
    ASSERT_TRUE(diff_msg.has_diff());
    EXPECT_EQ(diff_msg.diff().book_seq(), 1u);
}

TEST_F(AggregatorServiceTest, ApplyBatchProducesOneSeqBumpForMultipleLevels) {
    updates_.wait_for(0);  // initial snapshot

    std::array<std::pair<Price, Size>, 2> bids{{
        {Price(102.0), Size(1.0)},
        {Price(100.0), Size(2.0)},
    }};
    std::array<std::pair<Price, Size>, 1> asks{{
        {Price(101.0), Size(3.0)},
    }};
    ASSERT_TRUE(book().apply_batch("binance", bids, asks).has_value());

    // One message, seq 1 - not three separate diffs the way three
    // apply_delta() calls for the same levels would have produced.
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);

    // Bids descending, asks ascending - same ordering guarantee as
    // apply_snapshot's diffs, not raw input order (bids were given
    // 102-then-100 above; asks would coincidentally match either way with
    // only one level, so this is really testing the bids side).
    ASSERT_EQ(msg.diff().bids_size(), 2);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(102.0).raw());
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(1.0).raw());
    EXPECT_EQ(msg.diff().bids(1).price_raw(), Price(100.0).raw());
    EXPECT_EQ(msg.diff().bids(1).size_raw(), Size(2.0).raw());
    ASSERT_EQ(msg.diff().asks_size(), 1);
    EXPECT_EQ(msg.diff().asks(0).price_raw(), Price(101.0).raw());
    EXPECT_EQ(msg.diff().asks(0).size_raw(), Size(3.0).raw());
}

TEST_F(AggregatorServiceTest, ApplyBatchAggregatesAcrossVenuesLikeApplyDelta) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(book().apply_delta("okx", Side::Bid, Price(100.0), Size(5.0)).has_value());
    updates_.wait_for(1);

    // binance's batch touches the same price okx already holds, plus a
    // new one - the aggregate must reflect both venues' contributions.
    std::array<std::pair<Price, Size>, 2> bids{{
        {Price(100.0), Size(3.0)},
        {Price(99.0), Size(1.0)},
    }};
    ASSERT_TRUE(book().apply_batch("binance", bids, {}).has_value());

    L2Update msg = updates_.wait_for(2);
    ASSERT_TRUE(msg.has_diff());
    ASSERT_EQ(msg.diff().bids_size(), 2);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(8.0).raw());  // okx's 5 + binance's new 3
    EXPECT_EQ(msg.diff().bids(1).price_raw(), Price(99.0).raw());
    EXPECT_EQ(msg.diff().bids(1).size_raw(), Size(1.0).raw());  // binance only
}

TEST_F(AggregatorServiceTest, ApplyBatchRejectsNegativeSizeWithoutMutatingOrBroadcasting) {
    updates_.wait_for(0);  // initial snapshot

    std::array<std::pair<Price, Size>, 2> bad_bids{{
        {Price(100.0), Size(1.0)},
        {Price(99.0), Size(-1.0)},
    }};
    auto bad = book().apply_batch("binance", bad_bids, {});
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), std::errc::invalid_argument);

    // Neither level from the rejected batch was applied: a good call right
    // after must be seq 1 / the second message, not seq 2 / the third.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
}

TEST_F(AggregatorServiceTest, StuckSubscriberDoesNotBlockIngestionOrOtherSubscribers) {
    updates_.wait_for(0);  // initial snapshot for the fixture's own (fast) subscriber

    // A second subscriber that never reads from its stream, simulating one
    // that's stopped draining. broadcast_to_subscribers() only ever does a
    // non-blocking push into each subscriber's own queue (see
    // SubscriberQueue::push_or_close), so ingestion and the first (fast)
    // subscriber must be unaffected by this one - regardless of whether its
    // queue has overflowed yet, which depends on OS-level socket buffering
    // this test doesn't control and so doesn't assert on.
    grpc::ClientContext stuck_context;
    auto stuck_reader = stub_->SubscribeL2Diff(&stuck_context, subscribe_l2_diff_request(kSymbol));

    // Stays comfortably under kSubscriberQueueCapacity (256): this test is
    // about a non-draining subscriber not blocking anyone else, not about
    // burst volume exceeding a single subscriber's own queue capacity (a
    // real but separate concern - a big enough burst can overflow even an
    // actively-draining subscriber if it can't Write() fast enough, which
    // would otherwise make this test flaky for the wrong reason).
    constexpr int kUpdates = 100;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kUpdates; ++i) {
        ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(1.0 + i * 0.01), Size(1.0))
                        .has_value());
    }
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(2))
        << "ingestion stalled - likely blocked on the non-draining subscriber";

    // The fast subscriber must still have received every diff.
    updates_.wait_for(kUpdates);

    stuck_context.TryCancel();
}

TEST_F(AggregatorServiceTest, SubscribeBboOnEmptyBookHasNeitherSide) {
    auto bbo_sub = subscribe_bbo();
    BboUpdate first = bbo_sub->updates.wait_for(0);
    ASSERT_TRUE(first.has_bbo());
    EXPECT_EQ(first.bbo().book_seq(), 0u);
    EXPECT_FALSE(first.bbo().has_bid());
    EXPECT_FALSE(first.bbo().has_ask());
}

TEST_F(AggregatorServiceTest, SubscribeBboYieldsCurrentCompleteState) {
    updates_.wait_for(0);  // initial L2 snapshot

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(101.0), Size(2.0)).has_value());
    updates_.wait_for(2);

    // A fresh BBO subscription's first message is the complete current top
    // of book, not built up from deltas - same book_seq domain as
    // L2Diff.book_seq (see Bbo's proto comment).
    auto bbo_sub = subscribe_bbo();
    BboUpdate first = bbo_sub->updates.wait_for(0);
    ASSERT_TRUE(first.has_bbo());
    EXPECT_EQ(first.bbo().book_seq(), 2u);
    ASSERT_TRUE(first.bbo().has_bid());
    EXPECT_EQ(first.bbo().bid().price_raw(), Price(100.0).raw());
    EXPECT_EQ(first.bbo().bid().size_raw(), Size(1.0).raw());
    ASSERT_TRUE(first.bbo().has_ask());
    EXPECT_EQ(first.bbo().ask().price_raw(), Price(101.0).raw());
    EXPECT_EQ(first.bbo().ask().size_raw(), Size(2.0).raw());
}

TEST_F(AggregatorServiceTest, DeepBookChangeDoesNotEmitBbo) {
    updates_.wait_for(0);  // initial L2 snapshot

    auto bbo_sub = subscribe_bbo();
    bbo_sub->updates.wait_for(0);  // initial (empty) Bbo

    // Establishes a best bid at 100 - this DOES move the top of book.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);  // corresponding L2Diff, book_seq=1
    BboUpdate first_bbo = bbo_sub->updates.wait_for(1);
    ASSERT_TRUE(first_bbo.has_bbo());
    EXPECT_EQ(first_bbo.bbo().book_seq(), 1u);
    EXPECT_EQ(first_bbo.bbo().bid().price_raw(), Price(100.0).raw());

    // A worse (deeper) bid level doesn't change the best bid, so this
    // revision (book_seq=2) must produce an L2Diff but no new Bbo at all -
    // Bbo.book_seq is allowed to skip book_seq=2 entirely.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(99.0), Size(5.0)).has_value());
    updates_.wait_for(2);  // corresponding L2Diff did arrive on the L2 stream

    // A heartbeat proves the absence of a second Bbo message isn't just
    // "hasn't arrived yet" - it's genuinely the next thing delivered on
    // this stream.
    book().send_heartbeat();
    BboUpdate next = bbo_sub->updates.wait_for(2);
    EXPECT_TRUE(next.has_heartbeat());
}

TEST_F(AggregatorServiceTest, BestPriceSizeChangeEmitsNewBbo) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);

    auto bbo_sub = subscribe_bbo();
    BboUpdate first = bbo_sub->updates.wait_for(0);
    EXPECT_EQ(first.bbo().bid().size_raw(), Size(1.0).raw());

    // Same best price, different size (a second venue joins at the same
    // level) - still a top-of-book change, not just a deep-book one.
    ASSERT_TRUE(book().apply_delta("okx", Side::Bid, Price(100.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    BboUpdate second = bbo_sub->updates.wait_for(1);
    ASSERT_TRUE(second.has_bbo());
    EXPECT_EQ(second.bbo().bid().size_raw(), Size(3.0).raw());
}

TEST_F(AggregatorServiceTest, BestSideDisappearingIsReportedAsAbsent) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(1);

    auto bbo_sub = subscribe_bbo();
    BboUpdate first = bbo_sub->updates.wait_for(0);
    ASSERT_TRUE(first.bbo().has_ask());

    // Zeroing out the only ask level removes it entirely - the resulting
    // Bbo must represent "no ask" via message-field absence, not a sentinel
    // price or a zero size.
    ASSERT_TRUE(book().apply_delta("binance", Side::Ask, Price(101.0), Size(0.0)).has_value());
    updates_.wait_for(2);
    BboUpdate second = bbo_sub->updates.wait_for(1);
    ASSERT_TRUE(second.has_bbo());
    EXPECT_FALSE(second.bbo().has_ask());
}

TEST_F(AggregatorServiceTest, BboBookSeqCorrelatesWithL2DiffBookSeqAndSkipsNonTopRevisions) {
    updates_.wait_for(0);  // initial L2 snapshot

    auto bbo_sub = subscribe_bbo();
    bbo_sub->updates.wait_for(0);  // initial (empty) Bbo

    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);  // book_seq=1, top-of-book change
    bbo_sub->updates.wait_for(1);

    // book_seq=2: a deeper bid, doesn't touch the top on either side.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(90.0), Size(1.0)).has_value());
    L2Update deep_diff = updates_.wait_for(2);
    EXPECT_EQ(deep_diff.diff().book_seq(), 2u);

    // book_seq=3: a new best bid.
    ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(101.0), Size(1.0)).has_value());
    L2Update top_diff = updates_.wait_for(3);
    EXPECT_EQ(top_diff.diff().book_seq(), 3u);

    // The BBO stream must skip straight from book_seq=1 to book_seq=3 -
    // book_seq=2 (the deep change) never produced a Bbo message at all,
    // which is expected, not a loss (see Bbo's proto comment).
    BboUpdate bbo_after = bbo_sub->updates.wait_for(2);
    ASSERT_TRUE(bbo_after.has_bbo());
    EXPECT_EQ(bbo_after.bbo().book_seq(), 3u);
    EXPECT_EQ(bbo_after.bbo().bid().price_raw(), Price(101.0).raw());
}

TEST_F(AggregatorServiceTest, StuckBboSubscriberDoesNotBlockIngestionOrOtherSubscribers) {
    updates_.wait_for(0);

    // A BBO subscriber that never reads from its stream, simulating one
    // that's stopped draining - mirrors StuckSubscriberDoesNotBlock
    // IngestionOrOtherSubscribers above, but for the BBO stream/queue.
    grpc::ClientContext stuck_context;
    auto stuck_reader = stub_->SubscribeBbo(&stuck_context, subscribe_bbo_request(kSymbol));

    auto bbo_sub = subscribe_bbo();
    bbo_sub->updates.wait_for(0);  // initial (empty) Bbo for the fast BBO subscriber

    // Each iteration moves the best bid to a strictly higher price, so
    // every one of these produces a Bbo broadcast, not just an L2Diff.
    constexpr int kUpdates = 100;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kUpdates; ++i) {
        ASSERT_TRUE(book().apply_delta("binance", Side::Bid, Price(1.0 + i * 0.01), Size(1.0))
                        .has_value());
    }
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(2))
        << "ingestion stalled - likely blocked on the non-draining BBO subscriber";

    // The fast BBO subscriber must still have received every Bbo change -
    // the stuck one never gets to block or close it either way.
    bbo_sub->updates.wait_for(kUpdates);

    stuck_context.TryCancel();
}

// Not part of AggregatorServiceTest: these need their own multi-symbol
// AggregatorService instance rather than the fixture's single-symbol one.
class MultiSymbolAggregatorServiceTest : public ::testing::Test {
  protected:
    void SetUp() override {
        std::vector<std::string> symbols{"BTCUSDT", "ETHUSDT"};
        service_ = std::make_unique<AggregatorService>(symbols);

        grpc::ServerBuilder builder;
        int port = 0;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(service_.get());
        server_ = builder.BuildAndStart();
        ASSERT_NE(server_, nullptr);

        auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port),
                                            grpc::InsecureChannelCredentials());
        stub_ = Aggregator::NewStub(channel);
    }

    void TearDown() override { server_->Shutdown(); }

    std::unique_ptr<AggregatorService> service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<Aggregator::Stub> stub_;
};

TEST_F(MultiSymbolAggregatorServiceTest, SymbolsAreFullyIsolated) {
    grpc::ClientContext btc_context;
    auto btc_reader = stub_->SubscribeL2Diff(&btc_context, subscribe_l2_diff_request("BTCUSDT"));
    UpdateQueue<L2Update> btc_updates;
    std::thread btc_thread([&] {
        L2Update update;
        while (btc_reader->Read(&update)) btc_updates.push(update);
    });

    grpc::ClientContext eth_context;
    auto eth_reader = stub_->SubscribeL2Diff(&eth_context, subscribe_l2_diff_request("ETHUSDT"));
    UpdateQueue<L2Update> eth_updates;
    std::thread eth_thread([&] {
        L2Update update;
        while (eth_reader->Read(&update)) eth_updates.push(update);
    });

    btc_updates.wait_for(0);  // initial snapshot
    eth_updates.wait_for(0);

    // Ingestion routes through AggregatorService::book(symbol), the same
    // interface a real ingestion dispatch layer would use to reach the
    // right SymbolBook directly.
    ASSERT_TRUE(service_->book("BTCUSDT")
                    ->apply_delta("binance", Side::Bid, Price(100.0), Size(1.0))
                    .has_value());
    L2Update btc_diff = btc_updates.wait_for(1);
    ASSERT_TRUE(btc_diff.has_diff());
    EXPECT_EQ(btc_diff.diff().book_seq(), 1u);

    ASSERT_TRUE(service_->book("ETHUSDT")
                    ->apply_delta("binance", Side::Ask, Price(2000.0), Size(3.0))
                    .has_value());
    L2Update eth_diff = eth_updates.wait_for(1);
    ASSERT_TRUE(eth_diff.has_diff());
    // ETHUSDT's own seq starts independently at 1, not "2" - proof the two
    // symbols don't share a seq counter (or anything else): BTCUSDT's
    // update above must never have reached ETHUSDT's subscriber, and
    // vice versa below.
    EXPECT_EQ(eth_diff.diff().book_seq(), 1u);
    ASSERT_EQ(eth_diff.diff().asks_size(), 1);
    EXPECT_EQ(eth_diff.diff().asks(0).price_raw(), Price(2000.0).raw());
    EXPECT_EQ(eth_diff.diff().bids_size(), 0);

    btc_context.TryCancel();
    eth_context.TryCancel();
    if (btc_thread.joinable()) btc_thread.join();
    if (eth_thread.joinable()) eth_thread.join();
}

TEST_F(MultiSymbolAggregatorServiceTest, UnknownSymbolFailsWithNotFound) {
    grpc::ClientContext context;
    auto reader = stub_->SubscribeL2Diff(&context, subscribe_l2_diff_request("DOGEUSDT"));

    L2Update update;
    EXPECT_FALSE(reader->Read(&update));  // no snapshot ever sent - the RPC fails immediately

    grpc::Status status = reader->Finish();
    EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

}  // namespace
}  // namespace bobby::hermeneutic::aggregator

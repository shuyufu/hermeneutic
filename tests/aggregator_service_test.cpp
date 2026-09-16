#include "aggregator_service.hpp"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace bobby::hermeneutic::aggregator {
namespace {

// Thread-safe queue the client-reader thread pushes into and the test
// thread polls, so assertions can wait for a specific message to arrive
// without racing the streaming thread.
class UpdateQueue {
  public:
    void push(L2Update update) {
        std::lock_guard lock(mutex_);
        queue_.push_back(std::move(update));
        cv_.notify_all();
    }

    // Blocks until at least `index + 1` updates have arrived (or times out),
    // then returns the update at `index`.
    L2Update wait_for(std::size_t index) {
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
    std::deque<L2Update> queue_;
};

TEST(SubscriberQueueTest, DrainsInFifoOrderAndReportsResultKind) {
    SubscriberQueue queue(4);

    std::vector<L2Update> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue::DrainResult::TimedOut);
    EXPECT_TRUE(out.empty());

    L2Update first, second;
    first.mutable_heartbeat()->set_ts_ns(1);
    second.mutable_heartbeat()->set_ts_ns(2);
    EXPECT_TRUE(queue.push_or_close(first));
    EXPECT_TRUE(queue.push_or_close(second));

    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue::DrainResult::Drained);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].heartbeat().ts_ns(), 1u);
    EXPECT_EQ(out[1].heartbeat().ts_ns(), 2u);
}

TEST(SubscriberQueueTest, OverflowClosesAndDiscardsEverythingQueued) {
    SubscriberQueue queue(2);

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
              SubscriberQueue::DrainResult::Closed);
    EXPECT_TRUE(out.empty());  // the two successfully queued updates were discarded, not delivered
}

class AggregatorServiceTest : public ::testing::Test {
  protected:
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

        reader_ = stub_->Subscribe(&context_, SubscribeRequest{});
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

    AggregatorService service_;
    std::unique_ptr<grpc::Server> server_;
    std::unique_ptr<Aggregator::Stub> stub_;
    grpc::ClientContext context_;
    std::unique_ptr<grpc::ClientReaderInterface<L2Update>> reader_;
    std::thread reader_thread_;
    UpdateQueue updates_;
};

TEST_F(AggregatorServiceTest, SubscribingToEmptyBookYieldsEmptySnapshot) {
    L2Update first = updates_.wait_for(0);
    ASSERT_TRUE(first.has_snapshot());
    EXPECT_EQ(first.snapshot().seq(), 0u);
    EXPECT_EQ(first.snapshot().bids_size(), 0);
    EXPECT_EQ(first.snapshot().asks_size(), 0);
}

TEST_F(AggregatorServiceTest, ApplyDeltaAfterSubscribeProducesDiff) {
    updates_.wait_for(0);  // initial snapshot

    auto result = service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0));
    ASSERT_TRUE(result.has_value());

    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(1.0).raw());
    EXPECT_EQ(msg.diff().asks_size(), 0);
}

TEST_F(AggregatorServiceTest, MultipleVenuesAggregateAndPartialRemovalKeepsRemainder) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    EXPECT_EQ(updates_.wait_for(1).diff().bids(0).size_raw(), Size(1.0).raw());

    ASSERT_TRUE(service_.apply_delta("okx", Side::Bid, Price(100.0), Size(2.0)).has_value());
    EXPECT_EQ(updates_.wait_for(2).diff().bids(0).size_raw(), Size(3.0).raw());

    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(0.0)).has_value());
    L2Update msg = updates_.wait_for(3);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    // okx's remaining size, not a removal (binance's own contribution was
    // the only thing zeroed).
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(2.0).raw());
}

TEST_F(AggregatorServiceTest, InvalidateVenueRemovesOnlyItsExclusiveLevels) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(service_.apply_delta("okx", Side::Bid, Price(100.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Ask, Price(101.0), Size(5.0)).has_value());
    updates_.wait_for(3);

    service_.invalidate_venue("binance");
    L2Update msg = updates_.wait_for(4);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().seq(), 4u);

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
    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(103.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(service_.apply_delta("okx", Side::Bid, Price(100.0), Size(5.0)).has_value());
    updates_.wait_for(3);

    // Replace binance's entire bid side: 100 changes size, 103 is dropped
    // (absent from the new levels), 102 is a brand new price.
    std::array<std::pair<Price, Size>, 2> levels{{
        {Price(100.0), Size(3.0)},
        {Price(102.0), Size(4.0)},
    }};
    auto result = service_.apply_snapshot("binance", Side::Bid, levels);
    ASSERT_TRUE(result.has_value());

    L2Update msg = updates_.wait_for(4);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().seq(), 4u);
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

    ASSERT_TRUE(service_.apply_delta("binance", Side::Ask, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Ask, Price(105.0), Size(2.0)).has_value());
    updates_.wait_for(2);

    // Replace binance's entire ask side: 101 changes size, 105 is dropped,
    // 103 is a brand new price.
    std::array<std::pair<Price, Size>, 2> levels{{
        {Price(101.0), Size(4.0)},
        {Price(103.0), Size(3.0)},
    }};
    ASSERT_TRUE(service_.apply_snapshot("binance", Side::Ask, levels).has_value());

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

    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(102.0), Size(1.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(3);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Ask, Price(105.0), Size(1.0)).has_value());
    updates_.wait_for(4);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Ask, Price(103.0), Size(1.0)).has_value());
    updates_.wait_for(5);
    ASSERT_TRUE(service_.apply_delta("binance", Side::Ask, Price(104.0), Size(1.0)).has_value());
    updates_.wait_for(6);

    // A second, independent subscriber joining now must see the book's own
    // ordering directly in its initial snapshot: bids descending, asks
    // ascending.
    grpc::ClientContext second_context;
    auto second_reader = stub_->Subscribe(&second_context, SubscribeRequest{});
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

    auto bad = service_.apply_delta("binance", Side::Bid, Price(100.0), Size(-1.0));
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), std::errc::invalid_argument);

    auto good = service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0));
    ASSERT_TRUE(good.has_value());

    // If the failed call had broadcast anything, this would be seq 2 / the
    // third message overall instead of seq 1 / the second.
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(1.0).raw());
}

TEST_F(AggregatorServiceTest, NoOpApplyDeltaDoesNotBroadcast) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);

    // Re-applying the exact same size changes nothing in the aggregate.
    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());

    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(2.0)).has_value());

    // If the no-op call had broadcast anything, this would be seq 3 / the
    // third message overall instead of seq 2 / the second.
    L2Update msg = updates_.wait_for(2);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().seq(), 2u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(2.0).raw());
}

TEST_F(AggregatorServiceTest, HeartbeatIsDeliveredAndDoesNotAdvanceSeq) {
    updates_.wait_for(0);  // initial snapshot

    service_.send_heartbeat();
    L2Update heartbeat_msg = updates_.wait_for(1);
    ASSERT_TRUE(heartbeat_msg.has_heartbeat());
    EXPECT_NE(heartbeat_msg.heartbeat().ts_ns(), 0u);

    // A real book change right after must still be seq 1 / the third
    // message overall - proof the heartbeat above didn't touch seq_.
    ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(100.0), Size(1.0)).has_value());
    L2Update diff_msg = updates_.wait_for(2);
    ASSERT_TRUE(diff_msg.has_diff());
    EXPECT_EQ(diff_msg.diff().seq(), 1u);
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
    auto stuck_reader = stub_->Subscribe(&stuck_context, SubscribeRequest{});

    // Stays comfortably under kSubscriberQueueCapacity (256): this test is
    // about a non-draining subscriber not blocking anyone else, not about
    // burst volume exceeding a single subscriber's own queue capacity (a
    // real but separate concern - a big enough burst can overflow even an
    // actively-draining subscriber if it can't Write() fast enough, which
    // would otherwise make this test flaky for the wrong reason).
    constexpr int kUpdates = 100;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kUpdates; ++i) {
        ASSERT_TRUE(service_.apply_delta("binance", Side::Bid, Price(1.0 + i * 0.01), Size(1.0))
                        .has_value());
    }
    auto elapsed = std::chrono::steady_clock::now() - start;
    EXPECT_LT(elapsed, std::chrono::seconds(2))
        << "ingestion stalled - likely blocked on the non-draining subscriber";

    // The fast subscriber must still have received every diff.
    updates_.wait_for(kUpdates);

    stuck_context.TryCancel();
}

}  // namespace
}  // namespace bobby::hermeneutic::aggregator

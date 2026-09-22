#include "apps/aggregator/aggregator_service.hpp"

#include <gtest/gtest.h>

#include <algorithm>
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
#include <vector>

#include "apps/aggregator/book_id.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic::aggregator {
namespace {

using bobby::hermeneutic::symbol::BaseQuote;
using bobby::hermeneutic::symbol::BookId;
using bobby::hermeneutic::symbol::Exchange;
using bobby::hermeneutic::symbol::MarketType;

// Two arbitrary, distinct VenueIds - these tests only need "two different
// venues" to exercise per-venue isolation on the underlying
// AggregateOrderBook, never the real to_string(VenueId) spelling, so
// MarketType::Spot on both is an arbitrary (but fixed) choice, not a
// claim about what market either actually covers.
constexpr bobby::hermeneutic::VenueId kBinance{Exchange::Binance, MarketType::Spot};
constexpr bobby::hermeneutic::VenueId kOkx{Exchange::Okx, MarketType::Spot};

// SymbolBook has no single-level apply_delta() (see AggregateOrderBook's
// own doc comment on apply_batch() for why - it's unused on the real
// ingestion path, and apply_batch() is its exact functional superset).
// This recreates that old single-level convenience purely for these tests,
// via apply_batch() with a one-element span on the requested side and an
// empty span on the other.
std::expected<void, std::errc> apply_one(SymbolBook& book, const bobby::hermeneutic::VenueId& venue,
                                          Side side, Price price, Size size) {
    std::array<std::pair<Price, Size>, 1> level{{{price, size}}};
    if (side == Side::Bid) return book.apply_batch(venue, level, {});
    return book.apply_batch(venue, {}, level);
}

SubscribeL2DiffRequest subscribe_l2_diff_request(const BookId& book_id) {
    SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), book_id);
    return request;
}

SubscribeBboRequest subscribe_bbo_request(const BookId& book_id) {
    SubscribeBboRequest request;
    fill_wire_book_id(request.mutable_book(), book_id);
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

std::unique_ptr<BboSubscription> subscribe_bbo(Aggregator::Stub& stub, const BookId& book_id) {
    auto sub = std::make_unique<BboSubscription>();
    sub->reader = stub.SubscribeBbo(&sub->context, subscribe_bbo_request(book_id));
    sub->thread = std::thread([raw = sub.get()] {
        BboUpdate update;
        while (raw->reader->Read(&update)) raw->updates.push(update);
    });
    return sub;
}

TEST(SubscriberQueueTest, DrainsInFifoOrderAndReportsResultKind) {
    SubscriberQueue<L2Update> queue(4, OverflowPolicy::Close);

    std::vector<std::shared_ptr<const L2Update>> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<L2Update>::DrainResult::TimedOut);
    EXPECT_TRUE(out.empty());

    auto first = std::make_shared<const L2Update>([] {
        L2Update u;
        u.mutable_heartbeat()->set_ts_ns(1);
        return u;
    }());
    auto second = std::make_shared<const L2Update>([] {
        L2Update u;
        u.mutable_heartbeat()->set_ts_ns(2);
        return u;
    }());
    EXPECT_EQ(queue.push_or_close(first), SubscriberQueue<L2Update>::PushResult::Pushed);
    EXPECT_EQ(queue.push_or_close(second), SubscriberQueue<L2Update>::PushResult::Pushed);

    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<L2Update>::DrainResult::Drained);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0]->heartbeat().ts_ns(), 1u);
    EXPECT_EQ(out[1]->heartbeat().ts_ns(), 2u);
}

TEST(SubscriberQueueTest, BootstrappingAllowsAHigherCapacityThanSteadyState) {
    SubscriberQueue<L2Update> queue(2, OverflowPolicy::Close);

    // A freshly constructed queue starts in "bootstrapping" mode (see
    // SubscriberQueue's own class comment): capacity is raised by
    // kBootstrapCapacityMultiplier (4x) rather than removed, since
    // nobody has started draining yet and the caller that will
    // eventually own this queue (SymbolBook::subscribe()) hasn't even
    // finished its own initial Write() - but it still has to be a real
    // ceiling, not "no limit," or a client that connects and stops
    // reading could grow this queue without bound. 8 pushes (2*4) into a
    // capacity-2 queue must all still succeed here, even though the same
    // burst would have closed a steady-state (post-end_bootstrap())
    // queue after just 2.
    auto update = std::make_shared<const L2Update>();
    for (int i = 0; i < 8; ++i) {
        EXPECT_EQ(queue.push_or_close(update), SubscriberQueue<L2Update>::PushResult::Pushed);
    }

    // The 9th push exceeds even the bootstrap ceiling - the queue must
    // still close rather than grow further.
    EXPECT_EQ(queue.push_or_close(update), SubscriberQueue<L2Update>::PushResult::ClosedNow);

    std::vector<std::shared_ptr<const L2Update>> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<L2Update>::DrainResult::Closed);
    EXPECT_TRUE(out.empty());  // closed, so the 8 successfully queued updates were discarded
}

TEST(SubscriberQueueTest, CloseOverflowClosesAndDiscardsEverythingQueued) {
    SubscriberQueue<L2Update> queue(2, OverflowPolicy::Close);
    queue.end_bootstrap();  // capacity/OverflowPolicy only apply after this

    auto update = std::make_shared<const L2Update>();
    ASSERT_EQ(queue.push_or_close(update), SubscriberQueue<L2Update>::PushResult::Pushed);
    ASSERT_EQ(queue.push_or_close(update), SubscriberQueue<L2Update>::PushResult::Pushed);
    // Third push finds the queue already at capacity: closes it instead of
    // dropping the oldest entry, since a gap in an L2Diff stream leaves the
    // subscriber's book genuinely wrong - only a fresh snapshot recovers it,
    // so there's nothing worth keeping once it's fallen this far behind.
    EXPECT_EQ(queue.push_or_close(update), SubscriberQueue<L2Update>::PushResult::ClosedNow);
    // Pushes after closing are also rejected, not re-queued.
    EXPECT_EQ(queue.push_or_close(update), SubscriberQueue<L2Update>::PushResult::AlreadyClosed);

    std::vector<std::shared_ptr<const L2Update>> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<L2Update>::DrainResult::Closed);
    EXPECT_TRUE(out.empty());  // the two successfully queued updates were discarded, not delivered
}

TEST(SubscriberQueueTest, DropOldestOverflowKeepsNewestWithoutClosing) {
    SubscriberQueue<BboUpdate> queue(2, OverflowPolicy::DropOldest);
    queue.end_bootstrap();  // capacity/OverflowPolicy only apply after this

    auto first = std::make_shared<const BboUpdate>([] {
        BboUpdate u;
        u.mutable_bbo()->set_book_seq(1);
        return u;
    }());
    auto second = std::make_shared<const BboUpdate>([] {
        BboUpdate u;
        u.mutable_bbo()->set_book_seq(2);
        return u;
    }());
    auto third = std::make_shared<const BboUpdate>([] {
        BboUpdate u;
        u.mutable_bbo()->set_book_seq(3);
        return u;
    }());

    ASSERT_EQ(queue.push_or_close(first), SubscriberQueue<BboUpdate>::PushResult::Pushed);
    ASSERT_EQ(queue.push_or_close(second), SubscriberQueue<BboUpdate>::PushResult::Pushed);
    // Third push overflows capacity 2: drops the oldest (seq 1) instead of
    // closing, since a stale Bbo costs nothing to skip - each one is a
    // complete, self-contained state, not a delta.
    EXPECT_EQ(queue.push_or_close(third), SubscriberQueue<BboUpdate>::PushResult::Pushed);

    std::vector<std::shared_ptr<const BboUpdate>> out;
    EXPECT_EQ(queue.wait_and_drain(std::chrono::milliseconds(10), out),
              SubscriberQueue<BboUpdate>::DrainResult::Drained);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0]->bbo().book_seq(), 2u);
    EXPECT_EQ(out[1]->bbo().book_seq(), 3u);
}

class AggregatorServiceTest : public ::testing::Test {
  protected:
    static BookId TestBook() { return BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot}; }

    AggregatorServiceTest() : service_(std::vector<BookId>{TestBook()}) {}

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

        reader_ = stub_->SubscribeL2Diff(&context_, subscribe_l2_diff_request(TestBook()));
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
    // would.
    SymbolBook& book() { return *service_.book(TestBook()); }

    std::unique_ptr<BboSubscription> subscribe_bbo() {
        return ::bobby::hermeneutic::aggregator::subscribe_bbo(*stub_, TestBook());
    }

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

TEST_F(AggregatorServiceTest, MultipleVenuesAggregateAndPartialRemovalKeepsRemainder) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    EXPECT_EQ(updates_.wait_for(1).diff().bids(0).size_raw(), Size(1.0).raw());

    ASSERT_TRUE(apply_one(book(), kOkx, Side::Bid, Price(100.0), Size(2.0)).has_value());
    EXPECT_EQ(updates_.wait_for(2).diff().bids(0).size_raw(), Size(3.0).raw());

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(0.0)).has_value());
    L2Update msg = updates_.wait_for(3);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    // okx's remaining size, not a removal (binance's own contribution was
    // the only thing zeroed).
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(2.0).raw());
}

TEST_F(AggregatorServiceTest, InvalidateVenueRemovesOnlyItsExclusiveLevels) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(apply_one(book(), kOkx, Side::Bid, Price(100.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(101.0), Size(5.0)).has_value());
    updates_.wait_for(3);

    book().invalidate_venue(kBinance);
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
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(103.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(apply_one(book(), kOkx, Side::Bid, Price(100.0), Size(5.0)).has_value());
    updates_.wait_for(3);

    // Replace binance's entire bid side: 100 changes size, 103 is dropped
    // (absent from the new levels), 102 is a brand new price.
    std::array<std::pair<Price, Size>, 2> levels{{
        {Price(100.0), Size(3.0)},
        {Price(102.0), Size(4.0)},
    }};
    auto result = book().apply_snapshot(kBinance, levels, {});
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

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(105.0), Size(2.0)).has_value());
    updates_.wait_for(2);

    // Replace binance's entire ask side: 101 changes size, 105 is dropped,
    // 103 is a brand new price.
    std::array<std::pair<Price, Size>, 2> levels{{
        {Price(101.0), Size(4.0)},
        {Price(103.0), Size(3.0)},
    }};
    ASSERT_TRUE(book().apply_snapshot(kBinance, {}, levels).has_value());

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

TEST_F(AggregatorServiceTest, ApplySnapshotBothSidesProducesOneSeqBump) {
    updates_.wait_for(0);  // initial snapshot

    std::array<std::pair<Price, Size>, 1> bids{{
        {Price(100.0), Size(1.0)},
    }};
    std::array<std::pair<Price, Size>, 1> asks{{
        {Price(101.0), Size(2.0)},
    }};
    ASSERT_TRUE(book().apply_snapshot(kBinance, bids, asks).has_value());

    // One message, seq 1 - not two separate diffs the way two independent
    // per-side apply_snapshot() calls used to produce.
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(msg.diff().bids(0).size_raw(), Size(1.0).raw());
    ASSERT_EQ(msg.diff().asks_size(), 1);
    EXPECT_EQ(msg.diff().asks(0).price_raw(), Price(101.0).raw());
    EXPECT_EQ(msg.diff().asks(0).size_raw(), Size(2.0).raw());
}

// Guards against a bad level on one side going through while the other,
// valid side is already applied and broadcast: apply_snapshot() must
// validate both sides before touching either.
TEST_F(AggregatorServiceTest, ApplySnapshotRejectsBadLevelOnEitherSideWithoutMutatingOrBroadcasting) {
    updates_.wait_for(0);  // initial snapshot

    std::array<std::pair<Price, Size>, 1> good_bids{{
        {Price(100.0), Size(1.0)},
    }};
    std::array<std::pair<Price, Size>, 1> bad_asks{{
        {Price(101.0), Size(-1.0)},
    }};
    auto bad = book().apply_snapshot(kBinance, good_bids, bad_asks);
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), std::errc::invalid_argument);

    // Neither side from the rejected snapshot was applied: a good call
    // right after must be seq 1 / the second message, not seq 2 / the
    // third (which a stray broadcast from the rejected bids side would
    // have produced).
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
}

TEST_F(AggregatorServiceTest, SnapshotOrderingMatchesBookConvention) {
    updates_.wait_for(0);  // initial (empty) snapshot for the fixture's own subscriber

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(102.0), Size(1.0)).has_value());
    updates_.wait_for(2);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(3);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(105.0), Size(1.0)).has_value());
    updates_.wait_for(4);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(103.0), Size(1.0)).has_value());
    updates_.wait_for(5);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(104.0), Size(1.0)).has_value());
    updates_.wait_for(6);

    // A second, independent subscriber joining now must see the book's own
    // ordering directly in its initial snapshot: bids descending, asks
    // ascending.
    grpc::ClientContext second_context;
    auto second_reader = stub_->SubscribeL2Diff(&second_context, subscribe_l2_diff_request(TestBook()));
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

TEST_F(AggregatorServiceTest, NoOpChangeDoesNotBroadcast) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);

    // Re-applying the exact same size changes nothing in the aggregate.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(2.0)).has_value());

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
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    L2Update diff_msg = updates_.wait_for(2);
    ASSERT_TRUE(diff_msg.has_diff());
    EXPECT_EQ(diff_msg.diff().book_seq(), 1u);
}

// Guards live_venues content itself, which HeartbeatIsDeliveredAndDoesNotAdvanceSeq
// above doesn't check (only ts_ns()). Each of the three data-changing calls
// below is chosen to also move the top of book (a first bid, a first ask,
// then removing the only bid), so the L2 and Bbo streams stay in exact
// 1:1 lockstep - both get one message per action, letting the same
// wait_for() indices check both streams without separately tracking
// whether a given change happened to touch the top of book.
TEST_F(AggregatorServiceTest, HeartbeatLiveVenuesTracksContributingVenues) {
    auto bbo = subscribe_bbo();
    updates_.wait_for(0);      // initial L2 snapshot
    bbo->updates.wait_for(0);  // initial Bbo

    // Nothing has contributed yet - both streams' heartbeats should agree
    // on an empty live_venues.
    book().send_heartbeat();
    L2Update hb1 = updates_.wait_for(1);
    ASSERT_TRUE(hb1.has_heartbeat());
    EXPECT_EQ(hb1.heartbeat().live_venues_size(), 0);
    BboUpdate bbo_hb1 = bbo->updates.wait_for(1);
    ASSERT_TRUE(bbo_hb1.has_heartbeat());
    EXPECT_EQ(bbo_hb1.heartbeat().live_venues_size(), 0);

    // kBinance contributes the book's first-ever bid.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(2);
    bbo->updates.wait_for(2);

    book().send_heartbeat();
    L2Update hb2 = updates_.wait_for(3);
    ASSERT_TRUE(hb2.has_heartbeat());
    ASSERT_EQ(hb2.heartbeat().live_venues_size(), 1);
    EXPECT_EQ(hb2.heartbeat().live_venues(0), bobby::hermeneutic::symbol::to_string(kBinance));
    BboUpdate bbo_hb2 = bbo->updates.wait_for(3);
    ASSERT_TRUE(bbo_hb2.has_heartbeat());
    ASSERT_EQ(bbo_hb2.heartbeat().live_venues_size(), 1);
    EXPECT_EQ(bbo_hb2.heartbeat().live_venues(0), bobby::hermeneutic::symbol::to_string(kBinance));

    // kOkx contributes the book's first-ever ask - both venues now live.
    ASSERT_TRUE(apply_one(book(), kOkx, Side::Ask, Price(101.0), Size(2.0)).has_value());
    updates_.wait_for(4);
    bbo->updates.wait_for(4);

    book().send_heartbeat();
    L2Update hb3 = updates_.wait_for(5);
    ASSERT_TRUE(hb3.has_heartbeat());
    std::vector<std::string> venues3(hb3.heartbeat().live_venues().begin(), hb3.heartbeat().live_venues().end());
    std::sort(venues3.begin(), venues3.end());
    std::vector<std::string> expected3{bobby::hermeneutic::symbol::to_string(kBinance),
                                        bobby::hermeneutic::symbol::to_string(kOkx)};
    std::sort(expected3.begin(), expected3.end());
    EXPECT_EQ(venues3, expected3);
    // Bbo's own heartbeat must report the exact same set as L2's - proves
    // send_heartbeat() copying l2_heartbeat's already-built field into
    // bbo_heartbeat actually keeps both streams in sync, not just
    // coincidentally similar.
    BboUpdate bbo_hb3 = bbo->updates.wait_for(5);
    ASSERT_TRUE(bbo_hb3.has_heartbeat());
    std::vector<std::string> bbo_venues3(bbo_hb3.heartbeat().live_venues().begin(),
                                          bbo_hb3.heartbeat().live_venues().end());
    std::sort(bbo_venues3.begin(), bbo_venues3.end());
    EXPECT_EQ(bbo_venues3, expected3);

    // Invalidating kBinance removes the book's only bid - live_venues
    // shrinks back down to just kOkx, on both streams.
    book().invalidate_venue(kBinance);
    updates_.wait_for(6);
    bbo->updates.wait_for(6);

    book().send_heartbeat();
    L2Update hb4 = updates_.wait_for(7);
    ASSERT_TRUE(hb4.has_heartbeat());
    ASSERT_EQ(hb4.heartbeat().live_venues_size(), 1);
    EXPECT_EQ(hb4.heartbeat().live_venues(0), bobby::hermeneutic::symbol::to_string(kOkx));
    BboUpdate bbo_hb4 = bbo->updates.wait_for(7);
    ASSERT_TRUE(bbo_hb4.has_heartbeat());
    ASSERT_EQ(bbo_hb4.heartbeat().live_venues_size(), 1);
    EXPECT_EQ(bbo_hb4.heartbeat().live_venues(0), bobby::hermeneutic::symbol::to_string(kOkx));
}

// The AggregateOrderBook.ApplyBatch* cases in aggregate_order_book_test.cpp
// cover apply_batch()'s own aggregation/atomicity behavior directly and
// cheaply, in the always-built hermeneutic_tests binary. The three cases
// below are SymbolBook tests, not AggregateOrderBook tests that happen to
// live in the wrong file: they check book_seq/diff broadcast semantics
// (one seq bump per batch, diff ordering, no broadcast on rejection) that
// only exist at this layer, which is why they need the real gRPC server
// this fixture spins up and are gated behind HERMENEUTIC_BUILD_SERVICE.
TEST_F(AggregatorServiceTest, ApplyBatchProducesOneSeqBumpForMultipleLevels) {
    updates_.wait_for(0);  // initial snapshot

    std::array<std::pair<Price, Size>, 2> bids{{
        {Price(102.0), Size(1.0)},
        {Price(100.0), Size(2.0)},
    }};
    std::array<std::pair<Price, Size>, 1> asks{{
        {Price(101.0), Size(3.0)},
    }};
    ASSERT_TRUE(book().apply_batch(kBinance, bids, asks).has_value());

    // One message, seq 1 - not three separate diffs the way three
    // separate single-level calls for the same levels would have produced.
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

// AggregateOrderBook.ApplyBatchAggregatesAcrossVenues (aggregate_order_book_test.cpp)
// covers this same scenario's arithmetic cheaply, but only at the in-memory
// aggregate() layer - it can't see SymbolBook's own publish()/
// collect_changes() wiring. This is the one case that puts both together:
// a multi-level batch (so ordering/collect_changes has more than one price
// to get right) where one of those levels also happens to overlap another
// venue's existing contribution (so the aggregation itself isn't trivial),
// verified all the way through to the wire-level L2Diff.
TEST_F(AggregatorServiceTest, ApplyBatchMultiLevelAggregatesAcrossVenuesOverWire) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(apply_one(book(), kOkx, Side::Bid, Price(100.0), Size(5.0)).has_value());
    updates_.wait_for(1);

    // binance's batch touches the same price okx already holds, plus a
    // new one - the aggregate must reflect both venues' contributions.
    std::array<std::pair<Price, Size>, 2> bids{{
        {Price(100.0), Size(3.0)},
        {Price(99.0), Size(1.0)},
    }};
    ASSERT_TRUE(book().apply_batch(kBinance, bids, {}).has_value());

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
    auto bad = book().apply_batch(kBinance, bad_bids, {});
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), std::errc::invalid_argument);

    // Neither level from the rejected batch was applied: a good call right
    // after must be seq 1 / the second message, not seq 2 / the third.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
}

// Same as the negative-size case above, but for a non-positive price -
// this class's own pre-validation loop (the cheap rejection before
// mutex_/before_bids/before_asks) has to check price too, not just size,
// or a bad price would sail through it and only get caught later by
// book_.apply_batch()'s own validation - still correct, but after paying
// for lookups this batch was always going to fail anyway.
TEST_F(AggregatorServiceTest, ApplyBatchRejectsNonPositivePriceWithoutMutatingOrBroadcasting) {
    updates_.wait_for(0);  // initial snapshot

    std::array<std::pair<Price, Size>, 2> bad_bids{{
        {Price(100.0), Size(1.0)},
        {Price::from_raw(0), Size(1.0)},
    }};
    auto bad = book().apply_batch(kBinance, bad_bids, {});
    ASSERT_FALSE(bad.has_value());
    EXPECT_EQ(bad.error(), std::errc::invalid_argument);

    // Neither level from the rejected batch was applied: a good call right
    // after must be seq 1 / the second message, not seq 2 / the third.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    L2Update msg = updates_.wait_for(1);
    ASSERT_TRUE(msg.has_diff());
    EXPECT_EQ(msg.diff().book_seq(), 1u);
    ASSERT_EQ(msg.diff().bids_size(), 1);
    EXPECT_EQ(msg.diff().bids(0).price_raw(), Price(100.0).raw());
}

TEST_F(AggregatorServiceTest, StuckSubscriberDoesNotBlockIngestionOrOtherSubscribers) {
    updates_.wait_for(0);  // initial snapshot for the fixture's own (fast) subscriber

    // A second subscriber that never reads from its stream, simulating one
    // that's stopped draining. Fanout::broadcast() only ever does a
    // non-blocking push into each subscriber's own queue (see
    // SubscriberQueue::push_or_close), so ingestion and the first (fast)
    // subscriber must be unaffected by this one - regardless of whether its
    // queue has overflowed yet, which depends on OS-level socket buffering
    // this test doesn't control and so doesn't assert on.
    grpc::ClientContext stuck_context;
    auto stuck_reader = stub_->SubscribeL2Diff(&stuck_context, subscribe_l2_diff_request(TestBook()));

    // Stays comfortably under kSubscriberQueueCapacity (256): this test is
    // about a non-draining subscriber not blocking anyone else, not about
    // burst volume exceeding a single subscriber's own queue capacity (a
    // real but separate concern - a big enough burst can overflow even an
    // actively-draining subscriber if it can't Write() fast enough, which
    // would otherwise make this test flaky for the wrong reason).
    constexpr int kUpdates = 100;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kUpdates; ++i) {
        ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(1.0 + i * 0.01), Size(1.0))
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

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(101.0), Size(2.0)).has_value());
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
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);  // corresponding L2Diff, book_seq=1
    BboUpdate first_bbo = bbo_sub->updates.wait_for(1);
    ASSERT_TRUE(first_bbo.has_bbo());
    EXPECT_EQ(first_bbo.bbo().book_seq(), 1u);
    EXPECT_EQ(first_bbo.bbo().bid().price_raw(), Price(100.0).raw());

    // A worse (deeper) bid level doesn't change the best bid, so this
    // revision (book_seq=2) must produce an L2Diff but no new Bbo at all -
    // Bbo.book_seq is allowed to skip book_seq=2 entirely.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(99.0), Size(5.0)).has_value());
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

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);

    auto bbo_sub = subscribe_bbo();
    BboUpdate first = bbo_sub->updates.wait_for(0);
    EXPECT_EQ(first.bbo().bid().size_raw(), Size(1.0).raw());

    // Same best price, different size (a second venue joins at the same
    // level) - still a top-of-book change, not just a deep-book one.
    ASSERT_TRUE(apply_one(book(), kOkx, Side::Bid, Price(100.0), Size(2.0)).has_value());
    updates_.wait_for(2);
    BboUpdate second = bbo_sub->updates.wait_for(1);
    ASSERT_TRUE(second.has_bbo());
    EXPECT_EQ(second.bbo().bid().size_raw(), Size(3.0).raw());
}

TEST_F(AggregatorServiceTest, BestSideDisappearingIsReportedAsAbsent) {
    updates_.wait_for(0);  // initial snapshot

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(101.0), Size(1.0)).has_value());
    updates_.wait_for(1);

    auto bbo_sub = subscribe_bbo();
    BboUpdate first = bbo_sub->updates.wait_for(0);
    ASSERT_TRUE(first.bbo().has_ask());

    // Zeroing out the only ask level removes it entirely - the resulting
    // Bbo must represent "no ask" via message-field absence, not a sentinel
    // price or a zero size.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Ask, Price(101.0), Size(0.0)).has_value());
    updates_.wait_for(2);
    BboUpdate second = bbo_sub->updates.wait_for(1);
    ASSERT_TRUE(second.has_bbo());
    EXPECT_FALSE(second.bbo().has_ask());
}

TEST_F(AggregatorServiceTest, BboBookSeqCorrelatesWithL2DiffBookSeqAndSkipsNonTopRevisions) {
    updates_.wait_for(0);  // initial L2 snapshot

    auto bbo_sub = subscribe_bbo();
    bbo_sub->updates.wait_for(0);  // initial (empty) Bbo

    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(100.0), Size(1.0)).has_value());
    updates_.wait_for(1);  // book_seq=1, top-of-book change
    bbo_sub->updates.wait_for(1);

    // book_seq=2: a deeper bid, doesn't touch the top on either side.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(90.0), Size(1.0)).has_value());
    L2Update deep_diff = updates_.wait_for(2);
    EXPECT_EQ(deep_diff.diff().book_seq(), 2u);

    // book_seq=3: a new best bid.
    ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(101.0), Size(1.0)).has_value());
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
    auto stuck_reader = stub_->SubscribeBbo(&stuck_context, subscribe_bbo_request(TestBook()));

    auto bbo_sub = subscribe_bbo();
    bbo_sub->updates.wait_for(0);  // initial (empty) Bbo for the fast BBO subscriber

    // Each iteration moves the best bid to a strictly higher price, so
    // every one of these produces a Bbo broadcast, not just an L2Diff.
    constexpr int kUpdates = 100;
    auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < kUpdates; ++i) {
        ASSERT_TRUE(apply_one(book(), kBinance, Side::Bid, Price(1.0 + i * 0.01), Size(1.0))
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

TEST_F(AggregatorServiceTest, ListBooksReturnsTheSingleConfiguredBook) {
    grpc::ClientContext context;
    ListBooksRequest request;
    ListBooksResponse response;
    grpc::Status status = stub_->ListBooks(&context, request, &response);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(response.books_size(), 1);
    auto id = to_symbol_book_id(response.books(0));
    ASSERT_TRUE(id.has_value());
    EXPECT_EQ(*id, TestBook());
}

// Not part of AggregatorServiceTest: these need their own multi-symbol
// AggregatorService instance rather than the fixture's single-symbol one.
class MultiSymbolAggregatorServiceTest : public ::testing::Test {
  protected:
    static BookId BtcBook() { return BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot}; }
    static BookId EthBook() { return BookId{BaseQuote{{"ETH"}, {"USDT"}}, MarketType::Spot}; }
    // Same base/quote as BtcBook() but MarketType::Perp - a distinct BookId
    // (symbol::BookId equality includes market type) that exists purely to
    // exercise to_symbol_book_id()/fill_wire_book_id()'s PERP branch, which
    // every other book in this file leaves untouched (all Spot).
    static BookId BtcPerpBook() { return BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Perp}; }

    void SetUp() override {
        std::vector<BookId> books{BtcBook(), EthBook(), BtcPerpBook()};
        service_ = std::make_unique<AggregatorService>(books);

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
    auto btc_reader = stub_->SubscribeL2Diff(&btc_context, subscribe_l2_diff_request(BtcBook()));
    UpdateQueue<L2Update> btc_updates;
    std::thread btc_thread([&] {
        L2Update update;
        while (btc_reader->Read(&update)) btc_updates.push(update);
    });

    grpc::ClientContext eth_context;
    auto eth_reader = stub_->SubscribeL2Diff(&eth_context, subscribe_l2_diff_request(EthBook()));
    UpdateQueue<L2Update> eth_updates;
    std::thread eth_thread([&] {
        L2Update update;
        while (eth_reader->Read(&update)) eth_updates.push(update);
    });

    btc_updates.wait_for(0);  // initial snapshot
    eth_updates.wait_for(0);

    // Ingestion routes through AggregatorService::book(id), the same
    // interface a real ingestion dispatch layer would use to reach the
    // right SymbolBook directly.
    ASSERT_TRUE(apply_one(*service_->book(BtcBook()), kBinance, Side::Bid, Price(100.0), Size(1.0))
                    .has_value());
    L2Update btc_diff = btc_updates.wait_for(1);
    ASSERT_TRUE(btc_diff.has_diff());
    EXPECT_EQ(btc_diff.diff().book_seq(), 1u);

    ASSERT_TRUE(apply_one(*service_->book(EthBook()), kBinance, Side::Ask, Price(2000.0), Size(3.0))
                    .has_value());
    L2Update eth_diff = eth_updates.wait_for(1);
    ASSERT_TRUE(eth_diff.has_diff());
    // ETH's own seq starts independently at 1, not "2" - proof the two
    // symbols don't share a seq counter (or anything else): BTC's
    // update above must never have reached ETH's subscriber, and
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
    // A well-formed BookId (valid base/quote/market) that just isn't one of
    // this server's two books - distinct from a malformed request, see
    // MalformedBookFailsWithInvalidArgument below.
    grpc::ClientContext context;
    auto reader = stub_->SubscribeL2Diff(
        &context, subscribe_l2_diff_request(BookId{BaseQuote{{"DOGE"}, {"USDT"}}, MarketType::Spot}));

    L2Update update;
    EXPECT_FALSE(reader->Read(&update));  // no snapshot ever sent - the RPC fails immediately

    grpc::Status status = reader->Finish();
    EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

TEST_F(MultiSymbolAggregatorServiceTest, MalformedBookFailsWithInvalidArgument) {
    // Left unset: proto3 defaults base/quote to "" and market to
    // MARKET_TYPE_UNSPECIFIED - a malformed request no well-formed client
    // could produce via fill_wire_book_id(), but still a possible message
    // on the wire (a stale/buggy client, or nothing set at all). Must fail
    // differently from UnknownSymbolFailsWithNotFound above - INVALID_ARGUMENT
    // for "not a book at all", not NOT_FOUND for "not one of ours".
    grpc::ClientContext context;
    SubscribeL2DiffRequest request;
    auto reader = stub_->SubscribeL2Diff(&context, request);

    L2Update update;
    EXPECT_FALSE(reader->Read(&update));

    grpc::Status status = reader->Finish();
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

// SubscribeBbo counterpart of UnknownSymbolFailsWithNotFound above -
// SubscribeBbo() runs the exact same to_symbol_book_id()/book() sequence as
// SubscribeL2Diff() (see AggregatorService::SubscribeBbo()), but nothing
// previously exercised it directly.
TEST_F(MultiSymbolAggregatorServiceTest, UnknownSymbolFailsWithNotFoundBbo) {
    grpc::ClientContext context;
    auto reader = stub_->SubscribeBbo(
        &context, subscribe_bbo_request(BookId{BaseQuote{{"DOGE"}, {"USDT"}}, MarketType::Spot}));

    BboUpdate update;
    EXPECT_FALSE(reader->Read(&update));

    grpc::Status status = reader->Finish();
    EXPECT_EQ(status.error_code(), grpc::StatusCode::NOT_FOUND);
}

// SubscribeBbo counterpart of MalformedBookFailsWithInvalidArgument above.
TEST_F(MultiSymbolAggregatorServiceTest, MalformedBookFailsWithInvalidArgumentBbo) {
    grpc::ClientContext context;
    SubscribeBboRequest request;
    auto reader = stub_->SubscribeBbo(&context, request);

    BboUpdate update;
    EXPECT_FALSE(reader->Read(&update));

    grpc::Status status = reader->Finish();
    EXPECT_EQ(status.error_code(), grpc::StatusCode::INVALID_ARGUMENT);
}

// Every other test in this file uses MarketType::Spot books exclusively -
// this is the only one that exercises to_symbol_book_id()/fill_wire_book_id()'s
// PERP branch (see book_id.hpp) end to end: a well-formed PERP request must
// resolve to BtcPerpBook(), a distinct book from the Spot BtcBook() sharing
// the same base/quote, and receive updates applied to it specifically.
TEST_F(MultiSymbolAggregatorServiceTest, PerpBookSubscribesAndReceivesUpdates) {
    grpc::ClientContext context;
    auto reader = stub_->SubscribeL2Diff(&context, subscribe_l2_diff_request(BtcPerpBook()));
    UpdateQueue<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) updates.push(update);
    });

    L2Update snapshot = updates.wait_for(0);
    ASSERT_TRUE(snapshot.has_snapshot());

    ASSERT_TRUE(
        apply_one(*service_->book(BtcPerpBook()), kBinance, Side::Bid, Price(100.0), Size(1.0))
            .has_value());
    L2Update diff = updates.wait_for(1);
    ASSERT_TRUE(diff.has_diff());
    ASSERT_EQ(diff.diff().bids_size(), 1);
    EXPECT_EQ(diff.diff().bids(0).price_raw(), Price(100.0).raw());

    // service_->book() resolved BtcPerpBook() to a distinct SymbolBook from
    // the Spot BtcBook() sharing the same base/quote - proof
    // to_symbol_book_id()'s PERP branch produced a real, separate
    // symbol::BookId, not one that collided with (or was coerced to) Spot.
    EXPECT_NE(service_->book(BtcPerpBook()), service_->book(BtcBook()));

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
}

TEST_F(MultiSymbolAggregatorServiceTest, ListBooksReturnsEveryConfiguredBook) {
    grpc::ClientContext context;
    ListBooksRequest request;
    ListBooksResponse response;
    grpc::Status status = stub_->ListBooks(&context, request, &response);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(response.books_size(), 3);

    std::vector<BookId> returned;
    for (const auto& wire_book : response.books()) {
        auto id = to_symbol_book_id(wire_book);
        ASSERT_TRUE(id.has_value());
        returned.push_back(*id);
    }

    // ListBooksResponse.books' own proto comment says order isn't
    // guaranteed (it comes off std::unordered_map iteration order) - so
    // this checks set membership, not a fixed sequence.
    for (const auto& expected : {BtcBook(), EthBook(), BtcPerpBook()}) {
        EXPECT_NE(std::find(returned.begin(), returned.end(), expected), returned.end())
            << "missing " << bobby::hermeneutic::symbol::to_string(expected);
    }
}

}  // namespace
}  // namespace bobby::hermeneutic::aggregator

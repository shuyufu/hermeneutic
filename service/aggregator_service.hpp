#pragma once

#include <grpcpp/grpcpp.h>

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <expected>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "bobby/hermeneutic/aggregate_order_book.hpp"
#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"

namespace bobby::hermeneutic::aggregator {

// aggregator.proto's PriceLevel doc comment hardcodes the wire scale as
// price_raw/1e9, size_raw/1e6 (a fixed protocol contract, not sent per
// subscription) - keep it in sync with these.
static_assert(Price::decimals == 9);
static_assert(Size::decimals == 6);

// One bounded mailbox per subscriber. Publishers (SymbolBook::apply_delta/
// apply_snapshot/invalidate_venue/send_heartbeat, via broadcast_to_
// subscribers()) only ever push; the Subscribe() handler thread that owns
// this subscriber's ServerWriter is the only one that drains it and calls
// Write(). Never shared across subscribers, so one slow drainer never
// contends with another subscriber's push or drain.
//
// A full queue closes rather than drops the oldest entry: an L2Diff stream
// is not a series of independently useful events -- missing even one leaves
// the subscriber's replica of the book genuinely wrong, not just stale, and
// the only valid recovery is a fresh Subscribe() (a new full snapshot), not
// resuming the same stream with a gap in it. So there is nothing worth
// keeping once a subscriber has fallen behind past capacity; the queue is
// cleared and closed instead.
class SubscriberQueue {
  public:
    enum class DrainResult { TimedOut, Drained, Closed };

    explicit SubscriberQueue(std::size_t capacity) : capacity_(capacity) {}

    // Non-blocking. False means this push found the queue already closed,
    // or just closed it by overflowing `capacity_`.
    bool push_or_close(L2Update update) {
        std::lock_guard lock(mutex_);
        if (closed_) return false;
        if (queue_.size() >= capacity_) {
            closed_ = true;
            queue_.clear();
            cv_.notify_one();
            return false;
        }
        queue_.push_back(std::move(update));
        cv_.notify_one();
        return true;
    }

    // Blocks up to `timeout` for a queued update or a close. Closed takes
    // priority over whatever's queued, though push_or_close() never leaves
    // anything queued alongside a close. Returns Drained with `out`
    // populated, or TimedOut/Closed with `out` left untouched.
    DrainResult wait_and_drain(std::chrono::milliseconds timeout, std::vector<L2Update>& out) {
        std::unique_lock lock(mutex_);
        cv_.wait_for(lock, timeout, [this] { return !queue_.empty() || closed_; });
        if (closed_) return DrainResult::Closed;
        if (queue_.empty()) return DrainResult::TimedOut;
        while (!queue_.empty()) {
            out.push_back(std::move(queue_.front()));
            queue_.pop_front();
        }
        return DrainResult::Drained;
    }

  private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<L2Update> queue_;
    std::size_t capacity_;
    bool closed_ = false;
};

// Per-symbol aggregated book plus its gRPC fan-out state. Not a gRPC type
// itself: AggregatorService (the sole grpc::Service in this file) owns one
// SymbolBook per symbol and routes every Subscribe() call, and every
// ingestion call (apply_delta et al.), to the right one by symbol. This is
// what keeps a multi-symbol deployment to one gRPC service on one port
// instead of one process/port per symbol -- two grpc::Service instances of
// the same generated type can't be registered on one grpc::Server (their
// RPC method paths collide), but two SymbolBooks in one map have no such
// restriction.
//
// Concurrency model: `mutex_` guards only `book_`/`seq_` -- applying a delta
// and computing the resulting diff, never any socket I/O. Each subscriber
// gets its own SubscriberQueue; broadcasting is a non-blocking push into
// every subscriber's queue (still done under `mutex_`, since it can no
// longer block), and the actual Write() to gRPC happens later, off the
// caller's thread, on that subscriber's own Subscribe() handler thread. A
// stuck or slow subscriber can therefore only ever stall itself -- until it
// overflows its queue and SubscriberQueue closes it -- never ingestion or
// any other subscriber. A separate SymbolBook (and separate `mutex_`) per
// symbol means two symbols never contend with each other either, so
// ingestion for one symbol can never be slowed by another's traffic.
class SymbolBook {
  public:
    std::expected<void, std::errc> apply_delta(const VenueId& venue, Side side, Price price,
                                                 Size size) {
        std::lock_guard lock(mutex_);
        Size before = lookup_aggregate(side, price);
        auto result = book_.apply_delta(venue, side, price, size);
        if (result) {
            std::vector<Change> changed;
            collect_change(changed, side, price, before);
            publish(changed);
        }
        return result;
    }

    std::expected<void, std::errc> apply_snapshot(
        const VenueId& venue, Side side, std::span<const std::pair<Price, Size>> levels) {
        std::lock_guard lock(mutex_);
        auto venue_it = book_.venues().find(venue);

        // bids/asks are different std::map instantiations (opposite
        // comparators), so they can't share one branch via a ternary; each
        // branch uses the matching comparator for `before_by_price` too, so
        // the diff this produces comes out in the same order as the book
        // itself (descending for bids, ascending for asks) for free - see
        // capture_snapshot_before().
        std::vector<Change> changed;
        if (side == Side::Bid) {
            static const std::map<Price, Size, std::greater<Price>> kEmpty;
            auto before = capture_snapshot_before(
                side, venue_it != book_.venues().end() ? venue_it->second.bids : kEmpty, levels);
            auto result = book_.apply_snapshot(venue, side, levels);
            if (!result) return result;
            for (const auto& kv : before) collect_change(changed, side, kv.first, kv.second);
            publish(changed);
            return result;
        } else {
            static const std::map<Price, Size, std::less<Price>> kEmpty;
            auto before = capture_snapshot_before(
                side, venue_it != book_.venues().end() ? venue_it->second.asks : kEmpty, levels);
            auto result = book_.apply_snapshot(venue, side, levels);
            if (!result) return result;
            for (const auto& kv : before) collect_change(changed, side, kv.first, kv.second);
            publish(changed);
            return result;
        }
    }

    void invalidate_venue(const VenueId& venue) {
        std::lock_guard lock(mutex_);

        std::vector<std::pair<Price, Size>> before_bids, before_asks;
        if (auto it = book_.venues().find(venue); it != book_.venues().end()) {
            for (const auto& kv : it->second.bids) {
                before_bids.emplace_back(kv.first, lookup_aggregate(Side::Bid, kv.first));
            }
            for (const auto& kv : it->second.asks) {
                before_asks.emplace_back(kv.first, lookup_aggregate(Side::Ask, kv.first));
            }
        }

        book_.invalidate_venue(venue);

        std::vector<Change> changed;
        for (const auto& [price, before] : before_bids) {
            collect_change(changed, Side::Bid, price, before);
        }
        for (const auto& [price, before] : before_asks) {
            collect_change(changed, Side::Ask, price, before);
        }
        publish(changed);
    }

    // Broadcasts a liveness signal to every subscriber of this symbol,
    // independent of any book change - unlike publish(), this never touches
    // seq_ (a heartbeat is not a book revision). Callers (e.g.
    // AggregatorService::send_heartbeat(), driven by aggregator_main.cpp) are
    // expected to invoke this on a fixed interval; this class has no
    // internal timer of its own.
    void send_heartbeat() {
        std::lock_guard lock(mutex_);
        L2Update update;
        update.mutable_heartbeat()->set_ts_ns(now_ns());
        broadcast_to_subscribers(update);
    }

    // Writes the initial snapshot to `writer` and, if that succeeds,
    // registers it for subsequent updates and returns its queue. Returns
    // nullptr (without registering anything) if the initial Write() fails -
    // the caller (AggregatorService::Subscribe()) should end the RPC without
    // draining a queue that was never created.
    std::shared_ptr<SubscriberQueue> subscribe(grpc::ServerWriter<L2Update>* writer) {
        std::lock_guard lock(mutex_);
        if (!writer->Write(build_snapshot())) return nullptr;
        auto queue = std::make_shared<SubscriberQueue>(kSubscriberQueueCapacity);
        subscribers_.emplace(writer, queue);
        return queue;
    }

    void unsubscribe(grpc::ServerWriter<L2Update>* writer) {
        std::lock_guard lock(mutex_);
        subscribers_.erase(writer);
    }

  private:
    static constexpr std::size_t kSubscriberQueueCapacity = 256;

    struct Change {
        Side side;
        Price price;
        Size size;  // resulting aggregate size; 0 means the level was removed
    };

    // Must be called with mutex_ held. bids/asks are different std::map
    // instantiations (opposite comparators), so they can't share one branch
    // via a ternary.
    Size lookup_aggregate(Side side, Price price) const {
        if (side == Side::Bid) {
            auto it = book_.aggregate().bids.find(price);
            return it != book_.aggregate().bids.end() ? it->second : Size{};
        }
        auto it = book_.aggregate().asks.find(price);
        return it != book_.aggregate().asks.end() ? it->second : Size{};
    }

    // Must be called with mutex_ held. Appends a Change if the aggregate's
    // current size at (side, price) differs from `before`.
    void collect_change(std::vector<Change>& out, Side side, Price price, Size before) const {
        Size after = lookup_aggregate(side, price);
        if (after != before) out.push_back(Change{side, price, after});
    }

    // Must be called with mutex_ held, before book_.apply_snapshot() mutates
    // the book (captures pre-mutation aggregate sizes). Every price
    // apply_snapshot() can possibly change: `old_venue_side`'s prices (the
    // venue's current levels on this side) union `levels`'s prices (the new
    // snapshot). Returned in `Compare` order - the caller passes the same
    // comparator as the book's own bids/asks map for this side, so the
    // eventual diff comes out in that same order without an extra sort.
    template <typename Compare>
    std::map<Price, Size, Compare> capture_snapshot_before(
        Side side, const std::map<Price, Size, Compare>& old_venue_side,
        std::span<const std::pair<Price, Size>> levels) const {
        std::map<Price, Size, Compare> before_by_price;
        for (const auto& kv : old_venue_side) {
            before_by_price.try_emplace(kv.first, lookup_aggregate(side, kv.first));
        }
        for (const auto& level : levels) {
            before_by_price.try_emplace(level.first, lookup_aggregate(side, level.first));
        }
        return before_by_price;
    }

    static PriceLevel make_level(Price price, Size size) {
        PriceLevel level;
        level.set_price_raw(price.raw());
        level.set_size_raw(size.raw());
        return level;
    }

    // Wall-clock publish time for L2Snapshot.ts_ns/L2Diff.ts_ns (fixed64;
    // see aggregator.proto): observability only, not monotonic, never used
    // for ordering here or by clients.
    static std::uint64_t now_ns() {
        return static_cast<std::uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count());
    }

    // Must be called with mutex_ held.
    L2Update build_snapshot() const {
        L2Update update;
        auto* snapshot = update.mutable_snapshot();
        snapshot->set_seq(seq_);
        snapshot->set_ts_ns(now_ns());
        for (const auto& kv : book_.aggregate().bids) {
            *snapshot->add_bids() = make_level(kv.first, kv.second);
        }
        for (const auto& kv : book_.aggregate().asks) {
            *snapshot->add_asks() = make_level(kv.first, kv.second);
        }
        return update;
    }

    // Must be called with mutex_ held. No-op if `changed` is empty, so seq_
    // only advances on an observable change.
    void publish(const std::vector<Change>& changed) {
        if (changed.empty()) return;

        L2Update update;
        auto* diff = update.mutable_diff();
        diff->set_seq(++seq_);
        diff->set_ts_ns(now_ns());
        for (const auto& change : changed) {
            PriceLevel level = make_level(change.price, change.size);
            if (change.side == Side::Bid) {
                *diff->add_bids() = std::move(level);
            } else {
                *diff->add_asks() = std::move(level);
            }
        }

        broadcast_to_subscribers(update);
    }

    // Must be called with mutex_ held. Non-blocking: pushes `update` into
    // every subscriber's own queue. The actual Write() happens later, off
    // this thread, on that subscriber's own Subscribe() handler thread --
    // see the class comment. A subscriber whose queue overflows closes
    // itself; its Subscribe() handler notices on its own next drain and
    // removes it from `subscribers_` then, not here.
    void broadcast_to_subscribers(const L2Update& update) {
        for (auto& [writer, queue] : subscribers_) {
            queue->push_or_close(update);
        }
    }

    std::mutex mutex_;
    AggregateOrderBook book_;
    std::map<grpc::ServerWriter<L2Update>*, std::shared_ptr<SubscriberQueue>> subscribers_;
    std::uint64_t seq_ = 0;
};

// The sole gRPC service type in this file: one instance serves every symbol
// it was constructed with, on one port, by routing each RPC (and each
// ingestion call) to that symbol's own SymbolBook - see SymbolBook's class
// comment for why that beats one grpc::Service instance per symbol.
class AggregatorService final : public Aggregator::Service {
  public:
    // `symbols` must be non-empty with unique entries - both are startup
    // configuration preconditions (asserted, not runtime-checked: this
    // isn't external input), not something a client's request can violate.
    // The resulting symbol set is fixed for this instance's lifetime - no
    // dynamic add/remove.
    explicit AggregatorService(std::span<const std::string> symbols) {
        assert(!symbols.empty());
        for (const auto& symbol : symbols) {
            [[maybe_unused]] auto [it, inserted] = books_.try_emplace(symbol);
            assert(inserted);
        }
    }

    // Returns nullptr if `symbol` isn't one this instance was constructed
    // with. Used by Subscribe() below, and by an ingestion layer routing a
    // parsed delta/snapshot/invalidation to the right book.
    SymbolBook* book(std::string_view symbol) {
        auto it = books_.find(symbol);
        return it != books_.end() ? &it->second : nullptr;
    }

    // Broadcasts a liveness heartbeat to every subscriber of every symbol.
    void send_heartbeat() {
        for (auto& [symbol, symbol_book] : books_) symbol_book.send_heartbeat();
    }

    grpc::Status Subscribe(grpc::ServerContext* context, const SubscribeRequest* request,
                            grpc::ServerWriter<L2Update>* writer) override {
        SymbolBook* target = book(request->symbol());
        if (!target) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND,
                                 "unknown symbol: " + request->symbol());
        }

        auto queue = target->subscribe(writer);
        if (!queue) return grpc::Status::OK;

        // This thread is the sole owner of `writer`/`queue` from here on:
        // it's the only one that calls Write() on `writer`, and the only one
        // that drains `queue`. gRPC's synchronous API has no primitive to
        // block on "cancelled or a new update queued"; the 50ms timeout is
        // only there to re-check IsCancelled() -- an actual push wakes this
        // thread immediately via SubscriberQueue's condition variable.
        std::vector<L2Update> batch;
        while (true) {
            batch.clear();
            auto result = queue->wait_and_drain(std::chrono::milliseconds(50), batch);
            if (result == SubscriberQueue::DrainResult::Closed) {
                target->unsubscribe(writer);
                return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                     "subscriber fell too far behind; reconnect for a fresh "
                                     "snapshot");
            }
            for (auto& update : batch) {
                if (!writer->Write(update)) {
                    target->unsubscribe(writer);
                    return grpc::Status::OK;
                }
            }
            if (context->IsCancelled()) {
                target->unsubscribe(writer);
                return grpc::Status::OK;
            }
        }
    }

  private:
    std::map<std::string, SymbolBook, std::less<>> books_;
};

}  // namespace bobby::hermeneutic::aggregator

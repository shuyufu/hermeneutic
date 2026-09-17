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
#include <optional>
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
// subscribers()) only ever push; the SubscribeL2Diff()/SubscribeBbo()
// handler thread that owns this subscriber's ServerWriter is the only one
// that drains it and calls Write(). Never shared across subscribers, so one
// slow drainer never contends with another subscriber's push or drain.
//
// What a full queue should do differs by stream, hence `OverflowPolicy`:
//
// - Close (used for L2Update/SubscribeL2Diff): an L2Diff stream is not a
//   series of independently useful events -- missing even one leaves the
//   subscriber's replica of the book genuinely wrong, not just stale, and
//   the only valid recovery is a fresh SubscribeL2Diff() (a new full
//   snapshot), not resuming the same stream with a gap in it. So there is
//   nothing worth keeping once a subscriber has fallen behind past
//   capacity; the queue is cleared and closed instead.
// - DropOldest (used for BboUpdate/SubscribeBbo): a Bbo is the complete
//   current top-of-book state, not a delta -- it never depends on a
//   previously queued message for reconstruction. A slow BBO subscriber
//   costs nothing by having a stale queued Bbo replaced by a newer one, so
//   overflow drops the oldest entry and keeps accepting pushes instead of
//   disconnecting the subscriber for a condition that doesn't actually
//   corrupt anything it will eventually receive.
//
// Whichever policy is chosen, `closed_` (and DrainResult::Closed) is only
// ever set by the Close policy's overflow path; a DropOldest queue is never
// closed by this class, so its DrainResult is always TimedOut or Drained.
enum class OverflowPolicy { Close, DropOldest };

template <typename T>
class SubscriberQueue {
  public:
    enum class DrainResult { TimedOut, Drained, Closed };

    SubscriberQueue(std::size_t capacity, OverflowPolicy overflow_policy)
        : capacity_(capacity), overflow_policy_(overflow_policy) {}

    // Non-blocking. False means this push found the queue already closed,
    // or (Close policy only) just closed it by overflowing `capacity_`.
    bool push_or_close(T update) {
        std::lock_guard lock(mutex_);
        if (closed_) return false;
        if (queue_.size() >= capacity_) {
            if (overflow_policy_ == OverflowPolicy::Close) {
                closed_ = true;
                queue_.clear();
                cv_.notify_one();
                return false;
            }
            queue_.pop_front();
        }
        queue_.push_back(std::move(update));
        cv_.notify_one();
        return true;
    }

    // Blocks up to `timeout` for a queued update or a close. Closed takes
    // priority over whatever's queued, though push_or_close() never leaves
    // anything queued alongside a close. Returns Drained with `out`
    // populated, or TimedOut/Closed with `out` left untouched.
    DrainResult wait_and_drain(std::chrono::milliseconds timeout, std::vector<T>& out) {
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
    std::deque<T> queue_;
    std::size_t capacity_;
    OverflowPolicy overflow_policy_;
    bool closed_ = false;
};

// Per-symbol aggregated book plus its gRPC fan-out state. Not a gRPC type
// itself: AggregatorService (the sole grpc::Service in this file) owns one
// SymbolBook per symbol and routes every SubscribeL2Diff()/SubscribeBbo()
// call, and every ingestion call (apply_delta et al.), to the right one by
// symbol. This is what keeps a multi-symbol deployment to one gRPC service
// on one port instead of one process/port per symbol -- two grpc::Service
// instances of the same generated type can't be registered on one
// grpc::Server (their RPC method paths collide), but two SymbolBooks in one
// map have no such restriction.
//
// Concurrency model: `mutex_` guards only `book_`/`seq_`/the cached last-
// published BBO -- applying a delta and computing the resulting diff, never
// any socket I/O. Each subscriber (L2 or BBO) gets its own SubscriberQueue;
// broadcasting is a non-blocking push into every subscriber's queue (still
// done under `mutex_`, since it can no longer block), and the actual
// Write() to gRPC happens later, off the caller's thread, on that
// subscriber's own SubscribeL2Diff()/SubscribeBbo() handler thread. A stuck
// or slow L2 subscriber can therefore only ever stall itself -- until it
// overflows its queue and SubscriberQueue closes it -- never ingestion or
// any other subscriber; a stuck or slow BBO subscriber never gets closed at
// all (see SubscriberQueue's OverflowPolicy::DropOldest). A separate
// SymbolBook (and separate `mutex_`) per symbol means two symbols never
// contend with each other either, so ingestion for one symbol can never be
// slowed by another's traffic.
//
// `seq_` is a single counter shared by both the L2Diff stream and the Bbo
// stream: it names the aggregate-book revision, not "how many messages this
// stream has sent". A revision that doesn't move the top of book still
// bumps `seq_` and produces an L2Diff, but produces no Bbo at all -- so
// Bbo.book_seq is expected to skip values relative to L2Diff.book_seq. This
// is also what lets a client correlate a Bbo against the L2Diff stream (or
// a recording of it), which two independent per-stream counters could not
// do.
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

    // Applies one batch of delta changes (e.g. everything one upstream
    // exchange message carried) as a single book revision: one seq bump,
    // one broadcast - unlike calling apply_delta() once per level, which
    // would seq-bump and broadcast once per level even though the exchange
    // meant it as one atomic update. Same per-level negative-size rejection
    // as apply_delta, checked for every level before any of them is
    // applied, so a bad level anywhere in the batch leaves the book
    // untouched rather than partially updated for that reason specifically
    // (an allocation failure partway through is not rolled back - same
    // documented limitation as apply_snapshot()).
    std::expected<void, std::errc> apply_batch(const VenueId& venue,
                                                std::span<const std::pair<Price, Size>> bids,
                                                std::span<const std::pair<Price, Size>> asks) {
        for (const auto& [price, size] : bids) {
            if (size.raw() < 0) return std::unexpected(std::errc::invalid_argument);
        }
        for (const auto& [price, size] : asks) {
            if (size.raw() < 0) return std::unexpected(std::errc::invalid_argument);
        }

        std::lock_guard lock(mutex_);

        // Captured before any mutation, keyed in the same order the
        // aggregate book itself orders this side (descending for bids,
        // ascending for asks) - same trick as capture_snapshot_before(),
        // so the eventual diff comes out correctly ordered "for free"
        // instead of needing a separate sort. try_emplace() also means a
        // price repeated more than once in one batch (not expected from a
        // real exchange message, but not assumed against either) only
        // captures its state from before this whole batch, not an
        // intermediate value from earlier in the same batch.
        std::map<Price, Size, std::greater<Price>> before_bids;
        for (const auto& [price, size] : bids) {
            before_bids.try_emplace(price, lookup_aggregate(Side::Bid, price));
        }
        std::map<Price, Size, std::less<Price>> before_asks;
        for (const auto& [price, size] : asks) {
            before_asks.try_emplace(price, lookup_aggregate(Side::Ask, price));
        }

        for (const auto& [price, size] : bids) {
            auto result = book_.apply_delta(venue, Side::Bid, price, size);
            if (!result) return result;
        }
        for (const auto& [price, size] : asks) {
            auto result = book_.apply_delta(venue, Side::Ask, price, size);
            if (!result) return result;
        }

        std::vector<Change> changed;
        for (const auto& [price, before] : before_bids) collect_change(changed, Side::Bid, price, before);
        for (const auto& [price, before] : before_asks) collect_change(changed, Side::Ask, price, before);
        publish(changed);
        return {};
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

    // Broadcasts a liveness signal to every subscriber of this symbol (both
    // the L2Diff and the Bbo stream), independent of any book change -
    // unlike publish(), this never touches seq_ (a heartbeat is not a book
    // revision). Callers (e.g. AggregatorService::send_heartbeat(), driven
    // by aggregator_main.cpp) are expected to invoke this on a fixed
    // interval; this class has no internal timer of its own.
    void send_heartbeat() {
        std::lock_guard lock(mutex_);
        auto ts_ns = now_ns();

        L2Update l2_update;
        l2_update.mutable_heartbeat()->set_ts_ns(ts_ns);
        broadcast_to_subscribers(l2_update);

        BboUpdate bbo_update;
        bbo_update.mutable_heartbeat()->set_ts_ns(ts_ns);
        broadcast_to_bbo_subscribers(bbo_update);
    }

    // Writes the initial snapshot to `writer` and, if that succeeds,
    // registers it for subsequent updates and returns its queue. Returns
    // nullptr (without registering anything) if the initial Write() fails -
    // the caller (AggregatorService::SubscribeL2Diff()) should end the RPC
    // without draining a queue that was never created.
    std::shared_ptr<SubscriberQueue<L2Update>> subscribe(grpc::ServerWriter<L2Update>* writer) {
        std::lock_guard lock(mutex_);
        if (!writer->Write(build_snapshot())) return nullptr;
        auto queue =
            std::make_shared<SubscriberQueue<L2Update>>(kSubscriberQueueCapacity, OverflowPolicy::Close);
        subscribers_.emplace(writer, queue);
        return queue;
    }

    void unsubscribe(grpc::ServerWriter<L2Update>* writer) {
        std::lock_guard lock(mutex_);
        subscribers_.erase(writer);
    }

    // Writes the current complete BBO state to `writer` and, if that
    // succeeds, registers it for subsequent updates and returns its queue.
    // Returns nullptr (without registering anything) if the initial Write()
    // fails - mirrors subscribe() above. Uses OverflowPolicy::DropOldest,
    // not Close: see SubscriberQueue's class comment for why a slow BBO
    // subscriber should never be disconnected for falling behind.
    std::shared_ptr<SubscriberQueue<BboUpdate>> subscribe_bbo(
        grpc::ServerWriter<BboUpdate>* writer) {
        std::lock_guard lock(mutex_);
        if (!writer->Write(build_bbo())) return nullptr;
        auto queue = std::make_shared<SubscriberQueue<BboUpdate>>(kSubscriberQueueCapacity,
                                                                    OverflowPolicy::DropOldest);
        bbo_subscribers_.emplace(writer, queue);
        return queue;
    }

    void unsubscribe_bbo(grpc::ServerWriter<BboUpdate>* writer) {
        std::lock_guard lock(mutex_);
        bbo_subscribers_.erase(writer);
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
        snapshot->set_book_seq(seq_);
        snapshot->set_ts_ns(now_ns());
        for (const auto& kv : book_.aggregate().bids) {
            *snapshot->add_bids() = make_level(kv.first, kv.second);
        }
        for (const auto& kv : book_.aggregate().asks) {
            *snapshot->add_asks() = make_level(kv.first, kv.second);
        }
        return update;
    }

    // Must be called with mutex_ held. The book's own comparators make
    // begin() the best level on each side (descending for bids, ascending
    // for asks), so no bids/asks ternary is needed here (contrast
    // lookup_aggregate(), which does need the explicit branch because it
    // looks up an arbitrary price, not just the best one).
    std::optional<std::pair<Price, Size>> best_bid() const {
        const auto& bids = book_.aggregate().bids;
        if (bids.empty()) return std::nullopt;
        return *bids.begin();
    }
    std::optional<std::pair<Price, Size>> best_ask() const {
        const auto& asks = book_.aggregate().asks;
        if (asks.empty()) return std::nullopt;
        return *asks.begin();
    }

    // Must be called with mutex_ held. Builds the complete current BBO
    // state - bid/ask are left unset (proto3 message-field presence) when
    // that side of the book is currently empty, per Bbo's own comment.
    BboUpdate build_bbo() const {
        BboUpdate update;
        auto* bbo = update.mutable_bbo();
        bbo->set_book_seq(seq_);
        bbo->set_ts_ns(now_ns());
        if (auto bid = best_bid()) *bbo->mutable_bid() = make_level(bid->first, bid->second);
        if (auto ask = best_ask()) *bbo->mutable_ask() = make_level(ask->first, ask->second);
        return update;
    }

    // Must be called with mutex_ held. No-op if `changed` is empty, so seq_
    // only advances on an observable change. Also emits a Bbo to
    // bbo_subscribers_, sharing this same (already-bumped) seq_ value, but
    // only when the best bid or best ask actually changed - a deep-book
    // change that doesn't touch the top of book produces an L2Diff here but
    // no Bbo at all, which is why Bbo.book_seq is allowed to skip values
    // (see Bbo's proto comment).
    void publish(const std::vector<Change>& changed) {
        if (changed.empty()) return;

        L2Update update;
        auto* diff = update.mutable_diff();
        diff->set_book_seq(++seq_);
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

        auto bid = best_bid();
        auto ask = best_ask();
        if (bid != last_bbo_bid_ || ask != last_bbo_ask_) {
            last_bbo_bid_ = bid;
            last_bbo_ask_ = ask;
            broadcast_to_bbo_subscribers(build_bbo());
        }
    }

    // Must be called with mutex_ held. Non-blocking: pushes `update` into
    // every subscriber's own queue. The actual Write() happens later, off
    // this thread, on that subscriber's own SubscribeL2Diff() handler
    // thread -- see the class comment. A subscriber whose queue overflows
    // closes itself; its SubscribeL2Diff() handler notices on its own next
    // drain and removes it from `subscribers_` then, not here.
    void broadcast_to_subscribers(const L2Update& update) {
        for (auto& [writer, queue] : subscribers_) {
            queue->push_or_close(update);
        }
    }

    // Must be called with mutex_ held. Same shape as broadcast_to_
    // subscribers(), for the BBO stream's subscribers - see
    // SubscriberQueue's OverflowPolicy::DropOldest for why a full queue
    // here never closes the subscriber.
    void broadcast_to_bbo_subscribers(const BboUpdate& update) {
        for (auto& [writer, queue] : bbo_subscribers_) {
            queue->push_or_close(update);
        }
    }

    std::mutex mutex_;
    AggregateOrderBook book_;
    std::map<grpc::ServerWriter<L2Update>*, std::shared_ptr<SubscriberQueue<L2Update>>> subscribers_;
    std::map<grpc::ServerWriter<BboUpdate>*, std::shared_ptr<SubscriberQueue<BboUpdate>>>
        bbo_subscribers_;
    std::uint64_t seq_ = 0;
    // Last best bid/ask actually broadcast to bbo_subscribers_, compared
    // against on every publish() to decide whether this revision touched
    // the top of book. nullopt means that side had no level.
    std::optional<std::pair<Price, Size>> last_bbo_bid_;
    std::optional<std::pair<Price, Size>> last_bbo_ask_;
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
    // with. Used by SubscribeL2Diff()/SubscribeBbo() below, and by an
    // ingestion layer routing a parsed delta/snapshot/invalidation to the
    // right book.
    SymbolBook* book(std::string_view symbol) {
        auto it = books_.find(symbol);
        return it != books_.end() ? &it->second : nullptr;
    }

    // Broadcasts a liveness heartbeat to every subscriber of every symbol.
    void send_heartbeat() {
        for (auto& [symbol, symbol_book] : books_) symbol_book.send_heartbeat();
    }

    grpc::Status SubscribeL2Diff(grpc::ServerContext* context, const SubscribeL2DiffRequest* request,
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
            if (result == SubscriberQueue<L2Update>::DrainResult::Closed) {
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

    grpc::Status SubscribeBbo(grpc::ServerContext* context, const SubscribeBboRequest* request,
                               grpc::ServerWriter<BboUpdate>* writer) override {
        SymbolBook* target = book(request->symbol());
        if (!target) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND,
                                 "unknown symbol: " + request->symbol());
        }

        auto queue = target->subscribe_bbo(writer);
        if (!queue) return grpc::Status::OK;

        // Same single-owner-thread shape as SubscribeL2Diff() above. Unlike
        // there, DrainResult::Closed is unreachable in practice - this
        // queue uses OverflowPolicy::DropOldest, which never closes - but
        // the branch is kept for switch-style exhaustiveness, and returns
        // OK rather than RESOURCE_EXHAUSTED: a BBO subscriber has no
        // resync obligation, so that status code would be the wrong signal
        // even if this path were ever reached.
        std::vector<BboUpdate> batch;
        while (true) {
            batch.clear();
            auto result = queue->wait_and_drain(std::chrono::milliseconds(50), batch);
            if (result == SubscriberQueue<BboUpdate>::DrainResult::Closed) {
                target->unsubscribe_bbo(writer);
                return grpc::Status::OK;
            }
            for (auto& update : batch) {
                if (!writer->Write(update)) {
                    target->unsubscribe_bbo(writer);
                    return grpc::Status::OK;
                }
            }
            if (context->IsCancelled()) {
                target->unsubscribe_bbo(writer);
                return grpc::Status::OK;
            }
        }
    }

  private:
    std::map<std::string, SymbolBook, std::less<>> books_;
};

}  // namespace bobby::hermeneutic::aggregator

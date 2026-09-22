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
#include <unordered_map>
#include <utility>
#include <vector>

#include "bobby/hermeneutic/book/aggregate_order_book.hpp"
#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"
#include "bobby/hermeneutic/symbol/symbol.hpp"
#include "apps/aggregator/book_id.hpp"

namespace bobby::hermeneutic::aggregator {

// aggregator.proto's PriceLevel doc comment hardcodes the wire scale as
// price_raw/1e9, size_raw/1e6 (a fixed protocol contract, not sent per
// subscription) - keep it in sync with these.
static_assert(Price::decimals == 9);
static_assert(Size::decimals == 6);

// One bounded mailbox per subscriber. Publishers (via Fanout::broadcast())
// only ever push; the SubscribeL2Diff()/SubscribeBbo() handler thread that
// owns this subscriber's ServerWriter is the only one that drains it and
// calls Write(). Never shared across subscribers, so one slow drainer
// never contends with another subscriber's push or drain.
//
// What a full queue should do differs by stream, hence `OverflowPolicy`:
//
// - Close (L2Update/SubscribeL2Diff): missing even one diff leaves the
//   subscriber's replica of the book genuinely wrong, not just stale, and
//   the only valid recovery is a fresh SubscribeL2Diff() (a new snapshot).
//   So overflow clears the queue and closes it instead of keeping stale
//   entries.
// - DropOldest (BboUpdate/SubscribeBbo): a Bbo is the complete current
//   top-of-book state, not a delta, so a slow subscriber loses nothing by
//   having a stale queued Bbo replaced by a newer one - overflow drops the
//   oldest entry and keeps accepting pushes instead of disconnecting.
//
// Both policies apply at a higher (not absent) capacity during the
// bootstrap window between a queue's registration and its owner's first
// successful Write() - see push_or_close()/end_bootstrap() below.
//
// `closed_` (and DrainResult::Closed) is only ever set by the Close
// policy's overflow path; a DropOldest queue's DrainResult is always
// TimedOut or Drained.
enum class OverflowPolicy { Close, DropOldest };

template <typename T>
class SubscriberQueue {
  public:
    enum class DrainResult { TimedOut, Drained, Closed };

    // What one push_or_close() call actually did, so a caller broadcasting
    // to many queues at once (see Fanout::broadcast()) can tell "this
    // queue needs waking" (Pushed, ClosedNow) apart from "nothing changed,
    // don't bother" (AlreadyClosed - a prior call already woke anyone
    // waiting on the close).
    enum class PushResult { Pushed, ClosedNow, AlreadyClosed };

    SubscriberQueue(std::size_t capacity, OverflowPolicy overflow_policy)
        : capacity_(capacity), overflow_policy_(overflow_policy) {}

    // Non-blocking, and does NOT wake wait_and_drain() - call notify()
    // afterward once that's safe (see notify()'s own comment for why
    // Fanout::broadcast() defers it rather than calling it inline here).
    // AlreadyClosed means this push found the queue already closed;
    // ClosedNow (Close policy only) means this call just closed it by
    // overflowing its effective capacity. While bootstrapping (see
    // end_bootstrap() below), that effective capacity is
    // `capacity_ * kBootstrapCapacityMultiplier`, not `capacity_` itself:
    // a newly subscribed queue is registered for broadcast before its
    // caller's own initial Write() has completed, so an ordinary burst
    // landing before draining starts must not trip the same tight
    // threshold a genuinely slow, already-draining subscriber would.
    // Still a real ceiling, not unlimited: a client that stops reading and
    // stalls Write() indefinitely must not be able to drive unbounded
    // memory growth here.
    //
    // Takes a shared_ptr<const T>, not a T by value: every subscriber of
    // one broadcast() shares the same payload (a refcount bump) instead of
    // each getting its own deep copy of a protobuf message with repeated
    // fields - see Fanout::broadcast()'s own comment.
    PushResult push_or_close(std::shared_ptr<const T> update) {
        std::lock_guard lock(mutex_);
        if (closed_) return PushResult::AlreadyClosed;
        std::size_t effective_capacity = bootstrapping_ ? capacity_ * kBootstrapCapacityMultiplier : capacity_;
        if (queue_.size() >= effective_capacity) {
            if (overflow_policy_ == OverflowPolicy::Close) {
                closed_ = true;
                queue_.clear();
                return PushResult::ClosedNow;
            }
            queue_.pop_front();
        }
        queue_.push_back(std::move(update));
        return PushResult::Pushed;
    }

    // Wakes wait_and_drain(), if anything is currently blocked there -
    // harmless (just a wasted syscall, never a correctness issue) to call
    // when nothing is waiting, since wait_and_drain() always re-checks its
    // own predicate rather than trusting the wake alone. Deliberately
    // doesn't take mutex_: the state change it's reporting already
    // happened-before under push_or_close()'s own lock_guard, and
    // condition_variable::notify_one() needs no lock held to be safe to
    // call. Split out from push_or_close() so Fanout::broadcast() can push
    // into every subscriber's queue first and only notify afterward, once
    // its own caller (SymbolBook::mutex_) has released its lock - see that
    // method's own comment for why.
    void notify() { cv_.notify_one(); }

    // Ends the bootstrap window above, switching push_or_close() back to
    // normal capacity/policy enforcement. Called exactly once, by the
    // subscribe() that created this queue, right after its own initial
    // Write() succeeds.
    void end_bootstrap() {
        std::lock_guard lock(mutex_);
        bootstrapping_ = false;
    }

    // Blocks up to `timeout` for a queued update or a close. Closed takes
    // priority over whatever's queued, though push_or_close() never leaves
    // anything queued alongside a close. Returns Drained with `out`
    // populated, or TimedOut/Closed with `out` left untouched.
    DrainResult wait_and_drain(std::chrono::milliseconds timeout, std::vector<std::shared_ptr<const T>>& out) {
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
    // See push_or_close()'s own comment for why bootstrapping gets a
    // higher, not absent, ceiling.
    static constexpr std::size_t kBootstrapCapacityMultiplier = 4;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::shared_ptr<const T>> queue_;
    std::size_t capacity_;
    OverflowPolicy overflow_policy_;
    bool closed_ = false;
    bool bootstrapping_ = true;
};

// One stream's worth of subscriber bookkeeping: create a queue, broadcast
// into every queue, drop one. Shared by SymbolBook's L2Diff and Bbo
// streams.
//
// Deliberately does NOT take or store the gRPC writer: SymbolBook's own
// subscribe()/subscribe_bbo() still need `grpc::ServerWriter<T>*` for the
// initial Write(), but bookkeeping a subscriber's queue doesn't need to
// know what a gRPC writer is. Subscribers are keyed by the queue's own
// address, not the writer pointer, so unsubscribe() takes the queue
// itself - letting a caller un-subscribe using nothing but the shared_ptr
// subscribe() handed back, with no separate writer-to-queue lookup.
//
// Every method here must be called with the caller's own mutex_ held -
// this class keeps no lock of its own. SymbolBook already has one mutex_
// guarding book_/seq_/both fanouts together, and a subscribe() registering
// a queue must be atomic with the snapshot/BBO capture that goes with it;
// a second, separate lock here would only reintroduce that coordination
// problem.
template <typename T>
class Fanout {
  public:
    Fanout(std::size_t capacity, OverflowPolicy overflow_policy)
        : capacity_(capacity), overflow_policy_(overflow_policy) {}

    // Must be called with the caller's mutex_ held.
    std::shared_ptr<SubscriberQueue<T>> subscribe() {
        auto queue = std::make_shared<SubscriberQueue<T>>(capacity_, overflow_policy_);
        subscribers_.emplace(queue.get(), queue);
        return queue;
    }

    // Safe to call even if `queue` was never registered (e.g. subscribe()'s
    // own caller undoing a provisional registration after a failed initial
    // Write() - see SymbolBook::subscribe()). Must be called with the
    // caller's mutex_ held.
    void unsubscribe(const std::shared_ptr<SubscriberQueue<T>>& queue) { subscribers_.erase(queue.get()); }

    // Non-blocking: pushes `update` into every subscriber's own queue. The
    // actual Write() happens later, off this thread, on that subscriber's
    // own handler thread. Must be called with the caller's mutex_ held.
    //
    // Takes a shared_ptr<const T>, built once by the caller and shared
    // across every subscriber (a refcount bump per push, not a deep copy
    // of a protobuf message with repeated fields).
    //
    // Deliberately does NOT call SubscriberQueue::notify() itself - only
    // push_or_close(), which never blocks or wakes a scheduler. Returns
    // the subscriber queues that actually changed state (were pushed to,
    // or were just closed by this call) so the caller can call notify()
    // on each of them once it has released its own mutex_ (SymbolBook's,
    // shared by every venue and stream for one symbol). Doing the wake
    // here, inline, while that lock is still held would extend a
    // contended critical section by O(N subscribers) worth of
    // notify_one() calls for no ordering reason: only the enqueue itself
    // has to happen before the lock is released (that's what fixes each
    // update's relative seq order), a late wake is harmless because
    // wait_and_drain() has its own timeout as a backstop.
    std::vector<std::shared_ptr<SubscriberQueue<T>>> broadcast(const std::shared_ptr<const T>& update) {
        std::vector<std::shared_ptr<SubscriberQueue<T>>> to_notify;
        to_notify.reserve(subscribers_.size());
        for (auto& [ptr, queue] : subscribers_) {
            if (queue->push_or_close(update) != SubscriberQueue<T>::PushResult::AlreadyClosed) {
                to_notify.push_back(queue);
            }
        }
        return to_notify;
    }

  private:
    std::map<SubscriberQueue<T>*, std::shared_ptr<SubscriberQueue<T>>> subscribers_;
    std::size_t capacity_;
    OverflowPolicy overflow_policy_;
};

// Per-symbol aggregated book plus its gRPC fan-out state. Not a gRPC type
// itself: AggregatorService (the sole grpc::Service in this file) owns one
// SymbolBook per symbol and routes every RPC/ingestion call to the right
// one by symbol - this is what keeps a multi-symbol deployment to one gRPC
// service on one port instead of one process/port per symbol (two
// grpc::Service instances of the same generated type can't be registered
// on one grpc::Server; two SymbolBooks in one map can).
//
// Concurrency model: `mutex_` guards only `book_`/`seq_`/the cached last-
// published BBO, never socket I/O. Each subscriber gets its own
// SubscriberQueue; broadcasting is a non-blocking push into every
// subscriber's queue (under mutex_, since it can't block), and the actual
// Write() happens later, off the caller's thread, on that subscriber's
// own handler thread. A stuck L2 subscriber can therefore only stall
// itself, until it overflows its queue and gets closed; a stuck BBO
// subscriber never gets closed at all (OverflowPolicy::DropOldest). A
// separate SymbolBook (and `mutex_`) per symbol means two symbols never
// contend with each other either.
//
// `seq_` is shared by both the L2Diff and Bbo streams: it names the
// aggregate-book revision, not "messages sent on this stream". A revision
// that doesn't move the top of book still bumps `seq_` and produces an
// L2Diff but no Bbo, so Bbo.book_seq is expected to skip values relative
// to L2Diff.book_seq - this is what lets a client correlate a Bbo against
// the L2Diff stream.
class SymbolBook {
  public:
    // Resyncs both sides of `venue`'s book from a REST snapshot as a
    // single book revision: one seq bump, one broadcast - mirrors
    // apply_batch() below in both shape and reasoning. book_.apply_snapshot()
    // itself validates both sides before touching either, so a bad level
    // on one side doesn't leave this venue's other, valid side resynced
    // while the bad one is rejected.
    std::expected<void, std::errc> apply_snapshot(
        const VenueId& venue, std::span<const std::pair<Price, Size>> bids,
        std::span<const std::pair<Price, Size>> asks) {
        // Cheap enough to reject before taking mutex_: book_.apply_snapshot()
        // below re-validates regardless, but there's no reason to take the
        // lock for a snapshot already known to be bad.
        for (const auto& [price, size] : bids) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }
        for (const auto& [price, size] : asks) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }

        std::expected<void, std::errc> result;
        NotifyList to_notify;
        {
            std::lock_guard lock(mutex_);

            SideChanges changes;
            result = book_.apply_snapshot(venue, bids, asks, changes.bids, changes.asks);
            if (result) to_notify = publish(collect_changes(changes));
        }
        notify_all(to_notify);
        return result;
    }

    // Applies one batch of delta changes (e.g. everything one upstream
    // exchange message carried) as a single book revision: one seq bump,
    // one broadcast - unlike bumping and broadcasting once per level, which
    // would fragment what the exchange meant as one atomic update into
    // several separate revisions on our own wire protocol. Every level is
    // checked before any of them is applied, so a bad level anywhere in
    // the batch leaves the book untouched (an allocation failure partway
    // through is not rolled back - same documented limitation as
    // apply_snapshot()).
    std::expected<void, std::errc> apply_batch(const VenueId& venue,
                                                std::span<const std::pair<Price, Size>> bids,
                                                std::span<const std::pair<Price, Size>> asks) {
        // Cheap enough to reject before taking mutex_: book_.apply_batch()
        // below re-validates regardless, but there's no reason to take the
        // lock for a batch already known to be bad.
        for (const auto& [price, size] : bids) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }
        for (const auto& [price, size] : asks) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }

        NotifyList to_notify;
        {
            std::lock_guard lock(mutex_);

            // book_.apply_batch() re-validates on its own (it must hold
            // that guarantee for callers that skip this class's
            // pre-check), so this can only fail on allocation here, given
            // the same bids/asks already passed the check above.
            SideChanges changes;
            if (auto result = book_.apply_batch(venue, bids, asks, changes.bids, changes.asks); !result) {
                return result;
            }

            to_notify = publish(collect_changes(changes));
        }
        notify_all(to_notify);
        return {};
    }

    // book_.invalidate_venue() finishes mutating its own state for every
    // price before invoking either Sink at all (see its own doc comment),
    // so a plain SideChanges is safe here - a mid-report allocation
    // failure in the Sink can't corrupt book_'s already-committed state.
    void invalidate_venue(const VenueId& venue) {
        NotifyList to_notify;
        {
            std::lock_guard lock(mutex_);

            SideChanges changes;
            book_.invalidate_venue(venue, changes.bids, changes.asks);

            to_notify = publish(collect_changes(changes));
        }
        notify_all(to_notify);
    }

    // Broadcasts a liveness signal to every subscriber of this symbol (both
    // streams), independent of any book change - unlike publish(), this
    // never touches seq_ (a heartbeat is not a book revision). Callers are
    // expected to invoke this on a fixed interval; this class has no
    // internal timer of its own.
    //
    // live_venues is read from book_.venues() here, inside this same
    // mutex_ critical section, rather than captured separately - reading
    // it outside the lock could race against a concurrent
    // apply_batch()/invalidate_venue() and report a venue as live that
    // was invalidated a moment earlier, or vice versa.
    void send_heartbeat() {
        NotifyList to_notify;
        {
            std::lock_guard lock(mutex_);
            auto ts_ns = now_ns();

            L2Update l2_update;
            auto* l2_heartbeat = l2_update.mutable_heartbeat();
            l2_heartbeat->set_ts_ns(ts_ns);
            for (const auto& [venue, venue_book] : book_.venues()) {
                l2_heartbeat->add_live_venues(symbol::to_string(venue));
            }
            auto l2_ptr = std::make_shared<const L2Update>(std::move(l2_update));
            to_notify.l2 = l2_fanout_.broadcast(l2_ptr);

            BboUpdate bbo_update;
            auto* bbo_heartbeat = bbo_update.mutable_heartbeat();
            bbo_heartbeat->set_ts_ns(ts_ns);
            // Copies from l2_ptr's own live_venues, not the now-moved-from
            // l2_update/l2_heartbeat above, since building l2_ptr moved
            // l2_update's contents into the shared payload just broadcast.
            *bbo_heartbeat->mutable_live_venues() = l2_ptr->heartbeat().live_venues();
            to_notify.bbo = bbo_fanout_.broadcast(std::make_shared<const BboUpdate>(std::move(bbo_update)));
        }
        notify_all(to_notify);
    }

    // Writes the initial snapshot to `writer` and, if that succeeds,
    // registers it for subsequent updates and returns its queue. Returns
    // nullptr (without leaving it registered) if the initial Write() fails -
    // the caller should end the RPC without draining a queue that was
    // never meant to be used. See subscribe_impl() below for the shared
    // implementation this and subscribe_bbo() call.
    std::shared_ptr<SubscriberQueue<L2Update>> subscribe(grpc::ServerWriter<L2Update>* writer) {
        return subscribe_impl(l2_fanout_, writer, [this] { return build_snapshot(); });
    }

    // Takes the queue subscribe() handed back, not the writer that was
    // passed to it - see Fanout's own class comment for why: bookkeeping
    // a subscriber never needed the gRPC writer type in the first place,
    // only the caller's own initial Write() did.
    void unsubscribe(const std::shared_ptr<SubscriberQueue<L2Update>>& queue) {
        std::lock_guard lock(mutex_);
        l2_fanout_.unsubscribe(queue);
    }

    // Writes the current complete BBO state to `writer` and, if that
    // succeeds, registers it for subsequent updates and returns its queue.
    // Mirrors subscribe() above via the same subscribe_impl() helper.
    // Uses OverflowPolicy::DropOldest, not Close: see SubscriberQueue's
    // class comment for why a slow BBO subscriber is never disconnected.
    std::shared_ptr<SubscriberQueue<BboUpdate>> subscribe_bbo(
        grpc::ServerWriter<BboUpdate>* writer) {
        return subscribe_impl(bbo_fanout_, writer, [this] { return build_bbo(); });
    }

    // See unsubscribe()'s own comment above for why this takes the queue,
    // not the writer.
    void unsubscribe_bbo(const std::shared_ptr<SubscriberQueue<BboUpdate>>& queue) {
        std::lock_guard lock(mutex_);
        bbo_fanout_.unsubscribe(queue);
    }

  private:
    static constexpr std::size_t kSubscriberQueueCapacity = 256;

    // Shared by subscribe()/subscribe_bbo(), differing only in update
    // type, snapshot-builder, and which fanout to use.
    //
    // `writer->Write()` is a blocking network call - it must never run
    // while mutex_ is held, or one new subscriber's slow connection stalls
    // ingestion and every other subscriber's broadcast for as long as
    // Write() takes. So this registers into `fanout` *before* calling
    // Write() (atomically with the snapshot/BBO capture via
    // `build_initial`, so no update between "state captured" and
    // "registered" is missed), releases the lock, then writes. Safe
    // against a concurrent broadcast landing in that gap: broadcast()
    // only ever pushes into `queue` (non-blocking) and never calls
    // Write() itself - only this thread ever calls Write() on this
    // particular `writer`.
    //
    // The queue starts in SubscriberQueue's bootstrapping mode (see that
    // class's own comment for why); end_bootstrap() below is called
    // unconditionally, before branching on Write()'s result, so the
    // queue's mode never depends on which path is taken.
    //
    // On failure, the provisional registration is undone under a second,
    // separate critical section - Fanout::unsubscribe() erasing a key
    // that isn't present is a no-op.
    template <typename T, typename BuildInitial>
    std::shared_ptr<SubscriberQueue<T>> subscribe_impl(Fanout<T>& fanout, grpc::ServerWriter<T>* writer,
                                                         BuildInitial&& build_initial) {
        T initial;
        std::shared_ptr<SubscriberQueue<T>> queue;
        {
            std::lock_guard lock(mutex_);
            initial = build_initial();
            queue = fanout.subscribe();
        }
        bool wrote = writer->Write(initial);
        queue->end_bootstrap();
        if (wrote) return queue;

        std::lock_guard lock(mutex_);
        fanout.unsubscribe(queue);
        return nullptr;
    }

    struct Change {
        Side side;
        Price price;
        Size size;  // resulting aggregate size; 0 means the level was removed
    };

    // One side's collector for the `Sink` parameters book_'s own
    // apply_snapshot()/apply_batch()/invalidate_venue() take.
    // AggregateOrderBook calls operator() with a price and its resulting
    // aggregate size for every price that actually changed, already
    // netted and filtered to real changes only, so this only has to
    // record the latest value it's told per price. Keyed by the same
    // comparator as the book's own bids/asks map for this side
    // (std::greater for bids, std::less for asks), so the map's own
    // iteration order already matches L2Diff's ordering contract with no
    // separate sort needed.
    template <typename Compare>
    struct ChangeCollector {
        std::map<Price, Size, Compare> by_price;
        void operator()(Price price, Size new_size) { by_price.insert_or_assign(price, new_size); }
    };

    // The bid/ask ChangeCollector pair each of apply_snapshot()/
    // apply_batch()/invalidate_venue() above needs one of - named here so
    // there is exactly one place holding "bids use std::greater, asks use
    // std::less", not three.
    struct SideChanges {
        ChangeCollector<std::greater<Price>> bids;
        ChangeCollector<std::less<Price>> asks;
    };

    // What publish()/send_heartbeat() hand back to their own callers: the
    // subscriber queues each Fanout<T>::broadcast() call says need waking,
    // kept apart from the broadcast itself so the caller can release
    // mutex_ first and call notify_all() only afterward - see
    // Fanout::broadcast()'s own comment for why.
    struct NotifyList {
        std::vector<std::shared_ptr<SubscriberQueue<L2Update>>> l2;
        std::vector<std::shared_ptr<SubscriberQueue<BboUpdate>>> bbo;
    };

    static void notify_all(const NotifyList& to_notify) {
        for (auto& queue : to_notify.l2) queue->notify();
        for (auto& queue : to_notify.bbo) queue->notify();
    }

    // Flattens an already-populated SideChanges into the single Change
    // sequence publish() wants - bids first, then asks. Reads only its
    // own argument, so unlike most other private helpers here, this one
    // has no mutex_ requirement.
    static std::vector<Change> collect_changes(const SideChanges& changes) {
        std::vector<Change> changed;
        for (const auto& [price, size] : changes.bids.by_price) {
            changed.push_back(Change{Side::Bid, price, size});
        }
        for (const auto& [price, size] : changes.asks.by_price) {
            changed.push_back(Change{Side::Ask, price, size});
        }
        return changed;
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
    // for asks), so no bids/asks branch is needed here.
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

    // Must be called with mutex_ held, and returns the resulting
    // NotifyList for the caller to notify_all() only after releasing that
    // lock - see Fanout::broadcast()'s own comment for why. No-op
    // (returns an empty NotifyList) if `changed` is empty, so seq_ only
    // advances on an observable change. Also emits a Bbo, sharing this
    // same (already-bumped) seq_ value, but only when the best bid or ask
    // actually changed - a deep-book change produces an L2Diff but no Bbo,
    // which is why Bbo.book_seq is allowed to skip values.
    NotifyList publish(const std::vector<Change>& changed) {
        if (changed.empty()) return {};

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

        NotifyList to_notify;
        to_notify.l2 = l2_fanout_.broadcast(std::make_shared<const L2Update>(std::move(update)));

        auto bid = best_bid();
        auto ask = best_ask();
        if (bid != last_bbo_bid_ || ask != last_bbo_ask_) {
            last_bbo_bid_ = bid;
            last_bbo_ask_ = ask;
            to_notify.bbo = bbo_fanout_.broadcast(std::make_shared<const BboUpdate>(build_bbo()));
        }
        return to_notify;
    }

    std::mutex mutex_;
    AggregateOrderBook book_;
    Fanout<L2Update> l2_fanout_{kSubscriberQueueCapacity, OverflowPolicy::Close};
    Fanout<BboUpdate> bbo_fanout_{kSubscriberQueueCapacity, OverflowPolicy::DropOldest};
    std::uint64_t seq_ = 0;
    // Last best bid/ask actually broadcast to bbo_subscribers_, compared
    // against on every publish() to decide whether this revision touched
    // the top of book. nullopt means that side had no level.
    std::optional<std::pair<Price, Size>> last_bbo_bid_;
    std::optional<std::pair<Price, Size>> last_bbo_ask_;
};

// The sole gRPC service type in this file: one instance serves every book
// it was constructed with, on one port, by routing each RPC (and each
// ingestion call) to that book's own SymbolBook - see SymbolBook's class
// comment for why that beats one grpc::Service instance per book.
class AggregatorService final : public Aggregator::Service {
  public:
    // `books` must be non-empty with unique entries - both are startup
    // configuration preconditions (asserted, not runtime-checked: this
    // isn't external input), not something a client's request can violate.
    // The resulting book set is fixed for this instance's lifetime - no
    // dynamic add/remove.
    explicit AggregatorService(std::span<const symbol::BookId> books) {
        assert(!books.empty());
        for (const auto& id : books) {
            [[maybe_unused]] auto [it, inserted] = books_.try_emplace(id);
            assert(inserted);
        }
    }

    // Returns nullptr if `id` isn't one this instance was constructed with.
    // Used by SubscribeL2Diff()/SubscribeBbo() below, and by an ingestion
    // layer routing a parsed delta/snapshot/invalidation to the right book.
    SymbolBook* book(const symbol::BookId& id) {
        auto it = books_.find(id);
        return it != books_.end() ? &it->second : nullptr;
    }

    // Broadcasts a liveness heartbeat to every subscriber of every book.
    void send_heartbeat() {
        for (auto& [id, symbol_book] : books_) symbol_book.send_heartbeat();
    }

    grpc::Status SubscribeL2Diff(grpc::ServerContext* context, const SubscribeL2DiffRequest* request,
                                  grpc::ServerWriter<L2Update>* writer) override {
        auto id = to_symbol_book_id(request->book());
        if (!id) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                 "malformed book: base/quote must be non-empty and market must be "
                                 "SPOT or PERP");
        }
        SymbolBook* target = book(*id);
        if (!target) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown book: " + symbol::to_string(*id));
        }

        auto queue = target->subscribe(writer);
        if (!queue) return grpc::Status::OK;

        // This thread is the sole owner of `writer`/`queue` from here on.
        // gRPC's synchronous API has no primitive to block on "cancelled
        // or a new update queued"; the 50ms timeout only re-checks
        // IsCancelled() - an actual push wakes this thread immediately via
        // SubscriberQueue's condition variable.
        std::vector<std::shared_ptr<const L2Update>> batch;
        while (true) {
            batch.clear();
            auto result = queue->wait_and_drain(std::chrono::milliseconds(50), batch);
            if (result == SubscriberQueue<L2Update>::DrainResult::Closed) {
                target->unsubscribe(queue);
                return grpc::Status(grpc::StatusCode::RESOURCE_EXHAUSTED,
                                     "subscriber fell too far behind; reconnect for a fresh "
                                     "snapshot");
            }
            for (auto& update : batch) {
                if (!writer->Write(*update)) {
                    target->unsubscribe(queue);
                    return grpc::Status::OK;
                }
            }
            if (context->IsCancelled()) {
                target->unsubscribe(queue);
                return grpc::Status::OK;
            }
        }
    }

    grpc::Status SubscribeBbo(grpc::ServerContext* context, const SubscribeBboRequest* request,
                               grpc::ServerWriter<BboUpdate>* writer) override {
        auto id = to_symbol_book_id(request->book());
        if (!id) {
            return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                 "malformed book: base/quote must be non-empty and market must be "
                                 "SPOT or PERP");
        }
        SymbolBook* target = book(*id);
        if (!target) {
            return grpc::Status(grpc::StatusCode::NOT_FOUND, "unknown book: " + symbol::to_string(*id));
        }

        auto queue = target->subscribe_bbo(writer);
        if (!queue) return grpc::Status::OK;

        // Same single-owner-thread shape as SubscribeL2Diff() above.
        // DrainResult::Closed is unreachable here (DropOldest never
        // closes) but kept for exhaustiveness, and returns OK rather than
        // RESOURCE_EXHAUSTED: a BBO subscriber has no resync obligation.
        std::vector<std::shared_ptr<const BboUpdate>> batch;
        while (true) {
            batch.clear();
            auto result = queue->wait_and_drain(std::chrono::milliseconds(50), batch);
            if (result == SubscriberQueue<BboUpdate>::DrainResult::Closed) {
                target->unsubscribe_bbo(queue);
                return grpc::Status::OK;
            }
            for (auto& update : batch) {
                if (!writer->Write(*update)) {
                    target->unsubscribe_bbo(queue);
                    return grpc::Status::OK;
                }
            }
            if (context->IsCancelled()) {
                target->unsubscribe_bbo(queue);
                return grpc::Status::OK;
            }
        }
    }

    grpc::Status ListBooks(grpc::ServerContext*, const ListBooksRequest*,
                            ListBooksResponse* response) override {
        for (const auto& entry : books_) {
            fill_wire_book_id(response->add_books(), entry.first);
        }
        return grpc::Status::OK;
    }

  private:
    std::unordered_map<symbol::BookId, SymbolBook> books_;
};

}  // namespace bobby::hermeneutic::aggregator

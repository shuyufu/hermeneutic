#pragma once

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdint>
#include <expected>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <span>
#include <system_error>
#include <thread>
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

// Wraps an AggregateOrderBook and fans out every change to subscribed gRPC
// clients. Concurrency model (V1): a single mutex guards both the book and
// the subscriber set; apply_delta/apply_snapshot/invalidate_venue and
// Subscribe's initial snapshot all write to ServerWriters synchronously,
// under that same lock. Trade-off accepted: a slow subscriber can slow down
// broadcasting (and therefore ingestion) for everyone. Bounded, not solved,
// by the server-side gRPC keepalive configured in aggregator_main.cpp: an
// unresponsive connection eventually fails its Write() instead of blocking
// forever.
class AggregatorService final : public Aggregator::Service {
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

    // Broadcasts a liveness signal to every subscriber, independent of any
    // book change - unlike publish(), this never touches seq_ (a heartbeat
    // is not a book revision). Callers (e.g. aggregator_main.cpp) are
    // expected to invoke this on a fixed interval; this class has no
    // internal timer of its own.
    void send_heartbeat() {
        std::lock_guard lock(mutex_);
        L2Update update;
        update.mutable_heartbeat()->set_ts_ns(now_ns());
        broadcast_to_subscribers(update);
    }

    grpc::Status Subscribe(grpc::ServerContext* context, const SubscribeRequest*,
                            grpc::ServerWriter<L2Update>* writer) override {
        {
            std::lock_guard lock(mutex_);
            if (!writer->Write(build_snapshot())) return grpc::Status::OK;
            subscribers_.insert(writer);
        }

        // gRPC's synchronous API has no primitive to block on "cancelled or a
        // broadcast happened"; poll instead. Broadcasts (publish()) write to
        // `writer` directly from whichever thread calls apply_delta/
        // apply_snapshot/invalidate_venue, always under mutex_, so there is
        // never a concurrent Write() to the same writer from two threads.
        while (true) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            std::lock_guard lock(mutex_);
            if (context->IsCancelled() || !subscribers_.contains(writer)) {
                subscribers_.erase(writer);
                break;
            }
        }
        return grpc::Status::OK;
    }

  private:
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

    // Must be called with mutex_ held. Writes `update` to every subscriber,
    // removing any whose Write() fails (see the class comment's note on
    // Write() failures and cleanup).
    void broadcast_to_subscribers(const L2Update& update) {
        for (auto it = subscribers_.begin(); it != subscribers_.end();) {
            if ((*it)->Write(update)) {
                ++it;
            } else {
                it = subscribers_.erase(it);
            }
        }
    }

    std::mutex mutex_;
    AggregateOrderBook book_;
    std::set<grpc::ServerWriter<L2Update>*> subscribers_;
    std::uint64_t seq_ = 0;
};

}  // namespace bobby::hermeneutic::aggregator

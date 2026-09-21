#pragma once

#include <expected>
#include <map>
#include <new>
#include <span>
#include <system_error>
#include <unordered_map>
#include <utility>

#include "bobby/hermeneutic/book/l2_order_book.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic {

// A venue's identity, keyed by (Exchange, MarketType) - see symbol.hpp's
// own comment on why this is a structured type, not a hand-spelled
// string, and std::hash<VenueId> there for why venues_ below can be
// unordered_map.
using VenueId = bobby::hermeneutic::symbol::VenueId;

enum class Side { Bid, Ask };

// Combines per-venue L2 order books into one aggregated view.
// Each venue's L2OrderBook is the source of truth; every delta is also
// applied to the aggregate incrementally, so the aggregate never needs a
// full rebuild (Price/Size are exact fixed-point, so deltas never drift).
//
// Every mutating method below optionally takes a `Sink` (or one `Sink` per
// side): a callable invoked as `sink(price, new_aggregate_size)` for every
// price whose *aggregate* size actually changed - a removal is reported as
// Size{} (0), not a negative intermediate. Never invoked for a price whose
// net effect on the aggregate is zero (see adjust_aggregate()'s own
// comment). This lets a caller (aggregator::SymbolBook) observe exactly
// what changed, in whatever order it wants to collect it in, without this
// class needing to know anything about diffs, ordering, or wire formats -
// it only ever calls `sink` with a price and a size. Every sink parameter
// defaults to NoopSink, a stateless no-op, so a caller that doesn't pass
// one (VenueSession, most tests) keeps the exact same call shape as
// before and pays nothing for the feature.
//
// `Sink::operator()` must not throw anything other than std::bad_alloc:
// every method below is noexcept (or, for invalidate_venue(), was already
// throw-averse - see its own comment) and only ever catches that one
// exception type, exactly as it already trusted its own internal
// std::map operations to never throw anything else. A Sink that violates
// this terminates the program via a noexcept violation on apply_snapshot()/
// apply_batch(); see invalidate_venue()'s own comment for what it does
// instead.
class AggregateOrderBook {
  private:
    // Default Sink for every method below: does nothing, so a caller that
    // never names this type (just omits the argument) pays no allocation
    // and, once inlined, no call overhead either. Declared first (in its
    // own private: block, ahead of the public: one below) so it's a
    // complete type everywhere it's used as a default template argument -
    // default *function* arguments get complete-class-context lookup for
    // free, but default *template* arguments do not, so this can't rely
    // on being declared later the way the private helpers further down
    // can.
    struct NoopSink {
        void operator()(Price, Size) const noexcept {}
    };

  public:
    // Removes every level `venue` contributed and drops its book entirely.
    // Call this as soon as its feed is known to be untrustworthy (websocket
    // disconnect, or a sequence-number gap) so the aggregate does not keep
    // reflecting stale liquidity while the venue resynchronizes.
    //
    // Unlike the other three methods below, this one has no try/catch of
    // its own - it was never noexcept, but before Sink existed, nothing it
    // did could practically throw (venues_.erase() and every removal
    // adjust_aggregate() does here only ever erase, never allocate). A
    // `Sink` that itself allocates (e.g. inserting a new key into a
    // collector map) reintroduces a real throw path: an exception mid-loop
    // leaves `aggregate_` partially decremented, `venues_.erase(it)` below
    // never reached, and no way for anything downstream to know which
    // prices still need subtracting - worse than any of this class's other
    // documented not-rolled-back cases, because those leave venues_/
    // aggregate_ mutually consistent and this would not. This function
    // does not defend against that (catching here and still erasing would
    // erase the one record - `it->second` - of what remains unsubtracted,
    // turning a recoverable gap into an unrecoverable one). A caller whose
    // Sink can allocate should make sure it can't do so *inside* this
    // call - e.g. by pre-inserting every key it will receive (known
    // upfront: exactly `venue`'s own current bids/asks) before calling
    // this, so every call here only ever reassigns an existing entry - see
    // aggregator::SymbolBook::invalidate_venue() for the concrete
    // technique.
    template <typename BidSink = NoopSink, typename AskSink = NoopSink>
    void invalidate_venue(const VenueId& venue, BidSink&& on_bid_change = {},
                           AskSink&& on_ask_change = {}) {
        auto it = venues_.find(venue);
        if (it == venues_.end()) return;

        for (const auto& [price, size] : it->second.bids) {
            adjust_aggregate(aggregate_.bids, price, Size{} - size, on_bid_change);
        }
        for (const auto& [price, size] : it->second.asks) {
            adjust_aggregate(aggregate_.asks, price, Size{} - size, on_ask_change);
        }
        venues_.erase(it);
    }

    // Replaces `venue`'s entire book (both sides at once) wholesale from a
    // REST snapshot. Any price the venue previously held on a side that is
    // absent from that side's new `levels` is treated as removed. Use this
    // to resynchronize after invalidate_venue(). Both sides are validated
    // before either is touched - a bad level on one side leaves the other
    // side's still-good data un-replaced too, rather than resyncing one
    // side and leaving the other stale (see apply_batch()'s own doc
    // comment for why the two sides can't be validated/applied
    // independently: exactly the same reasoning applies here). Returns
    // std::errc::invalid_argument, without modifying any state, if any
    // level's price is not positive or its size is negative, or
    // std::errc::not_enough_memory if allocation fails partway through
    // applying the snapshot - partway through includes a `Sink` itself
    // throwing bad_alloc, not just this class's own maps: either way, the
    // mutation up to that point is not rolled back (same documented
    // limitation as apply_batch()).
    template <typename BidSink = NoopSink, typename AskSink = NoopSink>
    std::expected<void, std::errc> apply_snapshot(
        const VenueId& venue, std::span<const std::pair<Price, Size>> bids,
        std::span<const std::pair<Price, Size>> asks, BidSink&& on_bid_change = {},
        AskSink&& on_ask_change = {}) noexcept {
        for (const auto& [price, size] : bids) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }
        for (const auto& [price, size] : asks) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }

        try {
            auto& venue_book = venues_[venue];
            resync_side(venue_book.bids, aggregate_.bids, bids, on_bid_change);
            resync_side(venue_book.asks, aggregate_.asks, asks, on_ask_change);
        } catch (const std::bad_alloc&) {
            return std::unexpected(std::errc::not_enough_memory);
        }
        return {};
    }

    // Applies one batch of delta changes (e.g. everything one upstream
    // exchange message carried) through a single call, so a caller that
    // wants to treat the whole batch as one atomic revision (one sequence
    // bump, one broadcast - see aggregator::SymbolBook::apply_batch) can
    // hook that behavior onto exactly one call instead of reimplementing
    // this same validate-then-apply shape itself. Every level's price/size
    // is checked before any of them is applied, so a bad level anywhere in
    // the batch leaves the book untouched rather than partially updated
    // for that reason specifically (an allocation failure partway through
    // - including a `Sink` itself throwing bad_alloc - is not rolled back,
    // same documented limitation as apply_snapshot()).
    //
    // Levels within one side are applied in `bids`/`asks` order without
    // deduplicating a repeated price - a real exchange message never
    // repeats a price within one update, so this doesn't defend against
    // one that does, unlike the before/after diff this class's own
    // callers used to compute for themselves (deleted - see this class's
    // git history) - that comparison was, incidentally, immune to this
    // exact case, and the Sink-based replacement is a real if narrow
    // regression on it. If a repeated price ever happened, each
    // occurrence would still land correctly on `venue_side` and the
    // aggregate (apply_level() sets an absolute size, so applying the
    // same price twice in sequence converges to the same final state as
    // applying it once), but `Sink` would see one call per occurrence,
    // and - if the net effect of all of them together happened to match
    // this price's size from before the batch - a caller collecting "the
    // last size `Sink` saw per price" would report a no-op price as
    // changed. Low-severity (one spurious broadcast, not incorrect
    // state) and not reachable by real exchange data, so not worth the
    // extra pass this class would need to dedupe it away.
    template <typename BidSink = NoopSink, typename AskSink = NoopSink>
    std::expected<void, std::errc> apply_batch(const VenueId& venue,
                                                std::span<const std::pair<Price, Size>> bids,
                                                std::span<const std::pair<Price, Size>> asks,
                                                BidSink&& on_bid_change = {},
                                                AskSink&& on_ask_change = {}) noexcept {
        for (const auto& [price, size] : bids) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }
        for (const auto& [price, size] : asks) {
            if (!is_valid_level(price, size)) return std::unexpected(std::errc::invalid_argument);
        }

        try {
            auto& venue_book = venues_[venue];
            for (const auto& [price, size] : bids) {
                apply_level(venue_book.bids, aggregate_.bids, price, size, on_bid_change);
            }
            for (const auto& [price, size] : asks) {
                apply_level(venue_book.asks, aggregate_.asks, price, size, on_ask_change);
            }
        } catch (const std::bad_alloc&) {
            return std::unexpected(std::errc::not_enough_memory);
        }
        return {};
    }

    const L2OrderBook& aggregate() const noexcept { return aggregate_; }
    const std::unordered_map<VenueId, L2OrderBook>& venues() const noexcept { return venues_; }

  private:
    // Sets `price` to `new_size` in `side` (erasing it when <= 0) and
    // returns the size that was there before, so the caller can derive a delta.
    // `new_size` is validated non-negative by the public entry points; the
    // `<= 0` check (rather than `== 0`) is defense in depth so a level can
    // never get stuck at a negative size and silently corrupt every sum at
    // that price from then on.
    template <typename Map>
    static Size set_level(Map& side, Price price, Size new_size) {
        auto it = side.find(price);
        Size old_size = (it != side.end()) ? it->second : Size{};
        if (new_size.raw() <= 0) {
            if (it != side.end()) side.erase(it);
        } else if (it != side.end()) {
            it->second = new_size;
        } else {
            side.emplace(price, new_size);
        }
        return old_size;
    }

    // Adds `delta` (already computed by the caller - a difference between
    // two absolute sizes, or a straight negation to remove a level) to
    // `price` in `aggregate_side`, erasing it when the result is <= 0.
    // Unlike set_level(), there is no "new absolute size" here: the
    // aggregate never has one value to set to, since more than one venue
    // can hold the same price - only ever a delta to fold in.
    //
    // `delta.raw() == 0` is the single source of truth for "no observable
    // change": `sink` is only ever invoked past that check, so every call
    // to it reports a real change to the aggregate, reported as the
    // resulting size (Size{} if the level was removed, never the
    // intermediate `updated` value that triggered the removal).
    template <typename Map, typename Sink>
    static void adjust_aggregate(Map& aggregate_side, Price price, Size delta, Sink&& sink) {
        if (delta.raw() == 0) return;

        auto it = aggregate_side.find(price);
        Size updated = (it != aggregate_side.end() ? it->second : Size{}) + delta;
        if (updated.raw() <= 0) {
            if (it != aggregate_side.end()) aggregate_side.erase(it);
            sink(price, Size{});
        } else {
            if (it != aggregate_side.end()) {
                it->second = updated;
            } else {
                aggregate_side.emplace(price, updated);
            }
            sink(price, updated);
        }
    }

    // Sets one (price, new_size) level on both `venue_side` and, by the
    // delta that produces, `aggregate_side` - the one operation every
    // public entry point above bottoms out at for a single level, whether
    // it came from a delta, a batch, or diffing a snapshot.
    template <typename Map, typename Sink>
    static void apply_level(Map& venue_side, Map& aggregate_side, Price price, Size new_size,
                             Sink&& sink) {
        Size old_size = set_level(venue_side, price, new_size);
        adjust_aggregate(aggregate_side, price, new_size - old_size, sink);
    }

    // Resyncs one side of `venue_side` to `levels`: any price it currently
    // holds that's absent from `levels` is removed (propagating that
    // removal to `aggregate_side`), then every (price, size) in `levels`
    // is applied via apply_level(). Called once per side from
    // apply_snapshot(), after that function has already validated both
    // sides' levels - this function itself performs no validation and
    // offers no atomicity between the two calls; that guarantee comes
    // entirely from apply_snapshot() checking both spans before either
    // call happens.
    //
    // `new_levels` uses `Map::key_compare` (matching `venue_side`'s/
    // `aggregate_side`'s own comparator) purely for consistency, not
    // correctness: removals are reported before additions regardless of
    // either's order, so the two loops below concatenated are never one
    // globally-sorted sequence of `sink` calls on their own (e.g. bids
    // holding {105, 100}, resynced to {103}, reports 105, 100, then 103 -
    // not descending). A caller that needs the *changes* in the book's
    // own order, not just the final state, has to collect them into its
    // own comparator-ordered structure keyed by price - `sink` call order
    // was never a promise this function makes.
    template <typename Map, typename Sink>
    static void resync_side(Map& venue_side, Map& aggregate_side,
                             std::span<const std::pair<Price, Size>> levels, Sink&& sink) {
        std::map<Price, Size, typename Map::key_compare> new_levels(levels.begin(), levels.end());

        for (auto it = venue_side.begin(); it != venue_side.end();) {
            if (new_levels.contains(it->first)) {
                ++it;
            } else {
                adjust_aggregate(aggregate_side, it->first, Size{} - it->second, sink);
                it = venue_side.erase(it);
            }
        }

        for (const auto& [price, size] : new_levels) {
            apply_level(venue_side, aggregate_side, price, size, sink);
        }
    }

    std::unordered_map<VenueId, L2OrderBook> venues_;
    L2OrderBook aggregate_;
};

}  // namespace bobby::hermeneutic

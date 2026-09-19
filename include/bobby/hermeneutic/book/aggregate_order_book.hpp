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

// A venue's identity, keyed by (Venue, MarketType) - see symbol.hpp's own
// comment on why this is a structured type, not a hand-spelled string, and
// std::hash<VenueId> there for why venues_ below can be unordered_map.
using VenueId = bobby::hermeneutic::symbol::VenueId;

enum class Side { Bid, Ask };

// Combines per-venue L2 order books into one aggregated view.
// Each venue's L2OrderBook is the source of truth; every delta is also
// applied to the aggregate incrementally, so the aggregate never needs a
// full rebuild (Price/Size are exact fixed-point, so deltas never drift).
class AggregateOrderBook {
  public:
    // A `size` of zero removes that venue's level (Binance L2 diff semantics).
    // Returns std::errc::invalid_argument if `size` is negative, or
    // std::errc::not_enough_memory if a new venue's book could not be
    // allocated.
    std::expected<void, std::errc> apply_delta(const VenueId& venue, Side side, Price price,
                                                Size size) noexcept {
        if (auto check = require_non_negative_size(size); !check) return check;

        try {
            auto& venue_book = venues_[venue];
            if (side == Side::Bid) {
                apply_side(venue_book.bids, aggregate_.bids, price, size);
            } else {
                apply_side(venue_book.asks, aggregate_.asks, price, size);
            }
        } catch (const std::bad_alloc&) {
            return std::unexpected(std::errc::not_enough_memory);
        }
        return {};
    }

    // Removes every level `venue` contributed and drops its book entirely.
    // Call this as soon as its feed is known to be untrustworthy (websocket
    // disconnect, or a sequence-number gap) so the aggregate does not keep
    // reflecting stale liquidity while the venue resynchronizes.
    void invalidate_venue(const VenueId& venue) {
        auto it = venues_.find(venue);
        if (it == venues_.end()) return;

        for (const auto& [price, size] : it->second.bids) {
            apply_aggregate_delta(aggregate_.bids, price, Size{} - size);
        }
        for (const auto& [price, size] : it->second.asks) {
            apply_aggregate_delta(aggregate_.asks, price, Size{} - size);
        }
        venues_.erase(it);
    }

    // Replaces one side of `venue`'s book wholesale from a REST snapshot.
    // Any price the venue previously held that is absent from `levels` is
    // treated as removed. Use this to resynchronize after invalidate_venue().
    // Returns std::errc::invalid_argument, without modifying any state, if
    // any level's size is negative, or std::errc::not_enough_memory if
    // allocation fails partway through applying the snapshot.
    std::expected<void, std::errc> apply_snapshot(
        const VenueId& venue, Side side, std::span<const std::pair<Price, Size>> levels) noexcept {
        for (const auto& level : levels) {
            if (auto check = require_non_negative_size(level.second); !check) return check;
        }

        try {
            auto& venue_book = venues_[venue];
            if (side == Side::Bid) {
                apply_snapshot_side(venue_book.bids, aggregate_.bids, levels);
            } else {
                apply_snapshot_side(venue_book.asks, aggregate_.asks, levels);
            }
        } catch (const std::bad_alloc&) {
            return std::unexpected(std::errc::not_enough_memory);
        }
        return {};
    }

    // Applies one batch of delta changes (e.g. everything one upstream
    // exchange message carried) through a single call, rather than one
    // apply_delta() per level - lets a caller that wants to treat the
    // whole batch as one atomic revision (one sequence bump, one
    // broadcast - see aggregator::SymbolBook::apply_batch) hook that
    // behavior onto exactly one call instead of reimplementing this same
    // validate-then-apply shape itself. Same per-level negative-size
    // rejection as apply_delta, checked for every level before any of
    // them is applied, so a bad level anywhere in the batch leaves the
    // book untouched rather than partially updated for that reason
    // specifically (an allocation failure partway through is not rolled
    // back - same documented limitation as apply_snapshot()).
    std::expected<void, std::errc> apply_batch(const VenueId& venue,
                                                std::span<const std::pair<Price, Size>> bids,
                                                std::span<const std::pair<Price, Size>> asks) noexcept {
        for (const auto& [price, size] : bids) {
            if (auto check = require_non_negative_size(size); !check) return check;
        }
        for (const auto& [price, size] : asks) {
            if (auto check = require_non_negative_size(size); !check) return check;
        }

        for (const auto& [price, size] : bids) {
            if (auto result = apply_delta(venue, Side::Bid, price, size); !result) return result;
        }
        for (const auto& [price, size] : asks) {
            if (auto result = apply_delta(venue, Side::Ask, price, size); !result) return result;
        }
        return {};
    }

    const L2OrderBook& aggregate() const noexcept { return aggregate_; }
    const std::unordered_map<VenueId, L2OrderBook>& venues() const noexcept { return venues_; }

  private:
    static std::expected<void, std::errc> require_non_negative_size(Size size) noexcept {
        if (size.raw() < 0) {
            return std::unexpected(std::errc::invalid_argument);
        }
        return {};
    }

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

    template <typename Map>
    static void apply_aggregate_delta(Map& aggregate_side, Price price, Size delta) {
        if (delta.raw() == 0) return;

        auto it = aggregate_side.find(price);
        Size updated = (it != aggregate_side.end() ? it->second : Size{}) + delta;
        if (updated.raw() <= 0) {
            if (it != aggregate_side.end()) aggregate_side.erase(it);
        } else if (it != aggregate_side.end()) {
            it->second = updated;
        } else {
            aggregate_side.emplace(price, updated);
        }
    }

    template <typename Map>
    static void apply_side(Map& venue_side, Map& aggregate_side, Price price, Size new_size) {
        Size old_size = set_level(venue_side, price, new_size);
        apply_aggregate_delta(aggregate_side, price, new_size - old_size);
    }

    template <typename Map>
    static void apply_snapshot_side(Map& venue_side, Map& aggregate_side,
                                     std::span<const std::pair<Price, Size>> levels) {
        std::map<Price, Size> new_levels(levels.begin(), levels.end());

        for (auto it = venue_side.begin(); it != venue_side.end();) {
            if (new_levels.contains(it->first)) {
                ++it;
            } else {
                apply_aggregate_delta(aggregate_side, it->first, Size{} - it->second);
                it = venue_side.erase(it);
            }
        }

        for (const auto& [price, size] : new_levels) {
            apply_side(venue_side, aggregate_side, price, size);
        }
    }

    std::unordered_map<VenueId, L2OrderBook> venues_;
    L2OrderBook aggregate_;
};

}  // namespace bobby::hermeneutic

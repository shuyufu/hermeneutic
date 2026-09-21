#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <expected>
#include <span>
#include <system_error>
#include <type_traits>
#include <vector>

#include "bobby/hermeneutic/book/l2_order_book.hpp"
#include "bobby/hermeneutic/core/notional.hpp"

namespace bobby::hermeneutic {

// Depth within `bps` basis points of BBO on one side of the book.
// `boundary_price` is an inward-rounded Price representation of that
// boundary, used only for reporting (logging/UI/API) -- not to Price's
// own precision, it's exact only to the mathematical boundary. Membership
// is determined exactly by detail::within_bps(), not by comparing against
// `boundary_price`. If the book has a BBO but runs out of depth before a
// boundary, `cumulative_size`/`cumulative_notional` report the whole
// book's depth (a valid answer, not a failure); an empty side has no BBO
// to offset from and produces no bands at all.
struct PriceBand {
    int bps;
    Price boundary_price;
    Size cumulative_size;
    Notional cumulative_notional;
};

namespace detail {

// Offsets `price` by `signed_bps` basis points, rounded *inward* (floor
// for an ask boundary via round_down=true, ceil for a bid boundary via
// round_down=false) so `boundary_price` never overstates the band's
// actual reach. Display only -- membership is decided by within_bps()
// below, not this function.
//
// Fails with std::errc::result_out_of_range if the resulting boundary
// doesn't fit back into Price::raw_type - unlike notional.hpp's operators
// (from_raw_checked()'s usual debug-only assert), this is a genuine
// runtime possibility here, not just an internal invariant: bid_price_
// band_depths()/ask_price_band_depths() cap the *bid* side's bps at under
// 10000 (a domain requirement - a bid boundary at or past that would be
// zero or negative), but the ask side has no such ceiling, and this
// project validates no upper bound on price itself either (is_valid_level()
// only checks price > 0) - so a sufficiently large ask bps threshold
// against a sufficiently large price is a real, externally-reachable way
// to overflow the __int128 intermediate's narrowing back to raw_type, not
// a "no realistic input reaches this" case the way every other
// from_raw_checked() call site in this codebase is. Price::from_raw_safe()
// is the shared bounds check for exactly this situation - see its own
// comment.
constexpr std::expected<Price, std::errc> offset_by_bps(Price price, int signed_bps,
                                                          bool round_down) noexcept {
    assert(price.raw() > 0);

    // Widened to __int128 before adding: native `int` arithmetic would
    // overflow for signed_bps near INT_MIN/MAX. factor > 0 is required for
    // floor/ceil below to be correct, and for a bid boundary to stay
    // positive (needs bps < 10000).
    const __int128 factor = static_cast<__int128>(10'000) + static_cast<__int128>(signed_bps);
    assert(factor > 0);

    __int128 scaled_numerator = static_cast<__int128>(price.raw()) * factor;
    constexpr __int128 denom = 10'000;
    __int128 rounded = round_down ? scaled_numerator / denom
                                   : (scaled_numerator + denom - 1) / denom;

    return Price::from_raw_safe(rounded);
}

// Exact membership test: price <= best_price*(10000+signed_bps)/10000
// (ge=false, ask side) or price >= that ratio (ge=true, bid side), via
// __int128 cross-multiplication -- never rounds, so correctness here is
// independent of offset_by_bps()'s display rounding.
constexpr bool within_bps(Price price, Price best_price, int signed_bps, bool ge) noexcept {
    assert(best_price.raw() > 0);

    const __int128 factor = static_cast<__int128>(10'000) + static_cast<__int128>(signed_bps);
    assert(factor > 0);

    __int128 lhs = static_cast<__int128>(price.raw()) * 10'000;
    __int128 rhs = static_cast<__int128>(best_price.raw()) * factor;
    return ge ? lhs >= rhs : lhs <= rhs;
}

// Ask/Bid: the two side-specific policies for how offset_by_bps/within_bps's
// round_down/ge booleans are derived, so a caller states its side once
// (Ask:: or Bid::) instead of re-deriving both booleans from a signed bps
// value by hand and risking getting one of them backwards -- price_band_depth
// below used to do exactly that derivation inline. "reference" here is
// whatever Price the caller passes in -- this side's own best price in
// price_band_depth's use below, never a midprice (this file doesn't compute
// one). Each type takes an unsigned `bps` and applies its side's sign
// internally, rather than making the caller pre-multiply by +1/-1.
//
// round_away (the boundary that overstates reach, rounding outward instead
// of inward) has no caller today and is deliberately not implemented here --
// add it only when something actually needs it.
struct Ask {
    static constexpr std::expected<Price, std::errc> round_inner(Price price, int bps) noexcept {
        return offset_by_bps(price, bps, /*round_down=*/true);
    }
    static constexpr bool within(Price price, Price reference, int bps) noexcept {
        return within_bps(price, reference, bps, /*ge=*/false);
    }
};

struct Bid {
    static constexpr std::expected<Price, std::errc> round_inner(Price price, int bps) noexcept {
        return offset_by_bps(price, -bps, /*round_down=*/false);
    }
    static constexpr bool within(Price price, Price reference, int bps) noexcept {
        return within_bps(price, reference, -bps, /*ge=*/true);
    }
};

}  // namespace detail

// Walks `levels` (best price first -- a precondition, not checked) and,
// for each threshold in `bps_thresholds` (sorted ascending, non-negative,
// and strictly under 10000 for the bid side), reports the depth within
// that many bps of `levels`' own best price. `best_price` is taken from
// `levels` itself rather than passed separately, so it can't mismatch the
// map it's derived from. `Side` (detail::Ask or detail::Bid) is a template
// parameter rather than a runtime flag because every real caller already
// knows its side at compile time -- bid_price_band_depths/
// ask_price_band_depths below hardcode it, and so does every caller of
// those two. A level exactly at a boundary counts as within it.
// O(levels + bps_thresholds).
//
// Fails with std::errc::invalid_argument if `bps_thresholds` itself isn't
// sorted/non-negative/(for bids) under 10000 - checked at runtime rather
// than only asserted, since `bps_thresholds` comes from the caller (an API
// request, ultimately), not from this module's own internal bookkeeping
// the way `Side` does (a compile-time choice this module's only callers -
// bid_price_band_depths()/ask_price_band_depths() - already make via the
// static_assert below, not something a caller passes in at runtime). The
// algorithm below walks bps_thresholds with a single monotonically-
// increasing index (`next`), so an unsorted input would silently compute
// wrong band boundaries in a release build rather than failing loudly -
// the same reasoning is why offset_by_bps()/within_bps() below are still
// assert()-only for their own price>0 precondition: this function
// validates that before ever calling them, so it's never checked twice.
//
// Also fails with std::errc::argument_out_of_domain if any level has a
// non-positive price (or a negative size), checked as each level is
// walked - same rationale as volume_band_prices()'s own per-level check:
// `levels` is the caller's own already-mutated book, not something this
// function controls, so a level that shouldn't exist (a malformed
// upstream message that slipped past whatever inserted it) is a runtime
// condition this must reject, not an assert()-only precondition that
// silently no-ops in a release build. offset_by_bps()/within_bps() (reached
// through Side::round_inner()/Side::within()) only assert() their own
// price>0 precondition precisely because this function is what's
// responsible for upholding it before ever calling them - never validated
// twice.
//
// Also fails with std::errc::result_out_of_range if a boundary computed
// from a (validated-in-range, but still arbitrarily large) bps threshold
// against an (unbounded - this project validates no upper limit on price
// anywhere) level price doesn't fit back into Price - see
// offset_by_bps()'s own comment for why this is a real runtime
// possibility here, not just an internal invariant - or if the running
// cum_notional total itself overflows Notional while accumulating across
// levels: each individual price*size can be in-range while the series
// still isn't, which a plain per-call assert (BasicFixedPoint's own
// operator+=) can't catch - see Notional::from_raw_safe()'s own comment.
template <typename Side, typename Map>
std::expected<std::vector<PriceBand>, std::errc> price_band_depth(
    const Map& levels, std::span<const int> bps_thresholds) {
    static_assert(std::is_same_v<Side, detail::Ask> || std::is_same_v<Side, detail::Bid>,
                  "Side must be detail::Ask or detail::Bid");
    constexpr bool kBidUpperBoundApplies = std::is_same_v<Side, detail::Bid>;
    if (!std::ranges::is_sorted(bps_thresholds) ||
        !std::ranges::all_of(bps_thresholds, [](int bps) { return bps >= 0; }) ||
        (kBidUpperBoundApplies &&
         !std::ranges::all_of(bps_thresholds, [](int bps) { return bps < 10'000; }))) {
        return std::unexpected(std::errc::invalid_argument);
    }

    std::vector<PriceBand> result;
    result.reserve(bps_thresholds.size());
    if (levels.empty()) return result;  // no BBO to offset from -> no bands
    // best_price is levels.begin()->first, which the loop below validates
    // on its own first iteration (no separate check needed here).
    Price best_price = levels.begin()->first;

    Size cum_size{};
    Notional cum_notional{};
    std::size_t next = 0;

    for (const auto& [price, size] : levels) {
        if (!is_valid_level(price, size)) return std::unexpected(std::errc::argument_out_of_domain);

        while (next < bps_thresholds.size()) {
            int bps = bps_thresholds[next];
            if (Side::within(price, best_price, bps)) break;
            auto boundary = Side::round_inner(best_price, bps);
            if (!boundary) return std::unexpected(boundary.error());
            result.push_back({bps_thresholds[next], *boundary, cum_size, cum_notional});
            ++next;
        }
        if (next >= bps_thresholds.size()) break;

        cum_size += size;
        // Checked, not a plain `cum_notional += price * size`: the running
        // total across many levels is the actual overflow risk, not any
        // single level's own product (which is already bounds-checked, via
        // from_raw_checked, inside operator*) - see this function's own
        // doc comment.
        auto next_cum_notional = Notional::from_raw_safe(static_cast<__int128>(cum_notional.raw()) +
                                                           static_cast<__int128>((price * size).raw()));
        if (!next_cum_notional) return std::unexpected(next_cum_notional.error());
        cum_notional = *next_cum_notional;
    }

    while (next < bps_thresholds.size()) {
        auto boundary = Side::round_inner(best_price, bps_thresholds[next]);
        if (!boundary) return std::unexpected(boundary.error());
        result.push_back({bps_thresholds[next], *boundary, cum_size, cum_notional});
        ++next;
    }

    return result;
}

inline std::expected<std::vector<PriceBand>, std::errc> bid_price_band_depths(
    const L2OrderBook& book, std::span<const int> bps_thresholds) {
    return price_band_depth<detail::Bid>(book.bids, bps_thresholds);
}

inline std::expected<std::vector<PriceBand>, std::errc> ask_price_band_depths(
    const L2OrderBook& book, std::span<const int> bps_thresholds) {
    return price_band_depth<detail::Ask>(book.asks, bps_thresholds);
}

}  // namespace bobby::hermeneutic

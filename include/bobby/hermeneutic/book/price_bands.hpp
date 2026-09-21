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
constexpr Price offset_by_bps(Price price, int signed_bps, bool round_down) noexcept {
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

    // Narrowing back to Price::raw_type via from_raw_checked() is
    // debug-checked only, same pattern as notional.hpp's operators.
    return Price::from_raw_checked(rounded);
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
    static constexpr Price round_inner(Price price, int bps) noexcept {
        return offset_by_bps(price, bps, /*round_down=*/true);
    }
    static constexpr bool within(Price price, Price reference, int bps) noexcept {
        return within_bps(price, reference, bps, /*ge=*/false);
    }
};

struct Bid {
    static constexpr Price round_inner(Price price, int bps) noexcept {
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
// Fails with std::errc::argument_out_of_domain if any level has a
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
template <typename Side, typename Map>
std::expected<std::vector<PriceBand>, std::errc> price_band_depth(
    const Map& levels, std::span<const int> bps_thresholds) {
    static_assert(std::is_same_v<Side, detail::Ask> || std::is_same_v<Side, detail::Bid>,
                  "Side must be detail::Ask or detail::Bid");
    assert(std::ranges::is_sorted(bps_thresholds));
    assert(std::ranges::all_of(bps_thresholds, [](int bps) { return bps >= 0; }));
    if constexpr (std::is_same_v<Side, detail::Bid>) {
        assert(std::ranges::all_of(bps_thresholds, [](int bps) { return bps < 10'000; }));
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
            Price boundary = Side::round_inner(best_price, bps);
            result.push_back({bps_thresholds[next], boundary, cum_size, cum_notional});
            ++next;
        }
        if (next >= bps_thresholds.size()) break;

        cum_size += size;
        cum_notional += price * size;
    }

    while (next < bps_thresholds.size()) {
        Price boundary = Side::round_inner(best_price, bps_thresholds[next]);
        result.push_back({bps_thresholds[next], boundary, cum_size, cum_notional});
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

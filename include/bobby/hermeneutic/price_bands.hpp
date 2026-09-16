#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <limits>
#include <span>
#include <vector>

#include "bobby/hermeneutic/notional.hpp"
#include "bobby/hermeneutic/order_book.hpp"

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

    // Narrowing back to Price::raw_type is debug-checked only, same
    // pattern as notional.hpp's operators.
    assert(rounded >= std::numeric_limits<Price::raw_type>::min());
    assert(rounded <= std::numeric_limits<Price::raw_type>::max());

    return Price::from_raw(static_cast<Price::raw_type>(rounded));
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

}  // namespace detail

// Walks `levels` (best price first -- a precondition, not checked) and,
// for each threshold in `bps_thresholds` (sorted ascending, non-negative,
// and strictly under 10000 for the bid side), reports the depth within
// that many bps of `levels`' own best price. `best_price` is taken from
// `levels` itself rather than passed separately, so it can't mismatch the
// map it's derived from. `bps_sign` is +1 for asks, -1 for bids; a level
// exactly at a boundary counts as within it. O(levels + bps_thresholds).
template <typename Map>
std::vector<PriceBand> price_band_depth(const Map& levels, int bps_sign,
                                         std::span<const int> bps_thresholds) {
    assert(bps_sign == 1 || bps_sign == -1);
    assert(std::ranges::is_sorted(bps_thresholds));
    assert(std::ranges::all_of(bps_thresholds, [](int bps) { return bps >= 0; }));
    assert(bps_sign > 0 ||
           std::ranges::all_of(bps_thresholds, [](int bps) { return bps < 10'000; }));

    std::vector<PriceBand> result;
    result.reserve(bps_thresholds.size());
    if (levels.empty()) return result;  // no BBO to offset from -> no bands
    Price best_price = levels.begin()->first;

    Size cum_size{};
    Notional cum_notional{};
    std::size_t next = 0;
    bool round_down = bps_sign >= 0;  // ask boundary rounds down, bid rounds up: both inward.

    for (const auto& [price, size] : levels) {
        while (next < bps_thresholds.size()) {
            int signed_bps = bps_sign * bps_thresholds[next];
            bool within = detail::within_bps(price, best_price, signed_bps, /*ge=*/bps_sign < 0);
            if (within) break;
            Price boundary = detail::offset_by_bps(best_price, signed_bps, round_down);
            result.push_back({bps_thresholds[next], boundary, cum_size, cum_notional});
            ++next;
        }
        if (next >= bps_thresholds.size()) break;

        cum_size += size;
        cum_notional += price * size;
    }

    while (next < bps_thresholds.size()) {
        Price boundary =
            detail::offset_by_bps(best_price, bps_sign * bps_thresholds[next], round_down);
        result.push_back({bps_thresholds[next], boundary, cum_size, cum_notional});
        ++next;
    }

    return result;
}

inline std::vector<PriceBand> bid_price_band_depths(const L2OrderBook& book,
                                                      std::span<const int> bps_thresholds) {
    return price_band_depth(book.bids, -1, bps_thresholds);
}

inline std::vector<PriceBand> ask_price_band_depths(const L2OrderBook& book,
                                                      std::span<const int> bps_thresholds) {
    return price_band_depth(book.asks, +1, bps_thresholds);
}

}  // namespace bobby::hermeneutic

#pragma once

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <expected>
#include <optional>
#include <span>
#include <system_error>
#include <vector>

#include "bobby/hermeneutic/book/l2_order_book.hpp"
#include "bobby/hermeneutic/core/notional.hpp"
#include "bobby/hermeneutic/core/rounding.hpp"

namespace bobby::hermeneutic {

// One notional-volume band: the VWAP needed to accumulate `notional_threshold`
// of notional value by walking an order book side from its best price.
// If the side does not hold enough depth to reach `notional_threshold`,
// `vwap` is nullopt and `filled_notional` reports how much was actually
// accumulated instead.
struct VolumeBand {
    Notional notional_threshold;
    std::optional<Price> vwap;
    Notional filled_notional;
};

namespace detail {

// Computes the VWAP for filling `remaining` additional notional at `price`,
// on top of `cum_notional`/`cum_size` already accumulated from earlier
// levels, without materializing an intermediate Size for the partial-fill
// quantity -- deliberately: this is the exact quantity needed to reach
// `remaining` at `price` (an interpolated point on the book's liquidity
// curve), not a simulation of an order actually resting at a
// Size-quantized size. An earlier version computed this as two divisions
// -- `take_size = remaining / price` (Notional/Price -> Size), then
// `vwap = (cum_notional + remaining) / (cum_size + take_size)`
// (Notional/Size -> Price) -- which rounded the partial-fill quantity to
// Size's 6-decimal precision *before* the final division, discarding up to
// 0.5e-6 of it. For a small cum_size (e.g. a partial fill within the very
// first level) that error can exceed Price's own 1e-9 resolution.
//
// Algebraically, this is one fraction with the inner division cleared out
// of the denominator:
//   vwap = (cum_notional + remaining) * price
//          / (cum_size * price + remaining)
// In raw fixed-point units, cum_notional/remaining/price are already
// scaled by 1e9 and cum_size by 1e6, so converting the result back to
// Price's own 1e9 scale introduces a 1e6 factor that has nothing to do
// with the algebra above:
//   vwap_raw = (cum_notional_raw + remaining_raw) * price_raw * 1e6
//              / (cum_size_raw * price_raw + remaining_raw * 1e6)
// and rounds only once, at the end (round to nearest, ties away from zero,
// matching the rest of this codebase's fixed-point conventions).
//
// The numerator needs a 64x64x~20-bit triple product, which can exceed
// __int128's 127-bit range in one shot -- unlike the rest of this
// codebase's __int128 helpers, which only ever form a single 64x64 product
// and so cannot overflow __int128 itself regardless of input. Here the
// `(cum_notional + remaining) * price` product is bounds-checked *before*
// scaling by 1e6: by the time an assert could inspect the already-scaled
// result, an overflowing signed multiply would already be undefined
// behavior, not just a narrowing surprise. This bounds the safe *joint*
// magnitude of notional and price well below either type's own raw_type
// range considered alone -- still far beyond any real order book -- rather
// than the individually-generous-but-jointly-unbounded assumption the rest
// of this file's helpers make. Rounding uses notional.hpp's
// detail::round_div_nearest_away() rather than the more usual "add half
// the denominator, then divide", since `numerator` can already be close to
// __int128's range and that add could overflow on its own -- the multiply
// guard above only bounds `numerator` itself, not `numerator +
// denominator/2`.
constexpr std::expected<Price, std::errc> vwap_at_partial_fill(Notional cum_notional,
                                                                 Size cum_size, Notional remaining,
                                                                 Price price) noexcept {
    if (price.raw() <= 0) return std::unexpected(std::errc::argument_out_of_domain);
    // Internal invariants guaranteed by this function's only caller (a
    // negative cum_size or non-positive remaining would mean a bug in the
    // caller's bookkeeping, not malformed external input, hence asserted
    // rather than returned as an error -- unlike the price check above,
    // which is this function's own defense against being called directly
    // with an unvalidated level price).
    assert(cum_size.raw() >= 0);
    assert(remaining.raw() > 0);

    constexpr __int128 kScale = 1'000'000;
    constexpr __int128 kInt128Max =
        static_cast<__int128>((static_cast<unsigned __int128>(1) << 127) - 1);

    __int128 price_raw = static_cast<__int128>(price.raw());
    __int128 vwap_notional_raw =
        static_cast<__int128>(cum_notional.raw()) + static_cast<__int128>(remaining.raw());

    __int128 notional_price_product = vwap_notional_raw * price_raw;
    // Guards the *next* multiply (by kScale) from overflowing __int128
    // itself -- fires only for a notional/price combination far beyond any
    // real instrument.
    assert(notional_price_product <= kInt128Max / kScale);
    assert(notional_price_product >= -(kInt128Max / kScale));
    __int128 numerator = notional_price_product * kScale;

    __int128 denominator = static_cast<__int128>(cum_size.raw()) * price_raw +
                            static_cast<__int128>(remaining.raw()) * kScale;
    // Provably > 0 given the preconditions above (price > 0, cum_size >= 0,
    // remaining > 0), but checked rather than asserted: this is the last
    // guard before dividing, and the rounding below assumes it holds.
    if (denominator <= 0) return std::unexpected(std::errc::argument_out_of_domain);

    __int128 rounded = round_div_nearest_away(numerator, denominator);

    // Same narrowing caveat as notional.hpp's operators: from_raw_checked()'s
    // assert is debug-only, since no realistic notional/size/price
    // combination asks for a VWAP anywhere near Price::raw_type's max.
    return Price::from_raw_checked(rounded);
}

}  // namespace detail

// Walks `levels` (best price first) accumulating notional = price * size at
// each level, and for each threshold in `thresholds` (must be sorted
// ascending and strictly positive -- a 0 or negative threshold has no
// meaningful VWAP) reports the volume-weighted-average price needed to
// reach it. Single pass over `levels`: O(levels + thresholds).
//
// Fails with std::errc::invalid_argument if `thresholds` itself isn't
// sorted/strictly positive - checked at runtime rather than only asserted,
// since `thresholds` comes from the caller (an API request, ultimately),
// not from this module's own bookkeeping. The algorithm below walks
// `thresholds` with a single monotonically-increasing index (`next`), so an
// unsorted input would silently compute wrong VWAPs in a release build
// rather than failing loudly - see price_bands.hpp's price_band_depth()
// for the same reasoning applied to its own thresholds parameter.
//
// Also fails with std::errc::argument_out_of_domain if any level has a
// non-positive price or a negative size, checked as each level is walked --
// not only when a threshold happens to cross inside it. A non-positive-price
// level contributes zero (or negative) notional, so it may never trigger a
// crossing on its own; walked over unchecked, its size would still fold
// into cum_size with no corresponding notional, diluting the VWAP of any
// later, legitimate crossing. Checking eagerly aborts on the first bad
// level found rather than letting it silently corrupt later results.
//
// Also fails with std::errc::result_out_of_range if the running cum_notional
// total itself overflows Notional while accumulating across levels: each
// individual level's price*size can be in-range while the series still
// isn't, which a plain per-call assert (BasicFixedPoint's own operator+=)
// can't catch - see Notional::from_raw_safe()'s own comment.
template <typename Map>
std::expected<std::vector<VolumeBand>, std::errc> volume_band_prices(
    const Map& levels, std::span<const Notional> thresholds) {
    if (!std::ranges::is_sorted(thresholds) ||
        !std::ranges::all_of(thresholds, [](Notional t) { return t.raw() > 0; })) {
        return std::unexpected(std::errc::invalid_argument);
    }

    std::vector<VolumeBand> result;
    result.reserve(thresholds.size());

    Notional cum_notional{};
    Size cum_size{};
    std::size_t next = 0;

    for (const auto& [price, size] : levels) {
        if (!is_valid_level(price, size)) return std::unexpected(std::errc::argument_out_of_domain);

        Notional level_notional = price * size;

        // Checked once here, not as a plain `cum_notional + level_notional`
        // repeated on every while-loop iteration below (that sum is
        // loop-invariant within this level, so this also avoids redoing the
        // same check) - see this function's own doc comment.
        auto next_cum_notional = Notional::from_raw_safe(static_cast<__int128>(cum_notional.raw()) +
                                                           static_cast<__int128>(level_notional.raw()));
        if (!next_cum_notional) return std::unexpected(next_cum_notional.error());

        while (next < thresholds.size() && *next_cum_notional >= thresholds[next]) {
            Notional remaining = thresholds[next] - cum_notional;

            std::expected<Price, std::errc> vwap;
            if (remaining == level_notional) {
                // The threshold lands exactly at this level's far edge:
                // this level's `size` is already known exactly, so use it
                // directly rather than re-deriving a quantity from
                // `remaining / price`. That reconstruction (what
                // vwap_at_partial_fill does for a genuine partial fill,
                // where no exact quantity is available) isn't perfectly
                // exact here: `level_notional` is `price * size` already
                // rounded to Notional's own precision, and dividing it back
                // by `price` doesn't reliably recover the bit-exact `size`
                // it came from -- usually negligible, but the gap grows
                // large for a low enough price, since it's inversely
                // proportional to price.
                vwap = (cum_notional + remaining) / (cum_size + size);
            } else {
                vwap = detail::vwap_at_partial_fill(cum_notional, cum_size, remaining, price);
            }
            if (!vwap) return std::unexpected(vwap.error());

            result.push_back({thresholds[next], *vwap, thresholds[next]});
            ++next;
        }

        cum_notional = *next_cum_notional;
        cum_size += size;
    }

    while (next < thresholds.size()) {
        result.push_back({thresholds[next], std::nullopt, cum_notional});
        ++next;
    }

    return result;
}

inline std::expected<std::vector<VolumeBand>, std::errc> bid_volume_band_prices(
    const L2OrderBook& book, std::span<const Notional> thresholds) {
    return volume_band_prices(book.bids, thresholds);
}

inline std::expected<std::vector<VolumeBand>, std::errc> ask_volume_band_prices(
    const L2OrderBook& book, std::span<const Notional> thresholds) {
    return volume_band_prices(book.asks, thresholds);
}

}  // namespace bobby::hermeneutic

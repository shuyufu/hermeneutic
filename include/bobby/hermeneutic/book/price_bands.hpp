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

// Depth within `bps` basis points of a reference price (a side's own BBO,
// or another anchor such as last trade price or midprice -- see
// price_band_depth()) on one side of the book. `boundary_price` is an
// inward-rounded Price representation of that boundary, used only for
// reporting (logging/UI/API) -- not to Price's own precision, it's exact
// only to the mathematical boundary. Membership is determined exactly by
// detail::within_bps(), not by comparing against `boundary_price`. If
// there's depth but it runs out before a boundary, `cumulative_size`/
// `cumulative_notional` report the whole book's depth (a valid answer, not
// a failure); bid_price_band_depths()/ask_price_band_depths() report no
// bands at all for an empty side, having no BBO to offset from.
struct PriceBand {
    int bps;
    Price boundary_price;
    Size cumulative_size;
    Notional cumulative_notional;
};

// 1 bps = 1/kBpsDenominator (10000 bps = 100%). A bid-side bps threshold
// must be strictly under this - a boundary at or past 10000 bps would be
// zero or negative (see offset_by_bps()'s own comment) - which is what
// bps_thresholds_valid<Bid>() below enforces. Exposed (not just a repeated
// literal in this file) so an external caller that needs to pre-validate a
// bps list before it ever reaches bid_price_band_depths() - e.g.
// hermeneutic_aggregator_client's own --price-bps= flag - can check
// against the same named bound this file itself validates against,
// instead of keeping its own separate copy of the "10000" literal.
constexpr int kBpsDenominator = 10'000;

namespace detail {

// Offsets `price` by `signed_bps` basis points, rounded *inward* (floor
// for an ask boundary via round_down=true, ceil for a bid boundary via
// round_down=false) so `boundary_price` never overstates the band's
// actual reach. Display only -- membership is decided by within_bps()
// below, not this function.
//
// Fails with std::errc::result_out_of_range if the resulting boundary
// doesn't fit back into Price::raw_type - a genuine runtime possibility
// here, not just an internal invariant: the ask side has no bps ceiling
// (unlike the bid side, capped under 10000 since a boundary at or past
// that would be zero or negative), and this project validates no upper
// bound on price either, so a large ask bps threshold against a large
// price can genuinely overflow. Price::from_raw_safe() is the shared
// bounds check for exactly this.
constexpr std::expected<Price, std::errc> offset_by_bps(Price price, int signed_bps,
                                                          bool round_down) noexcept {
    assert(price.raw() > 0);

    // Widened to __int128 before adding: native `int` arithmetic would
    // overflow for signed_bps near INT_MIN/MAX. factor > 0 is required for
    // floor/ceil below to be correct, and for a bid boundary to stay
    // positive (needs bps < 10000).
    const __int128 factor = static_cast<__int128>(kBpsDenominator) + static_cast<__int128>(signed_bps);
    assert(factor > 0);

    __int128 scaled_numerator = static_cast<__int128>(price.raw()) * factor;
    constexpr __int128 denom = kBpsDenominator;
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

    const __int128 factor = static_cast<__int128>(kBpsDenominator) + static_cast<__int128>(signed_bps);
    assert(factor > 0);

    __int128 lhs = static_cast<__int128>(price.raw()) * kBpsDenominator;
    __int128 rhs = static_cast<__int128>(best_price.raw()) * factor;
    return ge ? lhs >= rhs : lhs <= rhs;
}

// Ask/Bid: the two side-specific policies for how offset_by_bps/within_bps's
// round_down/ge booleans are derived, so a caller states its side once
// (Ask:: or Bid::) instead of re-deriving both booleans from a signed bps
// value by hand and risking getting one of them backwards. "reference"
// here is whatever Price the caller passes in - price_band_depth() below
// takes it as a parameter rather than computing one itself, so it's free
// to be a side's own best price, a midprice, or any other anchor the
// caller wants. Each type takes an unsigned `bps` and applies its side's
// sign internally, rather than making the caller pre-multiply by +1/-1.
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

// Validates bps_thresholds' shape for `Side`: sorted ascending,
// non-negative, and (bid only) strictly under 10000. Split out from
// price_band_depth() so bid_price_band_depths()/ask_price_band_depths()
// can run the same check on their own empty-book fast path below, without
// duplicating it inline.
template <typename Side>
constexpr bool bps_thresholds_valid(std::span<const int> bps_thresholds) noexcept {
    constexpr bool kBidUpperBoundApplies = std::is_same_v<Side, Bid>;
    return std::ranges::is_sorted(bps_thresholds) &&
           std::ranges::all_of(bps_thresholds, [](int bps) { return bps >= 0; }) &&
           (!kBidUpperBoundApplies ||
            std::ranges::all_of(bps_thresholds, [](int bps) { return bps < kBpsDenominator; }));
}

// Applies bid_price_band_depths()/ask_price_band_depths()'s shared
// "empty side -> no bands at all" contract: still validates
// bps_thresholds (so a malformed list fails the same way whether or not
// the book happens to be empty), then returns an empty result rather than
// calling price_band_depth() at all, since there's no BBO to derive a
// reference from.
template <typename Side>
std::expected<std::vector<PriceBand>, std::errc> empty_side_bands(
    std::span<const int> bps_thresholds) {
    if (!bps_thresholds_valid<Side>(bps_thresholds)) {
        return std::unexpected(std::errc::invalid_argument);
    }
    return std::vector<PriceBand>{};
}

}  // namespace detail

// Walks `levels` (best price first -- a precondition, not checked) and,
// for each threshold in `bps_thresholds` (sorted ascending, non-negative,
// and strictly under 10000 for the bid side), reports the depth within
// that many bps of `reference`. `reference` is caller-supplied rather than
// derived from `levels` itself, so it can be a side's own best price (as
// bid_price_band_depths/ask_price_band_depths below use it), or any other
// anchor an application wants band depth reported against instead -- last
// trade price, midprice, a mark price. `levels` and `reference` are
// independent: this function never checks that `reference` relates to
// `levels`' contents, so an empty `levels` still produces well-defined
// zero-depth bands anchored at `reference` (unlike the BBO-derived
// wrappers below, which have no reference to fall back on for an empty
// side and report no bands at all instead). `Side` (detail::Ask or
// detail::Bid) is a template parameter rather than a runtime flag because
// every real caller already knows its side at compile time. A level
// exactly at a boundary counts as within it. O(levels + bps_thresholds).
//
// Fails with std::errc::invalid_argument if `bps_thresholds` itself isn't
// sorted/non-negative/(for bids) under 10000 - checked at runtime, since
// `bps_thresholds` comes from the caller (an API request), unlike `Side`
// (a compile-time choice). The algorithm walks bps_thresholds with a
// single monotonically-increasing index (`next`), so an unsorted input
// would silently compute wrong band boundaries rather than failing loudly.
//
// Also fails with std::errc::argument_out_of_domain if `reference` itself
// is non-positive, or if any level has a non-positive price or negative
// size, checked as each level is walked: both `reference` and `levels`
// come from the caller (e.g. a live venue's BBO/last-trade/mark price, and
// the caller's own book) rather than something this function controls, so
// a malformed value is a runtime condition to reject, not an assert()-only
// precondition. This is why offset_by_bps()/within_bps() (reached through
// Side::round_inner()/Side::within()) can stay assert()-only for their own
// price>0 precondition - this function upholds it before ever calling
// them, so it's never checked twice.
//
// Also fails with std::errc::result_out_of_range if a boundary computed
// from a validated-in-range bps threshold against an unbounded reference
// price doesn't fit back into Price (see offset_by_bps()'s own comment),
// or if the running cum_notional total itself overflows while
// accumulating across levels - each individual price*size can be
// in-range while the series isn't, which a plain per-call assert can't
// catch (see Notional::from_raw_safe()'s own comment).
template <typename Side, typename Map>
std::expected<std::vector<PriceBand>, std::errc> price_band_depth(
    const Map& levels, Price reference, std::span<const int> bps_thresholds) {
    static_assert(std::is_same_v<Side, detail::Ask> || std::is_same_v<Side, detail::Bid>,
                  "Side must be detail::Ask or detail::Bid");
    if (!detail::bps_thresholds_valid<Side>(bps_thresholds)) {
        return std::unexpected(std::errc::invalid_argument);
    }
    if (reference.raw() <= 0) return std::unexpected(std::errc::argument_out_of_domain);

    std::vector<PriceBand> result;
    result.reserve(bps_thresholds.size());

    Size cum_size{};
    Notional cum_notional{};
    std::size_t next = 0;

    for (const auto& [price, size] : levels) {
        if (!is_valid_level(price, size)) return std::unexpected(std::errc::argument_out_of_domain);

        while (next < bps_thresholds.size()) {
            int bps = bps_thresholds[next];
            if (Side::within(price, reference, bps)) break;
            auto boundary = Side::round_inner(reference, bps);
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
        auto boundary = Side::round_inner(reference, bps_thresholds[next]);
        if (!boundary) return std::unexpected(boundary.error());
        result.push_back({bps_thresholds[next], *boundary, cum_size, cum_notional});
        ++next;
    }

    return result;
}

namespace detail {

// Shared dispatch behind bid_price_band_depths()/ask_price_band_depths():
// an empty side has no BBO to offset from, so it reports no bands at all
// (via empty_side_bands(), still validating bps_thresholds along the way)
// rather than calling price_band_depth() with a reference pulled out of
// thin air; a non-empty side derives `reference` from its own best price
// and delegates to price_band_depth() directly.
template <typename Side, typename Map>
std::expected<std::vector<PriceBand>, std::errc> bbo_price_band_depths(
    const Map& levels, std::span<const int> bps_thresholds) {
    if (levels.empty()) return empty_side_bands<Side>(bps_thresholds);
    return price_band_depth<Side>(levels, levels.begin()->first, bps_thresholds);
}

}  // namespace detail

// Convenience wrapper that reports band depth against the book's own best
// bid, matching every existing caller's expectation. See
// detail::bbo_price_band_depths() for the empty-side behavior.
inline std::expected<std::vector<PriceBand>, std::errc> bid_price_band_depths(
    const L2OrderBook& book, std::span<const int> bps_thresholds) {
    return detail::bbo_price_band_depths<detail::Bid>(book.bids, bps_thresholds);
}

// Ask-side mirror of bid_price_band_depths() above.
inline std::expected<std::vector<PriceBand>, std::errc> ask_price_band_depths(
    const L2OrderBook& book, std::span<const int> bps_thresholds) {
    return detail::bbo_price_band_depths<detail::Ask>(book.asks, bps_thresholds);
}

}  // namespace bobby::hermeneutic

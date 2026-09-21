#pragma once

#include <functional>
#include <map>

#include "bobby/hermeneutic/core/fixed_point.hpp"

namespace bobby::hermeneutic {

// L2 order book keyed by price level.
// asks: ascending (best ask first) / bids: descending (best bid first).
struct L2OrderBook {
    std::map<Price, Size, std::less<Price>> asks;
    std::map<Price, Size, std::greater<Price>> bids;
};

// Whether (price, size) is a level this project's book/band machinery
// will accept: price must be strictly positive (a zero or negative
// price has no meaningful book position, and would corrupt every
// downstream price-ordered computation -- price_bands.hpp's
// offset_by_bps()/within_bps() only assert() this precondition, which
// compiles out under NDEBUG, so it's this check, not those asserts,
// that actually has to stop a bad level in a release build); size must
// be non-negative (a negative size has no removal/no-op meaning the way
// zero does).
//
// Shared by AggregateOrderBook::apply_snapshot/apply_batch,
// price_band_depth(), volume_band_prices(), and
// aggregator::SymbolBook::apply_batch's own pre-check, all of which used
// to hand-roll this identical `price.raw() <= 0 || size.raw() < 0`
// predicate separately (a code-review finding). Deliberately a plain
// bool, not an std::expected<void, std::errc>: the sites above disagree
// on which std::errc code this condition should produce
// (invalid_argument vs argument_out_of_domain) - that's each call site's
// own API contract with ITS OWN callers (AggregateOrderBook's is public,
// documented, and tested), not something a dedup of the underlying
// predicate should silently change. Each caller wraps this in its own
// std::unexpected(...) with whichever code it already returned.
constexpr bool is_valid_level(Price price, Size size) noexcept {
    return price.raw() > 0 && size.raw() >= 0;
}

}  // namespace bobby::hermeneutic

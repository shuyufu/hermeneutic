#pragma once

#include <cassert>

// Its own header, not folded into fixed_point.hpp or notional.hpp: this is
// generic 128-bit rounding arithmetic, not a BasicFixedPoint concern, and
// notional.hpp includes fixed_point.hpp, so fixed_point.hpp reusing this
// from notional.hpp directly would be a circular include.
namespace bobby::hermeneutic::detail {

// Divides `numerator` by `denominator`, rounding to the nearest integer
// with ties away from zero - the convention every rescaling operator in
// this codebase shares. Requires `denominator` be strictly positive: the
// more obvious-looking "numerator +/- denominator/2, then divide" shape
// silently rounds the wrong way for a negative denominator (e.g.
// 15 / -10 = -1.5, which should round to -2, but that shape gives -1) -
// callers with a possibly-signed denominator must normalize its sign
// themselves before calling this. `numerator`'s sign is unrestricted, so
// this stays usable for a future signed delta/adjustment, not just
// non-negative quantities.
constexpr __int128 round_div_nearest_away(__int128 numerator, __int128 denominator) noexcept {
    assert(denominator > 0);

    __int128 quotient = numerator / denominator;
    __int128 remainder = numerator % denominator;
    __int128 abs_remainder = remainder >= 0 ? remainder : -remainder;
    __int128 half = denominator / 2 + denominator % 2;  // ceil(denominator / 2)

    if (abs_remainder >= half) {
        quotient += (numerator >= 0 ? 1 : -1);
    }
    return quotient;
}

}  // namespace bobby::hermeneutic::detail

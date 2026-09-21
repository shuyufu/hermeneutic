#pragma once

#include <cassert>

// Split out of notional.hpp (its original, and until now only, home) so
// fixed_point.hpp's BasicFixedPoint::from_decimal_string can reuse it
// without pulling in notional.hpp - which itself includes fixed_point.hpp,
// so the reverse include would be circular. Kept as its own header rather
// than folded into fixed_point.hpp directly: this is generic 128-bit
// rounding arithmetic, not a BasicFixedPoint concern, and a future
// application (a different rounding convention, a different intermediate
// width) shouldn't have to grow fixed_point.hpp itself to get there.
namespace bobby::hermeneutic::detail {

// Divides `numerator` by `denominator`, rounding to the nearest integer
// with ties away from zero -- the convention BasicFixedPoint's
// double-constructor uses, and every rescaling operator in notional.hpp
// (and volume_bands.hpp's VWAP crossing math, and
// BasicFixedPoint::from_decimal_string's own excess-fractional-digit
// rescale) shares. Requires `denominator` be strictly positive: the more
// obvious-looking "numerator +/- denominator/2, then divide" shape
// silently rounds the wrong way for a negative denominator (e.g.
// 15 / -10 = -1.5, which should round to -2, but that shape gives -1) --
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

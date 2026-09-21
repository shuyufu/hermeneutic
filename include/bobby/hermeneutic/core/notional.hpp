#pragma once

#include <expected>
#include <system_error>

#include "bobby/hermeneutic/core/fixed_point.hpp"

namespace bobby::hermeneutic {

// Notional value in quote-currency terms (price * size). Kept at Price's
// precision (9 decimals) rather than Size's (6): notional shares Price's
// unit (quote currency), and a low-priced instrument can have a per-level
// notional too small to survive rounding at only 6 decimals.
using Notional = BasicFixedPoint<9>;

// Price * Size -> Notional. See detail::fixed_multiply (fixed_point.hpp)
// for the rescale/rounding/overflow mechanics.
constexpr Notional operator*(Price price, Size size) noexcept {
    return detail::fixed_multiply<Price, Size, Notional>(price, size);
}

// Notional / Price -> Size: the quantity needed to reach `notional` at
// `price`. See detail::fixed_divide (fixed_point.hpp) for the rescale/
// rounding/domain-check mechanics.
constexpr std::expected<Size, std::errc> operator/(Notional notional, Price price) noexcept {
    return detail::fixed_divide<Notional, Price, Size>(notional, price);
}

// Notional / Size -> Price: the VWAP that was paid for `size` at a total
// cost of `notional`. See detail::fixed_divide (fixed_point.hpp) for the
// rescale/rounding/domain-check mechanics.
constexpr std::expected<Price, std::errc> operator/(Notional notional, Size size) noexcept {
    return detail::fixed_divide<Notional, Size, Price>(notional, size);
}

}  // namespace bobby::hermeneutic

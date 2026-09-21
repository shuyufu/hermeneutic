#pragma once

#include <expected>
#include <system_error>

#include "bobby/hermeneutic/core/fixed_point.hpp"
#include "bobby/hermeneutic/core/rounding.hpp"

namespace bobby::hermeneutic {

// Notional value in quote-currency terms (price * size). Kept at Price's
// precision (9 decimals) rather than Size's (6): notional shares Price's
// unit (quote currency), and a low-priced instrument can have a per-level
// notional too small to survive rounding at only 6 decimals.
using Notional = BasicFixedPoint<9>;

// Price and Size raw values multiplied together span up to scale
// 10^(9+6) = 10^15, which does not fit in int64_t, so the raw multiply is
// done in a 128-bit intermediate before being rescaled down to Notional's
// precision (10^9).
constexpr Notional operator*(Price price, Size size) noexcept {
    __int128 raw_product =
        static_cast<__int128>(price.raw()) * static_cast<__int128>(size.raw());

    constexpr __int128 rescale = 1'000'000;  // 10^(9 + 6 - 9), always positive
    __int128 rounded = detail::round_div_nearest_away(raw_product, rescale);

    // The __int128 intermediate above only protects the multiply itself;
    // it says nothing about whether the rescaled result still fits back
    // into Notional's int64_t raw storage. from_raw_checked()'s assert is
    // debug-only, since no real price/size pair reaches this: it would need
    // notional near Notional::raw_type's max (~9.2 billion units of quote
    // currency).
    return Notional::from_raw_checked(rounded);
}

// Notional / Price -> Size: the quantity needed to reach `notional` at
// `price`. Notional and Price share the same scale (10^9), so their ratio
// is scale-free; the numerator is rescaled by 10^6 up front (in a 128-bit
// intermediate, since raw values already span up to ~9e18) to land directly
// on Size's precision (10^6) after the integer division. A non-positive
// `price` is a genuine domain error (unlike the narrowing case below, this
// is reachable from ordinary bad data — a default-constructed or malformed
// Price — not just extreme magnitudes), so it's reported via
// std::expected rather than asserted away. Rejecting <= 0 rather than just
// == 0 isn't only a domain judgment (a real price is always positive) --
// round_div_nearest_away() below requires a strictly positive denominator
// to round correctly, so this is also what makes that call valid.
// `notional` itself is left unrestricted (see round_div_nearest_away()).
constexpr std::expected<Size, std::errc> operator/(Notional notional, Price price) noexcept {
    if (price.raw() <= 0) return std::unexpected(std::errc::argument_out_of_domain);

    __int128 scaled_numerator = static_cast<__int128>(notional.raw()) * 1'000'000;
    __int128 denominator = static_cast<__int128>(price.raw());
    __int128 rounded = detail::round_div_nearest_away(scaled_numerator, denominator);

    // Same narrowing caveat as operator*: from_raw_checked()'s assert is
    // debug-only, since no realistic notional/price pair asks for a quantity
    // anywhere near Size::raw_type's max (~9.2 trillion units).
    return Size::from_raw_checked(rounded);
}

// Notional / Size -> Price: the VWAP that was paid for `size` at a total
// cost of `notional`. Same rescale-by-10^6 shape as the Size overload above
// (Notional is 10^9, Size is 10^6, Price is 10^9: 6 - 9 + 9 = 6), and the
// same reasoning for rejecting a non-positive `size` (both a domain
// judgment and what round_div_nearest_away() requires).
constexpr std::expected<Price, std::errc> operator/(Notional notional, Size size) noexcept {
    if (size.raw() <= 0) return std::unexpected(std::errc::argument_out_of_domain);

    __int128 scaled_numerator = static_cast<__int128>(notional.raw()) * 1'000'000;
    __int128 denominator = static_cast<__int128>(size.raw());
    __int128 rounded = detail::round_div_nearest_away(scaled_numerator, denominator);

    // Same narrowing caveat as operator*: from_raw_checked()'s assert is
    // debug-only, since no realistic notional/size pair asks for a VWAP
    // anywhere near Price::raw_type's max (~9.2 billion units of quote
    // currency).
    return Price::from_raw_checked(rounded);
}

}  // namespace bobby::hermeneutic

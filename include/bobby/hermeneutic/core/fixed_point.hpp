#pragma once

#include <algorithm>
#include <cassert>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <expected>
#include <limits>
#include <ostream>
#include <string_view>
#include <system_error>

#include "bobby/hermeneutic/core/rounding.hpp"

namespace bobby::hermeneutic {

// Fixed-point decimal value backed by a scaled 64-bit integer.
// `Decimals` is the number of digits kept after the decimal point.
template <int Decimals>
class BasicFixedPoint {
    static_assert(Decimals >= 0, "Decimals must be non-negative");

  public:
    using raw_type = std::int64_t;

    static constexpr int decimals = Decimals;
    static constexpr raw_type scale = [] {
        raw_type s = 1;
        for (int i = 0; i < Decimals; ++i) s *= 10;
        return s;
    }();

    constexpr BasicFixedPoint() noexcept = default;

    static constexpr BasicFixedPoint from_raw(raw_type raw) noexcept {
        BasicFixedPoint v;
        v.raw_ = raw;
        return v;
    }

    // Like from_raw(), but for a __int128 intermediate that's already been
    // rescaled to this type's raw storage and just needs narrowing back down
    // to raw_type. The assert is debug-only: no realistic Price/Size/Notional
    // magnitude in this codebase reaches raw_type's ~9.2e18 range, so this
    // documents an invariant rather than handling a reachable error - a
    // caller with a genuine runtime-reachable overflow risk should check
    // before calling this, not rely on it (see from_raw_safe() below).
    static constexpr BasicFixedPoint from_raw_checked(__int128 value) noexcept {
        assert(value >= static_cast<__int128>(std::numeric_limits<raw_type>::min()));
        assert(value <= static_cast<__int128>(std::numeric_limits<raw_type>::max()));
        return from_raw(static_cast<raw_type>(value));
    }

    // Same narrowing as from_raw_checked(), but returns
    // std::errc::result_out_of_range instead of a debug-only assert, for a
    // caller with a genuine runtime-reachable overflow risk - e.g.
    // price_bands.hpp's offset_by_bps() (no upper bound on Price is
    // validated anywhere, so a boundary computed from it can genuinely
    // overflow) or a caller accumulating many individually-valid values
    // (e.g. a running cum_notional) where the total can overflow even
    // though each individual add was in-range.
    static constexpr std::expected<BasicFixedPoint, std::errc> from_raw_safe(__int128 value) noexcept {
        constexpr __int128 kRawMax = static_cast<__int128>(std::numeric_limits<raw_type>::max());
        constexpr __int128 kRawMin = static_cast<__int128>(std::numeric_limits<raw_type>::min());
        if (value > kRawMax || value < kRawMin) return std::unexpected(std::errc::result_out_of_range);
        return from_raw(static_cast<raw_type>(value));
    }

    explicit constexpr BasicFixedPoint(double value) noexcept
        : raw_(static_cast<raw_type>(value * static_cast<double>(scale) +
                                      (value >= 0 ? 0.5 : -0.5))) {}

    // Parses a decimal string directly into raw integer scale, entirely in
    // integer arithmetic - no double intermediate anywhere (a double
    // intermediate loses precision above 15 total significant digits, and
    // the loss isn't confined to one step, so nothing short of avoiding it
    // entirely closes the gap).
    //
    // Accepted grammar: an optional leading '-' (never '+'), then digits,
    // optionally followed by '.' and more digits, with at least one digit
    // somewhere (rejects "", "-", ".", and "-."). Deliberately narrower
    // than std::from_chars<double>'s grammar: scientific notation ("1e5")
    // is a format error, and so are "nan"/"inf"/"infinity".
    //
    // Reports malformed input or an out-of-range result via std::expected
    // rather than throwing, so this file stays free of any particular
    // error-reporting convention a caller might want - a wire parser that
    // needs exceptions (see exchange/feed_wire.hpp) translates at its own
    // call site instead.
    static constexpr std::expected<BasicFixedPoint, std::errc> from_decimal_string(
        std::string_view text) noexcept {
        constexpr __int128 kRawMax = static_cast<__int128>(std::numeric_limits<raw_type>::max());

        // Stripped before splitting on '.', and applied to the whole
        // magnitude at the very end - not derived from int_part's own
        // sign - so a value like "-0.5" (integer part "0") doesn't lose
        // its sign.
        std::string_view rest = text;
        bool negative = false;
        if (!rest.empty() && rest.front() == '-') {
            negative = true;
            rest.remove_prefix(1);
        }

        auto dot = rest.find('.');
        std::string_view int_part = dot == std::string_view::npos ? rest : rest.substr(0, dot);
        std::string_view frac_part =
            dot == std::string_view::npos ? std::string_view{} : rest.substr(dot + 1);
        if (int_part.empty() && frac_part.empty()) {
            return std::unexpected(std::errc::invalid_argument);  // "", "-", ".", "-."
        }

        // Integer part: digit validation and accumulation fused into one
        // pass, accumulated in a 128-bit intermediate. Bailing as soon as
        // it exceeds raw_type's own max (a looser check than the final,
        // tight post-scale bound below) is what keeps this loop itself
        // safe from ever overflowing __int128 on a maliciously long digit
        // string, however many digits `int_part` has.
        unsigned __int128 int_value = 0;
        for (char c : int_part) {
            if (c < '0' || c > '9') return std::unexpected(std::errc::invalid_argument);
            int_value = int_value * 10 + static_cast<unsigned>(c - '0');
            if (int_value > static_cast<unsigned __int128>(kRawMax)) {
                return std::unexpected(std::errc::result_out_of_range);
            }
        }

        // Fractional part: every character still has to be validated
        // (untrusted input - "1.23abc" must be rejected, not truncated to
        // "1.23"), but only the first kDecimals+1 characters are ever
        // *accumulated*: under "round half up, ties away from zero", the
        // single digit right after the cut point already fully determines
        // the outcome, so digits beyond it can never change the result.
        std::size_t take = std::min(frac_part.size(), static_cast<std::size_t>(decimals) + 1);
        __int128 frac_numerator = 0;
        for (std::size_t i = 0; i < frac_part.size(); ++i) {
            char c = frac_part[i];
            if (c < '0' || c > '9') return std::unexpected(std::errc::invalid_argument);
            if (i < take) frac_numerator = frac_numerator * 10 + (c - '0');
        }

        __int128 frac_value;
        if (frac_part.size() > static_cast<std::size_t>(decimals)) {
            // Exactly kDecimals+1 digits were gathered - rescale by
            // dividing out the extra one, rounding ties away from zero:
            // the same helper/convention every other rescale in this
            // project uses (see notional.hpp).
            frac_value = detail::round_div_nearest_away(frac_numerator, 10);
        } else {
            // kDecimals or fewer digits were present - `frac_numerator`
            // already holds exactly `take` digits with nothing discarded;
            // pad with zeros on the right to reach kDecimals digits.
            frac_value = frac_numerator;
            for (std::size_t i = take; i < static_cast<std::size_t>(decimals); ++i) frac_value *= 10;
        }

        // Rounding the fractional part up can carry into the integer part
        // (e.g. "0.999" at 2 decimals rounds to "1.00") - round_div_
        // nearest_away can push frac_value to exactly kScale and no
        // further (the largest possible frac_numerator, kDecimals+1
        // nines, rounds to exactly 10^kDecimals), so a single carry is
        // the only case this `==` check needs to handle.
        if (frac_value == static_cast<__int128>(scale)) {
            int_value += 1;
            frac_value = 0;
        }
        // Re-checks the same loose bound as the accumulation loop above,
        // now including the carry - only load-bearing for a hypothetical
        // Decimals=0 type, where int_value is the final magnitude. The
        // tight post-scale check below is the real guard for Price/Size.
        if (int_value > static_cast<unsigned __int128>(kRawMax)) {
            return std::unexpected(std::errc::result_out_of_range);
        }

        // Final, tight bound against raw_type::max after scaling - the
        // actual overflow condition for the raw_type this returns. Applies
        // the same bound to both signs rather than letting a negative
        // result use raw_type::min's one-unit-larger magnitude, since
        // treating both signs identically here is simpler and real
        // price/size data never comes close to either bound.
        __int128 magnitude =
            static_cast<__int128>(int_value) * static_cast<__int128>(scale) + frac_value;
        if (magnitude > kRawMax) return std::unexpected(std::errc::result_out_of_range);

        return from_raw(static_cast<raw_type>(negative ? -magnitude : magnitude));
    }

    constexpr raw_type raw() const noexcept { return raw_; }

    constexpr double to_double() const noexcept {
        return static_cast<double>(raw_) / static_cast<double>(scale);
    }

    friend constexpr auto operator<=>(const BasicFixedPoint&,
                                       const BasicFixedPoint&) noexcept = default;

    // Widened to __int128 before the actual add/subtract/negate rather than
    // operating on raw_type directly and asserting after the fact:
    // raw_ + other.raw_ overflowing int64_t is itself undefined behavior,
    // so an assert on the result would run after UB already happened.
    constexpr BasicFixedPoint operator+(BasicFixedPoint other) const noexcept {
        return from_raw_checked(static_cast<__int128>(raw_) + static_cast<__int128>(other.raw_));
    }

    constexpr BasicFixedPoint operator-(BasicFixedPoint other) const noexcept {
        return from_raw_checked(static_cast<__int128>(raw_) - static_cast<__int128>(other.raw_));
    }

    constexpr BasicFixedPoint operator-() const noexcept {
        return from_raw_checked(-static_cast<__int128>(raw_));
    }

    // raw_ updated directly from the checked __int128 result, rather than
    // `*this = *this + other`, to avoid an extra copy-assignment on this
    // hot path (e.g. per-level cum_size accumulation) on top of the widen
    // that's actually required for overflow safety.
    constexpr BasicFixedPoint& operator+=(BasicFixedPoint other) noexcept {
        raw_ = from_raw_checked(static_cast<__int128>(raw_) + static_cast<__int128>(other.raw_)).raw();
        return *this;
    }

    constexpr BasicFixedPoint& operator-=(BasicFixedPoint other) noexcept {
        raw_ = from_raw_checked(static_cast<__int128>(raw_) - static_cast<__int128>(other.raw_)).raw();
        return *this;
    }

    friend std::ostream& operator<<(std::ostream& os, BasicFixedPoint v) {
        return os << v.to_double();
    }

  private:
    raw_type raw_ = 0;
};

using Price = BasicFixedPoint<9>;
using Size = BasicFixedPoint<6>;

namespace detail {

// 10^Exponent as a __int128, for rescaling between two BasicFixedPoint
// widths in fixed_multiply/fixed_divide below. A template rather than a
// runtime-argument function so the caller's own static_assert on Exponent's
// range (see fixed_multiply/fixed_divide) happens before this ever runs.
template <int Exponent>
constexpr __int128 pow10() noexcept {
    static_assert(Exponent >= 0, "pow10 exponent must be non-negative");
    __int128 result = 1;
    for (int i = 0; i < Exponent; ++i) result *= 10;
    return result;
}

// Cross-width multiply: A::raw() * B::raw(), widened to __int128 for the
// multiply itself since the product can exceed raw_type's range even when
// both operands and the final rescaled result fit, then rescaled down to
// Result's precision and narrowed back via from_raw_checked. The
// static_assert below makes the rescale direction a checked precondition
// rather than an assumption.
//
// Deliberately kept in detail:: rather than exposed as a public, directly
// callable helper: it's a building block for the explicit, individually-
// declared operators in notional.hpp (and similar files). A public
// fixed_multiply<A, B, Result> would let any caller instantiate a
// meaningless combination that no operator* actually exposes.
template <typename A, typename B, typename Result>
constexpr Result fixed_multiply(A a, B b) noexcept {
    constexpr int shift = A::decimals + B::decimals - Result::decimals;
    static_assert(shift >= 0 && shift <= 19,
                  "fixed_multiply needs a rescale this template can't do safely - "
                  "write an explicit operator for this pairing instead");
    __int128 raw_product = static_cast<__int128>(a.raw()) * static_cast<__int128>(b.raw());
    if constexpr (shift == 0) {
        return Result::from_raw_checked(raw_product);
    } else {
        return Result::from_raw_checked(round_div_nearest_away(raw_product, pow10<shift>()));
    }
}

// Cross-width divide: Dividend::raw() / Divisor::raw() -> Result, rescaled
// by 10^(Result::decimals + Divisor::decimals - Dividend::decimals) so the
// integer division lands directly on Result's precision. Mirrors
// fixed_multiply's shape and detail:: reasoning above.
template <typename Dividend, typename Divisor, typename Result>
constexpr std::expected<Result, std::errc> fixed_divide(Dividend a, Divisor b) noexcept {
    // Rejects a non-positive divisor as a domain judgment inherited from
    // the types this is used with (Price, Size, Notional are all
    // non-negative), not a property of division in general - so a
    // non-positive divisor here always means malformed input, not a
    // legitimate signed division. A future signed Divisor type would need
    // sign-normalization here instead of outright rejection (see
    // round_div_nearest_away's own doc comment on the same distinction).
    if (b.raw() <= 0) return std::unexpected(std::errc::argument_out_of_domain);

    constexpr int shift = Result::decimals + Divisor::decimals - Dividend::decimals;
    static_assert(shift >= 0 && shift <= 19,
                  "fixed_divide needs a rescale this template can't do safely - "
                  "write an explicit operator for this pairing instead");
    __int128 numerator = static_cast<__int128>(a.raw());
    if constexpr (shift > 0) numerator *= pow10<shift>();
    return Result::from_raw_checked(
        round_div_nearest_away(numerator, static_cast<__int128>(b.raw())));
}

}  // namespace detail

}  // namespace bobby::hermeneutic

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
    // to raw_type - the last step of every __int128-intermediate computation
    // in this codebase (notional.hpp's operator*/operator/, volume_bands.hpp's
    // vwap_at_partial_fill, price_bands.hpp's offset_by_bps all used to repeat
    // this exact debug-assert-then-narrow shape by hand). Debug-only, same as
    // those call sites always were: no realistic Price/Size/Notional magnitude
    // in this codebase reaches raw_type's ~9.2e18 range, so this documents an
    // invariant rather than handling a reachable error - a caller with a
    // genuine runtime-reachable overflow risk (unlike these internal
    // fixed-point helpers) should check before calling this, not rely on it.
    static constexpr BasicFixedPoint from_raw_checked(__int128 value) noexcept {
        assert(value >= static_cast<__int128>(std::numeric_limits<raw_type>::min()));
        assert(value <= static_cast<__int128>(std::numeric_limits<raw_type>::max()));
        return from_raw(static_cast<raw_type>(value));
    }

    explicit constexpr BasicFixedPoint(double value) noexcept
        : raw_(static_cast<raw_type>(value * static_cast<double>(scale) +
                                      (value >= 0 ? 0.5 : -0.5))) {}

    // Parses a decimal string directly into raw integer scale, entirely in
    // integer arithmetic - no double intermediate anywhere, unlike
    // std::from_chars (string -> double) + BasicFixedPoint(double)
    // (double -> raw int64_t, via `value * scale + 0.5`): a 2026-09-19
    // measurement (see this project's git history for the brute-force
    // numbers) found that two-step path safe below 15 total significant
    // digits and unsafe above it, and the unsafety isn't confined to the
    // first step either - BasicFixedPoint(double)'s own multiply is an
    // independent double-precision rounding, unmeasured by that first pass
    // and not fixed by improving the first step alone. Removing the double
    // intermediate entirely, not just narrowing one of its two uses, is the
    // only way to close both at once.
    //
    // Accepted grammar: an optional leading '-' (never '+' - std::from_
    // chars<double> itself rejects a leading '+', so this isn't a
    // narrowing versus the path this replaces), then digits, optionally
    // followed by '.' and more digits, with at least one digit somewhere
    // (rejects "", "-", ".", and "-."). Deliberately narrower than
    // std::from_chars<double>'s own grammar in two ways, both confirmed
    // safe against every wire fixture this project parses at the time this
    // was written: scientific notation ("1e5") is rejected as a format
    // error, and so are "nan"/"inf"/"infinity" (which std::from_chars
    // <double> parses successfully) - the latter an incidental fix for a
    // real, if never-observed-in-the-wild, gap in the path this replaces: a
    // literal "NaN" would previously parse into a double NaN, then into
    // `static_cast<raw_type>(NaN * scale + 0.5)`, which is undefined
    // behavior, not a caught parse error.
    //
    // Reports malformed input or an out-of-range result via std::expected
    // rather than throwing, so this file stays free of any particular
    // error-reporting convention (exceptions, error codes, ...) a caller
    // might want - a wire parser that needs to fold this into its own
    // exception-based error handling (see exchange/feed_wire.hpp) does that
    // translation at its own call site instead.
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
        // pass. Accumulated in a 128-bit intermediate. Bailing as soon as
        // it exceeds raw_type's own max (not raw_type's max/scale - the
        // final, tight bound is applied after scaling below) is a looser
        // check, but it's what keeps this loop itself safe from ever
        // overflowing __int128 on a maliciously long digit string:
        // __int128 has roughly 20 more decimal digits of headroom than
        // raw_type::max, so this trips long before that could happen,
        // however many digits `int_part` has.
        unsigned __int128 int_value = 0;
        for (char c : int_part) {
            if (c < '0' || c > '9') return std::unexpected(std::errc::invalid_argument);
            int_value = int_value * 10 + static_cast<unsigned>(c - '0');
            if (int_value > static_cast<unsigned __int128>(kRawMax)) {
                return std::unexpected(std::errc::result_out_of_range);
            }
        }

        // Fractional part: every character still has to be validated
        // (this is untrusted input - "1.23abc" must still be rejected,
        // not silently truncated to "1.23"), but only the first
        // kDecimals+1 characters are ever *accumulated*: under "round half
        // up, ties away from zero", the single digit immediately after
        // the cut point already fully determines the outcome (>=5 always
        // rounds up regardless of what follows, since further digits can
        // only make the discarded remainder larger, never pull it back
        // under half; <=4 always rounds down for the same reason in
        // reverse) - so digits beyond that one can never change the
        // result. One pass over the whole of frac_part does both at once,
        // rather than a full validation pass followed by a separate
        // (shorter) accumulation pass - same fusion as int_part above.
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
        // nearest_away can push frac_value to exactly kScale (never
        // beyond: the largest possible frac_numerator, kDecimals+1 nines,
        // rounds to exactly 10^kDecimals), so a single carry is the only
        // case to handle.
        if (frac_value == static_cast<__int128>(scale)) {
            int_value += 1;
            frac_value = 0;
        }
        // Re-checks the same loose bound as the accumulation loop above,
        // now including the carry - only load-bearing for a hypothetical
        // Decimals=0 type (where int_value IS the final magnitude, so a
        // carry landing exactly on kRawMax needs to be caught here). For
        // Price/Size (Decimals=9/6), int_value at this point is still
        // many orders of magnitude below kRawMax whenever the final
        // result is going to be valid at all - the real guard for those
        // is the tight post-scale check just below, not this one.
        if (int_value > static_cast<unsigned __int128>(kRawMax)) {
            return std::unexpected(std::errc::result_out_of_range);
        }

        // Final, tight bound: against raw_type::max after scaling, the
        // actual overflow condition for the raw_type this returns (unlike
        // the loose checks above, which only protect the accumulation
        // loop itself and the Decimals=0 edge case). Applies the same
        // bound to both signs rather than letting a negative result use
        // raw_type::min's one-unit-larger magnitude (two's complement) -
        // real price/size data never comes remotely close to either
        // bound, so this asymmetry has no practical effect, and treating
        // both signs identically here is simpler than the alternative.
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

    constexpr BasicFixedPoint operator+(BasicFixedPoint other) const noexcept {
        return from_raw(raw_ + other.raw_);
    }

    constexpr BasicFixedPoint operator-(BasicFixedPoint other) const noexcept {
        return from_raw(raw_ - other.raw_);
    }

    constexpr BasicFixedPoint operator-() const noexcept { return from_raw(-raw_); }

    constexpr BasicFixedPoint& operator+=(BasicFixedPoint other) noexcept {
        raw_ += other.raw_;
        return *this;
    }

    constexpr BasicFixedPoint& operator-=(BasicFixedPoint other) noexcept {
        raw_ -= other.raw_;
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
// multiply itself (same reason as every other __int128 intermediate in this
// codebase - the product can exceed raw_type's range even when both
// operands and the final rescaled result fit), then rescaled down to
// Result's precision and narrowed back via from_raw_checked. The rescale
// direction (A::decimals + B::decimals - Result::decimals) is always a
// right-shift (a division) for the type combinations this codebase actually
// uses (Price*Size -> Notional, at scale 10^15 rescaled to Notional's
// 10^9) - the static_assert below is what makes that a checked precondition
// rather than an assumption.
//
// Deliberately kept in detail:: rather than exposed as a public, directly
// callable helper: it's a building block for the explicit, individually-
// declared operators in notional.hpp (and similar files), not a substitute
// for declaring them. A public fixed_multiply<A, B, Result> would let any
// caller instantiate a meaningless combination (fixed_multiply<Price,
// Price, Notional>) that no operator* actually exposes - keeping it in
// detail:: confines that possibility to this file's own operator
// definitions, which only ever instantiate the combinations they declare.
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
// fixed_multiply's shape and the same non-templated logic every
// hand-written rescaling operator/ in notional.hpp used to repeat, and the
// same detail:: reasoning as fixed_multiply above for why this isn't public.
template <typename Dividend, typename Divisor, typename Result>
constexpr std::expected<Result, std::errc> fixed_divide(Dividend a, Divisor b) noexcept {
    // This rejects a non-positive divisor, but that's a domain judgment
    // inherited from the types this is used with, not a property of
    // division in general: Price, Size, and Notional are all non-negative
    // quantities, so a non-positive divisor here always means malformed
    // input (a default-constructed or corrupt value), not a legitimate
    // signed division. It also happens to be what round_div_nearest_away
    // below requires (a strictly positive denominator) - but that mechanical
    // requirement is a coincidence of the current all-non-negative type set,
    // not the reason for the check. A future signed Divisor type would need
    // sign-normalization here instead of outright rejection - see
    // round_div_nearest_away's own doc comment on the same distinction.
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

#pragma once

#include <compare>
#include <cstdint>
#include <cstdlib>
#include <ostream>

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

    explicit constexpr BasicFixedPoint(double value) noexcept
        : raw_(static_cast<raw_type>(value * static_cast<double>(scale) +
                                      (value >= 0 ? 0.5 : -0.5))) {}

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

}  // namespace bobby::hermeneutic

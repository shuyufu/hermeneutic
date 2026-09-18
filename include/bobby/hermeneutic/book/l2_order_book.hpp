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

}  // namespace bobby::hermeneutic

#pragma once

#include <chrono>

namespace bobby::hermeneutic::ingestion {

// The default WebSocket idle-read timeout (see WebSocketConnection::
// connect()'s own doc comment for what this guards against),
// shared by VenueSession's own constructor default and
// IdleTimeoutConfig's default_timeout (book_subscription.hpp) - a single
// definition rather than two independently hardcoded 30s constants that
// could silently drift apart. Deliberately its own header rather than
// living in either of those: VenueSession must stay simdjson-free (see
// book_subscription.hpp's own comment on why it, not the ingestion tier,
// owns the JSON dependency), so neither file can simply include the other.
inline constexpr std::chrono::seconds kDefaultIdleTimeout{30};

// Upper bound book_subscription.hpp's parse_idle_timeout_config() enforces
// on both "idle_timeout_seconds" and each venue_idle_timeout_overrides
// entry - not a value anyone should actually want (an operator trying to
// effectively disable the timeout should pick something merely generous,
// like an hour), but a real ceiling nonetheless: websocket::stream_base::
// timeout::duration is a nanosecond-resolution std::chrono::steady_clock::
// duration (int64), and an unvalidated seconds value large enough
// overflows on that conversion, silently wrapping into a near-zero or
// negative duration - the exact opposite of what a large value was meant
// to request, producing a reconnect-thrash loop instead of a long/lenient
// timeout. 24 hours is comfortably below the ~292-year point where that
// conversion would actually overflow, while still large enough that
// nothing legitimate should ever need more.
inline constexpr std::chrono::seconds kMaxIdleTimeout{24 * 60 * 60};

}  // namespace bobby::hermeneutic::ingestion

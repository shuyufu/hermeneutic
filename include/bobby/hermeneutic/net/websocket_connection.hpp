#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>  // async_teardown/teardown for net::ssl::stream<T>

#include <cassert>
#include <chrono>
#include <string>
#include <string_view>
#include <utility>

#include <openssl/ssl.h>

#include "bobby/hermeneutic/ingestion/idle_timeout.hpp"
#include "bobby/hermeneutic/net/net_traits.hpp"

namespace bobby::hermeneutic::ingestion {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;

// Thin, venue-agnostic WebSocket connection: connect/send/read/close only.
// Knows nothing about symbols, resync, or any particular exchange - that's
// VenueSession's job.
//
// Templated on the underlying stream so production code (wss://, a TLS
// stream) and tests (a plain TCP stream against a local server, avoiding
// any need for test certificates) share the exact same connect/send/read/
// close mechanics, verified once. Each reconnect gets a brand-new instance;
// this class does not retry or reconnect itself - see VenueSession.
template <typename NextLayer>
class WebSocketConnection {
  public:
    template <typename... Args>
    explicit WebSocketConnection(Args&&... args) : ws_(std::forward<Args>(args)...) {}

    // Resolves `host`, connects, performs a TLS handshake (only when
    // NextLayer is an SSL stream - a plain TCP stream skips straight to the
    // WebSocket upgrade), then performs the WebSocket upgrade handshake to
    // `target`.
    //
    // Idle-read detection (a half-open/black-holed connection - TCP still
    // up, but the peer has silently stopped responding to anything, not
    // even our own outgoing pings - never throwing from read() below) is
    // handled via Boost.Beast's own websocket::stream_base::timeout +
    // keep_alive_pings option, not a hand-rolled mechanism. A fake test
    // server that is itself a websocket::stream with a pending read is
    // not a valid way to test this: that pending read auto-answers an
    // incoming ping as a side effect of Beast's own read machinery,
    // regardless of whether the "application" ever sends anything, so it
    // never exercises the failure mode this option actually detects - a
    // valid black-hole test server must issue no read at all after the
    // handshake.
    // `idle_timeout` measures transport responsiveness, not
    // application-data freshness - deliberately: no exchange guarantees
    // it will push anything, not even a no-op heartbeat, during a
    // genuinely quiet market, so a check tied to application content
    // would misfire on an illiquid book with a perfectly healthy
    // connection (live-verified on Binance Futures CTKUSDT perp: ~14s
    // natural gaps between real updates, well within reach of a 30s
    // threshold).
    //
    // What "responsiveness" actually proves is narrower than "the peer is
    // alive": it's "the first hop that terminates our WS connection
    // answers something" - which for a venue fronted by a load balancer or
    // reverse proxy may be that edge, not the real backend feeding market
    // data. An edge that answers WS-level pings independently of its own
    // backend health would leave this mechanism reporting a healthy
    // connection indefinitely while no real data ever arrives again. This
    // is a known, structural blind spot of any transport-level check, not
    // something idle_timeout can be tuned to close - closing it needs a
    // second, independent, much-longer-window check on genuine
    // application data ever arriving at all, not yet built.
    net::awaitable<void> connect(std::string_view host, std::string_view port, std::string_view target,
                                  std::chrono::seconds idle_timeout = kDefaultIdleTimeout) {
        // book_subscription.hpp's parse_idle_timeout_config() is the only
        // path production code takes to get here, and it already rejects
        // a non-positive or over-kMaxIdleTimeout value - this is a debug-
        // only backstop against a caller that reaches this constructor
        // some other way (a test, a future direct construction) and
        // passes something that would silently misbehave: zero/negative
        // as a near-immediate timeout, or large enough to overflow
        // websocket::stream_base::timeout::duration (see idle_timeout.hpp's
        // own comment on kMaxIdleTimeout for why that's a real bug, not
        // just an unreasonable value) on conversion below.
        assert(idle_timeout.count() > 0 && idle_timeout <= kMaxIdleTimeout);
        auto executor = co_await net::this_coro::executor;
        net::ip::tcp::resolver resolver(executor);
        auto results = co_await resolver.async_resolve(host, port, net::use_awaitable);

        beast::get_lowest_layer(ws_).expires_after(std::chrono::seconds(30));
        co_await beast::get_lowest_layer(ws_).async_connect(results, net::use_awaitable);

        if constexpr (detail::is_ssl_stream_v<NextLayer>) {
            // SNI: without this, many servers (Binance included) either
            // reject the handshake or serve the wrong certificate.
            if (!SSL_set_tlsext_host_name(ws_.next_layer().native_handle(), std::string(host).c_str())) {
                throw beast::system_error(
                    beast::error_code(static_cast<int>(::ERR_get_error()), net::error::get_ssl_category()));
            }
            co_await ws_.next_layer().async_handshake(net::ssl::stream_base::client, net::use_awaitable);
        }

        // The connect (and, for TLS, handshake) timeout above no longer
        // applies once we're past it - the WebSocket-level timeout option
        // below takes over for the lifetime of the connection.
        beast::get_lowest_layer(ws_).expires_never();
        // Starts from Beast's own suggested(client) value (handshake_
        // timeout=30s as of boost-beast 1.92.0) rather than hardcoding
        // 30s a second time here, so a future Beast upgrade changing that
        // default is picked up automatically instead of silently going
        // stale against an independently-duplicated constant. Only
        // idle_timeout/keep_alive_pings are actually overridden -
        // suggested(client) sets idle_timeout=none()/keep_alive_pings=
        // false, no idle detection at all (see this function's own
        // comment above for why that's not what this needs).
        auto timeout_opt = websocket::stream_base::timeout::suggested(beast::role_type::client);
        timeout_opt.idle_timeout = idle_timeout;
        timeout_opt.keep_alive_pings = true;
        ws_.set_option(timeout_opt);

        co_await ws_.async_handshake(std::string(host), std::string(target), net::use_awaitable);
    }

    net::awaitable<void> send(std::string message) {
        co_await ws_.async_write(net::buffer(message), net::use_awaitable);
    }

    // Cancellable by stop() (via run_sig_) through the *ambient*
    // cancellation state that propagates automatically through nested
    // co_await calls in an awaitable coroutine - see run()'s own comments
    // in venue_session.hpp for how that's wired up. A timeout fires here
    // as beast::error::timeout, not a cancellation - see connect()'s own
    // comment above for what idle_timeout actually measures.
    net::awaitable<std::string> read() {
        beast::flat_buffer buffer;
        co_await ws_.async_read(buffer, net::use_awaitable);
        co_return beast::buffers_to_string(buffer.data());
    }

    net::awaitable<void> close() {
        co_await ws_.async_close(websocket::close_code::normal, net::use_awaitable);
    }

  private:
    websocket::stream<NextLayer> ws_;
};

using PlainWebSocketConnection = WebSocketConnection<beast::tcp_stream>;
using TlsWebSocketConnection = WebSocketConnection<net::ssl::stream<beast::tcp_stream>>;

}  // namespace bobby::hermeneutic::ingestion

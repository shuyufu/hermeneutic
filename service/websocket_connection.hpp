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

#include <chrono>
#include <string>
#include <string_view>
#include <utility>

#include <openssl/ssl.h>

#include "net_traits.hpp"

namespace bobby::hermeneutic::ingestion {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace websocket = beast::websocket;

// Thin, venue-agnostic WebSocket connection: connect/send/read/close only.
// Knows nothing about symbols, resync, or any particular exchange - see
// docs/ingestion_design.md for what owns that (VenueSession).
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
    net::awaitable<void> connect(std::string_view host, std::string_view port, std::string_view target) {
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
        ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));

        co_await ws_.async_handshake(std::string(host), std::string(target), net::use_awaitable);
    }

    net::awaitable<void> send(std::string message) {
        co_await ws_.async_write(net::buffer(message), net::use_awaitable);
    }

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

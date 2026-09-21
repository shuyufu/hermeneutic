#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <chrono>
#include <expected>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <openssl/ssl.h>

#include "bobby/hermeneutic/net/net_traits.hpp"

namespace bobby::hermeneutic::ingestion {

namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;

// One-shot HTTP(S) GET, not a persistent connection object -- a REST
// snapshot fetch is an infrequent, ad hoc call. Templated on the
// underlying stream the same way WebSocketConnection is: production uses an
// SSL stream (real exchanges are HTTPS-only), tests use a plain TCP stream
// against a local server, sharing the request/response mechanics through
// one function body.
template <typename NextLayer, typename... StreamArgs>
net::awaitable<std::expected<std::string, std::errc>> http_get(std::string_view host,
                                                                 std::string_view port,
                                                                 std::string_view target,
                                                                 StreamArgs&&... stream_args) {
    try {
        NextLayer stream(std::forward<StreamArgs>(stream_args)...);

        auto executor = co_await net::this_coro::executor;
        net::ip::tcp::resolver resolver(executor);
        auto results = co_await resolver.async_resolve(host, port, net::use_awaitable);

        beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(10));
        co_await beast::get_lowest_layer(stream).async_connect(results, net::use_awaitable);

        if constexpr (detail::is_ssl_stream_v<NextLayer>) {
            if (!SSL_set_tlsext_host_name(stream.native_handle(), std::string(host).c_str())) {
                co_return std::unexpected(std::errc::io_error);
            }
            co_await stream.async_handshake(net::ssl::stream_base::client, net::use_awaitable);
        }

        http::request<http::empty_body> request{http::verb::get, std::string(target), 11};
        request.set(http::field::host, std::string(host));
        request.set(http::field::user_agent, "hermeneutic-ingestion");
        co_await http::async_write(stream, request, net::use_awaitable);

        beast::flat_buffer buffer;
        http::response<http::string_body> response;
        co_await http::async_read(stream, buffer, response, net::use_awaitable);

        // Best-effort shutdown; a peer that closes first (very common) is
        // routine, not a failure worth reporting.
        beast::error_code shutdown_ec;
        if constexpr (detail::is_ssl_stream_v<NextLayer>) {
            co_await stream.async_shutdown(net::redirect_error(net::use_awaitable, shutdown_ec));
        } else {
            beast::get_lowest_layer(stream).socket().shutdown(net::ip::tcp::socket::shutdown_both,
                                                                shutdown_ec);
        }

        if (response.result() != http::status::ok) {
            co_return std::unexpected(std::errc::io_error);
        }
        co_return response.body();
    } catch (const std::exception&) {
        co_return std::unexpected(std::errc::io_error);
    }
}

}  // namespace bobby::hermeneutic::ingestion

#include "http_client.hpp"

#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/beast/core.hpp>

#include <exception>
#include <string>
#include <string_view>

namespace bobby::hermeneutic::ingestion {
namespace {

net::awaitable<void> run_http_server(net::ip::tcp::acceptor acceptor, http::status status_to_send,
                                      std::string body_to_send, std::string& received_target) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);

    beast::flat_buffer buffer;
    http::request<http::empty_body> request;
    co_await http::async_read(socket, buffer, request, net::use_awaitable);
    received_target = std::string(request.target());

    http::response<http::string_body> response{status_to_send, request.version()};
    response.set(http::field::content_type, "application/json");
    response.body() = std::move(body_to_send);
    response.prepare_payload();
    co_await http::async_write(socket, response, net::use_awaitable);

    beast::error_code ec;
    socket.shutdown(net::ip::tcp::socket::shutdown_send, ec);
}

auto fail_test_on_exception(std::string_view label) {
    return [label](std::exception_ptr e) {
        if (!e) return;
        try {
            std::rethrow_exception(e);
        } catch (const std::exception& ex) {
            ADD_FAILURE() << label << " coroutine threw: " << ex.what();
        }
    };
}

TEST(HttpGetTest, SuccessfulGetReturnsBody) {
    net::io_context io;
    net::ip::tcp::acceptor acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();

    std::string received_target;
    net::co_spawn(io, run_http_server(std::move(acceptor), http::status::ok, R"({"lastUpdateId":123})",
                                       received_target),
                  fail_test_on_exception("server"));

    std::expected<std::string, std::errc> result;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            result = co_await http_get<beast::tcp_stream>("127.0.0.1", std::to_string(port),
                                                           "/fapi/v1/depth?symbol=BTCUSDT&limit=1000",
                                                           io.get_executor());
        }(),
        fail_test_on_exception("client"));

    io.run();

    EXPECT_EQ(received_target, "/fapi/v1/depth?symbol=BTCUSDT&limit=1000");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(*result, R"({"lastUpdateId":123})");
}

TEST(HttpGetTest, NonOkStatusFailsWithIoError) {
    net::io_context io;
    net::ip::tcp::acceptor acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();

    std::string received_target;
    net::co_spawn(io, run_http_server(std::move(acceptor), http::status::not_found, "not found",
                                       received_target),
                  fail_test_on_exception("server"));

    std::expected<std::string, std::errc> result;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            result = co_await http_get<beast::tcp_stream>("127.0.0.1", std::to_string(port), "/missing",
                                                           io.get_executor());
        }(),
        fail_test_on_exception("client"));

    io.run();

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::io_error);
}

TEST(HttpGetTest, ConnectionRefusedFailsWithIoError) {
    net::io_context io;

    // Nothing listens on this port - resolves fine, connect fails.
    std::expected<std::string, std::errc> result;
    net::co_spawn(
        io,
        [&]() -> net::awaitable<void> {
            result = co_await http_get<beast::tcp_stream>("127.0.0.1", "1", "/", io.get_executor());
        }(),
        fail_test_on_exception("client"));

    io.run();

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), std::errc::io_error);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

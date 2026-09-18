#include "bobby/hermeneutic/net/websocket_connection.hpp"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <exception>
#include <string>
#include <string_view>

namespace bobby::hermeneutic::ingestion {
namespace {

// Minimal local WebSocket server for one connection: accepts, writes one
// message, reads one message back, then closes. Plain TCP only - tests
// exercise WebSocketConnection's mechanics without needing a test TLS
// certificate; the TLS handshake branch (used in production against real
// exchanges) is covered by SNI/handshake logic that's a thin, well-tested
// pass-through to Boost.Asio/OpenSSL, not this project's own code.
net::awaitable<void> run_echo_server(net::ip::tcp::acceptor acceptor, std::string& received_by_server) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);

    co_await ws.async_write(net::buffer(std::string_view("hello from server")), net::use_awaitable);

    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);
    received_by_server = beast::buffers_to_string(buffer.data());

    co_await ws.async_close(websocket::close_code::normal, net::use_awaitable);
}

net::awaitable<void> run_client(unsigned short port, std::string& first_message_received) {
    auto executor = co_await net::this_coro::executor;
    PlainWebSocketConnection conn(executor);
    co_await conn.connect("127.0.0.1", std::to_string(port), "/");
    first_message_received = co_await conn.read();
    co_await conn.send("hello from client");
    co_await conn.close();
}

// Rethrows so any exception raised inside the coroutine fails the test via
// gtest's usual mechanism, instead of silently vanishing into io.run().
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

TEST(WebSocketConnectionTest, ConnectsSendsAndReceivesOverPlainTcp) {
    net::io_context io;
    net::ip::tcp::acceptor acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();

    std::string received_by_server;
    net::co_spawn(io, run_echo_server(std::move(acceptor), received_by_server),
                  fail_test_on_exception("server"));

    std::string first_message;
    net::co_spawn(io, run_client(port, first_message), fail_test_on_exception("client"));

    io.run();

    EXPECT_EQ(first_message, "hello from server");
    EXPECT_EQ(received_by_server, "hello from client");
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

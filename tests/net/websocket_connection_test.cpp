#include "bobby/hermeneutic/net/websocket_connection.hpp"

#include <gtest/gtest.h>

#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <exception>
#include <string>
#include <string_view>
#include <utility>

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

// The inverse of fail_test_on_exception() above, for the error-path tests
// below where a thrown exception is the expected, correct outcome - stashes
// it into `out` so the test body can inspect it after io.run() returns,
// instead of failing (or silently discarding) it here.
auto capture_exception(std::exception_ptr& out) {
    return [&out](std::exception_ptr e) { out = e; };
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

// connect() covers three distinct failure points internally (TCP connect,
// TLS handshake when NextLayer is an SSL stream, WebSocket upgrade
// handshake) but until now nothing exercised any of them failing -
// VenueSessionTest only ever drives connect() against a cooperating fake
// server. The three tests below cover the plain-TCP-stream failure points
// that don't need a test TLS certificate (see run_echo_server's own comment
// on why TLS itself is out of scope here); connect()'s success path is
// already covered by ConnectsSendsAndReceivesOverPlainTcp above.

TEST(WebSocketConnectionTest, ConnectFailsWhenNothingIsListening) {
    net::io_context io;
    // Grabs an ephemeral port and then closes the acceptor without ever
    // calling async_accept - nothing answers this port afterward, so
    // connecting to it gets an immediate, deterministic connection-refused
    // from the OS rather than a hang or a flaky external unreachable-host
    // dependency.
    net::ip::tcp::acceptor acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();
    acceptor.close();

    std::exception_ptr thrown;
    net::co_spawn(
        io,
        [port]() -> net::awaitable<void> {
            auto executor = co_await net::this_coro::executor;
            PlainWebSocketConnection conn(executor);
            co_await conn.connect("127.0.0.1", std::to_string(port), "/");
        }(),
        capture_exception(thrown));

    io.run();

    ASSERT_TRUE(thrown);
    try {
        std::rethrow_exception(thrown);
        FAIL() << "connect() should have thrown - nothing was listening";
    } catch (const beast::system_error& e) {
        EXPECT_EQ(e.code(), net::error::connection_refused);
    }
}

net::awaitable<void> run_server_that_closes_before_ws_handshake(net::ip::tcp::acceptor acceptor) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    // Closes the raw TCP connection immediately after accepting it,
    // without ever performing ws.async_accept() - simulates a peer that
    // drops the connection mid-handshake (e.g. a load balancer health
    // check, or a server rejecting the upgrade at the TCP level) rather
    // than one that completes the handshake and only fails later.
    socket.close();
}

TEST(WebSocketConnectionTest, ConnectFailsWhenServerClosesDuringWebSocketHandshake) {
    net::io_context io;
    net::ip::tcp::acceptor acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();

    net::co_spawn(io, run_server_that_closes_before_ws_handshake(std::move(acceptor)),
                  fail_test_on_exception("server"));

    std::exception_ptr thrown;
    net::co_spawn(
        io,
        [port]() -> net::awaitable<void> {
            auto executor = co_await net::this_coro::executor;
            PlainWebSocketConnection conn(executor);
            co_await conn.connect("127.0.0.1", std::to_string(port), "/");
        }(),
        capture_exception(thrown));

    io.run();

    ASSERT_TRUE(thrown);
    // The exact code (eof, connection_reset, broken_pipe, ...) depends on
    // exactly when the OS notices the peer is gone relative to the
    // client's own connect/handshake writes - only that the WebSocket
    // upgrade never completes is guaranteed here.
    EXPECT_THROW(std::rethrow_exception(thrown), beast::system_error);
}

net::awaitable<void> run_server_that_force_closes_after_handshake(net::ip::tcp::acceptor acceptor) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);
    // Tears down the underlying TCP socket directly - unlike
    // run_echo_server's ws.async_close() above, this sends no WebSocket
    // close frame at all, simulating a peer that vanishes (crash, network
    // partition) rather than one that says goodbye.
    beast::get_lowest_layer(ws).close();
}

TEST(WebSocketConnectionTest, ReadFailsAfterPeerAbruptlyClosesConnection) {
    net::io_context io;
    net::ip::tcp::acceptor acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short port = acceptor.local_endpoint().port();

    net::co_spawn(io, run_server_that_force_closes_after_handshake(std::move(acceptor)),
                  fail_test_on_exception("server"));

    std::exception_ptr thrown;
    net::co_spawn(
        io,
        [port]() -> net::awaitable<void> {
            auto executor = co_await net::this_coro::executor;
            PlainWebSocketConnection conn(executor);
            co_await conn.connect("127.0.0.1", std::to_string(port), "/");
            co_await conn.read();  // expected to throw - the point of this test
        }(),
        capture_exception(thrown));

    io.run();

    ASSERT_TRUE(thrown);
    EXPECT_THROW(std::rethrow_exception(thrown), beast::system_error);
}

// idle_timeout.hpp's own comment: kMaxIdleTimeout is meant to stay
// comfortably below the point where converting a seconds count to
// websocket::stream_base::duration (a nanosecond-resolution,
// int64 steady_clock::duration - see connect()'s own use of it above)
// overflows and silently wraps into a near-zero or negative value. Until
// now only book_subscription.hpp's independent seconds-count range check
// was tested (ParseIdleTimeoutConfig.AcceptsDefaultExactlyAtMax/
// RejectsDefaultAboveMax) - the two assignments below are the same
// operation, on the same two types, as connect()'s own
// `timeout_opt.idle_timeout = idle_timeout;` (this class's own code,
// above): not a parallel re-implementation that could quietly drift from
// it, since there's no custom conversion logic in either place for the two
// to disagree on - both are exactly what std::chrono::duration's own
// converting assignment does for these types. Driven at kMaxIdleTimeout
// itself and at a value one second short of the real overflow point
// (duration::max(), ~292 years) - kMaxIdleTimeout (24h) alone is ~5 orders
// of magnitude below that point, nowhere near where a subtler overflow bug
// (an intermediate narrower multiply, a unit-mismatch factor upstream of
// this assignment) would actually surface. Deliberately no case that
// actually overflows: that conversion's overflow behavior is unspecified/
// UB, not a defined wraparound worth asserting on.
TEST(WebSocketConnectionTest, KMaxIdleTimeoutDoesNotOverflowBeastsTimeoutDuration) {
    websocket::stream_base::duration converted = kMaxIdleTimeout;
    EXPECT_GT(converted.count(), 0);
    EXPECT_EQ(std::chrono::duration_cast<std::chrono::seconds>(converted), kMaxIdleTimeout);

    auto near_overflow_boundary =
        std::chrono::duration_cast<std::chrono::seconds>(websocket::stream_base::duration::max()) -
        std::chrono::seconds(1);
    websocket::stream_base::duration near_boundary_converted = near_overflow_boundary;
    EXPECT_GT(near_boundary_converted.count(), 0);
    EXPECT_EQ(std::chrono::duration_cast<std::chrono::seconds>(near_boundary_converted), near_overflow_boundary);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

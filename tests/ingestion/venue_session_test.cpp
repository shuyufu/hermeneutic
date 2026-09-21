#include "bobby/hermeneutic/ingestion/venue_session.hpp"

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>

#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "apps/aggregator/aggregator_service.hpp"
#include "apps/aggregator/book_id.hpp"
#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"
#include "bobby/hermeneutic/exchange/binance/binance_futures_sequence_policy.hpp"
#include "bobby/hermeneutic/exchange/bybit/bybit_sequence_policy.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic::ingestion {
namespace {

using bobby::hermeneutic::aggregator::AggregatorService;
using bobby::hermeneutic::aggregator::fill_wire_book_id;
using bobby::hermeneutic::aggregator::L2Update;
using bobby::hermeneutic::aggregator::SymbolBook;
using bobby::hermeneutic::symbol::BaseQuote;
using bobby::hermeneutic::symbol::BookId;
using bobby::hermeneutic::symbol::Exchange;
using bobby::hermeneutic::symbol::MarketType;

// An arbitrary VenueId - these tests exercise VenueSession's own behavior
// (start/stop/resync/gap-handling), never AggregateOrderBook's per-venue
// isolation, so which real venue/market this names doesn't matter.
constexpr bobby::hermeneutic::VenueId kFakeVenue{Exchange::Binance, MarketType::Spot};

// The one book every AggregatorService in this file is constructed with -
// these tests are about VenueSession's own mechanics (backoff/stop/snapshot
// bridging), not about book identity, so a single fixed BookId is enough.
BookId TestBookId() { return BookId{BaseQuote{{"BTC"}, {"USDT"}}, MarketType::Spot}; }

std::vector<std::string_view> split(std::string_view text, char delimiter) {
    std::vector<std::string_view> fields;
    std::size_t start = 0;
    while (true) {
        auto pos = text.find(delimiter, start);
        fields.push_back(text.substr(start, pos - start));
        if (pos == std::string_view::npos) break;
        start = pos + 1;
    }
    return fields;
}

std::uint64_t parse_u64(std::string_view text) {
    std::uint64_t value = 0;
    std::from_chars(text.data(), text.data() + text.size(), value);
    return value;
}

double parse_double(std::string_view text) {
    double value = 0.0;
    std::from_chars(text.data(), text.data() + text.size(), value);
    return value;
}

// Same shape as feed_wire.hpp's HttpRequestSpec (host/port/target),
// redeclared here so this test doesn't need to include that header (and
// pull in simdjson, which FakeFeed's own trivial wire format has no use
// for).
struct FakeHttpRequestSpec {
    std::string host;
    std::string port;
    std::string target;
};

// Minimal test double for VenueFeed: a bare colon-delimited wire format
// instead of real exchange JSON, so this test doesn't need simdjson (or
// any real exchange's schema) to exercise VenueSession's own wiring.
// Deliberately reuses BinanceFuturesSequencePolicy as its SequencePolicy
// (see bobby/hermeneutic/symbol_sync.hpp) rather than inventing a second fake -
// the resync algorithm itself is already covered by SymbolSyncTest, this
// test only needs to prove VenueSession drives it correctly.
class FakeFeed {
  public:
    static constexpr bool kSnapshotViaRest = true;

    FakeFeed(std::string ws_port, std::string http_port)
        : ws_port_(std::move(ws_port)), http_port_(std::move(http_port)) {}

    std::string_view ws_host() const { return "127.0.0.1"; }
    std::string_view ws_port() const { return ws_port_; }
    std::string_view ws_target() const { return "/"; }

    std::string subscribe_message(std::span<const NativeSymbol>) const { return "SUBSCRIBE"; }

    // "DEPTH:<symbol>:<first_id>:<final_id>:<prev_final_id>:<bid_price>:<bid_size>"
    std::expected<std::optional<std::variant<SnapshotMessage, DepthUpdate>>, std::errc> parse_message(
        std::string_view text) const {
        if (text == "PING") return std::optional<std::variant<SnapshotMessage, DepthUpdate>>{std::nullopt};

        if (text.starts_with("DEPTH:")) {
            auto fields = split(text.substr(6), ':');
            if (fields.size() != 6) return std::unexpected(std::errc::bad_message);
            DepthUpdate update;
            update.symbol = std::string(fields[0]);
            update.first_id = parse_u64(fields[1]);
            update.final_id = parse_u64(fields[2]);
            update.prev_final_id = parse_u64(fields[3]);
            update.bids = {{Price(parse_double(fields[4])), Size(parse_double(fields[5]))}};
            return std::optional<std::variant<SnapshotMessage, DepthUpdate>>{std::in_place,
                                                                              std::move(update)};
        }
        return std::unexpected(std::errc::bad_message);
    }

    FakeHttpRequestSpec snapshot_request(const NativeSymbol& symbol) const {
        return FakeHttpRequestSpec{"127.0.0.1", http_port_, "/snapshot/" + symbol};
    }

    // "<last_update_id>:<bid_price>:<bid_size>"
    std::expected<SnapshotMessage, std::errc> parse_snapshot_response(NativeSymbol symbol,
                                                                       std::string_view body) const {
        auto fields = split(body, ':');
        if (fields.size() != 3) return std::unexpected(std::errc::bad_message);
        SnapshotMessage snapshot;
        snapshot.symbol = std::move(symbol);
        snapshot.last_update_id = parse_u64(fields[0]);
        snapshot.bids = {{Price(parse_double(fields[1])), Size(parse_double(fields[2]))}};
        return snapshot;
    }

  private:
    std::string ws_port_;
    std::string http_port_;
};

// Minimal test double for the *other* VenueFeed shape: kSnapshotViaRest ==
// false, matching Bybit/OKX - the exchange pushes its own snapshot as the
// first message on the WS connection instead of the client racing a
// separate REST fetch against the already-flowing diff stream. No
// snapshot_request()/parse_snapshot_response() at all: VenueSession's
// handle_request_snapshot() never calls either for a Feed shaped like
// this - see its own `if constexpr (!Feed::kSnapshotViaRest)` branch -
// so there is nothing here for those methods' absence to break, and no
// fake HTTP server is needed for any test using this Feed (a real one
// existing that this test never talks to still wouldn't prove anything;
// not existing at all is what actually proves REST is never touched).
class FakeFeedNoRest {
  public:
    static constexpr bool kSnapshotViaRest = false;

    explicit FakeFeedNoRest(std::string ws_port) : ws_port_(std::move(ws_port)) {}

    std::string_view ws_host() const { return "127.0.0.1"; }
    std::string_view ws_port() const { return ws_port_; }
    std::string_view ws_target() const { return "/"; }

    std::string subscribe_message(std::span<const NativeSymbol>) const { return "SUBSCRIBE"; }

    // "SNAPSHOT:<symbol>:<last_update_id>:<bid_price>:<bid_size>" (pushed
    // by the exchange, unprompted - not a response to any request this
    // Feed ever makes) or "DEPTH:<symbol>:<first_id>:<final_id>:
    // <prev_final_id>:<bid_price>:<bid_size>" (same shape FakeFeed above
    // uses, matching Bybit's real wire shape of one update id serving as
    // both first_id and final_id - see BybitSequencePolicy's own comment).
    std::expected<std::optional<std::variant<SnapshotMessage, DepthUpdate>>, std::errc> parse_message(
        std::string_view text) const {
        if (text == "PING") return std::optional<std::variant<SnapshotMessage, DepthUpdate>>{std::nullopt};

        if (text.starts_with("SNAPSHOT:")) {
            auto fields = split(text.substr(9), ':');
            if (fields.size() != 4) return std::unexpected(std::errc::bad_message);
            SnapshotMessage snapshot;
            snapshot.symbol = std::string(fields[0]);
            snapshot.last_update_id = parse_u64(fields[1]);
            snapshot.bids = {{Price(parse_double(fields[2])), Size(parse_double(fields[3]))}};
            return std::optional<std::variant<SnapshotMessage, DepthUpdate>>{std::in_place,
                                                                              std::move(snapshot)};
        }

        if (text.starts_with("DEPTH:")) {
            auto fields = split(text.substr(6), ':');
            if (fields.size() != 6) return std::unexpected(std::errc::bad_message);
            DepthUpdate update;
            update.symbol = std::string(fields[0]);
            update.first_id = parse_u64(fields[1]);
            update.final_id = parse_u64(fields[2]);
            update.prev_final_id = parse_u64(fields[3]);
            update.bids = {{Price(parse_double(fields[4])), Size(parse_double(fields[5]))}};
            return std::optional<std::variant<SnapshotMessage, DepthUpdate>>{std::in_place,
                                                                              std::move(update)};
        }
        return std::unexpected(std::errc::bad_message);
    }

  private:
    std::string ws_port_;
};

net::awaitable<void> run_fake_ws_server(net::ip::tcp::acceptor acceptor, std::string depth_message) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);

    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);  // the SUBSCRIBE message; content unchecked

    co_await ws.async_write(net::buffer(depth_message), net::use_awaitable);

    // Keeps the connection open for the rest of the test instead of letting
    // `ws` (and the underlying socket) get destroyed the instant this
    // coroutine returns - an immediate close here would make VenueSession's
    // very next read() see a disconnect and correctly invalidate/reset
    // (see docs/ingestion_design.md's on_disconnected()), which isn't what
    // this test is trying to exercise. Ends only when the test tears down
    // `io` (or, more likely, io.run_for()'s deadline elapses first).
    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
}

// Responds after a short artificial delay, so the depth event above (sent
// immediately) has already arrived and been buffered by the time this
// resolves - proving VenueSession keeps reading while the fetch is in
// flight, not only after it returns. `delay` defaults to comfortably longer
// than the depth event's arrival but short enough for a normal test
// timeout; StopDrainsInFlightSnapshotFetch below passes a much longer one
// to prove a cancelled fetch doesn't just sit there waiting it out.
// Reads one HTTP request off an already-accepted `socket` and answers it
// with `body` (optionally after an artificial `delay`) - the accept/read/
// (wait)/respond block every fake HTTP server below needs, factored out so
// each one only has to say how many connections it accepts and in what
// shape, not repeat this mechanics three times (a /code-review finding).
net::awaitable<void> respond_to_one_http_request(net::ip::tcp::socket& socket, std::string body,
                                                   std::chrono::milliseconds delay = std::chrono::milliseconds(0)) {
    beast::flat_buffer buffer;
    http::request<http::empty_body> request;
    co_await http::async_read(socket, buffer, request, net::use_awaitable);

    if (delay.count() > 0) {
        auto executor = co_await net::this_coro::executor;
        net::steady_timer timer(executor, delay);
        co_await timer.async_wait(net::use_awaitable);
    }

    http::response<http::string_body> response{http::status::ok, request.version()};
    response.body() = std::move(body);
    response.prepare_payload();
    co_await http::async_write(socket, response, net::use_awaitable);
}

net::awaitable<void> run_fake_http_server(net::ip::tcp::acceptor acceptor, std::string response_body,
                                           std::chrono::milliseconds delay = std::chrono::milliseconds(100)) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    co_await respond_to_one_http_request(socket, std::move(response_body), delay);
}

// Accepts a connection and closes it immediately - no request read, no
// response written - simulating a REST fetch attempt that fails (refused,
// reset, whatever the real cause), then accepts a second connection and
// answers it normally. http_get()'s own catch-all (any exception from the
// read/write chain becomes std::unexpected(std::errc::io_error)) turns the
// abrupt close into exactly the same "fetch failed" outcome
// handle_request_snapshot() sees from a real network failure.
net::awaitable<void> run_fake_http_server_fails_once_then_succeeds(net::ip::tcp::acceptor acceptor,
                                                                     std::string response_body) {
    { auto socket = co_await acceptor.async_accept(net::use_awaitable); }  // closed here, unanswered

    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    co_await respond_to_one_http_request(socket, std::move(response_body));
}

// Answers two separate REST requests in sequence on the same acceptor -
// for a scenario where handle_request_snapshot() gets triggered twice
// (once for the initial snapshot, once more for a later gap) without any
// WS reconnect in between, so the same acceptor has to serve both.
net::awaitable<void> run_fake_http_server_twice(net::ip::tcp::acceptor acceptor, std::string first_body,
                                                  std::string second_body,
                                                  std::chrono::milliseconds second_delay = std::chrono::milliseconds(0)) {
    {
        auto socket = co_await acceptor.async_accept(net::use_awaitable);
        co_await respond_to_one_http_request(socket, std::move(first_body));
    }

    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    // Unlike the first response, an artificial delay here (rather than
    // just relying on run_fake_ws_server_two_messages_gated's own gate) is
    // sometimes needed: that gate only proves the *first* batch's own
    // resync has already run by the time the gated WS message gets sent -
    // it says nothing about how long that message then takes to actually
    // reach and get buffered by SymbolSync (a WS write+read, on this same
    // io_context) relative to how fast *this* HTTP round trip (no delay of
    // its own, loopback) resolves. Without this, a fast enough second
    // response can arrive before the gated message does, find an empty
    // buffer, and retry immediately (SymbolSync's own no-bridge/no-op
    // RequestSnapshot loop) instead of exercising the bridge this test is
    // actually for.
    co_await respond_to_one_http_request(socket, std::move(second_body), second_delay);
}

// Accepts connections forever, each time consuming the SUBSCRIBE message
// and then immediately closing (ws/socket are loop-local, so they're
// destroyed - and the connection with them - the moment control loops back
// to accept the next one). Used to simulate a dropped connection without
// caring what the client does next: StopAbortsBackoffWaitAndDoesNotReconnect
// uses `connect_count` to prove a reconnect never happens once stop() has
// been called.
net::awaitable<void> run_flaky_ws_server(net::ip::tcp::acceptor acceptor, std::atomic<int>* connect_count) {
    while (true) {
        auto socket = co_await acceptor.async_accept(net::use_awaitable);
        connect_count->fetch_add(1);
        websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
        co_await ws.async_accept(net::use_awaitable);
        beast::flat_buffer buffer;
        boost::system::error_code ec;
        co_await ws.async_read(buffer, net::redirect_error(net::use_awaitable, ec));
    }
}

// Like run_fake_ws_server above, but sends two messages in sequence
// rather than one - what SnapshotPushedAsFirstMessage... below needs to
// prove both halves of the WS-push-snapshot path: the pushed snapshot
// itself applying correctly as literally the first message this symbol
// ever sees, and a live diff right after it applying correctly on top -
// not just the first half.
net::awaitable<void> run_fake_ws_server_two_messages(net::ip::tcp::acceptor acceptor,
                                                       std::string first_message,
                                                       std::string second_message) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);

    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);  // the SUBSCRIBE message; content unchecked

    co_await ws.async_write(net::buffer(first_message), net::use_awaitable);
    co_await ws.async_write(net::buffer(second_message), net::use_awaitable);

    // Same reasoning as run_fake_ws_server's own trailing idle read:
    // keeps the connection open so a disconnect-triggered invalidate/
    // reset isn't what this particular test is exercising.
    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
}

// Like run_fake_ws_server_two_messages, but blocks (polling `proceed`)
// between the two writes instead of sending them back-to-back - so a test
// can pin down exactly which SymbolSync state `second_message` arrives in
// (e.g. "only after the first message's own resync has already gone
// Live"), rather than racing it against however long an intervening REST
// round trip happens to take. Without this, a fast-enough loopback round
// trip could let `second_message` land while still Buffering, buffered
// alongside the first rather than exercising the Live-state code path the
// test is named for - the SyncAction/L2Update sequence can end up
// identical either way, so a flaky-looking pass wouldn't even reveal the
// problem (a /code-review finding on this file's own tests).
net::awaitable<void> run_fake_ws_server_two_messages_gated(net::ip::tcp::acceptor acceptor,
                                                              std::string first_message,
                                                              std::string second_message,
                                                              std::atomic<bool>* proceed) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);

    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);  // the SUBSCRIBE message; content unchecked

    co_await ws.async_write(net::buffer(first_message), net::use_awaitable);

    auto executor = co_await net::this_coro::executor;
    while (!proceed->load()) {
        net::steady_timer poll(executor, std::chrono::milliseconds(5));
        co_await poll.async_wait(net::use_awaitable);
    }

    co_await ws.async_write(net::buffer(second_message), net::use_awaitable);

    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
}

// First connection: sends `first_snapshot`, then drops (the socket/ws go
// out of scope at the end of the inner block, closing the connection
// right after that write - before any diff could arrive). Second
// connection (after VenueSession's own backoff+reconnect): sends
// `second_snapshot`, then holds the connection open. Proves VenueSession
// redoes the whole connect->subscribe->on_connected->snapshot-pushed-
// first flow correctly *again* after a reconnect, not just once.
// run_flaky_ws_server (below) already drops connections mid-stream - see
// StopAbortsBackoffWaitAndDoesNotReconnect, which relies on exactly that -
// but no existing test checks *book state* after a reconnect, and none
// does so for a kSnapshotViaRest=false feed at all.
net::awaitable<void> run_fake_ws_server_drop_then_reconnect(net::ip::tcp::acceptor acceptor,
                                                              std::string first_snapshot,
                                                              std::string second_snapshot) {
    {
        auto socket = co_await acceptor.async_accept(net::use_awaitable);
        websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
        co_await ws.async_accept(net::use_awaitable);
        beast::flat_buffer buffer;
        co_await ws.async_read(buffer, net::use_awaitable);
        co_await ws.async_write(net::buffer(first_snapshot), net::use_awaitable);
    }  // ws/socket destroyed here - the connection drops.

    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);
    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);
    co_await ws.async_write(net::buffer(second_snapshot), net::use_awaitable);

    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
}

// First connection: pushes a snapshot, then a live depth update carrying a
// sequencing gap, then holds the connection open (idle read) - deliberately
// never closing it itself. If VenueSession doesn't force a reconnect after
// the gap (the bug LiveGapForcesFullReconnectForTrustConnectionOrderVenue
// below guards against), this idle read simply never completes and no
// second connection ever arrives - the acceptor below blocks until the
// test's io.run_for() deadline, and the test fails on a missing update
// rather than hanging forever. Second connection: pushes a fresh,
// non-gapped snapshot, proving the symbol actually resyncs once the client
// itself tears down and reconnects.
net::awaitable<void> run_fake_ws_server_live_gap_then_reconnect(net::ip::tcp::acceptor acceptor,
                                                                  std::string snapshot,
                                                                  std::string gapped_depth,
                                                                  std::string second_snapshot) {
    {
        auto socket = co_await acceptor.async_accept(net::use_awaitable);
        websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
        co_await ws.async_accept(net::use_awaitable);
        beast::flat_buffer buffer;
        co_await ws.async_read(buffer, net::use_awaitable);
        co_await ws.async_write(net::buffer(snapshot), net::use_awaitable);
        co_await ws.async_write(net::buffer(gapped_depth), net::use_awaitable);

        beast::flat_buffer idle_buffer;
        boost::system::error_code ec;
        co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
    }  // Only reached once the client closes this connection.

    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);
    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);
    co_await ws.async_write(net::buffer(second_snapshot), net::use_awaitable);

    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
}

// First connection: pushes a snapshot, then - unlike every other "holds the
// connection open" fake server in this file - issues no further read at
// all, not even an idle one. That distinction matters: any pending read on
// a websocket::stream auto-answers an incoming ping as a side effect of
// Beast's own read machinery (impl/read.hpp), regardless of whether the
// "application" ever asked it to - so an idle *read* would still make this
// a responsive peer, not the true black hole this test needs to exercise
// WebSocketConnection::connect()'s real idle_timeout (Boost.Beast's own
// websocket::stream_base::timeout + keep_alive_pings). With no read
// pending, nothing this socket receives - including the client's own idle
// ping - is ever processed or answered, so the client's timeout is the only
// thing that can end this connection. `hold` just bounds how long this
// coroutine waits before moving on to accept the second connection - it
// doesn't detect the client's timeout firing (this side has no way to,
// short of reading), it just has to outlast it. Second connection: pushes a
// fresh snapshot, proving the symbol actually resyncs once the client
// reconnects.
net::awaitable<void> run_fake_ws_server_black_hole_then_reconnect(net::ip::tcp::acceptor acceptor,
                                                                     std::string snapshot,
                                                                     std::string second_snapshot,
                                                                     std::chrono::seconds hold) {
    {
        auto socket = co_await acceptor.async_accept(net::use_awaitable);
        websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
        co_await ws.async_accept(net::use_awaitable);
        beast::flat_buffer buffer;
        co_await ws.async_read(buffer, net::use_awaitable);
        co_await ws.async_write(net::buffer(snapshot), net::use_awaitable);

        auto executor = co_await net::this_coro::executor;
        net::steady_timer hold_timer(executor, hold);
        co_await hold_timer.async_wait(net::use_awaitable);
    }  // ws/socket destroyed here - closes this end too, harmless either way.

    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);
    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);
    co_await ws.async_write(net::buffer(second_snapshot), net::use_awaitable);

    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
}

// Like run_fake_ws_server_live_gap_then_reconnect above, but gaps on every
// connection except the last (`gap_count` times in a row), recording the
// wall-clock moment each connection is accepted into `connect_times` - what
// BackoffEscalatesAcrossRepeatedForcedReconnects below needs to prove
// run()'s backoff counter actually escalates across successive
// forced-reconnect cycles rather than resetting to a fast retry every
// single time (a code-review finding on the forced-reconnect fix this
// file's other Live/BufferingState/RestSnapshot gap tests exercise -
// execute_actions_and_maybe_force_reconnect()'s kTrustsConnectionOrder ==
// true branch, in venue_session.hpp).
net::awaitable<void> run_fake_ws_server_repeated_live_gaps(
    net::ip::tcp::acceptor acceptor, int gap_count, std::string snapshot, std::string gapped_depth,
    std::vector<std::chrono::steady_clock::time_point>* connect_times) {
    for (int i = 0; i < gap_count; ++i) {
        auto socket = co_await acceptor.async_accept(net::use_awaitable);
        connect_times->push_back(std::chrono::steady_clock::now());
        websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
        co_await ws.async_accept(net::use_awaitable);
        beast::flat_buffer buffer;
        co_await ws.async_read(buffer, net::use_awaitable);
        co_await ws.async_write(net::buffer(snapshot), net::use_awaitable);
        co_await ws.async_write(net::buffer(gapped_depth), net::use_awaitable);

        // The forced reconnect closes this connection by destroying
        // VenueSession's own `connection` local (stack unwinding out of
        // run()'s try block via the thrown exception -
        // execute_actions_and_maybe_force_reconnect()'s own comment), not
        // by this server ending it - so this just waits for that instead.
        beast::flat_buffer idle_buffer;
        boost::system::error_code ec;
        co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
    }

    // Final connection: no gap this time, so the session goes (and stays)
    // Live - lets the test end cleanly with io.run_for()'s deadline rather
    // than needing a further reconnect.
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    connect_times->push_back(std::chrono::steady_clock::now());
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);
    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);
    co_await ws.async_write(net::buffer(snapshot), net::use_awaitable);

    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
}

// For the RequestSnapshot/handle_request_snapshot() call site specifically
// (FakeFeed's REST path), not the WS-push one above: sends two buffered
// DEPTH events - the first bridges the snapshot the HTTP server below will
// answer with, the second is a gap relative to it - then holds the
// connection open, never closing it itself. `connect_count` is the actual
// proof this test needs: without VenueSession forcing a reconnect after
// on_snapshot()'s own bridge-tail gap check (reached only via
// handle_request_snapshot(), which runs net::co_spawn'ed rather than
// co_await'ed by run() - see execute_actions_and_maybe_force_reconnect()'s
// own comment on why that call site can't just reuse it), nothing about the
// L2Update sequence a subscriber sees would look any different - the
// forced-reconnect exception this bug swallows never touches the book, only
// whether a second connection happens at all.
net::awaitable<void> run_fake_ws_server_rest_snapshot_gap_then_reconnect(net::ip::tcp::acceptor acceptor,
                                                                           std::string bridge_depth,
                                                                           std::string gapped_depth,
                                                                           std::atomic<int>* connect_count) {
    {
        auto socket = co_await acceptor.async_accept(net::use_awaitable);
        connect_count->fetch_add(1);
        websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
        co_await ws.async_accept(net::use_awaitable);
        beast::flat_buffer buffer;
        co_await ws.async_read(buffer, net::use_awaitable);
        co_await ws.async_write(net::buffer(bridge_depth), net::use_awaitable);
        co_await ws.async_write(net::buffer(gapped_depth), net::use_awaitable);

        beast::flat_buffer idle_buffer;
        boost::system::error_code ec;
        co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
    }  // Only reached once the client closes this connection.

    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    connect_count->fetch_add(1);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);
    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);  // the SUBSCRIBE message

    beast::flat_buffer idle_buffer;
    boost::system::error_code ec;
    co_await ws.async_read(idle_buffer, net::redirect_error(net::use_awaitable, ec));
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

TEST(VenueSessionTest, SnapshotFetchOverlapsReadingSoBufferedLiveEventBridgesIt) {
    // Real AggregatorService/SymbolBook: VenueSession's whole job is to
    // drive these correctly, so verifying the actual result means reading
    // it back the same way a real subscriber would - over a real gRPC
    // connection - not inspecting private state.
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    // Guarantees reader_thread is cancelled+joined even if an ASSERT_*/
    // EXPECT_* below fails or wait_for()'s .at() throws on a timeout -
    // std::thread's destructor calls std::terminate() if it's still
    // joinable, which would otherwise mask the real assertion failure
    // behind a crash instead of a normal test-failure report. Destructed
    // before `reader_thread` itself (reverse declaration order), and
    // idempotent with the explicit cleanup at the end of this test (a
    // second join attempt is skipped once joinable() is already false).
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // Fake WS server: sends one buffered-before-the-snapshot-resolves depth
    // event, bid price 100.0 size 7.0, first_id=100 final_id=110.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io, run_fake_ws_server(std::move(ws_acceptor), "DEPTH:BTCUSDT:100:110:0:100.0:7.0"),
                  fail_test_on_exception("ws server"));

    // Fake HTTP server: snapshot last_update_id=105 (falls inside the
    // buffered event's [100,110] range), bid price 100.0 size 5.0 -
    // expected to be overwritten by the buffered delta above once bridged.
    net::ip::tcp::acceptor http_acceptor(io.get_executor(),
                                         net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short http_port = http_acceptor.local_endpoint().port();
    net::co_spawn(io, run_fake_http_server(std::move(http_acceptor), "105:100.0:5.0"),
                  fail_test_on_exception("http server"));

    FakeFeed feed(std::to_string(ws_port), std::to_string(http_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    io.run_for(std::chrono::seconds(2));

    // seq 1: ApplySnapshot (bid 100.0 -> 5.0); seq 2: ApplyDelta from the
    // bridged buffered event (bid 100.0 -> 7.0, the final expected state).
    L2Update after_snapshot = wait_for(1);
    ASSERT_TRUE(after_snapshot.has_diff());
    ASSERT_EQ(after_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(after_snapshot.diff().bids(0).size_raw(), Size(5.0).raw());

    L2Update after_delta = wait_for(2);
    ASSERT_TRUE(after_delta.has_diff());
    ASSERT_EQ(after_delta.diff().bids_size(), 1);
    EXPECT_EQ(after_delta.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(after_delta.diff().bids(0).size_raw(), Size(7.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// docs/ingestion_design.md 第 10 節第 2 項's stop() design: cancellation is
// not retroactive, so stop() must be checked (stopping_) right after the
// disconnect/backoff handling, not just emitted and assumed to take effect
// immediately. This test is exactly the failure mode that omission would
// produce: without it, a stop() that lands mid-backoff gets silently
// ignored and the session reconnects one more time after being told to
// stop.
TEST(VenueSessionTest, StopAbortsBackoffWaitAndDoesNotReconnect) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    std::atomic<int> connect_count{0};
    net::co_spawn(io, run_flaky_ws_server(std::move(ws_acceptor), &connect_count),
                  fail_test_on_exception("flaky ws server"));

    // No HTTP server: the RequestSnapshot fetch this triggers on connect is
    // left to fail with connection-refused, which is harmless noise here -
    // this test is about the backoff wait, not the snapshot path (see
    // StopDrainsInFlightSnapshotFetch below for that one).
    FakeFeed feed(std::to_string(ws_port), "1");
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    session.start([&](std::exception_ptr e) {
        if (e) {
            try {
                std::rethrow_exception(e);
            } catch (const std::exception& ex) {
                ADD_FAILURE() << "session threw: " << ex.what();
            }
        }
        std::lock_guard lock(mutex);
        done = true;
        cv.notify_all();
    });

    std::thread io_thread([&io] { io.run(); });

    // Let the first connect/subscribe/disconnect cycle happen and the
    // reconnect backoff (>=1s for attempt 1, per VenueSession::backoff())
    // begin.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    ASSERT_EQ(connect_count.load(), 1) << "fake server should have seen exactly one connect by now";

    session.stop();

    {
        std::unique_lock lock(mutex);
        // Well under the >=1s backoff delay: if stop() only asked
        // cooperatively instead of actually cancelling the wait, this
        // would time out here instead of firing early.
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return done; }))
            << "stop() should abort the in-flight backoff wait, not wait it out";
    }
    EXPECT_EQ(connect_count.load(), 1) << "stop() should prevent the reconnect attempt after backoff";

    io.stop();
    io_thread.join();
}

// docs/ingestion_design.md 第 10 節第 2 項's 孤兒 snapshot 問題: a
// handle_request_snapshot() spawned before stop() is called must actually
// finish (cancelled, here) before run() returns, or whatever destroys this
// session next would race an in-flight coroutine still holding a `this`
// pointing at it. Proven indirectly: a cancelled fetch makes on_done fire
// almost immediately; one that was merely left to finish on its own would
// take the full artificial HTTP delay below.
TEST(VenueSessionTest, StopDrainsInFlightSnapshotFetch) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io, run_fake_ws_server(std::move(ws_acceptor), "DEPTH:BTCUSDT:100:110:0:100.0:7.0"),
                  fail_test_on_exception("ws server"));

    net::ip::tcp::acceptor http_acceptor(io.get_executor(),
                                         net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short http_port = http_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_http_server(std::move(http_acceptor), "105:100.0:5.0", std::chrono::seconds(2)),
                  fail_test_on_exception("http server"));

    FakeFeed feed(std::to_string(ws_port), std::to_string(http_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    std::exception_ptr captured;
    session.start([&](std::exception_ptr e) {
        std::lock_guard lock(mutex);
        captured = e;
        done = true;
        cv.notify_all();
    });

    std::thread io_thread([&io] { io.run(); });

    // Give on_connected()'s RequestSnapshot a moment to actually spawn the
    // fetch before stopping - this is what makes it in flight when stop()
    // is called, not merely queued. run_fake_ws_server also holds the
    // connection open past the depth message it sends, so this session is
    // genuinely idle-blocked in connection.read() at this point too - not
    // just carrying an in-flight snapshot fetch.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    session.stop();

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return done; }))
            << "stop() should cancel the in-flight snapshot fetch instead of waiting out the full "
               "artificial HTTP delay";
    }
    // A non-null exception_ptr here means run() escaped via an uncaught
    // exception (see docs/ingestion_design.md 第 10 節第 2 項) instead of
    // actually invalidating and draining - this check is what would have
    // caught that the first time around; discarding `e` (as this test
    // originally did) let it ship silently.
    EXPECT_FALSE(captured) << "on_done should fire cleanly (nullptr), not via an uncaught exception";

    io.stop();
    io_thread.join();
}

// docs/ingestion_design.md 第 10 節第 2 項's code review addendum: stop()
// landing while the connection is alive and idle-blocked in
// connection.read() - not just mid-backoff, which is the *common* shutdown
// case (e.g. runner.stop_all() while genuinely connected to a live
// exchange) - is a distinct scenario from StopAbortsBackoffWaitAndDoesNot-
// Reconnect above. It used to escape run() via an uncaught exception:
// stop() cancels the read, disconnected=true, and the very next co_await
// (on_disconnected()'s invalidate) inherited the same latched cancellation
// state read() itself had just been aborted by - throwing immediately and
// skipping stopping_'s check entirely. Isolated here (no snapshot fetch in
// flight) so a regression points straight at this code path.
TEST(VenueSessionTest, StopWhileConnectedAndReadingReturnsCleanly) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    // Never sends anything after the handshake, so the session is
    // genuinely idle-blocked in connection.read() - not disconnected by
    // the far end - when stop() is called below.
    net::co_spawn(io, run_fake_ws_server(std::move(ws_acceptor), ""), fail_test_on_exception("ws server"));

    // No HTTP server: same reasoning as StopAbortsBackoffWaitAndDoesNotReconnect
    // above - this test is about the read/invalidate path, not snapshots.
    FakeFeed feed(std::to_string(ws_port), "1");
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    std::exception_ptr captured;
    session.start([&](std::exception_ptr e) {
        std::lock_guard lock(mutex);
        captured = e;
        done = true;
        cv.notify_all();
    });

    std::thread io_thread([&io] { io.run(); });

    // Give connect/subscribe/on_connected() time to finish and settle into
    // the idle read - this is what makes stop() land while genuinely
    // blocked in connection.read(), not mid-connect.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    session.stop();

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return done; }))
            << "stop() should abort the in-flight read and return, not hang";
    }
    EXPECT_FALSE(captured) << "on_done should fire cleanly (nullptr) - a non-null exception_ptr here "
                              "means run() escaped via an uncaught exception instead of invalidating "
                              "and draining normally";

    io.stop();
    io_thread.join();
}

// code review addendum: stop() before start() has ever run must not be a
// silent no-op. stop()'s net::post only needs strand_ (built at
// construction, before start() is ever called), so it succeeds and sets
// stopping_ - but run_sig_.emit(terminal) is a harmless no-op at that
// point (nothing bound to its slot yet). Without a stopping_ check at the
// very top of run()'s loop, run() would go ahead and connect/read from a
// live exchange regardless, only noticing the pending stop at the next
// disconnect/backoff cycle - or never, on a healthy connection. This test
// proves the opposite: run() must never even attempt to connect.
TEST(VenueSessionTest, StopBeforeStartPreventsConnecting) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    std::atomic<int> connect_count{0};
    net::co_spawn(io, run_flaky_ws_server(std::move(ws_acceptor), &connect_count),
                  fail_test_on_exception("flaky ws server"));

    FakeFeed feed(std::to_string(ws_port), "1");
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());

    session.stop();  // before start() - this is what's under test

    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;
    std::exception_ptr captured;
    session.start([&](std::exception_ptr e) {
        std::lock_guard lock(mutex);
        captured = e;
        done = true;
        cv.notify_all();
    });

    std::thread io_thread([&io] { io.run(); });

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::milliseconds(500), [&] { return done; }))
            << "a stop() queued before start() should still take effect immediately, not hang";
    }
    EXPECT_FALSE(captured) << "on_done should fire cleanly (nullptr)";
    EXPECT_EQ(connect_count.load(), 0)
        << "run() must never attempt to connect once a pre-queued stop() has been observed";

    io.stop();
    io_thread.join();
}

// Everything above drives FakeFeed (kSnapshotViaRest = true), Binance's
// REST-race shape: SymbolSync starts Buffering, RequestSnapshot fires a
// REST fetch, and on_snapshot() bridges whatever depth events arrived
// while that fetch was in flight. None of it exercises the other real
// shape - Bybit/OKX, kSnapshotViaRest = false - where the exchange pushes
// its own snapshot as the first message on the same ordered WS
// connection and RequestSnapshot is a no-op. on_snapshot()'s
// empty-buffer branch (see BybitSequencePolicy's own comment for the
// live bug this caused before it was fixed) is only reachable that way,
// and only a real VenueSession/AggregatorService/SymbolBook wiring - not
// SymbolSyncTest's unit tests - proves this project drives it correctly
// end to end.
TEST(VenueSessionTest, SnapshotPushedAsFirstMessageGoesLiveDirectlyForTrustConnectionOrderVenue) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(5), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // No HTTP server anywhere in this test - part of the proof that this
    // path never does a REST fetch. Snapshot (last_update_id=100, bid
    // 100.0 -> 5.0) arrives first; a contiguous depth update
    // (final_id=101, bid 100.0 -> 7.0) right behind it on the same
    // connection.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_ws_server_two_messages(std::move(ws_acceptor), "SNAPSHOT:BTCUSDT:100:100.0:5.0",
                                                   "DEPTH:BTCUSDT:101:101:0:100.0:7.0"),
                  fail_test_on_exception("ws server"));

    FakeFeedNoRest feed(std::to_string(ws_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeedNoRest, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    io.run_for(std::chrono::seconds(2));

    // seq 1: ApplySnapshot from the pushed snapshot (bid 100.0 -> 5.0);
    // seq 2: ApplyDelta from the contiguous depth update right after it
    // (bid 100.0 -> 7.0) - both applied straight, no RequestSnapshot/REST
    // round trip in between.
    L2Update after_snapshot = wait_for(1);
    ASSERT_TRUE(after_snapshot.has_diff());
    ASSERT_EQ(after_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(after_snapshot.diff().bids(0).size_raw(), Size(5.0).raw());

    L2Update after_delta = wait_for(2);
    ASSERT_TRUE(after_delta.has_diff());
    ASSERT_EQ(after_delta.diff().bids_size(), 1);
    EXPECT_EQ(after_delta.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(after_delta.diff().bids(0).size_raw(), Size(7.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// Proves the snapshot-pushed-as-first-message flow above isn't a
// one-shot fluke of connect()/subscribe()/on_connected() happening to
// run once correctly - it must also be redone correctly after a
// reconnect, the same on_disconnected() + backoff + reconnect contract
// every other venue shape already has covered.
TEST(VenueSessionTest, ReconnectRepeatsSnapshotPushedAsFirstMessageFlow) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // First connection pushes a snapshot (last_update_id=100, bid
    // 100.0 -> 5.0) then drops. Second connection (after VenueSession's
    // own backoff+reconnect) pushes a different snapshot
    // (last_update_id=200, bid 100.0 -> 9.0).
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(
        io,
        run_fake_ws_server_drop_then_reconnect(std::move(ws_acceptor), "SNAPSHOT:BTCUSDT:100:100.0:5.0",
                                                "SNAPSHOT:BTCUSDT:200:100.0:9.0"),
        fail_test_on_exception("ws server"));

    FakeFeedNoRest feed(std::to_string(ws_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeedNoRest, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    // >=1s reconnect backoff (see StopAbortsBackoffWaitAndDoesNotReconnect's
    // own comment on that minimum) plus both connections' round trips.
    io.run_for(std::chrono::seconds(4));

    // seq 1: ApplySnapshot from the first pushed snapshot (bid 5.0).
    L2Update first_snapshot = wait_for(1);
    ASSERT_TRUE(first_snapshot.has_diff());
    ASSERT_EQ(first_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(first_snapshot.diff().bids(0).size_raw(), Size(5.0).raw());

    // seq 2: InvalidateVenue from the drop - this venue's only
    // contribution (bid @100.0) is reported removed (size_raw 0).
    L2Update invalidated = wait_for(2);
    ASSERT_TRUE(invalidated.has_diff());
    ASSERT_EQ(invalidated.diff().bids_size(), 1);
    EXPECT_EQ(invalidated.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(invalidated.diff().bids(0).size_raw(), 0);

    // seq 3: ApplySnapshot from the reconnect's pushed snapshot (bid
    // 9.0) - proves connect->subscribe->on_connected->snapshot-pushed-
    // first runs correctly a second time, not just once.
    L2Update second_snapshot = wait_for(3);
    ASSERT_TRUE(second_snapshot.has_diff());
    ASSERT_EQ(second_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(second_snapshot.diff().bids(0).size_raw(), Size(9.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// websocket_connection.hpp's connect(): a venue whose TCP connection stays
// up but silently stops responding to anything at all (no close, no error,
// not even a pong to our own idle ping - unlike every other reconnect
// scenario this file covers, which all eventually produce either a real
// close or a SymbolSync-detected gap) used to be undetectable - the read()
// loop in run() would simply block forever. This proves Boost.Beast's own
// websocket::stream_base::timeout/keep_alive_pings (idle_timeout passed to
// VenueSession's constructor here, threaded through to
// WebSocketConnection::connect()) actually fires against a true black hole,
// and that firing forces the same disconnect/backoff/reconnect path a real
// drop takes - not a new, separate mechanism.
// run_fake_ws_server_black_hole_then_reconnect's first connection
// deliberately issues no read at all after its own snapshot (see its own
// comment for why even an *idle* read would be a responsive peer, not a
// black hole, from Beast's perspective) - a second connection only ever
// arrives if the client's own idle timeout tore the first one down.
TEST(VenueSessionTest, IdleTimeoutForcesReconnectOnSilentConnection) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_ws_server_black_hole_then_reconnect(std::move(ws_acceptor),
                                                                "SNAPSHOT:BTCUSDT:100:100.0:5.0",
                                                                "SNAPSHOT:BTCUSDT:200:100.0:9.0",
                                                                /*hold=*/std::chrono::seconds(6)),
                  fail_test_on_exception("ws server"));

    FakeFeedNoRest feed(std::to_string(ws_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    // A short idle_timeout (well below idle_timeout.hpp's kDefaultIdleTimeout)
    // so this test doesn't have to wait out a production-sized window -
    // the mechanism being proven doesn't depend on the specific value.
    VenueSession<FakeFeedNoRest, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor(),
        /*ssl_ctx=*/nullptr, /*idle_timeout=*/std::chrono::seconds(1));
    session.start(fail_test_on_exception("session"));

    // Beast's idle_timeout is the total detection window (an idle ping at
    // idle_timeout/2, then beast::error::timeout if nothing - not even a
    // pong - arrives within the next idle_timeout/2), so ~1s here, plus
    // >=1s reconnect backoff (see StopAbortsBackoffWaitAndDoesNotReconnect's
    // own comment) plus both connections' round trips - comfortably under
    // the server's own 6s `hold` above, but this budget still needs
    // headroom *after* that 6s for the second connection's handshake/
    // read/write to complete too, not just up to it; a /code-review pass
    // flagged the previous 8s value as leaving only ~2s for that, tight
    // enough to flake on a loaded CI runner even though the mechanism
    // under test would still be working correctly.
    io.run_for(std::chrono::seconds(12));

    // seq 1: ApplySnapshot from the first connection's snapshot (bid 5.0).
    L2Update first_snapshot = wait_for(1);
    ASSERT_TRUE(first_snapshot.has_diff());
    ASSERT_EQ(first_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(first_snapshot.diff().bids(0).size_raw(), Size(5.0).raw());

    // seq 2: InvalidateVenue from the idle-timeout-forced disconnect - this
    // venue's only contribution (bid @100.0) is reported removed.
    L2Update invalidated = wait_for(2);
    ASSERT_TRUE(invalidated.has_diff());
    ASSERT_EQ(invalidated.diff().bids_size(), 1);
    EXPECT_EQ(invalidated.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(invalidated.diff().bids(0).size_raw(), 0);

    // seq 3: ApplySnapshot from the reconnect's snapshot (bid 9.0) - proves
    // a second connection actually arrived, i.e. the idle timeout really
    // did force VenueSession to tear down and reconnect, not just stall.
    L2Update second_snapshot = wait_for(3);
    ASSERT_TRUE(second_snapshot.has_diff());
    ASSERT_EQ(second_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(second_snapshot.diff().bids(0).size_raw(), Size(9.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// docs/ingestion_design.md 第 10 節第 8 項: a Live-state gap for a
// kTrustsConnectionOrder venue (Bybit/OKX) used to leave the symbol stuck
// in Buffering forever, because RequestSnapshot is a no-op for a Feed with
// kSnapshotViaRest == false and nothing else ever re-requested one short of
// an actual reconnect. This proves VenueSession itself now forces that
// reconnect: the fake server below never closes the first connection on its
// own (see its own comment) - a second connection only ever arrives if
// VenueSession's execute_actions_and_maybe_force_reconnect() tears the first
// one down.
TEST(VenueSessionTest, LiveGapForcesFullReconnectForTrustConnectionOrderVenue) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // First connection: pushed snapshot (last_update_id=100, bid 100.0 ->
    // 5.0), then a depth update whose final_id (105) isn't last_final_id+1
    // (101) - a real gap under BybitSequencePolicy's is_contiguous(). Second
    // connection (only reachable if VenueSession itself reconnects): a
    // fresh snapshot (last_update_id=200, bid 100.0 -> 9.0).
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_ws_server_live_gap_then_reconnect(
                      std::move(ws_acceptor), "SNAPSHOT:BTCUSDT:100:100.0:5.0",
                      "DEPTH:BTCUSDT:105:105:0:100.0:1.0", "SNAPSHOT:BTCUSDT:200:100.0:9.0"),
                  fail_test_on_exception("ws server"));

    FakeFeedNoRest feed(std::to_string(ws_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeedNoRest, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    // >=1s reconnect backoff (see StopAbortsBackoffWaitAndDoesNotReconnect's
    // own comment on that minimum) plus both connections' round trips.
    io.run_for(std::chrono::seconds(4));

    // seq 1: ApplySnapshot from the first pushed snapshot (bid 5.0).
    L2Update first_snapshot = wait_for(1);
    ASSERT_TRUE(first_snapshot.has_diff());
    ASSERT_EQ(first_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(first_snapshot.diff().bids(0).size_raw(), Size(5.0).raw());

    // seq 2: InvalidateVenue from the live gap - this venue's only
    // contribution (bid @100.0) is reported removed (size_raw 0).
    L2Update invalidated = wait_for(2);
    ASSERT_TRUE(invalidated.has_diff());
    ASSERT_EQ(invalidated.diff().bids_size(), 1);
    EXPECT_EQ(invalidated.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(invalidated.diff().bids(0).size_raw(), 0);

    // seq 3: ApplySnapshot from the forced reconnect's fresh snapshot (bid
    // 9.0) - proves the symbol actually left Buffering again, rather than
    // sitting there forever waiting for a RequestSnapshot that would have
    // been a no-op on the still-open first connection.
    L2Update second_snapshot = wait_for(3);
    ASSERT_TRUE(second_snapshot.has_diff());
    ASSERT_EQ(second_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(second_snapshot.diff().bids(0).size_raw(), Size(9.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// A second /code-review pass on the two fixes above found that the forced
// reconnect only actually worked for 3 of its 4 call sites -
// handle_request_snapshot() itself (RequestSnapshot's own fulfillment,
// reached only for a Feed with kSnapshotViaRest == true) threw the same
// exception the other 3 do, but that coroutine runs net::co_spawn'ed
// detached rather than co_await'ed by run() - the exception only ever
// reached its own completion handler (which logs and discards it), never
// run()'s try/catch. This manufactures the one Feed/Policy combination
// that exercises that call site - no shipped venue pairs kSnapshotViaRest
// with kTrustsConnectionOrder today, but VenueSession's own driver logic
// has to behave correctly if one ever does, and this proves it now does.
TEST(VenueSessionTest, RestSnapshotBridgeTailGapForcesFullReconnectEvenThoughItsCoroutineIsDetached) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    // Two DEPTH events buffered while the REST snapshot fetch below is in
    // flight: the first (final_id=101) bridges the snapshot's
    // last_update_id=100 (101 == 100+1, BybitSequencePolicy's exact-+1
    // bridge condition); the second (final_id=105) is a gap relative to it
    // (105 != 101+1) - discovered only once on_snapshot() replays the
    // buffered tail past the bridge event, exactly like
    // BridgingSnapshotDetectsAGapBetweenTwoBufferedTailEvents in
    // tests/book/symbol_sync_test.cpp, but reached here through
    // VenueSession's real REST-fetch path instead of calling SymbolSync
    // directly.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    std::atomic<int> connect_count{0};
    net::co_spawn(io,
                  run_fake_ws_server_rest_snapshot_gap_then_reconnect(
                      std::move(ws_acceptor), "DEPTH:BTCUSDT:101:101:0:100.0:1.0",
                      "DEPTH:BTCUSDT:105:105:0:100.0:2.0", &connect_count),
                  fail_test_on_exception("ws server"));

    net::ip::tcp::acceptor http_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short http_port = http_acceptor.local_endpoint().port();
    net::co_spawn(io, run_fake_http_server(std::move(http_acceptor), "100:100.0:5.0"),
                  fail_test_on_exception("http server"));

    // FakeFeed (kSnapshotViaRest == true) paired with BybitSequencePolicy
    // (kTrustsConnectionOrder == true) - the manufactured combination this
    // test exists to exercise; see this test's own comment.
    FakeFeed feed(std::to_string(ws_port), std::to_string(http_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    // >=1s reconnect backoff plus both connections' round trips (same
    // budget as LiveGapForcesFullReconnectForTrustConnectionOrderVenue
    // above).
    io.run_for(std::chrono::seconds(4));

    // The actual proof: without the fix, the forced-reconnect exception
    // handle_request_snapshot() throws is silently swallowed by its own
    // completion handler - the book-level effects (ApplySnapshot/ApplyDelta/
    // InvalidateVenue all still execute, since they run *before* that
    // exception is thrown) would look identical either way, so only the
    // WS layer actually reconnecting - a second real TCP connection -
    // distinguishes "fixed" from "silently broken."
    EXPECT_EQ(connect_count.load(), 2);

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// A third /code-review pass found on_snapshot()'s *other* no-bridge branch
// (buffer non-empty at entry, but nothing in it bridges the snapshot - the
// "some event arrived before the snapshot despite this venue's ordering
// guarantee" case) still only ever returned RequestSnapshot, never
// InvalidateVenue - for a kTrustsConnectionOrder venue that's the exact
// same dead end the other two fixes in this commit exist to close
// (RequestSnapshot is a no-op for these venues), just reached through
// on_snapshot()'s no-bridge path instead of on_depth_update()'s live-gap
// path or on_snapshot()'s bridge-tail path. Fixed in symbol_sync.hpp by
// routing this branch through handle_gap() too. This test proves the
// existing VenueSession-level plumbing (dispatch_snapshot()'s
// execute_actions_and_maybe_force_reconnect(), unchanged by that fix) picks
// up the new InvalidateVenue and forces a reconnect through it, the same
// way it already does for the other two SymbolSync-level fixes - reusing
// run_fake_ws_server_live_gap_then_reconnect with its two messages
// reinterpreted: here the first message is a DEPTH event arriving *before*
// any snapshot (violating BybitSequencePolicy's trust assumption on
// purpose) and the second is a SNAPSHOT that doesn't bridge it, rather than
// a snapshot followed by a live-state gap.
TEST(VenueSessionTest, BufferingStateGapForcesFullReconnectForTrustConnectionOrderVenue) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // First connection: a DEPTH event (final_id=105) arrives before any
    // snapshot - buffered while still Buffering. Then a SNAPSHOT
    // (last_update_id=100) that this event doesn't bridge (bridges_snapshot
    // needs final_id == 101; should_drop_buffered needs final_id <= 100 -
    // neither holds for 105) - InvalidateVenue, per the fix under test.
    // Because this venue never contributed anything before this point,
    // that InvalidateVenue itself produces no observable book change
    // (AggregateOrderBook::invalidate_venue() is a no-op for a venue with
    // no existing entry) - the only way to observe whether the fix worked
    // is whether a real reconnect (and the second connection's own fresh
    // snapshot) ever happens at all.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_ws_server_live_gap_then_reconnect(
                      std::move(ws_acceptor), "DEPTH:BTCUSDT:105:105:0:100.0:1.0",
                      "SNAPSHOT:BTCUSDT:100:100.0:5.0", "SNAPSHOT:BTCUSDT:200:100.0:9.0"),
                  fail_test_on_exception("ws server"));

    FakeFeedNoRest feed(std::to_string(ws_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeedNoRest, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    // Same budget as the other forced-reconnect tests in this file.
    io.run_for(std::chrono::seconds(4));

    // Without the fix: on_snapshot() returns only RequestSnapshot (a no-op
    // for this venue), nothing ever gets applied or invalidated, no
    // reconnect happens, and this wait_for(1) times out - the book stays
    // permanently, silently empty. With the fix: exactly one more update
    // arrives, from the second connection's own fresh snapshot (bid 9.0).
    L2Update reconnected_snapshot = wait_for(1);
    ASSERT_TRUE(reconnected_snapshot.has_diff());
    ASSERT_EQ(reconnected_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(reconnected_snapshot.diff().bids(0).size_raw(), Size(9.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// A book-level rejection (apply_failed) partway through a multi-action
// batch used to abort the rest of that same batch for a
// kTrustsConnectionOrder venue - execute_action()'s apply_failed branch
// routed its own nested resync through the throwing
// execute_actions_and_maybe_force_reconnect(), and that throw unwound
// execute_actions()'s for-loop over the *outer* batch, skipping whatever
// was still queued after the rejected action even though SymbolSync had
// already computed it as valid, contiguous data. This constructs exactly
// that batch shape: a rejected ApplySnapshot (index 0) followed by a valid
// ApplyDelta (index 1) from a buffered event that bridges it.
TEST(VenueSessionTest, RejectedActionMidBatchStillLetsLaterActionsInTheSameBatchApply) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // DEPTH sent *before* any snapshot - deliberately violates
    // BybitSequencePolicy's usual "snapshot arrives first" assumption, so
    // it lands in buffer_ while still Buffering. When the snapshot below
    // then arrives, this buffered event bridges it (final_id=101 ==
    // last_update_id 100 + 1) - on_snapshot() takes the bridge-found path,
    // which always starts its returned batch with ApplySnapshot followed by
    // this event's own ApplyDelta, regardless of kTrustsConnectionOrder
    // (that flag's shortcut only ever fires from the *other*, no-bridge
    // branch - see symbol_sync.hpp). The snapshot's own bid size (-1.0) is
    // malformed - AggregateOrderBook::apply_snapshot() validates every
    // level before applying any, so the whole ApplySnapshot action is
    // rejected outright, with nothing broadcast for it.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_ws_server_two_messages(std::move(ws_acceptor), "DEPTH:BTCUSDT:101:101:0:100.0:2.0",
                                                   "SNAPSHOT:BTCUSDT:100:100.0:-1.0"),
                  fail_test_on_exception("ws server"));

    FakeFeedNoRest feed(std::to_string(ws_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeedNoRest, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    io.run_for(std::chrono::seconds(2));

    // Without the fix: the rejected ApplySnapshot (index 0 of this batch)
    // triggers a nested resync that throws immediately, aborting the batch
    // before the buffered event's own ApplyDelta (index 1) ever runs - the
    // book would never see this bid at all, and wait_for(1) below would
    // time out. With the fix: the batch keeps going after the rejection,
    // so this is the only update ever broadcast for this venue.
    L2Update after_delta = wait_for(1);
    ASSERT_TRUE(after_delta.has_diff());
    ASSERT_EQ(after_delta.diff().bids_size(), 1);
    EXPECT_EQ(after_delta.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(after_delta.diff().bids(0).size_raw(), Size(2.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// docs/ingestion_design.md's own comment on handle_request_snapshot() used
// to claim a failed REST fetch would recover via "next reconnect or
// steady-state gap retries" - false for a kTrustsConnectionOrder == false
// venue (Binance): the symbol never reaches Live if its very first fetch
// fails (on_depth_update() while Buffering just buffers, with no gap check
// at all - that only exists in the Live branch), so neither a gap-driven
// retry nor an unrelated reconnect would ever re-trigger a request. Caught
// by another session working on a related connection-event/book-validity
// design question, not by any test in this file. Fixed by retrying the
// fetch itself in place - same connection, no WS teardown at all - with the
// same backoff() helper run()'s own reconnect loop already uses.
TEST(VenueSessionTest, FailedSnapshotFetchRetriesInPlaceWithoutTouchingTheConnection) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // WS side: one connection, holding open past its one DEPTH message -
    // same shape as SnapshotFetchOverlapsReadingSoBufferedLiveEventBridgesIt
    // above, reusing its exact numbers (buffered event first_id=100
    // final_id=110, snapshot last_update_id=105) so the bridge condition is
    // already proven correct; this test is only about whether the snapshot
    // ever arrives at all. REST side: first connection attempt is dropped
    // unanswered (a stand-in for any transient network failure), second
    // succeeds.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io, run_fake_ws_server(std::move(ws_acceptor), "DEPTH:BTCUSDT:100:110:0:100.0:7.0"),
                  fail_test_on_exception("ws server"));

    net::ip::tcp::acceptor http_acceptor(io.get_executor(),
                                         net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short http_port = http_acceptor.local_endpoint().port();
    net::co_spawn(io, run_fake_http_server_fails_once_then_succeeds(std::move(http_acceptor), "105:100.0:5.0"),
                  fail_test_on_exception("http server"));

    FakeFeed feed(std::to_string(ws_port), std::to_string(http_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    // >=500ms backoff(0) wait between the failed first attempt and the
    // retry, plus both HTTP round trips and the WS side.
    io.run_for(std::chrono::seconds(2));

    // Without the fix: the failed first attempt gives up outright (no
    // retry, no reconnect - the WS side never drops), so no snapshot ever
    // arrives to bridge the buffered DEPTH event, and wait_for(1) times
    // out. With the fix: the retry succeeds - seq 1 is ApplySnapshot (bid
    // 5.0), seq 2 is ApplyDelta from the bridged buffered event (bid
    // 5.0 -> 7.0).
    L2Update after_snapshot = wait_for(1);
    ASSERT_TRUE(after_snapshot.has_diff());
    ASSERT_EQ(after_snapshot.diff().bids_size(), 1);
    EXPECT_EQ(after_snapshot.diff().bids(0).size_raw(), Size(5.0).raw());

    L2Update after_delta = wait_for(2);
    ASSERT_TRUE(after_delta.has_diff());
    ASSERT_EQ(after_delta.diff().bids_size(), 1);
    EXPECT_EQ(after_delta.diff().bids(0).size_raw(), Size(7.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// code-review finding on the forced-reconnect fix: run()'s reconnect loop
// used to reset `attempt` back to 0 unconditionally the instant
// connect+subscribe succeeded - before the connection had actually proven
// itself healthy. Harmless for a genuine, infrequent disconnect, but for a
// kTrustsConnectionOrder venue whose SymbolSync keeps reporting
// InvalidateVenue right after each forced reconnect, this gave backoff()'s
// *fastest* retry (attempt=1, ~1-1.2s) every single cycle instead of the
// escalating series backoff() is meant to provide, risking a reconnect
// storm against the real exchange. No grpc server/reader needed here
// (unlike the tests above) - this is purely about run()'s own reconnect
// timing, not book content.
TEST(VenueSessionTest, BackoffEscalatesAcrossRepeatedForcedReconnects) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    std::vector<std::chrono::steady_clock::time_point> connect_times;
    net::co_spawn(io,
                  run_fake_ws_server_repeated_live_gaps(std::move(ws_acceptor), /*gap_count=*/2,
                                                          "SNAPSHOT:BTCUSDT:100:100.0:5.0",
                                                          "DEPTH:BTCUSDT:105:105:0:100.0:1.0", &connect_times),
                  fail_test_on_exception("ws server"));

    FakeFeedNoRest feed(std::to_string(ws_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeedNoRest, BybitSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    // Three connections total: the first is immediate (no backoff before
    // it), the second follows attempt=1's backoff (~1.0-1.2s), the third
    // follows attempt=2's (~2.0-2.4s) if - and only if - the counter
    // actually escalated instead of resetting. Comfortable margin above
    // both.
    io.run_for(std::chrono::seconds(8));

    ASSERT_EQ(connect_times.size(), 3u) << "expected exactly 3 connections (2 forced reconnects + 1 "
                                            "stable) within the test's time budget";

    auto interval_1_to_2 = connect_times[1] - connect_times[0];
    auto interval_2_to_3 = connect_times[2] - connect_times[1];

    // Ratio-based, not tight absolute millisecond windows pinned to
    // backoff()'s exact jittered ranges (a code-review finding: every other
    // test in this file uses only generous, one-sided timeouts - 500ms to
    // 10s - never a tight two-sided window like the ones this replaced).
    // Connect/handshake/read overhead is roughly constant per reconnect
    // cycle, so it shifts both intervals by about the same amount and
    // mostly cancels out in their ratio, while the ratio itself still
    // cleanly separates "escalated" (backoff()'s ~2x-per-attempt growth,
    // still comfortably >1.4x after jitter) from "reset back to the fast
    // retry every time" (~1x - the bug this fix addresses) under far more
    // scheduling noise than a fixed millisecond threshold would tolerate.
    EXPECT_GT(interval_2_to_3, interval_1_to_2 * 7 / 5)
        << "backoff between successive forced reconnects should escalate roughly geometrically, not "
           "stay flat";

    // Loose sanity bounds, not tight ones: catch a fully broken run (a
    // near-instant reconnect with no backoff wait at all, or a hang) without
    // being sensitive to ordinary scheduling jitter.
    EXPECT_GT(interval_1_to_2, std::chrono::milliseconds(200))
        << "first reconnect happened suspiciously fast for any backoff wait at all";
    EXPECT_LT(interval_2_to_3, std::chrono::seconds(6))
        << "second forced reconnect took far longer than any expected backoff attempt";
}

// Another session's line-by-line review of the merged fix above found one
// more instance of the same "stuck forever" bug class this whole file is
// about, this time for a kTrustsConnectionOrder == false venue (Binance):
// a Live-state gap reset the symbol to Buffering (handle_gap()) but never
// re-requested a snapshot - VenueSession never forces a reconnect for this
// venue shape (by design, tearing down the connection over one gap would
// be the wrong fix for a REST venue), so nothing else would ever
// re-trigger one either. This proves the fix
// (execute_actions_and_maybe_force_reconnect()'s kTrustsConnectionOrder ==
// false branch calls resync_rest_venue_after_gap() to request a fresh
// snapshot directly for this venue shape): the WS side sends a bridging
// DEPTH event, then a live-state gap, and never closes the connection
// itself - a second REST fetch (proving handle_request_snapshot()'s
// in-place retry path fired) is the
// only way this test's own wait_for() calls below don't time out, and
// nothing here ever exercises a second WS connection.
TEST(VenueSessionTest, LiveStateGapForARestVenueRetriesTheSnapshotFetchWithoutTouchingTheConnection) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    // Set once DEPTH1's own bridge (ApplyDelta, index 2 below) has been
    // observed - the signal run_fake_ws_server_two_messages_gated waits on
    // before sending DEPTH2, so DEPTH2 deterministically arrives only
    // after the symbol has actually gone Live (see that helper's own
    // comment on why racing it otherwise wouldn't reliably exercise
    // on_depth_update()'s Live-state gap branch specifically). Set from
    // this reader thread, not the main thread: io.run_for() below blocks
    // the main thread for its whole duration, but this thread keeps
    // receiving gRPC updates concurrently with it.
    std::atomic<bool> depth1_bridged{false};
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            if (updates.size() == 3) depth1_bridged.store(true);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // WS side: DEPTH1 (final_id=110) bridges the first snapshot below and
    // goes Live (last_final_id_ == 110); DEPTH2's prev_final_id (50)
    // doesn't match that, a genuine Live-state gap - held back until
    // depth1_bridged confirms the symbol is actually Live first, so this
    // deterministically exercises on_depth_update()'s Live-state gap
    // branch rather than possibly racing into on_snapshot()'s bridge-tail
    // one instead (already covered by
    // BridgingSnapshotDetectsAGapBetweenTwoBufferedTailEvents in
    // tests/book/symbol_sync_test.cpp - not what this test is for). The
    // connection is never closed by this server - see
    // run_fake_ws_server_two_messages_gated's own comment. REST side:
    // first response (last_update_id=105) bridges DEPTH1; second
    // (last_update_id=155, only requested because of the fix under test)
    // bridges DEPTH2.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_ws_server_two_messages_gated(std::move(ws_acceptor), "DEPTH:BTCUSDT:100:110:0:100.0:7.0",
                                                          "DEPTH:BTCUSDT:151:160:50:100.0:3.0", &depth1_bridged),
                  fail_test_on_exception("ws server"));

    net::ip::tcp::acceptor http_acceptor(io.get_executor(),
                                         net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short http_port = http_acceptor.local_endpoint().port();
    net::co_spawn(io, run_fake_http_server_twice(std::move(http_acceptor), "105:100.0:5.0", "155:100.0:9.0"),
                  fail_test_on_exception("http server"));

    FakeFeed feed(std::to_string(ws_port), std::to_string(http_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    io.run_for(std::chrono::seconds(2));

    // seq 1: ApplySnapshot (bid 5.0); seq 2: ApplyDelta from DEPTH1's
    // bridge (bid 5.0 -> 7.0) - now Live.
    L2Update after_snapshot = wait_for(1);
    ASSERT_TRUE(after_snapshot.has_diff());
    EXPECT_EQ(after_snapshot.diff().bids(0).size_raw(), Size(5.0).raw());
    L2Update after_delta = wait_for(2);
    ASSERT_TRUE(after_delta.has_diff());
    EXPECT_EQ(after_delta.diff().bids(0).size_raw(), Size(7.0).raw());

    // seq 3: InvalidateVenue from DEPTH2's gap - this venue's only
    // contribution (bid @100.0) reported removed (size_raw 0).
    L2Update invalidated = wait_for(3);
    ASSERT_TRUE(invalidated.has_diff());
    ASSERT_EQ(invalidated.diff().bids_size(), 1);
    EXPECT_EQ(invalidated.diff().bids(0).price_raw(), Price(100.0).raw());
    EXPECT_EQ(invalidated.diff().bids(0).size_raw(), 0);

    // seq 4: ApplySnapshot from the second REST fetch (bid 9.0) - without
    // the fix, nothing after seq 3 ever arrives and this times out. seq 5:
    // ApplyDelta from DEPTH2's own bridge (bid 9.0 -> 3.0).
    L2Update second_snapshot = wait_for(4);
    ASSERT_TRUE(second_snapshot.has_diff());
    EXPECT_EQ(second_snapshot.diff().bids(0).size_raw(), Size(9.0).raw());
    L2Update second_delta = wait_for(5);
    ASSERT_TRUE(second_delta.has_diff());
    EXPECT_EQ(second_delta.diff().bids(0).size_raw(), Size(3.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

// The same review pass that found the Live-state gap bug above found a
// fourth instance of the same "stuck forever" bug class: a book-level
// rejection (apply_failed) resyncs a symbol through
// SymbolSync::on_disconnected(), which deliberately never re-requests a
// snapshot itself (correct for a real drop, see its own comment) -
// what a kTrustsConnectionOrder == false venue does about *any*
// InvalidateVenue, including this one, is decided once, centrally, by
// VenueSession::resync_rest_venue_after_gap() at the outermost caller
// (see its own comment for why that decision isn't duplicated into
// SymbolSync). This proves it end to end: a malformed first REST
// snapshot (rejected by AggregateOrderBook, so no broadcast for it)
// still lets the batch's other, valid action (DEPTH1's own bridge)
// apply, and the resync it triggers succeeds via a second REST fetch
// with the WS connection never touched throughout.
TEST(VenueSessionTest, BookRejectionRetriesTheSnapshotFetchForARestVenueWithoutTouchingTheConnection) {
    std::vector<std::string> symbols{"BTCUSDT"};
    AggregatorService service(std::vector<BookId>{TestBookId()});

    grpc::ServerBuilder builder;
    int grpc_port = 0;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &grpc_port);
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    ASSERT_NE(server, nullptr);

    auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(grpc_port),
                                        grpc::InsecureChannelCredentials());
    auto stub = bobby::hermeneutic::aggregator::Aggregator::NewStub(channel);
    grpc::ClientContext context;
    bobby::hermeneutic::aggregator::SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), TestBookId());
    auto reader = stub->SubscribeL2Diff(&context, request);

    std::mutex mutex;
    std::condition_variable cv;
    std::deque<L2Update> updates;
    // Set once DEPTH1's own bridge (index 1 below - the only broadcast
    // from the first batch, since its ApplySnapshot was rejected) has been
    // observed - by then the nested on_disconnected() resync has already
    // run (it happens earlier in the same batch, before this ApplyDelta),
    // so it's safe to send DEPTH2 for the second snapshot to bridge. See
    // run_fake_ws_server_two_messages_gated's own comment.
    std::atomic<bool> first_batch_done{false};
    std::thread reader_thread([&] {
        L2Update update;
        while (reader->Read(&update)) {
            std::lock_guard lock(mutex);
            updates.push_back(update);
            if (updates.size() == 2) first_batch_done.store(true);
            cv.notify_all();
        }
    });
    struct ReaderThreadGuard {
        grpc::ClientContext& context;
        std::thread& thread;
        ~ReaderThreadGuard() {
            context.TryCancel();
            if (thread.joinable()) thread.join();
        }
    } reader_guard{context, reader_thread};

    auto wait_for = [&](std::size_t index) -> L2Update {
        std::unique_lock lock(mutex);
        cv.wait_for(lock, std::chrono::seconds(10), [&] { return updates.size() > index; });
        return updates.at(index);
    };
    wait_for(0);  // initial (empty) snapshot

    // DEPTH1 (final_id=110) bridges the first (malformed) snapshot below.
    // DEPTH2 (held back until first_batch_done) bridges the second,
    // well-formed one.
    net::io_context io;
    net::ip::tcp::acceptor ws_acceptor(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short ws_port = ws_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_ws_server_two_messages_gated(std::move(ws_acceptor), "DEPTH:BTCUSDT:100:110:0:100.0:7.0",
                                                          "DEPTH:BTCUSDT:151:160:0:100.0:3.0", &first_batch_done),
                  fail_test_on_exception("ws server"));

    // First response (last_update_id=105, bridging DEPTH1): bid size is
    // -1.0 - AggregateOrderBook::apply_snapshot() rejects the whole batch
    // for a malformed level, so this produces no broadcast at all. Second
    // (last_update_id=155, only requested because of the fix under test):
    // well-formed, bridges DEPTH2.
    net::ip::tcp::acceptor http_acceptor(io.get_executor(),
                                         net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
    unsigned short http_port = http_acceptor.local_endpoint().port();
    net::co_spawn(io,
                  run_fake_http_server_twice(std::move(http_acceptor), "105:100.0:-1.0", "155:100.0:9.0",
                                              std::chrono::milliseconds(300)),
                  fail_test_on_exception("http server"));

    FakeFeed feed(std::to_string(ws_port), std::to_string(http_port));
    SymbolRegistry<SymbolBook> registry;
    registry.add("BTCUSDT", service.book(TestBookId()));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream, SymbolBook> session(
        std::move(feed), kFakeVenue, symbols, std::move(registry), io.get_executor());
    session.start(fail_test_on_exception("session"));

    // Two full REST round trips (not just one), plus the gating poll -
    // more headroom than this file's other single-round-trip tests need.
    io.run_for(std::chrono::seconds(4));

    // seq 1: ApplyDelta from DEPTH1's own bridge (bid 7.0) - no preceding
    // ApplySnapshot broadcast, since that action was rejected; the batch's
    // remaining action still applies (see
    // RejectedActionMidBatchStillLetsLaterActionsInTheSameBatchApply).
    L2Update after_delta = wait_for(1);
    ASSERT_TRUE(after_delta.has_diff());
    ASSERT_EQ(after_delta.diff().bids_size(), 1);
    EXPECT_EQ(after_delta.diff().bids(0).size_raw(), Size(7.0).raw());

    // seq 2: ApplySnapshot from the second REST fetch (bid 9.0) - proves
    // resync_rest_venue_after_gap() actually gets reached from this
    // trigger point; without it, nothing would ever re-request a
    // snapshot and this wait_for(2) times out. seq 3: ApplyDelta from
    // DEPTH2's own bridge (bid 3.0).
    L2Update second_snapshot = wait_for(2);
    ASSERT_TRUE(second_snapshot.has_diff());
    EXPECT_EQ(second_snapshot.diff().bids(0).size_raw(), Size(9.0).raw());
    L2Update second_delta = wait_for(3);
    ASSERT_TRUE(second_delta.has_diff());
    EXPECT_EQ(second_delta.diff().bids(0).size_raw(), Size(3.0).raw());

    context.TryCancel();
    if (reader_thread.joinable()) reader_thread.join();
    server->Shutdown();
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

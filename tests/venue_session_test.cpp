#include "venue_session.hpp"

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>

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

#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"

namespace bobby::hermeneutic::ingestion {
namespace {

using bobby::hermeneutic::aggregator::AggregatorService;
using bobby::hermeneutic::aggregator::L2Update;

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

// Same shape as binance_futures_feed.hpp's HttpRequestSpec (host/port/
// target), redeclared here so this test doesn't need to include that
// header (and pull in simdjson, which FakeFeed's own trivial wire format
// has no use for).
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

    std::string subscribe_message(std::span<const SymbolId>) const { return "SUBSCRIBE"; }

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

    FakeHttpRequestSpec snapshot_request(const SymbolId& symbol) const {
        return FakeHttpRequestSpec{"127.0.0.1", http_port_, "/snapshot/" + symbol};
    }

    // "<last_update_id>:<bid_price>:<bid_size>"
    std::expected<SnapshotMessage, std::errc> parse_snapshot_response(SymbolId symbol,
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
// flight, not only after it returns.
net::awaitable<void> run_fake_http_server(net::ip::tcp::acceptor acceptor, std::string response_body) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);

    beast::flat_buffer buffer;
    http::request<http::empty_body> request;
    co_await http::async_read(socket, buffer, request, net::use_awaitable);

    auto executor = co_await net::this_coro::executor;
    net::steady_timer delay(executor, std::chrono::milliseconds(100));
    co_await delay.async_wait(net::use_awaitable);

    http::response<http::string_body> response{http::status::ok, request.version()};
    response.body() = std::move(response_body);
    response.prepare_payload();
    co_await http::async_write(socket, response, net::use_awaitable);
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
    AggregatorService service(symbols);

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
    bobby::hermeneutic::aggregator::SubscribeRequest request;
    request.set_symbol("BTCUSDT");
    auto reader = stub->Subscribe(&context, request);

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
    SymbolRegistry registry;
    registry.add("BTCUSDT", service.book("BTCUSDT"));
    VenueSession<FakeFeed, BinanceFuturesSequencePolicy, beast::tcp_stream> session(
        std::move(feed), "fake_venue", symbols, std::move(registry));
    net::co_spawn(io, session.run(), fail_test_on_exception("session"));

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

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

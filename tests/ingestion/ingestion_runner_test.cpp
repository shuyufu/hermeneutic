#include "bobby/hermeneutic/ingestion/ingestion_runner.hpp"

#include <gtest/gtest.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "bobby/hermeneutic/book/aggregate_order_book.hpp"
#include "bobby/hermeneutic/exchange/binance/binance_futures_sequence_policy.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace bobby::hermeneutic::ingestion {
namespace {

using bobby::hermeneutic::symbol::Exchange;
using bobby::hermeneutic::symbol::MarketType;

// Two arbitrary, distinct VenueIds - this test is about IngestionRunner
// driving differently-typed VenueSessions uniformly, not about venue/
// market identity, so any two distinct values would do.
constexpr bobby::hermeneutic::VenueId kVenueA{Exchange::Binance, MarketType::Spot};
constexpr bobby::hermeneutic::VenueId kVenueB{Exchange::Binance, MarketType::Perp};

// A minimal VenueFeed test double: no snapshot/HTTP path at all
// (kSnapshotViaRest=false), no messages ever parsed. This test is about
// IngestionRunner's own plumbing (does it drive N differently-typed
// VenueSessions uniformly, does stop_all() actually let their io_context
// finish on its own) - the resync algorithm and the snapshot-fetch drain
// are already covered by symbol_sync_test.cpp and
// VenueSessionTest.StopDrainsInFlightSnapshotFetch respectively.
//
// A template on an unused tag rather than one fixed class: instantiating
// it at two different Tags gives two genuinely distinct C++ types (so
// VenueSessionAdapter<MinimalFakeFeed<0>, ...> and
// VenueSessionAdapter<MinimalFakeFeed<1>, ...> really are two different
// concrete classes, both behind the same IVenueSession) without writing
// the same trivial double out twice.
template <int Tag>
class MinimalFakeFeed {
  public:
    static constexpr bool kSnapshotViaRest = false;

    explicit MinimalFakeFeed(std::string ws_port) : ws_port_(std::move(ws_port)) {}

    std::string_view ws_host() const { return "127.0.0.1"; }
    std::string_view ws_port() const { return ws_port_; }
    std::string_view ws_target() const { return "/"; }

    std::string subscribe_message(std::span<const NativeSymbol>) const { return "SUBSCRIBE"; }

    std::expected<std::optional<std::variant<SnapshotMessage, DepthUpdate>>, std::errc> parse_message(
        std::string_view) const {
        return std::optional<std::variant<SnapshotMessage, DepthUpdate>>{std::nullopt};
    }

  private:
    std::string ws_port_;
};

using FakeFeedA = MinimalFakeFeed<0>;
using FakeFeedB = MinimalFakeFeed<1>;

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

// Accepts exactly one connection, consumes the SUBSCRIBE message, then lets
// `ws`/`socket` go out of scope - closing the connection - and returns for
// good. Deliberately single-shot (not a loop): once this coroutine finishes,
// it leaves no pending work behind on `io`, so once both VenueSessions in
// the fixture below also finish, nothing keeps io.run() from returning on
// its own.
net::awaitable<void> run_single_shot_ws_server(net::ip::tcp::acceptor acceptor) {
    auto socket = co_await acceptor.async_accept(net::use_awaitable);
    websocket::stream<net::ip::tcp::socket> ws(std::move(socket));
    co_await ws.async_accept(net::use_awaitable);
    beast::flat_buffer buffer;
    co_await ws.async_read(buffer, net::use_awaitable);
}

// Shared scaffolding for both stop_all()-driven tests below: an
// io_context with two single-shot WS servers already spawned on it, and
// an IngestionRunner wired with two *differently-typed* VenueSessionAdapter
// instantiations (FakeFeedA vs FakeFeedB) against them - the whole point
// of IngestionRunner is managing a heterogeneous group uniformly, so a
// single-type test wouldn't exercise that. Construction only wires
// things up; the caller still calls runner.start_all() itself (with or
// without an on_all_drained callback - that's the one thing the two
// tests below differ on) and spawns its own io_thread.
struct TwoSessionSetup {
    net::io_context io;
    IngestionRunner runner;

    TwoSessionSetup() {
        net::ip::tcp::acceptor acceptor_a(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
        unsigned short port_a = acceptor_a.local_endpoint().port();
        net::co_spawn(io, run_single_shot_ws_server(std::move(acceptor_a)), fail_test_on_exception("venue_a server"));

        net::ip::tcp::acceptor acceptor_b(io.get_executor(), net::ip::tcp::endpoint(net::ip::tcp::v4(), 0));
        unsigned short port_b = acceptor_b.local_endpoint().port();
        net::co_spawn(io, run_single_shot_ws_server(std::move(acceptor_b)), fail_test_on_exception("venue_b server"));

        std::vector<std::string> symbols{"BTCUSDT"};
        runner.add<FakeFeedA, BinanceFuturesSequencePolicy, beast::tcp_stream, AggregateOrderBook>(
            FakeFeedA(std::to_string(port_a)), kVenueA, symbols, SymbolRegistry<AggregateOrderBook>{},
            io.get_executor());
        runner.add<FakeFeedB, BinanceFuturesSequencePolicy, beast::tcp_stream, AggregateOrderBook>(
            FakeFeedB(std::to_string(port_b)), kVenueB, symbols, SymbolRegistry<AggregateOrderBook>{},
            io.get_executor());
    }
};

// Calls runner.stop_all() and joins io_thread, guarded by a watchdog that
// force io.stop()s after 3s and sets `watchdog_fired` - a safety net for
// both tests below, not the thing either one is actually testing: in
// case a regression makes stop_all() not actually drain, this keeps that
// failure a fast, reported EXPECT_FALSE(watchdog_fired) instead of a
// hung test binary. Shared because the two callers differ only in what
// else they assert once this returns (a plain "did it drain" check vs.
// also checking on_all_drained's own call count).
void stop_all_and_join_with_watchdog(net::io_context& io, IngestionRunner& runner, std::thread& io_thread,
                                      std::atomic<bool>& watchdog_fired) {
    std::mutex watchdog_mutex;
    std::condition_variable watchdog_cv;
    bool test_finished = false;
    std::thread watchdog([&] {
        std::unique_lock lock(watchdog_mutex);
        if (!watchdog_cv.wait_for(lock, std::chrono::seconds(3), [&] { return test_finished; })) {
            watchdog_fired = true;
            io.stop();
        }
    });

    runner.stop_all();
    // Not itself the claim either test is actually making: for
    // StopAllLetsIoContextFinishWithoutIoStop below, this returning
    // without io.stop() *is* the claim under test; for
    // OnAllDrainedFiresExactlyOnceAfterBothSessionsFinish, the real
    // claim is on_all_drained's exactly-once contract (checked via
    // drained_count after this helper returns) - this join is just what
    // both callers need done first, for the same reason (stop_all()
    // driving both properties).
    io_thread.join();

    {
        std::lock_guard lock(watchdog_mutex);
        test_finished = true;
    }
    watchdog_cv.notify_all();
    watchdog.join();
}

// After stop_all(), io_thread.join() should return on its own - no
// io.stop() needed - because every session's own drain (cancel the
// in-flight read/backoff wait, wait out any in-flight snapshot fetch)
// has actually finished.
TEST(IngestionRunnerTest, StopAllLetsIoContextFinishWithoutIoStop) {
    TwoSessionSetup setup;
    setup.runner.start_all();

    std::thread io_thread([&setup] { setup.io.run(); });

    // Let both sessions connect, get closed on by the single-shot servers
    // above, invalidate, and settle into their reconnect backoff wait
    // (>=1s, per VenueSession::backoff()) before asking them to stop.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    std::atomic<bool> watchdog_fired{false};
    stop_all_and_join_with_watchdog(setup.io, setup.runner, io_thread, watchdog_fired);

    EXPECT_FALSE(watchdog_fired.load())
        << "io_thread.join() needed io.stop() as a backstop - stop_all() isn't draining on its own";
}

// start_all()'s on_all_drained callback exists so a caller (server_main.cpp)
// can cancel something else - there, a signal_set's re-armed wait - exactly
// once every session has actually finished, instead of on a timing guess.
// This test is that contract, directly: the callback must not fire while
// either session is still running (asserted immediately after start_all(),
// well before stop_all() below), and must fire exactly once - not once per
// session - only once both have drained.
TEST(IngestionRunnerTest, OnAllDrainedFiresExactlyOnceAfterBothSessionsFinish) {
    TwoSessionSetup setup;
    std::atomic<int> drained_count{0};
    setup.runner.start_all([&drained_count] { ++drained_count; });

    std::thread io_thread([&setup] { setup.io.run(); });

    // Same reconnect-backoff settling window as the test above - both
    // sessions are alive and reconnecting, nowhere near drained, so the
    // callback firing here would be the bug this test exists to catch.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(drained_count.load(), 0) << "on_all_drained fired before stop_all() was even called";

    std::atomic<bool> watchdog_fired{false};
    stop_all_and_join_with_watchdog(setup.io, setup.runner, io_thread, watchdog_fired);

    ASSERT_FALSE(watchdog_fired.load());
    EXPECT_EQ(drained_count.load(), 1);
}

// start_all() with nothing ever add()ed has nothing to wait on, so
// on_all_drained must fire synchronously, inline, rather than never firing
// at all - a caller like server_main.cpp that cancels its own signal_set
// from this callback needs that to happen even for a config with zero
// venues wired.
TEST(IngestionRunnerTest, OnAllDrainedFiresImmediatelyWithNoSessions) {
    IngestionRunner runner;
    bool drained = false;
    runner.start_all([&drained] { drained = true; });
    EXPECT_TRUE(drained);
}

}  // namespace
}  // namespace bobby::hermeneutic::ingestion

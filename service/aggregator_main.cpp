#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "aggregator_service.hpp"
#include "binance_futures_feed.hpp"
#include "binance_spot_feed.hpp"
#include "bybit_linear_feed.hpp"
#include "bybit_spot_feed.hpp"
#include "ingestion_runner.hpp"
#include "venue_session.hpp"

namespace {

namespace net = boost::asio;

// Splits a comma-separated symbol list ("BTCUSDT,ETHUSDT") into its parts.
// Empty entries (e.g. a trailing comma) are dropped rather than producing a
// blank symbol no client could ever usefully subscribe to.
std::vector<std::string> split_symbols(const std::string& csv) {
    std::vector<std::string> symbols;
    std::stringstream stream(csv);
    std::string symbol;
    while (std::getline(stream, symbol, ',')) {
        if (!symbol.empty()) symbols.push_back(symbol);
    }
    return symbols;
}

}  // namespace

int main(int argc, char** argv) {
    std::string address = argc > 1 ? argv[1] : "0.0.0.0:50051";
    std::vector<std::string> symbols = argc > 2 ? split_symbols(argv[2]) : std::vector{
        std::string("BTCUSDT")};
    if (symbols.empty()) {
        std::cerr << "no symbols given (usage: hermeneutic_aggregator_service [address] "
                     "[SYMBOL1,SYMBOL2,...])\n";
        return 1;
    }

    // AggregatorService's book set is keyed by ".PERP"/".SPOT"-suffixed
    // canonical symbols, not the bare "BTCUSDT" `symbols` themselves -
    // perp and spot liquidity for the same base pair are kept in two
    // independent SymbolBooks, not merged into one keyed by the bare
    // symbol the way an earlier revision of this file did. A client that
    // wants BTCUSDT's perpetual book now subscribes to "BTCUSDT.PERP", not
    // "BTCUSDT" - see docs/ingestion_design.md's OKX section for the reasoning.
    std::vector<std::string> book_symbols;
    book_symbols.reserve(symbols.size() * 2);
    for (const auto& symbol : symbols) {
        book_symbols.push_back(symbol + ".PERP");
        book_symbols.push_back(symbol + ".SPOT");
    }
    bobby::hermeneutic::aggregator::AggregatorService service(book_symbols);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    // Bounds how long a subscriber that stops reading (without closing the
    // connection) can stall broadcasting: an unresponsive connection fails
    // its keepalive ping within kKeepaliveTimeoutMs of kKeepaliveTimeMs, so
    // Write() eventually fails and the subscriber is dropped. See
    // AggregatorService's class comment for the full trade-off this bounds
    // rather than solves.
    constexpr int kKeepaliveTimeMs = 10'000;
    constexpr int kKeepaliveTimeoutMs = 5'000;
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIME_MS, kKeepaliveTimeMs);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, kKeepaliveTimeoutMs);
    builder.AddChannelArgument(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);

    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server) {
        std::cerr << "failed to start aggregator service on " << address << '\n';
        return 1;
    }

    // AggregatorService has no timer of its own (see send_heartbeat()'s doc
    // comment); drive it here so a subscriber can tell "book genuinely
    // unchanged" apart from "aggregator/feed stalled" during a quiet period,
    // well inside the keepalive timeout above so it isn't the only sign of
    // life on an idle connection.
    //
    // Joined at the end of main(), not detached: a detached thread that's
    // still alive (mid-sleep) when main() reaches the end of this
    // function calls service.send_heartbeat() on an object that's about
    // to be (or already is) destroyed - a real use-after-free, not
    // hypothetical, once anything actually makes server->Wait() return
    // (nothing does yet - see the shutdown comment below - but this
    // thread shouldn't be a landmine waiting for whoever wires that up).
    constexpr auto kHeartbeatInterval = std::chrono::seconds(1);
    std::atomic<bool> heartbeat_stop{false};
    std::thread heartbeat_thread([&service, &heartbeat_stop, kHeartbeatInterval] {
        while (!heartbeat_stop) {
            std::this_thread::sleep_for(kHeartbeatInterval);
            if (heartbeat_stop) break;
            service.send_heartbeat();
        }
    });

    // Ingestion: one VenueSession per venue, covering every symbol this
    // process serves, feeding the same AggregatorService the gRPC server
    // above exposes. Binance USDS-M Futures, Binance Spot, Bybit linear
    // (USDT perpetuals), and Bybit spot for now (see
    // docs/ingestion_design.md - IngestionRunner is what any further venue
    // would go through too - see 第 10 節第 2 項). The two perp/futures
    // venues write into the ".PERP" book, the two spot venues into the
    // ".SPOT" book - two independent SymbolBooks per base symbol, each
    // aggregating two venues' liquidity under distinct VenueIds
    // (SymbolBook::apply_batch/invalidate_venue are per-VenueId, so this is
    // safe - see 第 10 節第 2 項's note on multiple sessions writing the
    // same SymbolBook*), not all four venues sharing one book the way an
    // earlier revision of this file did. A real TLS context, not the
    // plain-TCP instantiation the tests use: this is the path that runs
    // against real exchanges rather than a local test server.
    net::ssl::context ssl_ctx(net::ssl::context::tlsv12_client);
    ssl_ctx.set_default_verify_paths();
    ssl_ctx.set_verify_mode(net::ssl::verify_peer);

    // Four independent registries, not one shared/moved: each VenueSession
    // takes its registry by value and moves it in, so passing the same
    // moved-from registry to a later add<>() would leave that venue's
    // SymbolRegistry::book() returning nullptr for every symbol.
    bobby::hermeneutic::ingestion::SymbolRegistry binance_futures_registry;
    bobby::hermeneutic::ingestion::SymbolRegistry binance_spot_registry;
    bobby::hermeneutic::ingestion::SymbolRegistry bybit_linear_registry;
    bobby::hermeneutic::ingestion::SymbolRegistry bybit_spot_registry;
    for (const auto& symbol : symbols) {
        binance_futures_registry.add(symbol, service.book(symbol + ".PERP"));
        binance_spot_registry.add(symbol, service.book(symbol + ".SPOT"));
        bybit_linear_registry.add(symbol, service.book(symbol + ".PERP"));
        bybit_spot_registry.add(symbol, service.book(symbol + ".SPOT"));
    }

    net::io_context io;
    bobby::hermeneutic::ingestion::IngestionRunner runner;
    runner.add<bobby::hermeneutic::ingestion::BinanceFuturesFeed, bobby::hermeneutic::BinanceFuturesSequencePolicy,
               net::ssl::stream<boost::beast::tcp_stream>>(
        bobby::hermeneutic::ingestion::BinanceFuturesFeed{}, "binance_futures", symbols,
        std::move(binance_futures_registry), io.get_executor(), &ssl_ctx);
    runner.add<bobby::hermeneutic::ingestion::BinanceSpotFeed, bobby::hermeneutic::BinanceSpotSequencePolicy,
               net::ssl::stream<boost::beast::tcp_stream>>(
        bobby::hermeneutic::ingestion::BinanceSpotFeed{}, "binance_spot", symbols,
        std::move(binance_spot_registry), io.get_executor(), &ssl_ctx);
    runner.add<bobby::hermeneutic::ingestion::BybitLinearFeed, bobby::hermeneutic::BybitSequencePolicy,
               net::ssl::stream<boost::beast::tcp_stream>>(
        bobby::hermeneutic::ingestion::BybitLinearFeed{}, "bybit_linear", symbols,
        std::move(bybit_linear_registry), io.get_executor(), &ssl_ctx);
    runner.add<bobby::hermeneutic::ingestion::BybitSpotFeed, bobby::hermeneutic::BybitSequencePolicy,
               net::ssl::stream<boost::beast::tcp_stream>>(
        bobby::hermeneutic::ingestion::BybitSpotFeed{}, "bybit_spot", symbols, std::move(bybit_spot_registry),
        io.get_executor(), &ssl_ctx);
    runner.start_all();
    std::thread io_thread([&io] { io.run(); });

    std::cout << "hermeneutic_aggregator_service listening on " << address << " for "
              << symbols.size() << " symbol(s):";
    for (const auto& symbol : symbols) std::cout << ' ' << symbol;
    std::cout << ", ingesting from binance_futures + binance_spot + bybit_linear + bybit_spot" << std::endl;
    server->Wait();

    // No io.stop() as the primary shutdown mechanism: stop_all() aborts
    // every session's in-flight read/backoff wait and drains any in-flight
    // snapshot fetch (see VenueSession::stop()), so io.run() should return
    // on its own - see docs/ingestion_design.md 第 10 節第 2 項's 驗收標準
    // (verified live against wss://fstream.binance.com: join returned
    // ~3ms after stop_all()). kShutdownTimeout is a backstop, not the
    // expected path, and has to be comfortably longer than the longest
    // timeout any single in-flight operation could legitimately still be
    // running under when stop() lands - currently
    // WebSocketConnection::connect()'s own 30s connect/TLS-handshake
    // timeout (service/websocket_connection.hpp), the longest of the two
    // (http_get's is 10s). A shorter watchdog would routinely fire and
    // force the crude io.stop() fallback for a stop() that simply landed
    // during a slow-but-still-progressing connect, not a genuinely stuck
    // one. Even 35s doesn't cover every stage unconditionally: a snapshot
    // fetch's DNS resolution (tcp::resolver, under the REST fetch's own
    // cancellation) runs the underlying getaddrinfo() call on a
    // background thread that keeps going until the OS call itself
    // returns, regardless of the awaitable-level cancellation completing
    // - a genuinely stuck/black-holed resolution can still delay process
    // teardown past this bound. That's a Boost.Asio/OS-level limitation,
    // not something this code can fix by waiting longer.
    constexpr auto kShutdownTimeout = std::chrono::seconds(35);
    std::mutex shutdown_mutex;
    std::condition_variable shutdown_cv;
    bool shutdown_complete = false;
    std::thread shutdown_watchdog([&] {
        std::unique_lock lock(shutdown_mutex);
        if (!shutdown_cv.wait_for(lock, kShutdownTimeout, [&] { return shutdown_complete; })) {
            std::cerr << "runner.stop_all() did not drain within " << kShutdownTimeout.count()
                      << "s - forcing io.stop() as a backstop\n";
            io.stop();
        }
    });

    runner.stop_all();
    io_thread.join();

    {
        std::lock_guard lock(shutdown_mutex);
        shutdown_complete = true;
    }
    shutdown_cv.notify_all();
    shutdown_watchdog.join();

    // Stopped and joined here, before `service` goes out of scope below -
    // see the comment on heartbeat_thread's construction for why this
    // can't be a detach().
    heartbeat_stop = true;
    heartbeat_thread.join();
    return 0;
}

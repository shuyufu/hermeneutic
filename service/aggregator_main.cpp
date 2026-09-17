#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <chrono>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "aggregator_service.hpp"
#include "binance_futures_feed.hpp"
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

    bobby::hermeneutic::aggregator::AggregatorService service(symbols);

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
    constexpr auto kHeartbeatInterval = std::chrono::seconds(1);
    std::thread heartbeat_thread([&service, kHeartbeatInterval] {
        while (true) {
            std::this_thread::sleep_for(kHeartbeatInterval);
            service.send_heartbeat();
        }
    });
    heartbeat_thread.detach();

    // Ingestion: one VenueSession per venue, covering every symbol this
    // process serves, feeding the same AggregatorService the gRPC server
    // above exposes. Binance USDS-M Futures only for now (see
    // docs/ingestion_design.md - Binance Spot and other venues aren't
    // implemented yet). A real TLS context, not the plain-TCP instantiation
    // the tests use: this is the first time that path runs against a real
    // exchange rather than a local test server.
    net::ssl::context ssl_ctx(net::ssl::context::tlsv12_client);
    ssl_ctx.set_default_verify_paths();
    ssl_ctx.set_verify_mode(net::ssl::verify_peer);

    bobby::hermeneutic::ingestion::SymbolRegistry registry;
    for (const auto& symbol : symbols) registry.add(symbol, service.book(symbol));

    net::io_context io;
    bobby::hermeneutic::ingestion::VenueSession<bobby::hermeneutic::ingestion::BinanceFuturesFeed,
                                         bobby::hermeneutic::BinanceFuturesSequencePolicy,
                                         net::ssl::stream<boost::beast::tcp_stream>>
        binance_session(bobby::hermeneutic::ingestion::BinanceFuturesFeed{}, "binance_futures", symbols,
                         std::move(registry), &ssl_ctx);
    net::co_spawn(io, binance_session.run(), [](std::exception_ptr e) {
        if (!e) return;
        try {
            std::rethrow_exception(e);
        } catch (const std::exception& ex) {
            // VenueSession::run() only returns via an uncaught exception in
            // its own setup (co_await net::this_coro::executor, backoff's
            // timer, etc.) - a dropped/failed venue connection is already
            // handled internally (reconnect with backoff), so reaching
            // here means ingestion for this venue has stopped for good.
            std::cerr << "binance_futures ingestion session ended: " << ex.what() << '\n';
        }
    });
    std::thread io_thread([&io] { io.run(); });

    std::cout << "hermeneutic_aggregator_service listening on " << address << " for "
              << symbols.size() << " symbol(s):";
    for (const auto& symbol : symbols) std::cout << ' ' << symbol;
    std::cout << ", ingesting from binance_futures" << std::endl;
    server->Wait();

    io.stop();
    io_thread.join();
    return 0;
}

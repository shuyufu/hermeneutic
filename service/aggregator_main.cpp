#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include <chrono>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "aggregator_service.hpp"

namespace {

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

    std::cout << "hermeneutic_aggregator_service listening on " << address << " for "
              << symbols.size() << " symbol(s):";
    for (const auto& symbol : symbols) std::cout << ' ' << symbol;
    std::cout << std::endl;
    server->Wait();
    return 0;
}

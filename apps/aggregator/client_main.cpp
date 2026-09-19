// Minimal example client for hermeneutic_aggregator_service: subscribes to
// one or more books' SubscribeBbo and/or SubscribeL2Diff stream and logs
// what arrives. Unlike the throwaway live-verification programs referenced
// in docs/ingestion_design.md (written, run once, then deleted), this one
// is meant to stay - a starting point for whatever actually consumes the
// aggregator's output next, and a manual way to poke at a running
// aggregator instance during development.
#include <grpcpp/grpcpp.h>

#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using bobby::hermeneutic::aggregator::Aggregator;
using bobby::hermeneutic::aggregator::BboUpdate;
using bobby::hermeneutic::aggregator::L2Update;
using bobby::hermeneutic::aggregator::PriceLevel;
using bobby::hermeneutic::aggregator::SubscribeBboRequest;
using bobby::hermeneutic::aggregator::SubscribeL2DiffRequest;

namespace {

std::string format_level(const PriceLevel& level) {
    std::ostringstream out;
    out << (level.price_raw() / 1e9) << "@" << (level.size_raw() / 1e6);
    return out.str();
}

// Cancels `context` once `stop` flips - the only way to make a blocking
// ClientReader::Read() loop below actually return once a caller-supplied
// duration elapses (see main()'s `stop` watcher), or, for the run-until-
// interrupted case (duration_seconds <= 0), never fires at all - Read()
// then only returns when the server ends the stream or the process is
// killed.
std::thread make_canceller(grpc::ClientContext& context, std::atomic<bool>* stop) {
    return std::thread([&context, stop] {
        while (!*stop) std::this_thread::sleep_for(std::chrono::milliseconds(200));
        context.TryCancel();
    });
}

void subscribe_bbo(const std::string& address, const std::string& symbol, std::atomic<bool>* stop) {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = Aggregator::NewStub(channel);

    grpc::ClientContext context;
    SubscribeBboRequest request;
    request.set_symbol(symbol);
    auto reader = stub->SubscribeBbo(&context, request);
    std::thread canceller = make_canceller(context, stop);

    BboUpdate update;
    long bbo_count = 0, heartbeat_count = 0;
    std::string last_line;
    while (reader->Read(&update)) {
        if (update.has_bbo()) {
            ++bbo_count;
            const auto& bbo = update.bbo();
            std::ostringstream line;
            line << "book_seq=" << bbo.book_seq() << " bid=" << (bbo.has_bid() ? format_level(bbo.bid()) : "(none)")
                 << " ask=" << (bbo.has_ask() ? format_level(bbo.ask()) : "(none)");
            last_line = line.str();
            std::cout << "[" << symbol << " BBO] " << last_line << std::endl;
        } else if (update.has_heartbeat()) {
            ++heartbeat_count;
        }
    }
    canceller.join();
    auto status = reader->Finish();
    std::cout << "[" << symbol << " BBO] DONE bbo_count=" << bbo_count << " heartbeat_count=" << heartbeat_count
              << " last=(" << last_line << ") grpc_status=" << status.error_code() << " ("
              << status.error_message() << ")" << std::endl;
}

void subscribe_l2diff(const std::string& address, const std::string& symbol, std::atomic<bool>* stop) {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = Aggregator::NewStub(channel);

    grpc::ClientContext context;
    SubscribeL2DiffRequest request;
    request.set_symbol(symbol);
    auto reader = stub->SubscribeL2Diff(&context, request);
    std::thread canceller = make_canceller(context, stop);

    L2Update update;
    long snapshot_count = 0, diff_count = 0, heartbeat_count = 0, gap_count = 0;
    std::optional<std::uint64_t> last_seq;
    std::string last_line;
    while (reader->Read(&update)) {
        if (update.has_snapshot()) {
            ++snapshot_count;
            const auto& snapshot = update.snapshot();
            std::ostringstream line;
            line << "SNAPSHOT book_seq=" << snapshot.book_seq() << " bids=" << snapshot.bids_size()
                 << " asks=" << snapshot.asks_size();
            if (snapshot.bids_size() > 0) line << " top_bid=" << format_level(snapshot.bids(0));
            if (snapshot.asks_size() > 0) line << " top_ask=" << format_level(snapshot.asks(0));
            last_line = line.str();
            last_seq = snapshot.book_seq();
            std::cout << "[" << symbol << " L2] " << last_line << std::endl;
        } else if (update.has_diff()) {
            ++diff_count;
            const auto& diff = update.diff();
            // book_seq is contiguous by contract on this stream (unlike
            // Bbo's) - see aggregator.proto's L2Diff.book_seq comment. A
            // gap means a revision was missed - worth surfacing loudly
            // here since that's exactly the failure mode this stream's
            // contract exists to make detectable.
            if (last_seq && diff.book_seq() != *last_seq + 1) {
                ++gap_count;
                std::cout << "[" << symbol << " L2] GAP: expected book_seq=" << (*last_seq + 1) << " got "
                          << diff.book_seq() << std::endl;
            }
            last_seq = diff.book_seq();
            std::ostringstream line;
            line << "DIFF book_seq=" << diff.book_seq() << " bids=" << diff.bids_size()
                 << " asks=" << diff.asks_size();
            last_line = line.str();
            std::cout << "[" << symbol << " L2] " << last_line << std::endl;
        } else if (update.has_heartbeat()) {
            ++heartbeat_count;
        }
    }
    canceller.join();
    auto status = reader->Finish();
    std::cout << "[" << symbol << " L2] DONE snapshot_count=" << snapshot_count << " diff_count=" << diff_count
              << " heartbeat_count=" << heartbeat_count << " gap_count=" << gap_count << " last=(" << last_line
              << ") grpc_status=" << status.error_code() << " (" << status.error_message() << ")" << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: hermeneutic_aggregator_client <address> <bbo|l2|both> <duration_seconds> "
                     "<symbol1> [symbol2 ...]\n"
                     "  duration_seconds <= 0 means run until interrupted or the server ends the stream\n"
                     "  symbols are book keys, e.g. BTCUSDT.SPOT or BTCUSDT.PERP\n";
        return 1;
    }
    std::string address = argv[1];
    std::string mode = argv[2];
    int duration_s = std::stoi(argv[3]);
    std::vector<std::string> symbols(argv + 4, argv + argc);

    if (mode != "bbo" && mode != "l2" && mode != "both") {
        std::cerr << "unknown mode \"" << mode << "\" (expected bbo, l2, or both)\n";
        return 1;
    }

    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (const auto& symbol : symbols) {
        if (mode == "bbo" || mode == "both") threads.emplace_back(subscribe_bbo, address, symbol, &stop);
        if (mode == "l2" || mode == "both") threads.emplace_back(subscribe_l2diff, address, symbol, &stop);
    }

    if (duration_s > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(duration_s));
        stop = true;
    }
    for (auto& t : threads) t.join();
    return 0;
}

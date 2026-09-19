// Example/diagnostic client for hermeneutic_aggregator_service: subscribes
// to one or more books and publishes a chosen view of the order book to
// stdout. Unlike the throwaway live-verification programs referenced in
// docs/ingestion_design.md (written, run once, then deleted), this one is
// meant to stay - a starting point for whatever actually consumes the
// aggregator's output next, and a manual way to poke at a running
// aggregator instance during development.
//
// Three publisher modes, selected on the command line:
//   bbo           best bid/offer - subscribes to SubscribeBbo only.
//   volume-bands  VWAP needed to fill 1M/5M/10M/25M/50M+ notional on each
//                 side - subscribes to SubscribeL2Diff and maintains a
//                 local order book.
//   price-bands   depth within 50/100/200/500/1000+ bps of BBO on each
//                 side - subscribes to SubscribeL2Diff and maintains a
//                 local order book.
// volume-bands/price-bands both track SubscribeL2Diff's own book_seq
// contiguity guarantee (aggregator.proto: "book_seq must be contiguous...
// a gap means a revision was missed") and log a GAP line loudly if that
// contract is ever violated, rather than silently tracking the counter and
// never checking it.
#include <grpcpp/grpcpp.h>

#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"

#include <array>
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

#include "bobby/hermeneutic/book/l2_order_book.hpp"
#include "bobby/hermeneutic/book/price_bands.hpp"
#include "bobby/hermeneutic/book/volume_bands.hpp"

using bobby::hermeneutic::L2OrderBook;
using bobby::hermeneutic::Notional;
using bobby::hermeneutic::Price;
using bobby::hermeneutic::PriceBand;
using bobby::hermeneutic::Size;
using bobby::hermeneutic::VolumeBand;
using bobby::hermeneutic::ask_price_band_depths;
using bobby::hermeneutic::ask_volume_band_prices;
using bobby::hermeneutic::bid_price_band_depths;
using bobby::hermeneutic::bid_volume_band_prices;

using bobby::hermeneutic::aggregator::Aggregator;
using bobby::hermeneutic::aggregator::BboUpdate;
using bobby::hermeneutic::aggregator::L2Update;
using bobby::hermeneutic::aggregator::PriceLevel;
using bobby::hermeneutic::aggregator::SubscribeBboRequest;
using bobby::hermeneutic::aggregator::SubscribeL2DiffRequest;

namespace {

enum class Mode { Bbo, VolumeBands, PriceBands };

std::optional<Mode> parse_mode(const std::string& token) {
    if (token == "bbo") return Mode::Bbo;
    if (token == "volume-bands") return Mode::VolumeBands;
    if (token == "price-bands") return Mode::PriceBands;
    return std::nullopt;
}

// Notional/bps thresholds are fixed by this tool, not caller-configurable -
// see client_main.cpp's own mode list above. Kept as two parallel arrays
// (values for volume_band_prices()/price_band_depth(), labels for display)
// rather than a struct-of-two-fields array, since std::span<const Notional>/
// std::span<const int> - the shape those functions actually take - needs a
// contiguous run of just the values.
constexpr std::array<const char*, 5> kVolumeBandLabels = {"1M", "5M", "10M", "25M", "50M+"};
constexpr std::array<Notional, 5> kVolumeBandThresholds = {
    Notional(1e6), Notional(5e6), Notional(10e6), Notional(25e6), Notional(50e6),
};

constexpr std::array<int, 5> kPriceBandBps = {50, 100, 200, 500, 1000};
constexpr std::array<const char*, 5> kPriceBandLabels = {"50bps", "100bps", "200bps", "500bps", "1000bps+"};

std::string format_level(const PriceLevel& level) {
    std::ostringstream out;
    out << (level.price_raw() / 1e9) << "@" << (level.size_raw() / 1e6);
    return out.str();
}

// One band's worth of `label=value`. `bands` is expected to line up
// index-for-index with kVolumeBandLabels (volume_band_prices() always
// returns exactly one entry per input threshold, even for an empty/thin
// book - see its own comment - so this always holds for a well-formed
// result).
std::string format_volume_bands(const std::vector<VolumeBand>& bands) {
    std::ostringstream out;
    for (std::size_t i = 0; i < bands.size() && i < kVolumeBandLabels.size(); ++i) {
        if (i) out << ' ';
        out << kVolumeBandLabels[i] << '=';
        if (bands[i].vwap) out << *bands[i].vwap;
        else out << "NA(filled=" << bands[i].filled_notional << ')';
    }
    return out.str();
}

// price_band_depth() returns an empty vector for a side with no BBO at all
// (see its own comment) rather than one entry per threshold - the only
// case where `bands.size()` doesn't match kPriceBandLabels.size().
std::string format_price_bands(const std::vector<PriceBand>& bands) {
    if (bands.empty()) return "(no bbo)";
    std::ostringstream out;
    for (std::size_t i = 0; i < bands.size() && i < kPriceBandLabels.size(); ++i) {
        if (i) out << ' ';
        out << kPriceBandLabels[i] << '=' << bands[i].cumulative_size << '@' << bands[i].cumulative_notional;
    }
    return out.str();
}

// Applies one side of a snapshot or diff onto the local book. A snapshot
// level always replaces the side wholesale (hence `clear()` first, done by
// the caller before the first side); a diff level is a replacement at that
// price (size_raw > 0) or a removal (size_raw == 0) - same per-level rule
// either way, see aggregator.proto's PriceLevel/L2Diff comment. `Levels` is
// left as a template parameter (rather than naming the protobuf
// RepeatedPtrField type) purely to avoid an extra include here.
template <typename Map, typename Levels>
void apply_levels(Map& side, const Levels& levels) {
    for (const auto& level : levels) {
        Price price = Price::from_raw(level.price_raw());
        Size size = Size::from_raw(level.size_raw());
        if (size.raw() == 0) side.erase(price);
        else side[price] = size;
    }
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

void publish_bbo(const std::string& address, const std::string& symbol, std::atomic<bool>* stop) {
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

// Shared by volume-bands and price-bands: both subscribe to SubscribeL2Diff
// and maintain the same local L2OrderBook, differing only in which bands
// get computed/printed from it on every update.
void publish_l2_bands(Mode mode, const std::string& address, const std::string& symbol, std::atomic<bool>* stop) {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = Aggregator::NewStub(channel);

    grpc::ClientContext context;
    SubscribeL2DiffRequest request;
    request.set_symbol(symbol);
    auto reader = stub->SubscribeL2Diff(&context, request);
    std::thread canceller = make_canceller(context, stop);

    const char* mode_tag = mode == Mode::VolumeBands ? "VOLUME_BANDS" : "PRICE_BANDS";
    L2OrderBook book;

    auto print_bands = [&](std::uint64_t book_seq) -> std::string {
        std::ostringstream line;
        line << "book_seq=" << book_seq;
        if (mode == Mode::VolumeBands) {
            auto bids = bid_volume_band_prices(book, kVolumeBandThresholds);
            auto asks = ask_volume_band_prices(book, kVolumeBandThresholds);
            line << " bid[" << (bids.has_value() ? format_volume_bands(*bids) : std::string("ERROR")) << "]";
            line << " ask[" << (asks.has_value() ? format_volume_bands(*asks) : std::string("ERROR")) << "]";
        } else {
            line << " bid[" << format_price_bands(bid_price_band_depths(book, kPriceBandBps)) << "]";
            line << " ask[" << format_price_bands(ask_price_band_depths(book, kPriceBandBps)) << "]";
        }
        std::string result = line.str();
        std::cout << "[" << symbol << " " << mode_tag << "] " << result << std::endl;
        return result;
    };

    L2Update update;
    long snapshot_count = 0, diff_count = 0, heartbeat_count = 0, gap_count = 0;
    std::optional<std::uint64_t> last_seq;
    std::string last_line;
    while (reader->Read(&update)) {
        if (update.has_snapshot()) {
            ++snapshot_count;
            const auto& snapshot = update.snapshot();
            book.bids.clear();
            book.asks.clear();
            apply_levels(book.bids, snapshot.bids());
            apply_levels(book.asks, snapshot.asks());
            last_seq = snapshot.book_seq();
            last_line = print_bands(snapshot.book_seq());
        } else if (update.has_diff()) {
            ++diff_count;
            const auto& diff = update.diff();
            // book_seq is contiguous by contract on this stream (unlike
            // Bbo's) - see aggregator.proto's L2Diff.book_seq comment. A
            // gap means a revision was missed, and per that same comment
            // the locally reconstructed book is no longer valid - so this
            // stops applying/publishing immediately rather than computing
            // bands off a book that's silently wrong from here on. This
            // tool doesn't resubscribe to recover (that needs a whole new
            // RPC, not just a fresh snapshot on this one) - a GAP line is
            // the operator's signal to restart it.
            if (last_seq && diff.book_seq() != *last_seq + 1) {
                ++gap_count;
                std::cout << "[" << symbol << " " << mode_tag << "] GAP: expected book_seq="
                          << (*last_seq + 1) << " got " << diff.book_seq()
                          << " - local book invalid, stopping this stream" << std::endl;
                break;
            }
            last_seq = diff.book_seq();
            apply_levels(book.bids, diff.bids());
            apply_levels(book.asks, diff.asks());
            last_line = print_bands(diff.book_seq());
        } else if (update.has_heartbeat()) {
            ++heartbeat_count;
        }
    }
    canceller.join();
    auto status = reader->Finish();
    std::cout << "[" << symbol << " " << mode_tag << "] DONE snapshot_count=" << snapshot_count
              << " diff_count=" << diff_count << " heartbeat_count=" << heartbeat_count
              << " gap_count=" << gap_count << " last=(" << last_line << ") grpc_status=" << status.error_code()
              << " (" << status.error_message() << ")" << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: hermeneutic_aggregator_client <address> <bbo|volume-bands|price-bands> "
                     "<duration_seconds> <symbol1> [symbol2 ...]\n"
                     "  duration_seconds <= 0 means run until interrupted or the server ends the stream\n"
                     "  symbols are book keys, e.g. BTCUSDT.SPOT or BTCUSDT.PERP\n"
                     "  bbo:          subscribes to SubscribeBbo, prints best bid/ask on every update\n"
                     "  volume-bands: subscribes to SubscribeL2Diff, prints the VWAP needed to fill\n"
                     "                1M/5M/10M/25M/50M+ notional on each side on every update\n"
                     "  price-bands:  subscribes to SubscribeL2Diff, prints depth within\n"
                     "                50/100/200/500/1000+ bps of BBO on each side on every update\n";
        return 1;
    }
    std::string address = argv[1];
    std::string mode_str = argv[2];
    int duration_s = std::stoi(argv[3]);
    std::vector<std::string> symbols(argv + 4, argv + argc);

    auto mode = parse_mode(mode_str);
    if (!mode) {
        std::cerr << "unknown mode \"" << mode_str << "\" (expected bbo, volume-bands, or price-bands)\n";
        return 1;
    }

    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (const auto& symbol : symbols) {
        if (*mode == Mode::Bbo) threads.emplace_back(publish_bbo, address, symbol, &stop);
        else threads.emplace_back(publish_l2_bands, *mode, address, symbol, &stop);
    }

    if (duration_s > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(duration_s));
        stop = true;
    }
    for (auto& t : threads) t.join();
    return 0;
}

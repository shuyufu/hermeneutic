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

#include "apps/aggregator/book_id.hpp"
#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"
#include "bobby/hermeneutic/symbol/symbol.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
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
using bobby::hermeneutic::aggregator::fill_wire_book_id;
using bobby::hermeneutic::aggregator::L2Update;
using bobby::hermeneutic::aggregator::PriceLevel;
using bobby::hermeneutic::aggregator::SubscribeBboRequest;
using bobby::hermeneutic::aggregator::SubscribeL2DiffRequest;
using bobby::hermeneutic::symbol::BookId;
using bobby::hermeneutic::symbol::to_string;

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

// BasicFixedPoint's own operator<< streams to_double() through the
// ostream's ambient (default) precision, which is only 6 significant
// digits - fine for a Price/Size around 80000.5, but a Notional in this
// tool's own 1M-50M+ band range overflows that into scientific notation
// ("1.23457e+07"), defeating the readability these two publisher modes
// exist for. Precision is FixedPoint::decimals (Price/Notional=9,
// Size=6) rather than a fixed "2" - that's the exact number of decimal
// digits the type's raw scale actually stores, so it prints losslessly
// for a small value (e.g. a sub-cent VWAP) instead of a fixed "2"
// truncating it to "0.00", while std::fixed still keeps a large Notional
// out of scientific notation. Used for every fixed-point value
// volume-bands/price-bands print, not just Notional, so a VWAP/size
// prints with the same style rather than mixing formatting conventions.
template <typename FixedPoint>
std::string format_fixed(FixedPoint value) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(FixedPoint::decimals) << value.to_double();
    return out.str();
}

// Raw wire values reconstructed into their real fixed-point types before
// formatting (rather than the raw int64 divided by a literal 1e9/1e6 and
// streamed at ostream's default precision) so this shares format_fixed()'s
// fix for the same scientific-notation risk - this is the bbo mode's own
// price/size display and every mode's DONE line, not just volume-bands/
// price-bands.
std::string format_level(const PriceLevel& level) {
    std::ostringstream out;
    out << format_fixed(Price::from_raw(level.price_raw())) << "@" << format_fixed(Size::from_raw(level.size_raw()));
    return out.str();
}

// Guards stdout: each publish_bbo()/publish_l2_bands() call runs on its
// own thread (one per book - see main()), and a bare `std::cout << a <<
// b << c` is a sequence of independent stream operations, not one atomic
// write - two threads' chains can interleave mid-line into a single
// garbled, unparseable line. Callers build the complete line first (this
// file already does, via ostringstream) and hand it here as one string so
// the lock covers the entire write.
void print_line(const std::string& line) {
    static std::mutex out_mutex;
    {
        std::lock_guard lock(out_mutex);
        std::cout << line << '\n';
    }
    // Flushed outside the lock: the write above (the part that must not
    // interleave with another thread's line) is already complete once the
    // lock is released, so the flush syscall's cost no longer serializes
    // across every symbol thread - only actual writes to std::cout do,
    // which the standard library's own stream synchronization still
    // protects against corruption.
    std::cout.flush();
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
        if (bands[i].vwap) out << format_fixed(*bands[i].vwap);
        else out << "NA(filled=" << format_fixed(bands[i].filled_notional) << ')';
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
        out << kPriceBandLabels[i] << '=' << format_fixed(bands[i].cumulative_size) << '@'
            << format_fixed(bands[i].cumulative_notional);
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
//
// Deliberately does not validate price_raw/size_raw itself (unlike
// AggregateOrderBook::apply_delta()'s require_valid_level() on the
// server side, which rejects a non-positive price or negative size
// before it ever reaches a book): a wire-level malformed value here would
// still get caught downstream, by price_band_depth()/volume_band_prices()
// erroring out on the next print_bands() call - printing "ERROR" for that
// side going forward rather than silently computing a wrong band. That's
// an accepted, known-limited response (the bad level stays in the local
// book forever; nothing here removes it or breaks the stream the way a
// book_seq gap does), not an oversight - this is a diagnostic client, and
// "ERROR" already surfaces the problem to whoever's watching it, which
// was this fix's actual goal. A gap-triggered break()-out-of-read-loop
// treatment for this case, if ever wanted, is future scope, not implied
// by fixing the validation gap itself.
template <typename Map, typename Levels>
void apply_levels(Map& side, const Levels& levels) {
    for (const auto& level : levels) {
        Price price = Price::from_raw(level.price_raw());
        Size size = Size::from_raw(level.size_raw());
        if (size.raw() == 0) side.erase(price);
        else side[price] = size;
    }
}

// Shared by every StreamCanceller and by main()'s `stop` watcher below:
// one mutex/condition_variable pair the whole process waits on and
// notifies through, rather than each canceller thread polling on a timer
// (the old behavior cost every stream teardown up to 200ms of needless
// wait). Contention is a non-issue - each thread touches this only once
// to wait and once (from the other side) to notify.
std::mutex& cancel_mutex() {
    static std::mutex m;
    return m;
}
std::condition_variable& cancel_cv() {
    static std::condition_variable cv;
    return cv;
}

// RAII owner of the background thread that cancels `context` if the
// caller-supplied duration elapses (`*stop` flips - see main()) while
// this call's read loop is still running. Construct it, run the read
// loop, then let it go out of scope (or destroy it explicitly) as soon as
// the read loop ends *for any reason* - a clean/natural stream
// completion, a book_seq gap triggering an early `break`, or the `stop`
// deadline itself - before calling `reader->Finish()`.
//
// Deliberately does NOT call TryCancel() just because the read loop
// ended: if it ended on its own (not because `stop` fired), the stream is
// already over and there is nothing to cancel - calling TryCancel()
// anyway would race with the client library's own handling of that
// already-finished call, and risks Finish() misreporting a real OK
// completion as CANCELLED. Only `*stop` becoming true is a reason to
// cancel.
class StreamCanceller {
  public:
    StreamCanceller(grpc::ClientContext& context, std::atomic<bool>* stop)
        : stop_(stop), thread_([this, &context] { run(context); }) {}

    ~StreamCanceller() {
        {
            std::lock_guard<std::mutex> lock(cancel_mutex());
            read_loop_done_ = true;
        }
        cancel_cv().notify_all();
        thread_.join();
    }

    StreamCanceller(const StreamCanceller&) = delete;
    StreamCanceller& operator=(const StreamCanceller&) = delete;

  private:
    void run(grpc::ClientContext& context) {
        std::unique_lock<std::mutex> lock(cancel_mutex());
        cancel_cv().wait(lock, [this] { return stop_->load() || read_loop_done_; });
        bool should_cancel = stop_->load();
        lock.unlock();
        if (should_cancel) context.TryCancel();
    }

    std::atomic<bool>* stop_;
    bool read_loop_done_ = false;
    std::thread thread_;
};

void publish_bbo(const std::string& address, const BookId& book_id, std::atomic<bool>* stop) {
    std::string label = to_string(book_id);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = Aggregator::NewStub(channel);

    grpc::ClientContext context;
    SubscribeBboRequest request;
    fill_wire_book_id(request.mutable_book(), book_id);
    auto reader = stub->SubscribeBbo(&context, request);

    BboUpdate update;
    long bbo_count = 0, heartbeat_count = 0;
    std::string last_line;
    {
        StreamCanceller canceller(context, stop);
        while (reader->Read(&update)) {
            if (update.has_bbo()) {
                ++bbo_count;
                const auto& bbo = update.bbo();
                std::ostringstream line;
                line << "book_seq=" << bbo.book_seq()
                     << " bid=" << (bbo.has_bid() ? format_level(bbo.bid()) : "(none)")
                     << " ask=" << (bbo.has_ask() ? format_level(bbo.ask()) : "(none)");
                last_line = line.str();
                print_line("[" + label + " BBO] " + last_line);
            } else if (update.has_heartbeat()) {
                ++heartbeat_count;
            }
        }
    }  // canceller destroyed here: joins its thread before Finish() below.
    auto status = reader->Finish();
    std::ostringstream done_line;
    done_line << "[" << label << " BBO] DONE bbo_count=" << bbo_count << " heartbeat_count=" << heartbeat_count
              << " last=(" << last_line << ") grpc_status=" << status.error_code() << " ("
              << status.error_message() << ")";
    print_line(done_line.str());
}

// Shared by volume-bands and price-bands: both subscribe to SubscribeL2Diff
// and maintain the same local L2OrderBook, differing only in which bands
// get computed/printed from it on every update.
void publish_l2_bands(Mode mode, const std::string& address, const BookId& book_id, std::atomic<bool>* stop) {
    std::string label = to_string(book_id);
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    auto stub = Aggregator::NewStub(channel);

    grpc::ClientContext context;
    SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), book_id);
    auto reader = stub->SubscribeL2Diff(&context, request);

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
            auto bids = bid_price_band_depths(book, kPriceBandBps);
            auto asks = ask_price_band_depths(book, kPriceBandBps);
            line << " bid[" << (bids.has_value() ? format_price_bands(*bids) : std::string("ERROR")) << "]";
            line << " ask[" << (asks.has_value() ? format_price_bands(*asks) : std::string("ERROR")) << "]";
        }
        std::string result = line.str();
        print_line("[" + label + " " + mode_tag + "] " + result);
        return result;
    };

    L2Update update;
    long snapshot_count = 0, diff_count = 0, heartbeat_count = 0, gap_count = 0;
    std::optional<std::uint64_t> last_seq;
    std::string last_line;
    {
        StreamCanceller canceller(context, stop);
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
                    std::ostringstream gap_line;
                    gap_line << "[" << label << " " << mode_tag << "] GAP: expected book_seq="
                              << (*last_seq + 1) << " got " << diff.book_seq()
                              << " - local book invalid, stopping this stream";
                    print_line(gap_line.str());
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
    }  // canceller destroyed here: joins its thread before Finish() below.
    auto status = reader->Finish();
    std::ostringstream done_line;
    done_line << "[" << label << " " << mode_tag << "] DONE snapshot_count=" << snapshot_count
              << " diff_count=" << diff_count << " heartbeat_count=" << heartbeat_count
              << " gap_count=" << gap_count << " last=(" << last_line << ") grpc_status=" << status.error_code()
              << " (" << status.error_message() << ")";
    print_line(done_line.str());
}

void print_usage() {
    std::cerr << "usage: hermeneutic_aggregator_client <address> <bbo|volume-bands|price-bands> "
                 "<duration_seconds> <book1> [book2 ...]\n"
                 "  duration_seconds <= 0 means run until interrupted or the server ends the stream\n"
                 "  books look like BTC_USDT.SPOT or BTC_USDT.PERP\n"
                 "  bbo:          subscribes to SubscribeBbo, prints best bid/ask on every update\n"
                 "  volume-bands: subscribes to SubscribeL2Diff, prints the VWAP needed to fill\n"
                 "                1M/5M/10M/25M/50M+ notional on each side on every update\n"
                 "  price-bands:  subscribes to SubscribeL2Diff, prints depth within\n"
                 "                50/100/200/500/1000+ bps of BBO on each side on every update\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 5) {
        print_usage();
        return 1;
    }
    std::string address = argv[1];
    std::string mode_str = argv[2];
    std::string duration_str = argv[3];
    int duration_s;
    try {
        std::size_t consumed = 0;
        duration_s = std::stoi(duration_str, &consumed);
        // std::stoi is a partial parse by design (stops at the first
        // non-digit and returns what it has, e.g. "10abc" -> 10, "5.5" ->
        // 5, "3,600" -> 3) rather than rejecting trailing garbage - so
        // the try/catch below alone doesn't actually validate the whole
        // argument. `consumed` short of the full string length is that
        // trailing-garbage case.
        if (consumed != duration_str.size()) {
            std::cerr << "invalid duration_seconds \"" << duration_str << "\" (trailing characters after the integer)\n";
            print_usage();
            return 1;
        }
    } catch (const std::out_of_range&) {
        std::cerr << "invalid duration_seconds \"" << duration_str << "\" (integer out of range)\n";
        print_usage();
        return 1;
    } catch (const std::invalid_argument&) {
        std::cerr << "invalid duration_seconds \"" << duration_str << "\" (expected an integer)\n";
        print_usage();
        return 1;
    }

    // Parsed once here, at this program's own input boundary - every
    // downstream use (the log label, the wire BookId) works with the
    // structured value, never the original string again. See
    // bobby::hermeneutic::symbol::parse_book_id's own comment.
    std::vector<BookId> books;
    for (int i = 4; i < argc; ++i) {
        auto book_id = bobby::hermeneutic::symbol::parse_book_id(argv[i]);
        if (!book_id) {
            std::cerr << "invalid book \"" << argv[i] << "\" (expected e.g. BTC_USDT.SPOT or BTC_USDT.PERP)\n";
            return 1;
        }
        books.push_back(*book_id);
    }

    auto mode = parse_mode(mode_str);
    if (!mode) {
        std::cerr << "unknown mode \"" << mode_str << "\" (expected bbo, volume-bands, or price-bands)\n";
        print_usage();
        return 1;
    }

    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (const auto& book_id : books) {
        if (*mode == Mode::Bbo) threads.emplace_back(publish_bbo, address, book_id, &stop);
        else threads.emplace_back(publish_l2_bands, *mode, address, book_id, &stop);
    }

    if (duration_s > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(duration_s));
        // Setting `stop` while holding cancel_mutex() (not just relying on
        // it being std::atomic) closes a lost-wakeup window: without the
        // lock, a canceller thread could re-check its predicate (see
        // `false`), then get preempted right before blocking on the CV -
        // this write and the notify_all() below would then land in that
        // gap and be missed entirely, leaving the thread asleep until its
        // own read loop happens to end on its own. That would reintroduce
        // the exact class of hang 2055487 fixed, just for the
        // duration-elapsed path instead of the natural-completion one.
        {
            std::lock_guard<std::mutex> lock(cancel_mutex());
            stop = true;
        }
        cancel_cv().notify_all();
    }
    for (auto& t : threads) t.join();
    return 0;
}

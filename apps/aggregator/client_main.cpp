// Example/diagnostic client for hermeneutic_aggregator_service: subscribes
// to one or more books and publishes a chosen view of the order book to
// stdout. Unlike the throwaway live-verification programs referenced in
// docs/ingestion_design.md (written, run once, then deleted), this one is
// meant to stay - a starting point for whatever actually consumes the
// aggregator's output next, and a manual way to poke at a running
// aggregator instance during development.
//
// Three publisher modes plus one query mode, selected on the command line:
//   bbo           best bid/offer - subscribes to SubscribeBbo only.
//   volume-bands  VWAP needed to fill 1M/5M/10M/25M/50M+ notional on each
//                 side - subscribes to SubscribeL2Diff and maintains a
//                 local order book.
//   price-bands   depth within 50/100/200/500/1000+ bps of BBO on each
//                 side - subscribes to SubscribeL2Diff and maintains a
//                 local order book.
//   list          calls the unary ListBooks RPC and prints every book the
//                 server was started with, then exits - unlike the other
//                 three modes, this takes no duration/book arguments.
// volume-bands/price-bands both track SubscribeL2Diff's own book_seq
// contiguity guarantee (aggregator.proto: "book_seq must be contiguous...
// a gap means a revision was missed") and log a GAP line loudly if that
// contract is ever violated, rather than silently tracking the counter and
// never checking it.
#include <grpcpp/grpcpp.h>

#include "apps/aggregator/book_id.hpp"
#include "apps/aggregator/client_book.hpp"
#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"
#include "bobby/hermeneutic/symbol/symbol.hpp"

#include <algorithm>
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
using bobby::hermeneutic::aggregator::apply_levels;
using bobby::hermeneutic::aggregator::BboUpdate;
using bobby::hermeneutic::aggregator::fill_wire_book_id;
using bobby::hermeneutic::aggregator::Heartbeat;
using bobby::hermeneutic::aggregator::is_book_seq_gap;
using bobby::hermeneutic::aggregator::L2Update;
using bobby::hermeneutic::aggregator::ListBooksRequest;
using bobby::hermeneutic::aggregator::ListBooksResponse;
using bobby::hermeneutic::aggregator::PriceLevel;
using bobby::hermeneutic::aggregator::SubscribeBboRequest;
using bobby::hermeneutic::aggregator::SubscribeL2DiffRequest;
using bobby::hermeneutic::aggregator::to_symbol_book_id;
using bobby::hermeneutic::symbol::BookId;
using bobby::hermeneutic::symbol::to_string;

namespace {

enum class Mode { Bbo, VolumeBands, PriceBands, List };

std::optional<Mode> parse_mode(const std::string& token) {
    if (token == "bbo") return Mode::Bbo;
    if (token == "volume-bands") return Mode::VolumeBands;
    if (token == "price-bands") return Mode::PriceBands;
    if (token == "list") return Mode::List;
    return std::nullopt;
}

// Notional/bps thresholds are fixed by this tool, not caller-configurable.
// Kept as two parallel arrays (values for volume_band_prices()/
// price_band_depth(), labels for display) rather than a struct-of-two-
// fields array, since std::span<const Notional>/std::span<const int> -
// the shape those functions actually take - needs a contiguous run of
// just the values.
constexpr std::array<const char*, 5> kVolumeBandLabels = {"1M", "5M", "10M", "25M", "50M+"};
constexpr std::array<Notional, 5> kVolumeBandThresholds = {
    Notional(1e6), Notional(5e6), Notional(10e6), Notional(25e6), Notional(50e6),
};

constexpr std::array<int, 5> kPriceBandBps = {50, 100, 200, 500, 1000};
constexpr std::array<const char*, 5> kPriceBandLabels = {"50bps", "100bps", "200bps", "500bps", "1000bps+"};

// BasicFixedPoint's own operator<< streams to_double() through ostream's
// default precision (6 significant digits) - fine for a Price/Size around
// 80000.5, but a Notional in this tool's 1M-50M+ band range overflows
// that into scientific notation. Precision is FixedPoint::decimals
// (Price/Notional=9, Size=6), not a fixed "2": that's the exact number of
// decimal digits the type's raw scale stores, so a small value (e.g. a
// sub-cent VWAP) still prints losslessly while std::fixed keeps a large
// Notional out of scientific notation.
template <typename FixedPoint>
std::string format_fixed(FixedPoint value) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(FixedPoint::decimals) << value.to_double();
    return out.str();
}

// Raw wire values reconstructed into their real fixed-point types before
// formatting, rather than the raw int64 divided by a literal 1e9/1e6 and
// streamed at ostream's default precision, so this shares format_fixed()'s
// fix for the same scientific-notation risk.
std::string format_level(const PriceLevel& level) {
    std::ostringstream out;
    out << format_fixed(Price::from_raw(level.price_raw())) << "@" << format_fixed(Size::from_raw(level.size_raw()));
    return out.str();
}

// Formats Heartbeat.live_venues as "[venue venue ...]" - empty means "no
// venue currently backs this book" (aggregator.proto). Sorted, not printed
// in wire order: element order off the wire is unspecified and may change
// between heartbeats even when the live set itself hasn't; both publisher
// modes below print this only when it changes from the last one printed,
// so an unsorted reorder with no actual liveness change would look like one.
std::string format_live_venues(const Heartbeat& heartbeat) {
    std::vector<std::string> venues(heartbeat.live_venues().begin(), heartbeat.live_venues().end());
    std::sort(venues.begin(), venues.end());
    std::ostringstream out;
    out << "live_venues=[";
    for (const auto& venue : venues) out << venue << ' ';
    out << ']';
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
    // Flushed outside the lock: the write above is already complete once
    // the lock is released, so the flush syscall's cost doesn't serialize
    // across every symbol thread.
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

// bid_price_band_depths()/ask_price_band_depths() return an empty vector
// for a side with no BBO at all (see their own comments) rather than one
// entry per threshold - the only case where `bands.size()` doesn't match
// kPriceBandLabels.size().
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

class StreamCanceller;

// Registry of every currently-live StreamCanceller, so main()'s global
// `stop` flag can wake each one individually through its own private
// condition_variable, instead of every canceller sharing one cv/mutex
// pair - which would make any single stream's *natural* completion wake
// every other concurrently-subscribed stream's canceller thread too, a
// thundering-herd wakeup on every individual stream teardown, not just at
// actual shutdown, scaling with subscription count.
//
// This registry's own mutex is held only briefly, for registration/
// deregistration/notify-everyone bookkeeping - never while any canceller
// thread is actually blocked waiting - so it introduces no new contention
// on the wait path itself.
std::mutex& canceller_registry_mutex() {
    static std::mutex m;
    return m;
}
std::vector<StreamCanceller*>& canceller_registry() {
    static std::vector<StreamCanceller*> registry;
    return registry;
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
        : stop_(stop), thread_([this, &context] { run(context); }) {
        std::lock_guard<std::mutex> lock(canceller_registry_mutex());
        canceller_registry().push_back(this);
    }

    ~StreamCanceller() {
        // Deregister first, before touching anything else this instance
        // owns: notify_stop() (called concurrently from another thread by
        // main()'s stop path) only ever reaches instances still in the
        // registry, so removing `this` here - before mutex_/cv_/thread_
        // are torn down below - guarantees notify_stop() can never be
        // called on a partially-destroyed object.
        {
            std::lock_guard<std::mutex> lock(canceller_registry_mutex());
            std::erase(canceller_registry(), this);
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            read_loop_done_ = true;
        }
        cv_.notify_all();
        thread_.join();
    }

    StreamCanceller(const StreamCanceller&) = delete;
    StreamCanceller& operator=(const StreamCanceller&) = delete;

    // Called by main()'s stop path (via notify_all_of_stop() below) to
    // wake this one instance without touching any other canceller's own
    // wait. Must take `mutex_` - the same lock run()'s wait() uses - even
    // though `*stop_` itself is already a plain atomic write done by the
    // caller before this runs: the atomicity of the *value* alone doesn't
    // close the lost-wakeup window between a waiter re-checking its
    // predicate and actually starting to block. Taking `mutex_` here
    // serializes against exactly that transition.
    void notify_stop() {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_all();
    }

  private:
    void run(grpc::ClientContext& context) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return stop_->load() || read_loop_done_; });
        bool should_cancel = stop_->load();
        lock.unlock();
        if (should_cancel) context.TryCancel();
    }

    std::atomic<bool>* stop_;
    bool read_loop_done_ = false;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
};

// Wakes every currently-registered StreamCanceller - see
// canceller_registry()'s own comment for why each gets its own
// individually-locked notify rather than one shared broadcast.
void notify_all_of_stop() {
    std::lock_guard<std::mutex> lock(canceller_registry_mutex());
    for (StreamCanceller* canceller : canceller_registry()) canceller->notify_stop();
}

// Shared by publish_bbo()/publish_l2_bands()/list_books() so a future
// change to how a channel is built (credentials, keepalive, message-size
// limits) doesn't have to be made at three call sites in lockstep.
std::unique_ptr<Aggregator::Stub> make_stub(const std::string& address) {
    auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
    return Aggregator::NewStub(channel);
}

void publish_bbo(const std::string& address, const BookId& book_id, std::atomic<bool>* stop) {
    std::string label = to_string(book_id);
    auto stub = make_stub(address);

    grpc::ClientContext context;
    SubscribeBboRequest request;
    fill_wire_book_id(request.mutable_book(), book_id);
    auto reader = stub->SubscribeBbo(&context, request);

    BboUpdate update;
    long bbo_count = 0, heartbeat_count = 0;
    std::string last_line;
    // Printed only when it changes, not on every heartbeat (every second) -
    // see format_live_venues()'s own comment for why this is here at all.
    std::optional<std::string> last_live_venues;
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
                std::string live_venues = format_live_venues(update.heartbeat());
                if (live_venues != last_live_venues) {
                    print_line("[" + label + " BBO] " + live_venues);
                    last_live_venues = live_venues;
                }
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

// Unlike publish_bbo()/publish_l2_bands(), this is a single blocking unary
// call, not a stream, so it has no StreamCanceller/duration handling and
// runs straight from main(). Given its own deadline below (list takes no
// duration argument): without one, a server that accepts the connection
// but never replies would hang this call forever. Returns false on a
// non-OK status so main() can turn that into a non-zero exit code.
constexpr std::chrono::seconds kListBooksTimeout{10};

bool list_books(const std::string& address) {
    auto stub = make_stub(address);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + kListBooksTimeout);
    ListBooksRequest request;
    ListBooksResponse response;
    auto status = stub->ListBooks(&context, request, &response);
    if (!status.ok()) {
        std::cerr << "ListBooks failed: grpc_status=" << status.error_code() << " (" << status.error_message()
                   << ")\n";
        return false;
    }

    // fill_wire_book_id() (the only thing that produces this response,
    // server-side) never emits a malformed BookId, so this is unreached
    // against this project's own server. Kept as a defensive fallback
    // anyway: a future/buggy server on the other end of this wire is still
    // bound by the .proto contract, not by this binary.
    std::vector<std::string> labels;
    for (const auto& wire_book : response.books()) {
        auto book_id = to_symbol_book_id(wire_book);
        labels.push_back(book_id ? to_string(*book_id) : "(malformed book in response)");
    }
    // Order isn't guaranteed on the wire (server-side unordered_map
    // iteration) - sorted here purely so this CLI's output is stable
    // across runs for a human diffing them.
    std::sort(labels.begin(), labels.end());

    // An OK status with zero books printed would look identical to any
    // other successful-but-empty run - print an explicit marker instead so
    // "genuinely no books configured" is never indistinguishable from a
    // query that silently came back empty for some other reason.
    if (labels.empty()) {
        print_line("(server reports no configured books)");
        return true;
    }
    for (const auto& label : labels) print_line(label);
    return true;
}

// Shared by volume-bands and price-bands: both subscribe to SubscribeL2Diff
// and maintain the same local L2OrderBook, differing only in which bands
// get computed/printed from it on every update.
void publish_l2_bands(Mode mode, const std::string& address, const BookId& book_id, std::atomic<bool>* stop) {
    std::string label = to_string(book_id);
    auto stub = make_stub(address);

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
    // Printed only when it changes, not on every heartbeat (every second) -
    // see format_live_venues()'s own comment for why this is here at all.
    std::optional<std::string> last_live_venues;
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
                // Bbo's). A gap means a revision was missed and the locally
                // reconstructed book is no longer valid, so this stops
                // applying/publishing immediately rather than computing
                // bands off a silently-wrong book. This tool doesn't
                // resubscribe to recover - a GAP line is the operator's
                // signal to restart it.
                if (is_book_seq_gap(last_seq, diff.book_seq())) {
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
                std::string live_venues = format_live_venues(update.heartbeat());
                if (live_venues != last_live_venues) {
                    print_line("[" + label + " " + mode_tag + "] " + live_venues);
                    last_live_venues = live_venues;
                }
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
                 "       hermeneutic_aggregator_client <address> list\n"
                 "  duration_seconds <= 0 means run until interrupted or the server ends the stream\n"
                 "  books look like BTC_USDT.SPOT or BTC_USDT.PERP\n"
                 "  bbo:          subscribes to SubscribeBbo, prints best bid/ask on every update\n"
                 "  volume-bands: subscribes to SubscribeL2Diff, prints the VWAP needed to fill\n"
                 "                1M/5M/10M/25M/50M+ notional on each side on every update\n"
                 "  price-bands:  subscribes to SubscribeL2Diff, prints depth within\n"
                 "                50/100/200/500/1000+ bps of BBO on each side on every update\n"
                 "  list:         calls ListBooks and prints every book the server was started\n"
                 "                with, one per line, then exits (no duration/book arguments)\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        print_usage();
        return 1;
    }
    std::string address = argv[1];
    std::string mode_str = argv[2];

    auto mode = parse_mode(mode_str);
    if (!mode) {
        std::cerr << "unknown mode \"" << mode_str << "\" (expected bbo, volume-bands, price-bands, or list)\n";
        print_usage();
        return 1;
    }

    // list takes no duration/book arguments and needs neither a stream nor
    // a background thread - handled here, before the duration/book parsing
    // the other three modes share below, and returns directly rather than
    // falling into their thread-per-book dispatch.
    if (*mode == Mode::List) {
        if (argc != 3) {
            std::cerr << "list takes no further arguments\n";
            print_usage();
            return 1;
        }
        return list_books(address) ? 0 : 1;
    }

    if (argc < 5) {
        print_usage();
        return 1;
    }
    std::string duration_str = argv[3];
    int duration_s;
    try {
        std::size_t consumed = 0;
        duration_s = std::stoi(duration_str, &consumed);
        // std::stoi is a partial parse by design (e.g. "10abc" -> 10)
        // rather than rejecting trailing garbage, so the try/catch below
        // alone doesn't validate the whole argument; `consumed` short of
        // the full string length is that trailing-garbage case.
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

    std::atomic<bool> stop{false};
    std::vector<std::thread> threads;
    for (const auto& book_id : books) {
        if (*mode == Mode::Bbo) threads.emplace_back(publish_bbo, address, book_id, &stop);
        else threads.emplace_back(publish_l2_bands, *mode, address, book_id, &stop);
    }

    if (duration_s > 0) {
        std::this_thread::sleep_for(std::chrono::seconds(duration_s));
        // `stop` itself is a plain atomic write; the lost-wakeup window is
        // closed on the reader side instead (see StreamCanceller::
        // notify_stop()) - writing `stop` under a lock here wouldn't help,
        // since no single lock covers every canceller's own wait().
        stop = true;
        notify_all_of_stop();
    }
    for (auto& t : threads) t.join();
    return 0;
}

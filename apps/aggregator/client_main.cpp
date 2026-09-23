// Example/diagnostic client for hermeneutic_aggregator_service: subscribes
// to one or more books and publishes a chosen view of the order book to
// stdout. Unlike this project's earlier throwaway live-verification
// programs (written, run once, then deleted), this one is meant to stay -
// a starting point for whatever actually consumes the aggregator's output
// next, and a manual way to poke at a running aggregator instance during
// development.
//
// Three publisher modes plus one query mode, selected on the command line:
//   bbo           best bid/offer - subscribes to SubscribeBbo only.
//   volume-bands  VWAP needed to fill 1M/5M/10M/25M/50M+ notional (default,
//                 overridable via --volume-thresholds=) on each side -
//                 subscribes to SubscribeL2Diff and maintains a local
//                 order book.
//   price-bands   depth within 50/100/200/500/1000+ bps of BBO (default,
//                 overridable via --price-bps=) on each side - subscribes
//                 to SubscribeL2Diff and maintains a local order book.
//   list          calls the unary ListBooks RPC and prints every book the
//                 server was started with, then exits - unlike the other
//                 three modes, this takes no duration/book arguments.
// volume-bands/price-bands both track SubscribeL2Diff's own book_seq
// contiguity guarantee (aggregator.proto: "book_seq must be contiguous...
// a gap means a revision was missed") and log a GAP line loudly if that
// contract is ever violated, rather than silently tracking the counter and
// never checking it.
#include <grpcpp/grpcpp.h>

#include "apps/aggregator/band_config.hpp"
#include "apps/aggregator/book_id.hpp"
#include "apps/aggregator/client_book.hpp"
#include "bobby/hermeneutic/aggregator/aggregator.grpc.pb.h"
#include "bobby/hermeneutic/symbol/symbol.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <pthread.h>
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
using bobby::hermeneutic::aggregator::BandConfig;
using bobby::hermeneutic::aggregator::BboUpdate;
using bobby::hermeneutic::aggregator::CliFlags;
using bobby::hermeneutic::aggregator::extract_flags;
using bobby::hermeneutic::aggregator::fill_wire_book_id;
using bobby::hermeneutic::aggregator::format_bps_label;
using bobby::hermeneutic::aggregator::format_fixed;
using bobby::hermeneutic::aggregator::format_notional_label;
using bobby::hermeneutic::aggregator::Heartbeat;
using bobby::hermeneutic::aggregator::is_book_seq_gap;
using bobby::hermeneutic::aggregator::kDefaultPriceBandBps;
using bobby::hermeneutic::aggregator::kDefaultVolumeBandThresholds;
using bobby::hermeneutic::aggregator::L2Update;
using bobby::hermeneutic::aggregator::ListBooksRequest;
using bobby::hermeneutic::aggregator::ListBooksResponse;
using bobby::hermeneutic::aggregator::parse_price_bps;
using bobby::hermeneutic::aggregator::parse_volume_thresholds;
using bobby::hermeneutic::aggregator::PriceLevel;
using bobby::hermeneutic::aggregator::SubscribeBboRequest;
using bobby::hermeneutic::aggregator::SubscribeL2DiffRequest;
using bobby::hermeneutic::aggregator::to_symbol_book_id;
using bobby::hermeneutic::aggregator::build_labels;
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

// Guards a stream against interleaved writes from concurrent threads:
// each publish_bbo()/publish_l2_bands() call runs on its own thread (one
// per book - see main()), watch_for_shutdown_signal() runs on yet
// another, and a bare `os << a << b << c` is a sequence of independent
// stream operations, not one atomic write - two threads' chains can
// interleave mid-line into a single garbled, unparseable line. Callers
// build the complete line first (this file already does, via
// ostringstream) and hand it here as one string so the lock covers the
// entire write.
void print_to(std::ostream& os, std::mutex& mutex, const std::string& line) {
    {
        std::lock_guard lock(mutex);
        os << line << '\n';
    }
    // Flushed outside the lock: the write above is already complete once
    // the lock is released, so the flush syscall's cost doesn't serialize
    // across every symbol thread.
    os.flush();
}

void print_line(const std::string& line) {
    static std::mutex out_mutex;
    print_to(std::cout, out_mutex, line);
}

void print_err_line(const std::string& line) {
    static std::mutex err_mutex;
    print_to(std::cerr, err_mutex, line);
}

// One band's worth of `label=value`. `bands` is expected to line up
// index-for-index with `labels` (volume_band_prices() always returns
// exactly one entry per input threshold, even for an empty/thin book -
// see its own comment - so this always holds for a well-formed result,
// given `labels` was itself built from that same threshold list).
std::string format_volume_bands(const std::vector<VolumeBand>& bands, const std::vector<std::string>& labels) {
    std::ostringstream out;
    for (std::size_t i = 0; i < bands.size() && i < labels.size(); ++i) {
        if (i) out << ' ';
        out << labels[i] << '=';
        if (bands[i].vwap) out << format_fixed(*bands[i].vwap);
        else out << "NA(filled=" << format_fixed(bands[i].filled_notional) << ')';
    }
    return out.str();
}

// bid_price_band_depths()/ask_price_band_depths() return an empty vector
// for a side with no BBO at all (see their own comments) rather than one
// entry per threshold - the only case where `bands.size()` doesn't match
// `labels.size()`.
std::string format_price_bands(const std::vector<PriceBand>& bands, const std::vector<std::string>& labels) {
    if (bands.empty()) return "(no bbo)";
    std::ostringstream out;
    for (std::size_t i = 0; i < bands.size() && i < labels.size(); ++i) {
        if (i) out << ' ';
        out << labels[i] << '=' << format_fixed(bands[i].cumulative_size) << '@'
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
// on the wait path itself. Plain function-local statics, not leaked
// never-destroyed singletons: watch_for_shutdown_signal() runs on a
// thread main() always cancels and joins before returning (see
// ShutdownSignalWatcher below), so nothing can still be touching these
// once main() returns and static destruction begins.
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

// `*stop = true` plus waking every registered StreamCanceller, as one
// operation - both watch_for_shutdown_signal() and main()'s own
// duration_s > 0 path need exactly this pair done together; a future
// third thing that should happen on stop (a log line, a metric) then
// only needs to be added here once instead of at both call sites.
void request_stop(std::atomic<bool>* stop) {
    *stop = true;
    notify_all_of_stop();
}

// Used only to wake watch_for_shutdown_signal()'s thread out of
// sigwait() from ShutdownSignalWatcher's destructor when main() is
// tearing it down with no real shutdown ever requested (e.g. every
// stream ended on its own) - see that class's own comment for why a
// dedicated signal, not SIGINT/SIGTERM themselves, is used for this.
// SIGUSR1 is a real, externally-sendable signal with no kernel-enforced
// ownership - a `kill -USR1` from an operator or monitoring tool for
// some unrelated reason is a genuine possibility, not a hypothetical -
// see wait_for_real_signal()'s own comment for how that's told apart
// from this object's own use of it.
constexpr int kWatcherWakeupSignal = SIGUSR1;

// SIGINT/SIGTERM plus kWatcherWakeupSignal, as one sigset_t: main() (to
// block them on every thread) and watch_for_shutdown_signal() (to
// sigwait() on them) need the exact same set, so it's built in one place
// rather than two copies that could drift out of sync.
sigset_t shutdown_signal_set() {
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    sigaddset(&set, kWatcherWakeupSignal);
    return set;
}

// Unlike the server (server_main.cpp), this binary previously registered
// no SIGINT/SIGTERM handler at all: with duration_seconds <= 0 (every
// docker-compose client service), `stop`/notify_all_of_stop() were only
// ever reachable from the timer path in main(), so a signal just hit the
// OS's default disposition - immediate termination with no Finish()/join
// (or, if this process is a container's PID 1, no effect whatsoever,
// since the kernel skips default signal actions for PID 1 unless a
// handler is installed).
//
// Run on its own thread via sigwait() rather than a signal handler:
// main() blocks SIGINT/SIGTERM on every thread (via pthread_sigmask, done
// before any thread - including this one - is spawned, so the mask is
// inherited everywhere), which makes them pending-but-undelivered instead
// of asynchronously interrupting whatever this process happens to be
// doing. sigwait() then picks one up synchronously, so the rest of this
// function runs like any other application code - no async-signal-safety
// constraints.
//
// The first signal drives the same graceful stop the duration timer
// path already does. A second one means that path is stuck - e.g. a
// stream whose reader->Finish() is blocked on the server instead of on
// `stop` (see the TryCancel() call added at the book_seq-gap `break` in
// publish_l2_bands) - and hard-exits, mirroring server_main.cpp's own
// signal handler.
//
// Blocks until a genuine SIGINT/SIGTERM arrives, transparently looping
// past any kWatcherWakeupSignal delivery that *isn't* this object's own
// teardown wakeup: SIGUSR1 is a real, deliverable signal with no kernel-
// enforced ownership, so an operator or monitoring tool sending it for
// an unrelated reason (a common ad hoc convention) is otherwise
// indistinguishable from ShutdownSignalWatcher's own destructor - and
// mistaking it for that would return from watch_for_shutdown_signal()
// early, leaving nothing armed to catch a real signal afterward.
// `*teardown_requested`, set only by that destructor immediately before
// it sends the wakeup, is what actually disambiguates the two: this
// function only ever treats kWatcherWakeupSignal as "stop waiting" when
// that flag is already true, and otherwise just resumes waiting.
//
// Returns the real signal number, or -1 if this returned because
// `*teardown_requested` became true instead - the caller must check for
// that before treating the return value as a signal to log/act on.
// Failure is via std::_Exit(1), not a return value: sigwait() reports it
// via its return code (not errno) and leaves `signal_number` untouched,
// so an unchecked call could fall through into treating a failure as
// "received signal 0" and drive a shutdown nothing actually requested -
// this can only fail with EINVAL against `set`, built fresh by the
// caller and known valid, but that's exactly the kind of "impossible"
// this project's own std::stoi/consumed check elsewhere still validates
// rather than assumes.
int wait_for_real_signal(const sigset_t& set, std::atomic<bool>* teardown_requested) {
    for (;;) {
        int signal_number = 0;
        if (int err = sigwait(&set, &signal_number); err != 0) {
            print_err_line(std::string("sigwait failed: ") + std::strerror(err));
            std::_Exit(1);
        }
        if (signal_number != kWatcherWakeupSignal) return signal_number;
        if (teardown_requested->load()) return -1;
        // Else: some unrelated SIGUSR1 landed here - not a real shutdown
        // signal and not our own teardown, so just keep waiting.
    }
}

// kWatcherWakeupSignal (via `teardown_requested`) is checked at *both*
// wait_for_real_signal() calls below, not just whichever one main()
// usually reaches: after a real SIGINT/SIGTERM already fired, this
// thread is parked in the *second* wait by the time main() tears it
// down (see ShutdownSignalWatcher) - if that call didn't also recognize
// the wakeup, it would fall into the "signal again" branch and hard-exit
// with a nonzero code on every ordinary graceful shutdown, not just a
// genuinely stuck one.
void watch_for_shutdown_signal(std::atomic<bool>* stop, std::atomic<bool>* teardown_requested) {
    sigset_t set = shutdown_signal_set();
    int signal_number = wait_for_real_signal(set, teardown_requested);
    if (signal_number < 0) return;  // torn down, no shutdown ever requested
    print_err_line("received signal " + std::to_string(signal_number) + ", shutting down");
    request_stop(stop);
    signal_number = wait_for_real_signal(set, teardown_requested);
    if (signal_number < 0) return;  // torn down after a graceful shutdown
    print_err_line("received signal " + std::to_string(signal_number) + " again, forcing exit");
    std::_Exit(1);
}

// Owns watch_for_shutdown_signal()'s background thread and guarantees it
// is woken and joined before this object is destroyed - main() declares
// one of these and lets scope handle it on every return path (including
// the list-mode early return). This is what makes it safe for
// canceller_registry_mutex()/canceller_registry() above to be plain
// function-local statics rather than never-destroyed leaked singletons:
// nothing can still be running on that thread, touching those objects,
// once main() has returned and static destruction begins.
//
// pthread_kill() with a dedicated signal, not pthread_cancel(): sigwait()
// is blocking and there is no way to wake it early short of delivering
// an actual signal from its watched set. An earlier version of this
// class used pthread_cancel() (sigwait() is a documented POSIX
// cancellation point), verified empirically to interrupt it cleanly on
// macOS - but pthread_cancel is a forced *unwind* on glibc/Linux (this
// binary's actual deployment target, per docker/Dockerfile), and if this
// function - or anything it calls - ever gains a `catch (...)` that
// doesn't rethrow, that unwind hitting it aborts the process
// ("FATAL: exception not rethrown"). Signalling instead sidesteps that
// class of risk entirely: the thread always returns from
// watch_for_shutdown_signal() normally, so join() can't hang on it and
// pthread_kill()'s own return value is meaningfully checkable, unlike a
// destructor built around a call that either works or the thread never
// comes back.
class ShutdownSignalWatcher {
  public:
    // teardown_requested_ declared (and therefore constructed) before
    // thread_, not just listed first in the initializer list below: the
    // spawned thread starts running as soon as thread_'s own constructor
    // launches it, potentially before this constructor body even runs,
    // so teardown_requested_ must already be fully initialized by then -
    // C++ constructs members in declaration order regardless of
    // initializer-list order.
    explicit ShutdownSignalWatcher(std::atomic<bool>* stop)
        : thread_(watch_for_shutdown_signal, stop, &teardown_requested_) {}

    ~ShutdownSignalWatcher() {
        // Set before sending the wakeup, not after - see
        // wait_for_real_signal()'s own comment for what this
        // disambiguates it from.
        teardown_requested_ = true;
        if (int err = pthread_kill(thread_.native_handle(), kWatcherWakeupSignal); err != 0) {
            print_err_line(std::string("pthread_kill failed: ") + std::strerror(err));
        }
        thread_.join();
    }

    ShutdownSignalWatcher(const ShutdownSignalWatcher&) = delete;
    ShutdownSignalWatcher& operator=(const ShutdownSignalWatcher&) = delete;

  private:
    std::atomic<bool> teardown_requested_{false};
    std::thread thread_;
};

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

    BboUpdate update;
    long bbo_count = 0, heartbeat_count = 0;
    std::string last_line;
    // Printed only when it changes, not on every heartbeat (every second) -
    // see format_live_venues()'s own comment for why this is here at all.
    std::optional<std::string> last_live_venues;
    std::unique_ptr<grpc::ClientReader<BboUpdate>> reader;
    {
        // Constructed before stub->SubscribeBbo() below, not after: that
        // call's own ClientReader constructor blocks until the request is
        // actually written out, which needs a real connection - against
        // an unresponsive address/black-holed network, this can block for
        // as long as gRPC's own connect-attempt/backoff takes. Without a
        // canceller already running, `stop` firing during that window has
        // nothing to act on, and this stream doesn't react to a shutdown
        // signal until the second-signal hard-exit backstop.
        StreamCanceller canceller(context, stop);
        reader = stub->SubscribeBbo(&context, request);
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

// Given its own deadline below (list takes no duration argument):
// without one, a server that accepts the connection but never replies
// would hang this call for however long `stop` takes to fire (or
// forever, before main() grew signal handling at all). Wrapped in a
// StreamCanceller exactly like publish_bbo()/publish_l2_bands()'s
// streams so a shutdown signal cuts this short too, rather than every
// other mode reacting to one within milliseconds while this one still
// sits out the rest of kListBooksTimeout. Returns false on a non-OK
// status so main() can turn that into a non-zero exit code.
constexpr std::chrono::seconds kListBooksTimeout{10};

bool list_books(const std::string& address, std::atomic<bool>* stop) {
    auto stub = make_stub(address);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + kListBooksTimeout);
    ListBooksRequest request;
    ListBooksResponse response;
    grpc::Status status;
    {
        StreamCanceller canceller(context, stop);
        status = stub->ListBooks(&context, request, &response);
    }  // canceller destroyed here: joins its thread before this call's status is inspected below.
    if (!status.ok()) {
        // A CANCELLED status while `*stop` is set means this call's own
        // StreamCanceller cut it short because a shutdown signal arrived
        // mid-RPC - the operator asked list mode to stop, and it did.
        // That's not a failure of the list query itself (a bad address,
        // an actual server error) and shouldn't be reported as one: a
        // script checking main()'s exit code needs to tell "you asked
        // for this" apart from "something is actually wrong," and
        // returning false here (main() exits 1) would make a deliberate
        // Ctrl-C indistinguishable from a real ListBooks failure.
        if (stop->load() && status.error_code() == grpc::StatusCode::CANCELLED) {
            print_err_line("list cancelled by shutdown signal");
            return true;
        }
        // print_err_line(), not a bare std::cerr chain - see its own
        // comment: this now runs concurrently with
        // watch_for_shutdown_signal()'s own stderr writes (list mode
        // gained a StreamCanceller above, so a shutdown signal arriving
        // while ListBooks is still in flight can land at the same
        // instant this fires), and an unlocked multi-op chain here would
        // reopen exactly the interleaving hazard those exist to close.
        std::ostringstream line;
        line << "ListBooks failed: grpc_status=" << status.error_code() << " (" << status.error_message() << ")";
        print_err_line(line.str());
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
void publish_l2_bands(Mode mode, const std::string& address, const BookId& book_id, std::atomic<bool>* stop,
                       const BandConfig& band_config) {
    std::string label = to_string(book_id);
    auto stub = make_stub(address);

    grpc::ClientContext context;
    SubscribeL2DiffRequest request;
    fill_wire_book_id(request.mutable_book(), book_id);

    const char* mode_tag = mode == Mode::VolumeBands ? "VOLUME_BANDS" : "PRICE_BANDS";
    L2OrderBook book;

    auto print_bands = [&](std::uint64_t book_seq) -> std::string {
        std::ostringstream line;
        line << "book_seq=" << book_seq;
        if (mode == Mode::VolumeBands) {
            auto bids = bid_volume_band_prices(book, band_config.volume_thresholds);
            auto asks = ask_volume_band_prices(book, band_config.volume_thresholds);
            line << " bid["
                 << (bids.has_value() ? format_volume_bands(*bids, band_config.volume_labels) : std::string("ERROR"))
                 << "]";
            line << " ask["
                 << (asks.has_value() ? format_volume_bands(*asks, band_config.volume_labels) : std::string("ERROR"))
                 << "]";
        } else {
            auto bids = bid_price_band_depths(book, band_config.price_bps);
            auto asks = ask_price_band_depths(book, band_config.price_bps);
            line << " bid["
                 << (bids.has_value() ? format_price_bands(*bids, band_config.price_labels) : std::string("ERROR"))
                 << "]";
            line << " ask["
                 << (asks.has_value() ? format_price_bands(*asks, band_config.price_labels) : std::string("ERROR"))
                 << "]";
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
    std::unique_ptr<grpc::ClientReader<L2Update>> reader;
    {
        // See the identical comment in publish_bbo(): constructed before
        // stub->SubscribeL2Diff() below so a `stop` firing while that
        // call itself is still blocked (establishing the stream) has a
        // canceller already running to act on it.
        StreamCanceller canceller(context, stop);
        reader = stub->SubscribeL2Diff(&context, request);
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
                    // The RPC is still fully live here - Read() didn't
                    // return false and the server hasn't ended the stream -
                    // unlike a natural loop exit. `stop` itself was never
                    // set, so StreamCanceller won't cancel on its own (see
                    // its class comment); without this, reader->Finish()
                    // below would block until the server independently ends
                    // the call (its subscriber queue overflowing, or the
                    // server shutting down).
                    context.TryCancel();
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

// Shared by main()'s --volume-thresholds=/--price-bps= override blocks,
// which otherwise differ only in field/flag names and the hint text: on
// no override, `*target` is left as whatever main() already defaulted it
// to; on a present-but-malformed one, prints a single diagnostic line
// naming `flag_name` and returns false (main() turns that into a non-zero
// exit); on success, replaces `*target` with the parsed list.
template <typename T, typename ParseFn>
bool apply_band_override(const std::optional<std::string>& csv, ParseFn parse_fn, std::string_view flag_name,
                          std::string_view hint, std::vector<T>* target) {
    if (!csv) return true;
    auto parsed = parse_fn(*csv);
    if (!parsed) {
        std::cerr << "invalid " << flag_name << "=\"" << *csv << "\" (" << hint << ")\n";
        return false;
    }
    *target = *std::move(parsed);
    return true;
}

void print_usage() {
    std::cerr << "usage: hermeneutic_aggregator_client <address> <bbo|volume-bands|price-bands> "
                 "[--volume-thresholds=<n1,n2,...>] [--price-bps=<b1,b2,...>]\n"
                 "       <duration_seconds> <book1> [book2 ...]\n"
                 "       hermeneutic_aggregator_client <address> list\n"
                 "  duration_seconds <= 0 means run until interrupted or the server ends the stream\n"
                 "  books look like BTC_USDT.SPOT or BTC_USDT.PERP\n"
                 "  bbo:          subscribes to SubscribeBbo, prints best bid/ask on every update\n"
                 "  volume-bands: subscribes to SubscribeL2Diff, prints the VWAP needed to fill\n"
                 "                1M/5M/10M/25M/50M+ notional (default) on each side on every update\n"
                 "  price-bands:  subscribes to SubscribeL2Diff, prints depth within\n"
                 "                50/100/200/500/1000+ bps (default) of BBO on each side on every update\n"
                 "  list:         calls ListBooks and prints every book the server was started\n"
                 "                with, one per line, then exits (no duration/book arguments)\n"
                 "  --volume-thresholds=<n1,n2,...>  overrides volume-bands' notional thresholds\n"
                 "                                    (comma-separated integers, ascending, strictly\n"
                 "                                    positive - no decimals or scientific notation,\n"
                 "                                    e.g. 1000000,5000000,10000000,25000000,50000000)\n"
                 "  --price-bps=<b1,b2,...>           overrides price-bands' bps thresholds\n"
                 "                                    (comma-separated, ascending, in [0, 10000),\n"
                 "                                    e.g. 50,100,200,500,1000)\n"
                 "  flags may appear anywhere on the command line; the last band in each list is\n"
                 "  labeled with a trailing '+' (e.g. \"50M+\") to mark it as the highest/open-ended one\n";
}

}  // namespace

int main(int argc, char** argv) {
    // Flags are pulled out before any positional parsing below, so
    // --volume-thresholds=/--price-bps= can appear anywhere on the command
    // line and the remaining positional arguments - address, mode,
    // duration, books - line up exactly as they did before these flags
    // existed.
    std::vector<std::string> args(argv + 1, argv + argc);
    auto flags_result = extract_flags(args);
    if (!flags_result) {
        std::cerr << flags_result.error() << '\n';
        print_usage();
        return 1;
    }
    CliFlags flags = *std::move(flags_result);

    if (args.size() < 2) {
        print_usage();
        return 1;
    }
    std::string address = args[0];
    std::string mode_str = args[1];

    auto mode = parse_mode(mode_str);
    if (!mode) {
        std::cerr << "unknown mode \"" << mode_str << "\" (expected bbo, volume-bands, price-bands, or list)\n";
        print_usage();
        return 1;
    }

    // Set up before the list-mode early return just below, not after it
    // (alongside the other three modes' own thread spawns further down):
    // list_books() is a single blocking unary call with its own
    // kListBooksTimeout deadline, but with no shutdown signal wired up
    // yet, a slow/unresponsive server would still leave a signal with no
    // effect until that deadline - list mode gets the exact same prompt
    // shutdown the other three modes do, not "eventually, within 10s."
    std::atomic<bool> stop{false};

    // Blocked here, before any thread (including the watcher spawned
    // right below) exists, so every thread this process ever creates
    // inherits the same blocked mask - see watch_for_shutdown_signal()'s
    // own comment for why that matters.
    sigset_t blocked_signals = shutdown_signal_set();
    // Checked, not fire-and-forget: an unnoticed failure here would leave
    // SIGINT/SIGTERM unblocked on this thread and every thread spawned
    // below, silently reproducing this binary's pre-fix behavior (default
    // disposition - immediate termination with no Finish()/join, or none
    // at all as a container's PID 1) with nothing in the output pointing
    // at why.
    if (int err = pthread_sigmask(SIG_BLOCK, &blocked_signals, nullptr); err != 0) {
        print_err_line(std::string("pthread_sigmask failed: ") + std::strerror(err));
        return 1;
    }
    // Declared (not detached) so its destructor - which cancels and
    // joins the watcher thread, see ShutdownSignalWatcher's own comment -
    // runs on every return path out of main() from here on, including
    // the list-mode early return just below.
    ShutdownSignalWatcher signal_watcher(&stop);

    // list takes no duration/book arguments and needs neither a stream nor
    // a background thread - handled here, before the duration/book parsing
    // the other three modes share below, and returns directly rather than
    // falling into their thread-per-book dispatch. It also takes neither
    // band flag, since it never computes bands.
    if (*mode == Mode::List) {
        if (args.size() != 2 || flags.volume_thresholds_csv || flags.price_bps_csv) {
            std::cerr << "list takes no further arguments\n";
            print_usage();
            return 1;
        }
        return list_books(address, &stop) ? 0 : 1;
    }

    // Band thresholds: this tool's own defaults unless overridden by
    // --volume-thresholds=/--price-bps=, validated here at this program's
    // own input boundary so a malformed flag is a clear startup error
    // rather than a silent "ERROR" on every printed line once streaming
    // starts. Only the flag matching `*mode` is applied - bbo never reads
    // band_config at all, and volume-bands/price-bands each only read
    // their own half of it - so an irrelevant flag (e.g. --price-bps= on
    // a bbo run) can't abort a run it has no effect on; a non-fatal note
    // below still surfaces that it was given and ignored, rather than
    // leaving that silent.
    BandConfig band_config;
    band_config.volume_thresholds.assign(kDefaultVolumeBandThresholds.begin(), kDefaultVolumeBandThresholds.end());
    band_config.price_bps.assign(kDefaultPriceBandBps.begin(), kDefaultPriceBandBps.end());
    if (*mode == Mode::VolumeBands) {
        if (!apply_band_override(flags.volume_thresholds_csv, parse_volume_thresholds, "--volume-thresholds",
                                  "expected a comma-separated, ascending, strictly positive list of integers - "
                                  "no decimals or scientific notation - e.g. 1000000,5000000,10000000,25000000,50000000",
                                  &band_config.volume_thresholds)) {
            print_usage();
            return 1;
        }
    } else if (flags.volume_thresholds_csv) {
        std::cerr << "note: --volume-thresholds= has no effect in " << mode_str << " mode (ignored)\n";
    }
    if (*mode == Mode::PriceBands) {
        if (!apply_band_override(flags.price_bps_csv, parse_price_bps, "--price-bps",
                                  "expected a comma-separated, ascending bps list in [0, 10000), "
                                  "e.g. 50,100,200,500,1000",
                                  &band_config.price_bps)) {
            print_usage();
            return 1;
        }
    } else if (flags.price_bps_csv) {
        std::cerr << "note: --price-bps= has no effect in " << mode_str << " mode (ignored)\n";
    }
    band_config.volume_labels = build_labels(band_config.volume_thresholds, format_notional_label);
    band_config.price_labels = build_labels(band_config.price_bps, format_bps_label);

    if (args.size() < 4) {
        print_usage();
        return 1;
    }
    std::string duration_str = args[2];
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
    for (std::size_t i = 3; i < args.size(); ++i) {
        auto book_id = bobby::hermeneutic::symbol::parse_book_id(args[i]);
        if (!book_id) {
            std::cerr << "invalid book \"" << args[i] << "\" (expected e.g. BTC_USDT.SPOT or BTC_USDT.PERP)\n";
            return 1;
        }
        books.push_back(*book_id);
    }

    std::vector<std::thread> threads;
    for (const auto& book_id : books) {
        if (*mode == Mode::Bbo) threads.emplace_back(publish_bbo, address, book_id, &stop);
        // std::cref(), not a plain `band_config`: std::thread stores a
        // decayed copy of every argument, so passing the struct itself
        // would copy its four vectors (including the label strings) once
        // per book thread, even though every thread reads the identical,
        // never-mutated-after-this-point band_config for the run's entire
        // duration - band_config outlives every thread (this loop's join()
        // below runs before main() returns, so before it's destroyed).
        else threads.emplace_back(publish_l2_bands, *mode, address, book_id, &stop, std::cref(band_config));
    }

    if (duration_s > 0) {
        // Polls `stop` on a short interval rather than a bare sleep_for()
        // for the full duration: a signal reaching
        // watch_for_shutdown_signal() while this is "asleep" must be able
        // to cut this wait short, or a duration_s > 0 run would keep the
        // whole process (not just its book-publisher threads, which do
        // stop early via each stream's own StreamCanceller) alive until
        // the timer expires even after being asked to shut down. A poll
        // is simpler than a second mutex+condvar pair purely to wake this
        // early, and the added latency (at most kPollInterval) is
        // negligible next to the multi-second/full-duration hang this
        // was fixed to close - this isn't even the deployed case anyway
        // (every docker-compose client service uses duration_s <= 0).
        constexpr auto kPollInterval = std::chrono::milliseconds(100);
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(duration_s);
        while (!stop.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(kPollInterval);
        }
        // Skipped, not called unconditionally, when the loop above exited
        // because a shutdown signal already called this itself: harmless
        // either way today (request_stop() is idempotent), but redundant
        // work regardless - a second full canceller_registry() notify
        // sweep over whatever's still registered - that only grows if
        // request_stop() ever gains real work of its own, as its own
        // comment anticipates.
        if (!stop.load()) request_stop(&stop);
    }
    for (auto& t : threads) t.join();
    return 0;
}

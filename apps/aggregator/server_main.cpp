#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <simdjson.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <ranges>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "apps/aggregator/aggregator_service.hpp"
#include "bobby/hermeneutic/exchange/binance/binance_futures_feed.hpp"
#include "bobby/hermeneutic/exchange/binance/binance_futures_sequence_policy.hpp"
#include "bobby/hermeneutic/exchange/binance/binance_spot_feed.hpp"
#include "bobby/hermeneutic/exchange/binance/binance_spot_sequence_policy.hpp"
#include "bobby/hermeneutic/exchange/bybit/bybit_feed.hpp"
#include "bobby/hermeneutic/exchange/bybit/bybit_sequence_policy.hpp"
#include "bobby/hermeneutic/exchange/okx/okx_feed.hpp"
#include "bobby/hermeneutic/exchange/okx/okx_sequence_policy.hpp"
#include "bobby/hermeneutic/ingestion/book_subscription.hpp"
#include "bobby/hermeneutic/ingestion/ingestion_runner.hpp"
#include "bobby/hermeneutic/ingestion/venue_session.hpp"
#include "bobby/hermeneutic/symbol/symbol.hpp"

namespace {

namespace net = boost::asio;

using bobby::hermeneutic::aggregator::SymbolBook;
using bobby::hermeneutic::ingestion::VenueSubscription;
using bobby::hermeneutic::symbol::Exchange;
using bobby::hermeneutic::symbol::MarketType;
using bobby::hermeneutic::symbol::NativeSymbol;
using bobby::hermeneutic::symbol::VenueId;

// Every venue's contribution, gathered from the flat VenueSubscription
// list into what IngestionRunner::add() actually wants: one native-symbol
// list plus one SymbolRegistry per venue, not one per symbol. See the
// grouping loop in main() below.
struct VenueGroup {
    std::vector<NativeSymbol> native_symbols;
    bobby::hermeneutic::ingestion::SymbolRegistry<SymbolBook> registry;
};

// Keyed by VenueId (not a hand-rolled std::pair<Exchange, MarketType>,
// a second, independent spelling of the exact same pairing VenueId
// already owns). unordered_map, not map: nothing needs to sort this.
using VenueGroups = std::unordered_map<VenueId, VenueGroup>;

VenueGroup* find_group(VenueGroups& groups, const VenueId& venue_id) {
    auto it = groups.find(venue_id);
    return it == groups.end() ? nullptr : &it->second;
}

// Registers `venue_id`'s group with `runner`, if the config actually
// asked for it - a no-op otherwise (e.g. a config with no OKX venues at
// all never touches OkxFeed). One instantiation of this per (Feed, Policy)
// pair means a fix here (or a future venue) only has to happen once.
// `group->native_symbols`/`group->registry` are moved out, not copied -
// `groups` isn't read again afterward - and `wired_venues`/`wired_venue_ids`
// are grown here so the caller can both log what was actually wired
// and, below, fail startup if the config asked for a venue none of these
// calls claimed.
template <typename Feed, typename Policy>
void wire_venue(bobby::hermeneutic::ingestion::IngestionRunner& runner, VenueGroups& groups,
                 const VenueId& venue_id, std::vector<std::string>& wired_venues,
                 std::unordered_set<VenueId>& wired_venue_ids, net::any_io_executor executor,
                 net::ssl::context* ssl_ctx,
                 const bobby::hermeneutic::ingestion::IdleTimeoutConfig& idle_timeout_config) {
    auto* group = find_group(groups, venue_id);
    if (!group) return;
    // to_string() is only for wired_venues, this function's own
    // startup-log contribution - `venue_id` itself, not its string form,
    // is what actually gets wired into IngestionRunner/VenueSession/
    // AggregateOrderBook below.
    wired_venues.push_back(bobby::hermeneutic::symbol::to_string(venue_id));
    wired_venue_ids.insert(venue_id);
    runner.add<Feed, Policy, net::ssl::stream<boost::beast::tcp_stream>, SymbolBook>(
        Feed{}, venue_id, std::move(group->native_symbols), std::move(group->registry), executor, ssl_ctx,
        idle_timeout_config.for_venue(venue_id));
}

}  // namespace

int main(int argc, char** argv) {
    std::string address = argc > 1 ? argv[1] : "0.0.0.0:50051";

    // A required path to a JSON subscription config - see
    // bobby/hermeneutic/ingestion/book_subscription.hpp for the document
    // shape (apps/aggregator/subscriptions.example.json has a worked
    // example). Each book independently names exactly the venues that feed
    // it (SPOT-book venues serve that symbol's spot market, PERP-book
    // venues its perpetual/futures/swap market - book_subscription.hpp's
    // native_symbol() enforces this by construction). No built-in default:
    // a config file is meant to be an explicit, operator-owned artifact,
    // not a value baked into the binary that's easy to forget is even there.
    if (argc <= 2) {
        std::cerr << "usage: hermeneutic_aggregator_service [address] <subscription-config.json>\n";
        return 1;
    }
    // Read once, shared by both parse_*() calls below rather than each
    // opening and fully simdjson-parsing this same file independently.
    // Safe to share: each parse_*() function makes its own private
    // simdjson::padded_string copy of the text it's given rather than
    // parsing config_json in place, so the two parses don't interfere.
    simdjson::padded_string config_json;
    if (auto error = simdjson::padded_string::load(argv[2]).get(config_json)) {
        std::cerr << "failed to read subscription config \"" << argv[2]
                   << "\": " << simdjson::error_message(error) << '\n';
        return 1;
    }

    auto parsed = bobby::hermeneutic::ingestion::parse_book_subscriptions(std::string_view(config_json));
    if (!parsed) {
        std::cerr << "invalid subscription config: " << parsed.error() << '\n';
        return 1;
    }
    const std::vector<VenueSubscription>& subscriptions = *parsed;

    // Same config file, a separate (optional) section - see
    // book_subscription.hpp's IdleTimeoutConfig/parse_idle_timeout_config()
    // for the document shape and why this is parsed independently of
    // parse_book_subscriptions() above.
    auto idle_timeout_config =
        bobby::hermeneutic::ingestion::parse_idle_timeout_config(std::string_view(config_json));
    if (!idle_timeout_config) {
        std::cerr << "invalid subscription config: " << idle_timeout_config.error() << '\n';
        return 1;
    }

    // Same config file, a separate (optional) field - see
    // book_subscription.hpp's parse_io_threads() for the document shape,
    // why an absent field keeps single-thread behavior, and the one
    // lock-contention tradeoff N>1 introduces.
    auto io_threads = bobby::hermeneutic::ingestion::parse_io_threads(std::string_view(config_json));
    if (!io_threads) {
        std::cerr << "invalid subscription config: " << io_threads.error() << '\n';
        return 1;
    }

    // AggregatorService's book set is keyed by symbol::BookId - deduplicated
    // here, in first-seen order, since several subscriptions (one per venue)
    // share the same book_id and AggregatorService requires unique entries.
    std::vector<bobby::hermeneutic::symbol::BookId> book_symbols;
    {
        std::unordered_set<bobby::hermeneutic::symbol::BookId> seen;
        book_symbols.reserve(subscriptions.size());
        for (const auto& sub : subscriptions) {
            if (seen.insert(sub.book_id).second) book_symbols.push_back(sub.book_id);
        }
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

    // AggregatorService has no timer of its own; drive it here so a
    // subscriber can tell "book genuinely unchanged" apart from
    // "aggregator/feed stalled" during a quiet period, well inside the
    // keepalive timeout above so it isn't the only sign of life on an
    // idle connection.
    //
    // Joined at the end of main(), not detached: a detached thread still
    // alive (mid-sleep) when main() reaches the end of this function
    // would call service.send_heartbeat() on an object that's about to be
    // (or already is) destroyed.
    constexpr auto kHeartbeatInterval = std::chrono::seconds(1);
    std::atomic<bool> heartbeat_stop{false};
    std::thread heartbeat_thread([&service, &heartbeat_stop, kHeartbeatInterval] {
        while (!heartbeat_stop) {
            std::this_thread::sleep_for(kHeartbeatInterval);
            if (heartbeat_stop) break;
            service.send_heartbeat();
        }
    });

    // Ingestion: one VenueSession per (venue, book type) pair the config
    // above actually asked for, feeding the same AggregatorService the
    // gRPC server above exposes. SymbolBook::apply_batch/invalidate_venue
    // are per-VenueId, so several venues safely aggregate into the same
    // book. A real TLS context, not the plain-TCP instantiation the tests
    // use: this is the path that runs against real exchanges.
    net::ssl::context ssl_ctx(net::ssl::context::tlsv12_client);
    ssl_ctx.set_default_verify_paths();
    ssl_ctx.set_verify_mode(net::ssl::verify_peer);

    // Grouped by (venue, type) rather than kept as the flat
    // VenueSubscription list: IngestionRunner::add() wants one native-symbol
    // list and one SymbolRegistry per venue session, not one call per
    // symbol. Each subscription's own registry entry is keyed by its
    // native_symbol (what the venue's Feed subscribes with and what
    // dispatch_snapshot/dispatch_depth_update look it up by), pointing at
    // the book its BookId resolves to.
    ::VenueGroups groups;
    for (const auto& sub : subscriptions) {
        auto& group = groups[sub.venue_id];
        group.native_symbols.push_back(sub.native_symbol);
        // book_symbols above is derived from this same `subscriptions`
        // list, so this can never miss.
        auto* book = service.book(sub.book_id);
        assert(book);
        group.registry.add(sub.native_symbol, book);
    }

    net::io_context io;
    bobby::hermeneutic::ingestion::IngestionRunner runner;
    std::vector<std::string> wired_venues;
    std::unordered_set<VenueId> wired_venue_ids;

    wire_venue<bobby::hermeneutic::ingestion::BinanceSpotFeed, bobby::hermeneutic::BinanceSpotSequencePolicy>(
        runner, groups, VenueId{Exchange::Binance, MarketType::Spot}, wired_venues, wired_venue_ids,
        io.get_executor(), &ssl_ctx, *idle_timeout_config);
    wire_venue<bobby::hermeneutic::ingestion::BinanceFuturesFeed, bobby::hermeneutic::BinanceFuturesSequencePolicy>(
        runner, groups, VenueId{Exchange::Binance, MarketType::Perp}, wired_venues, wired_venue_ids,
        io.get_executor(), &ssl_ctx, *idle_timeout_config);
    wire_venue<bobby::hermeneutic::ingestion::BybitSpotFeed, bobby::hermeneutic::BybitSequencePolicy>(
        runner, groups, VenueId{Exchange::Bybit, MarketType::Spot}, wired_venues, wired_venue_ids,
        io.get_executor(), &ssl_ctx, *idle_timeout_config);
    wire_venue<bobby::hermeneutic::ingestion::BybitLinearFeed, bobby::hermeneutic::BybitSequencePolicy>(
        runner, groups, VenueId{Exchange::Bybit, MarketType::Perp}, wired_venues, wired_venue_ids,
        io.get_executor(), &ssl_ctx, *idle_timeout_config);
    // OKX is a single Feed/Policy pair covering both spot and perpetual
    // swap instIds (the `books` channel is protocol-identical for both -
    // see okx_feed.hpp); "okx_spot"/"okx_swap" are two independent
    // VenueSessions of that same Feed type, one per book type, matching
    // every other venue's shape here.
    wire_venue<bobby::hermeneutic::ingestion::OkxFeed, bobby::hermeneutic::OkxSequencePolicy>(
        runner, groups, VenueId{Exchange::Okx, MarketType::Spot}, wired_venues, wired_venue_ids, io.get_executor(),
        &ssl_ctx, *idle_timeout_config);
    wire_venue<bobby::hermeneutic::ingestion::OkxFeed, bobby::hermeneutic::OkxSequencePolicy>(
        runner, groups, VenueId{Exchange::Okx, MarketType::Perp}, wired_venues, wired_venue_ids, io.get_executor(),
        &ssl_ctx, *idle_timeout_config);

    // Shared tail for both "config names a venue that isn't actually
    // wired" checks below, so a future third such check gets the same
    // shutdown sequence for free. Returns whether it fired, so each call
    // site's own `return 1;` stays visible there rather than this helper
    // silently ending main() from inside a lambda.
    auto fail_on_bad_venues = [&](const std::vector<std::string>& bad_venues, std::string_view what,
                                    std::string_view consequence) {
        if (bad_venues.empty()) return false;
        std::cerr << "fatal: " << what << " names " << bad_venues.size() << " venue(s) " << consequence << ":";
        for (const auto& venue : bad_venues) std::cerr << ' ' << venue;
        std::cerr << std::endl;
        heartbeat_stop = true;
        heartbeat_thread.join();
        server->Shutdown();
        return true;
    };

    // Fail startup, rather than run with silent zero-data ingestion for
    // some book, if the config named a (venue, market type) none of the
    // wire_venue<>() calls above claimed - e.g. a typo'd/future Exchange
    // enumerator, or a market type this binary has no Feed for yet.
    // Without this check, `groups` still holds that venue's entry, the
    // process starts and listens normally, and the operator only
    // discovers the gap when that venue's book quietly never receives an
    // update.
    auto unwired_venues =
        bobby::hermeneutic::symbol::venues_missing_from(std::views::keys(groups), wired_venue_ids);
    if (fail_on_bad_venues(unwired_venues, "subscription config",
                            "this binary has no wire_venue<>() call for (they would silently receive zero "
                            "ingestion)")) {
        return 1;
    }

    // Same failure class as the unwired-venue check above, for
    // venue_idle_timeout_overrides: an override naming a venue that isn't
    // actually subscribed would otherwise parse successfully and silently
    // do nothing, leaving an operator believing their tuning took effect
    // when it didn't.
    auto unused_overrides = bobby::hermeneutic::symbol::venues_missing_from(
        std::views::keys(idle_timeout_config->overrides), wired_venue_ids);
    if (fail_on_bad_venues(unused_overrides, "venue_idle_timeout_overrides",
                            "not present in \"books\" (the override would silently do nothing)")) {
        return 1;
    }

    // Lets a real SIGINT/SIGTERM (Ctrl-C, `docker stop`) make
    // server->Wait() below return, instead of the OS killing the process
    // outright. Delivered through `io`'s own event loop (runs on
    // io_thread) rather than a raw signal()/sigaction() handler, so the
    // callback isn't restricted to async-signal-safe calls.
    //
    // grpc_shutdown_thread, not an inline server->Shutdown() call: the
    // handler runs on io_thread, and Shutdown() blocks for up to
    // kGrpcShutdownDeadline draining in-flight RPCs - doing that inline
    // would stall every VenueSession's read/write/reconnect-timer
    // dispatch on `io` for that whole window. Joined right after
    // server->Wait() returns below, never detached, for the same
    // use-after-free reason as heartbeat_thread. Guarded with joinable(),
    // not asserted: Server::Wait() returns immediately without this
    // thread ever starting if `started_` is false, and joining a
    // default-constructed std::thread is std::terminate, not merely a
    // wrong value.
    //
    // A deadline, not a bare Shutdown(): AggregatorService's subscriber
    // streams are intentionally long-lived, so a subscriber that never
    // disconnects would otherwise make Shutdown() wait forever.
    // kGrpcShutdownDeadline only bounds grpc's own *first* internal phase,
    // though: past this deadline grpc force-cancels every in-flight RPC
    // and then waits again, untimed, for that cancellation to actually be
    // observed - so a genuinely wedged subscriber Write() can still leave
    // server->Wait() blocked indefinitely. That's a known, accepted gap:
    // the backstop is a second SIGINT/SIGTERM (forcing std::_Exit() below)
    // or the orchestrator's own SIGKILL. This stacks with kShutdownTimeout
    // below rather than overlapping it - Shutdown() must finish before
    // runner.stop_all() even starts - so anything driving this process
    // needs a shutdown budget of at least their sum (see
    // docker-compose.yml's stop_grace_period).
    //
    // Re-armed, not one-shot: boost::asio::signal_set keeps the OS
    // disposition pointed at this handler for `signals`' whole lifetime,
    // so a truly one-shot registration would silently swallow every
    // signal after the first. Re-arming lets a second signal reach this
    // same handler, which calls std::_Exit() instead of asking nicely again.
    constexpr auto kGrpcShutdownDeadline = std::chrono::seconds(5);
    std::thread grpc_shutdown_thread;
    bool shutdown_requested = false;
    net::signal_set signals(io, SIGINT, SIGTERM);
    std::function<void(const boost::system::error_code&, int)> on_signal;
    on_signal = [&server, &signals, &on_signal, &shutdown_requested, &grpc_shutdown_thread,
                 kGrpcShutdownDeadline](const boost::system::error_code& ec, int signal_number) {
        if (ec) return;  // e.g. the signal_set was cancelled/destroyed first
        // Single std::cerr call, same reasoning as venue_session.hpp's
        // log_exception(): this handler runs on one of io_threads_pool's
        // threads (it's delivered through `io`'s own event loop), so with
        // io_threads > 1 it can now genuinely run at the same instant as a
        // VenueSession's own logging on a different thread.
        {
            std::ostringstream line;
            line << "received signal " << signal_number << ", shutting down\n";
            std::cerr << line.str();
        }
        if (shutdown_requested) {
            std::_Exit(1);
        }
        shutdown_requested = true;
        grpc_shutdown_thread = std::thread([&server, kGrpcShutdownDeadline] {
            server->Shutdown(std::chrono::system_clock::now() + kGrpcShutdownDeadline);
        });
        signals.async_wait(on_signal);
    };
    signals.async_wait(on_signal);

    runner.start_all();
    // One thread when *io_threads == 1 (the default, and every existing
    // deployment's behavior), N when the config asked for more - see
    // parse_io_threads()'s own comment for why calling io.run() from
    // several threads on this one shared io_context is safe.
    std::vector<std::thread> io_threads_pool;
    io_threads_pool.reserve(*io_threads);
    for (int i = 0; i < *io_threads; ++i) {
        io_threads_pool.emplace_back([&io] { io.run(); });
    }

    std::cout << "hermeneutic_aggregator_service listening on " << address << " for " << book_symbols.size()
              << " book(s):";
    for (const auto& id : book_symbols) std::cout << ' ' << bobby::hermeneutic::symbol::to_string(id);
    std::cout << ", ingesting from";
    for (const auto& venue : wired_venues) std::cout << ' ' << venue;
    std::cout << std::endl;
    server->Wait();
    if (grpc_shutdown_thread.joinable()) grpc_shutdown_thread.join();

    // No io.stop() as the primary shutdown mechanism: stop_all() aborts
    // every session's in-flight read/backoff wait and drains any in-flight
    // snapshot fetch (see VenueSession::stop()), so io.run() should return
    // on its own. kShutdownTimeout is a backstop, not the expected path,
    // and has to be comfortably longer than the longest timeout any
    // single in-flight operation could legitimately still be running
    // under when stop() lands - currently WebSocketConnection::connect()'s
    // own 30s connect/TLS-handshake timeout, the longer of the two
    // (http_get's is 10s). A shorter watchdog would routinely fire and
    // force the crude io.stop() fallback for a stop() that simply landed
    // during a slow-but-still-progressing connect, not a genuinely stuck
    // one. Even 35s doesn't cover every stage unconditionally: a snapshot
    // fetch's DNS resolution runs the underlying getaddrinfo() call on a
    // background thread that keeps going until the OS call itself
    // returns, regardless of the awaitable-level cancellation completing -
    // a genuinely stuck/black-holed resolution can still delay process
    // teardown past this bound. That's a Boost.Asio/OS-level limitation,
    // not something this code can fix by waiting longer.
    constexpr auto kShutdownTimeout = std::chrono::seconds(35);
    std::mutex shutdown_mutex;
    std::condition_variable shutdown_cv;
    bool shutdown_complete = false;
    std::thread shutdown_watchdog([&] {
        std::unique_lock lock(shutdown_mutex);
        if (!shutdown_cv.wait_for(lock, kShutdownTimeout, [&] { return shutdown_complete; })) {
            // Single std::cerr call, same reasoning as on_signal's own
            // comment above: this thread can still be racing an
            // io_threads_pool thread's own VenueSession logging at this
            // point, since io_threads_pool hasn't joined yet.
            std::ostringstream line;
            line << "runner.stop_all() did not drain within " << kShutdownTimeout.count()
                 << "s - forcing io.stop() as a backstop\n";
            std::cerr << line.str();
            io.stop();
        }
    });

    runner.stop_all();
    for (auto& t : io_threads_pool) t.join();

    {
        std::lock_guard lock(shutdown_mutex);
        shutdown_complete = true;
    }
    shutdown_cv.notify_all();
    shutdown_watchdog.join();

    // Stopped and joined here, before `service` goes out of scope below -
    // see heartbeat_thread's own construction comment for why this can't
    // be a detach().
    heartbeat_stop = true;
    heartbeat_thread.join();
    return 0;
}

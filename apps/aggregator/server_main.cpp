#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast/core/tcp_stream.hpp>

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <iostream>
#include <memory>
#include <mutex>
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
using bobby::hermeneutic::symbol::VenueId;

// Every venue's contribution, gathered from the flat VenueSubscription
// list into what IngestionRunner::add() actually wants: one native-symbol
// list plus one SymbolRegistry per venue, not one per symbol. See the
// grouping loop in main() below.
struct VenueGroup {
    std::vector<std::string> native_symbols;
    bobby::hermeneutic::ingestion::SymbolRegistry<SymbolBook> registry;
};

// Keyed by VenueId (not a hand-rolled std::pair<Exchange, MarketType>,
// which would be a second, independent spelling of the exact same
// pairing VenueId already exists to own - see symbol.hpp's own comment
// on why that duplication is the thing this type was introduced to
// remove). unordered_map, not map: same "nothing needs to sort this"
// rationale as AggregateOrderBook's venues_.
using VenueGroups = std::unordered_map<VenueId, VenueGroup>;

VenueGroup* find_group(VenueGroups& groups, const VenueId& venue_id) {
    auto it = groups.find(venue_id);
    return it == groups.end() ? nullptr : &it->second;
}

// Registers `venue_id`'s group with `runner`, if the config actually
// asked for it - a no-op otherwise (e.g. a config with no OKX venues at
// all never touches OkxFeed). One instantiation of this per (Feed, Policy)
// pair replaces what used to be six near-identical inline blocks in
// main(), so a fix here (or a future venue) only has to happen once.
// `group->native_symbols`/`group->registry` are moved out, not copied -
// `groups` isn't read again afterward - and `wired_venues` is grown here
// so the caller's startup log line can list only what was actually wired.
template <typename Feed, typename Policy>
void wire_venue(bobby::hermeneutic::ingestion::IngestionRunner& runner, VenueGroups& groups,
                 const VenueId& venue_id, std::vector<std::string>& wired_venues, net::any_io_executor executor,
                 net::ssl::context* ssl_ctx) {
    auto* group = find_group(groups, venue_id);
    if (!group) return;
    // to_string() is only for wired_venues, this function's own
    // startup-log contribution - `venue_id` itself, not its string form,
    // is what actually gets wired into IngestionRunner/VenueSession/
    // AggregateOrderBook below.
    wired_venues.push_back(bobby::hermeneutic::symbol::to_string(venue_id));
    runner.add<Feed, Policy, net::ssl::stream<boost::beast::tcp_stream>, SymbolBook>(
        Feed{}, venue_id, std::move(group->native_symbols), std::move(group->registry), executor, ssl_ctx);
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
    // native_symbol() enforces this by construction, there is no way to
    // wire a venue's derivatives feed into a spot book or vice versa). No
    // built-in default: unlike the CLI string this replaced, a config file
    // is meant to be an explicit, operator-owned artifact, not a value baked
    // into the binary that's easy to forget is even there.
    if (argc <= 2) {
        std::cerr << "usage: hermeneutic_aggregator_service [address] <subscription-config.json>\n";
        return 1;
    }
    auto parsed = bobby::hermeneutic::ingestion::load_book_subscriptions(argv[2]);
    if (!parsed) {
        std::cerr << "invalid subscription config: " << parsed.error() << '\n';
        return 1;
    }
    const std::vector<VenueSubscription>& subscriptions = *parsed;

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

    // Ingestion: one VenueSession per (venue, book type) pair the config
    // above actually asked for, feeding the same AggregatorService the
    // gRPC server above exposes. SymbolBook::apply_batch/invalidate_venue
    // are per-VenueId, so several venues safely aggregate into the same
    // book. A real TLS context, not the plain-TCP instantiation the tests
    // use: this is the path that runs against real exchanges rather than a
    // local test server.
    net::ssl::context ssl_ctx(net::ssl::context::tlsv12_client);
    ssl_ctx.set_default_verify_paths();
    ssl_ctx.set_verify_mode(net::ssl::verify_peer);

    // Grouped by (venue, type) rather than kept as the flat
    // VenueSubscription list: IngestionRunner::add() wants one native-symbol
    // list and one SymbolRegistry per venue session, not one call per
    // symbol. Each subscription's own registry entry is keyed by its
    // native_symbol (what the venue's Feed actually subscribes with and what
    // dispatch_snapshot/dispatch_depth_update look it up by - see
    // venue_session.hpp), pointing at the book its BookId resolves to.
    ::VenueGroups groups;
    for (const auto& sub : subscriptions) {
        auto& group = groups[sub.venue_id];
        group.native_symbols.push_back(sub.native_symbol);
        // book_symbols above is derived from this same `subscriptions` list,
        // so this can never miss - asserted, not runtime-checked, the same
        // startup-invariant treatment AggregatorService's own constructor
        // gives its unique-symbols precondition.
        auto* book = service.book(sub.book_id);
        assert(book);
        group.registry.add(sub.native_symbol, book);
    }

    net::io_context io;
    bobby::hermeneutic::ingestion::IngestionRunner runner;
    std::vector<std::string> wired_venues;

    wire_venue<bobby::hermeneutic::ingestion::BinanceSpotFeed, bobby::hermeneutic::BinanceSpotSequencePolicy>(
        runner, groups, VenueId{Exchange::Binance, MarketType::Spot}, wired_venues, io.get_executor(), &ssl_ctx);
    wire_venue<bobby::hermeneutic::ingestion::BinanceFuturesFeed, bobby::hermeneutic::BinanceFuturesSequencePolicy>(
        runner, groups, VenueId{Exchange::Binance, MarketType::Perp}, wired_venues, io.get_executor(), &ssl_ctx);
    wire_venue<bobby::hermeneutic::ingestion::BybitSpotFeed, bobby::hermeneutic::BybitSequencePolicy>(
        runner, groups, VenueId{Exchange::Bybit, MarketType::Spot}, wired_venues, io.get_executor(), &ssl_ctx);
    wire_venue<bobby::hermeneutic::ingestion::BybitLinearFeed, bobby::hermeneutic::BybitSequencePolicy>(
        runner, groups, VenueId{Exchange::Bybit, MarketType::Perp}, wired_venues, io.get_executor(), &ssl_ctx);
    // OKX is a single Feed/Policy pair covering both spot and perpetual
    // swap instIds (the `books` channel is protocol-identical for both -
    // see okx_feed.hpp); "okx_spot"/"okx_swap" are two independent
    // VenueSessions of that same Feed type, one per book type, matching
    // every other venue's shape here.
    wire_venue<bobby::hermeneutic::ingestion::OkxFeed, bobby::hermeneutic::OkxSequencePolicy>(
        runner, groups, VenueId{Exchange::Okx, MarketType::Spot}, wired_venues, io.get_executor(), &ssl_ctx);
    wire_venue<bobby::hermeneutic::ingestion::OkxFeed, bobby::hermeneutic::OkxSequencePolicy>(
        runner, groups, VenueId{Exchange::Okx, MarketType::Perp}, wired_venues, io.get_executor(), &ssl_ctx);

    runner.start_all();
    std::thread io_thread([&io] { io.run(); });

    std::cout << "hermeneutic_aggregator_service listening on " << address << " for " << book_symbols.size()
              << " book(s):";
    for (const auto& id : book_symbols) std::cout << ' ' << bobby::hermeneutic::symbol::to_string(id);
    std::cout << ", ingesting from";
    for (const auto& venue : wired_venues) std::cout << ' ' << venue;
    std::cout << std::endl;
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

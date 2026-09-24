# hermeneutic

A multi-venue crypto order book aggregator. `hermeneutic_aggregator_service`
ingests real-time L2 order book data from Binance, Bybit, and OKX (spot and
perpetual/futures) over their public WebSocket feeds, merges each venue's
view into one `AggregateOrderBook` per book, and re-publishes the merged
book over gRPC (`SubscribeL2Diff`/`SubscribeBbo`, plus a `ListBooks` query
RPC for discovering which books a given instance serves) so downstream consumers
never have to speak to an exchange directly. `hermeneutic_aggregator_client`
is a minimal example consumer of that stream. See "Building and running
with Docker" below for how the two run together, "Documentation" below
for the system's architecture, thread model, and gRPC API/wire-protocol
contract, and "Known limitations" for accepted architectural gaps.

## Building and running with Docker

`docker/Dockerfile` is one multi-stage file with two final targets,
`service` and `client`, sharing a single `builder` stage that builds both
executables against a vcpkg-built gRPC/protobuf/Boost toolchain. Requires
Docker with BuildKit (the default since Docker 23) and Compose v2.

```sh
docker compose build
docker compose up -d                            # brings up the service and all three clients
```

This serves the three-venue `BTC_USDT` spot+perp example config
(`apps/aggregator/subscriptions.example.json`, bind-mounted in by
`docker-compose.yml` - the image itself bakes in no config) on
`localhost:50051`. To ingest a different set of books/venues, point the
compose volume at your own config instead of editing the image:

```yaml
volumes:
  - ./path/to/your-subscriptions.json:/etc/hermeneutic/subscriptions.json:ro
```

`aggregator-service` runs as a non-root, home-less system user, so make
sure your own config is world-readable (`chmod 644 your-subscriptions.json`).

None of the four services (`aggregator-service`, `aggregator-client-bbo`,
`aggregator-client-volume-bands`, `aggregator-client-price-bands`) sit
behind a Compose profile, so plain `docker compose up` always brings up
all of them together. To run a subset instead, name the wanted
service(s) explicitly - Compose starts exactly those plus whatever they
`depends_on`, regardless of what else is defined in the file:

```sh
docker compose up -d aggregator-service         # just the server
docker compose up -d aggregator-client-bbo      # one client (pulls in aggregator-service too)
docker compose logs -f aggregator-client-bbo    # follow one client's output
docker compose down aggregator-client-bbo       # tear down just that one client
docker compose down                             # tear down everything
```

## Test coverage

```sh
scripts/coverage.sh                    # full build (needs VCPKG_ROOT, default ~/vcpkg)
scripts/coverage.sh --no-service       # vcpkg-free, faster iteration
scripts/coverage.sh --html             # also writes an HTML report
scripts/coverage.sh --gcc --no-service # GCC/gcov coverage (via gcovr) instead of Clang's
```

Clang source-based coverage by default; `--gcc` uses gcov/gcovr instead.
Both gated behind `-DHERMENEUTIC_COVERAGE=ON`. See `scripts/coverage.sh`'s
own comments for configure/report mechanics and troubleshooting notes.

Full-build coverage as of commit `6958875`: 97.55% line, 99.12%
function, 91.03% region, 88.17% branch - a snapshot, not a live number;
re-run `scripts/coverage.sh` for the current state.

## Technical decisions

The project-level decisions with the most day-to-day impact:

- **Structured identity types, not concatenated strings, wherever one
  crosses a boundary.** `BaseQuote` (`{Asset base, Asset quote}`), `BookId`
  (`{BaseQuote, MarketType}`), and `VenueId` (`{Exchange, MarketType}`) are
  all real types - over the wire, in `AggregatorService`'s book map, in the
  client's CLI parsing - not a spelling like `"BTC_USDT.SPOT"` or
  `"binance_spot"`. Splitting a concatenated ticker back into base/quote is
  fundamentally ambiguous without a maintained quote-asset dictionary (is
  `"BTCUSDT"` `BTC/USDT`, or something else?), so these keep that boundary
  in the type instead of re-deriving it by parsing at every lookup site.
- **Price/Size/Notional are fixed-point, not floating-point**
  (`Price = BasicFixedPoint<9>`, `Size = BasicFixedPoint<6>`,
  `Notional = BasicFixedPoint<9>`, scaled 64-bit integers).
  `from_decimal_string()` parses a venue's decimal string directly into
  that exact representation, no `double` intermediate, so two venues'
  identical price always compares exactly equal as an `L2OrderBook` map key.
- **Spot and perp are always separate books**, even on venues (OKX) whose
  WebSocket channel is protocol-identical for both. A perpetual's price
  tracks spot plus a funding-rate basis, so the two genuinely trade at
  different prices - merging their liquidity wouldn't be a real aggregate
  of anything.
- **The ingestion core is sans-io.** `SymbolSync<SequencePolicy>` never
  touches a socket - it's a pure state machine that consumes parsed events
  and returns actions for `VenueSession` (the one layer with real I/O) to
  execute, so the trickiest logic (resync, gap detection) is unit-testable
  against hand-constructed message sequences alone.
- **Ingestion and the gRPC surface are two independently reusable layers.**
  `hermeneutic::ingestion` never mentions `aggregator::SymbolBook` or
  protobuf by name - it's templated on any `Book` with the right interface,
  so the gRPC service is one consumer of it, not baked into it.
- **Every `Heartbeat` carries the book's currently contributing venues.**
  `live_venues` lists them by name (a `repeated string`), so a consumer
  can always tell a book fed by all three venues from one down to its
  last - a single "healthy" flag couldn't. Which venues count as live
  comes from Beast's own WebSocket keep-alive ping/pong, tunable per
  venue via `idle_timeout_seconds`/`venue_idle_timeout_overrides`.
- **Ingestion scales to more symbols/venues by adding threads, not just
  by adding capacity per thread.** `"io_threads"` in the subscription
  config sets how many threads call `io.run()` (default 1 - single-
  threaded, every existing deployment's behavior). Every `VenueSession` is
  strand-confined, so raising it is safe: each venue's CPU work (JSON
  parsing, book diffing) genuinely overlaps across threads instead of
  being time-sliced on one core, so a deployment ingesting many
  symbols/venues isn't capped by a single core's throughput. The cost:
  two `VenueSession`s feeding the same `SymbolBook` can then genuinely
  contend on `SymbolBook::mutex_`, contention a single thread serialized
  for free.
- **Each gRPC subscriber gets its own queue, with a stream-specific
  overflow policy.** `SubscribeL2Diff` closes the stream on overflow (a
  missed diff leaves the subscriber's book genuinely wrong, not just
  stale); `SubscribeBbo` drops the oldest queued update instead (a `Bbo`
  is a full snapshot, so a stale one is harmless to replace).
- **Every third-party C++ dependency is built from source, not resolved
  from whatever the host happens to have installed** - gRPC/protobuf/
  abseil/Boost via vcpkg's pinned manifest, simdjson/googletest via CMake
  `FetchContent` - so a build is reproducible across machines instead of
  quietly depending on the host's package manager and its version drift.
  OpenSSL is the one deliberate exception: resolved via system
  `find_package(OpenSSL REQUIRED)`, not vcpkg.json, since it's a system
  library every target platform already needs for TLS regardless.

## Documentation

- `docs/system_architecture.md` - whole-system component overview (venue
  → `VenueSession` → sans-io core → `SymbolBook` → gRPC → client) with
  thread boundaries marked; start here, then follow its links into the
  documents below for detail.
- `docs/thread_model.md` - the server- and client-side thread/lock
  inventory (`io_threads_pool`, `heartbeat_thread`, shutdown threads,
  `SymbolBook::mutex_`, the gRPC subscriber pool; client `Reader`/
  `StreamCanceller` threads), audited against current source.
- `docs/api_protocol_design.md` - the `Aggregator` gRPC service's API/
  wire-protocol contract (message semantics, the `book_seq` contiguity
  guarantee, subscriber lifecycle/backpressure) and how to extend it:
  adding a venue, adding a market type, adding new RPCs/fields.

## Known limitations

- **`SymbolSync` reconnects blast too wide a radius**: a live data gap on
  one symbol forces the entire shared `VenueSession` to reconnect,
  dragging down every other symbol on that session.
- **No rate limiting against exchange REST/WS APIs.** There's no cap on
  the *aggregate* request rate across a venue's symbols: one
  `VenueSession` covers every symbol configured for a venue, so a
  REST-snapshot venue can see all of them fire a legitimate,
  non-redundant `RequestSnapshot` within moments of each other at
  startup. Reconnects and in-place REST retries only back off
  reactively, after a failure - nothing throttles this kind of
  simultaneous, individually-legitimate burst before it happens.
- **The gRPC thread count is unbounded** - no `SetSyncServerOption` or
  `ResourceQuota` is configured, so it grows linearly with the number of
  concurrently active streaming subscriptions.
- **No standard gRPC health-check service** (`grpc.health.v1.Health` is
  never registered) - `docker-compose.yml`'s healthcheck is a raw TCP
  connect, which only proves the port is open, not that ingestion is
  actually healthy.
- **`ListBooks` is read-only.** `AggregatorService::books_` is populated
  once in the constructor and never modified after; changing which books
  a running instance serves means editing the config and restarting the
  whole process, not an RPC or live reload.

## Appendix: building and testing without Docker

```sh
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
~/vcpkg/bootstrap-vcpkg.sh
export VCPKG_ROOT=~/vcpkg
cmake --preset vcpkg
cmake --build build-vcpkg
ctest --test-dir build-vcpkg
```

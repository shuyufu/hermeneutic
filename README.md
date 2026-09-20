# hermeneutic

A multi-venue crypto order book aggregator. `hermeneutic_aggregator_service`
ingests real-time L2 order book data from Binance, Bybit, and OKX (spot and
perpetual/futures) over their public WebSocket feeds, merges each venue's
view into one `AggregateOrderBook` per book, and re-publishes the merged
book over gRPC (`SubscribeL2Diff`/`SubscribeBbo`) so downstream consumers
never have to speak to an exchange directly. `hermeneutic_aggregator_client`
is a minimal example consumer of that stream. See "Aggregator service"
below for how the two run together, and `docs/ingestion_design.md` (中文)
for the full design history and open questions.

## Technical decisions

The project-level decisions with the most day-to-day impact:

- **Structured `BookId`, not a string key.** A book is addressed as
  `{base, quote, market}` end-to-end - over the wire, in
  `AggregatorService`'s book map, and in the client's CLI parsing - rather
  than a concatenated string like `"BTC_USDT.SPOT"` that has to be
  re-parsed at every boundary. `docs/ingestion_design.md` 第10節第10項
  covers why the wire format moved off the string key.
- **Spot and perp are always separate books**, even on venues (OKX) whose
  WebSocket channel is protocol-identical for both: mixing their liquidity
  into one book would silently blend two different instruments' prices.
  See `docs/ingestion_design.md`'s OKX section.
- **Ingestion and the gRPC surface are two independently reusable layers.**
  `hermeneutic::ingestion` (`VenueSession`/`IngestionRunner`) never mentions
  `aggregator::SymbolBook` or protobuf by name - it's templated on any
  `Book` that exposes `apply_snapshot`/`apply_batch`/`invalidate_venue` - so
  the gRPC service is one consumer of it, not baked into it. Every
  interface tier (`core`/`book`/`symbol`/`exchange_policy`/`exchange`/
  `net`/`ingestion`) is header-only for the same reason: consuming any
  subset needs no more than the headers it actually includes.
- **The subscription config is a required, operator-owned file**, not a
  CLI flag with a built-in default. Which venues feed which book is an
  operational decision, not something to bake into the binary and forget
  is there - see `apps/aggregator/subscriptions.example.json` and
  `server_main.cpp`'s comment on this.
- **Real market-data code is exercised against the real exchanges**, not
  just fixtures, before being trusted: `docs/ingestion_design.md` records
  live verification runs (and the bugs they caught, e.g. a `SymbolSync`
  empty-buffer case) that fixture-only tests would have missed.

Containerization-specific decisions (multi-stage build, why the runtime
image needs `libssl3` but not vcpkg, etc.) are covered in "Running with
Docker" below.

## Building with vcpkg

Some build targets fetch dependencies via
[vcpkg](https://github.com/microsoft/vcpkg) in manifest mode (`vcpkg.json`,
pinned via `builtin-baseline`). vcpkg itself is not vendored in this repo;
clone it once and point `VCPKG_ROOT` at it:

```sh
git clone https://github.com/microsoft/vcpkg.git ~/vcpkg
~/vcpkg/bootstrap-vcpkg.sh
export VCPKG_ROOT=~/vcpkg
```

The default build (no preset) needs no vcpkg and only builds
`hermeneutic_tests` as before.

## Building the gRPC-based service targets

The service build (`HERMENEUTIC_BUILD_SERVICE=ON`, set by the `vcpkg` preset)
fetches gRPC and its dependencies via vcpkg:

```sh
cmake --preset vcpkg
```

The first configure builds gRPC/protobuf/abseil from source, which takes a
while; vcpkg's binary cache (`~/.cache/vcpkg/archives` by default) speeds up
subsequent configures and other checkouts on the same machine.

### Adding a `.proto`

Every file under `proto/` is picked up automatically (`cmake --build` alone
regenerates and recompiles after you add, edit, or remove one). Put each
`.proto` at a path that mirrors its `package`, e.g. `package bobby.hermeneutic.foo`
goes in `proto/bobby/hermeneutic/foo/*.proto`, matching the generated headers under
`generated/bobby/hermeneutic/foo/`. This keeps the C++ namespace, the file layout,
and the generated include paths consistent, and avoids filename collisions
between packages. `proto/bobby/hermeneutic/aggregator/aggregator.proto` is a worked
example, alongside its hand-written service in `apps/aggregator/aggregator_service.hpp`.

### Aggregator service

`hermeneutic_aggregator_service` (`apps/aggregator/server_main.cpp`) streams the
aggregated L2 order book to subscribers: a client calls `SubscribeL2Diff` or
`SubscribeBbo` with the `BookId` it wants (a structured `{base, quote, market}`
message, not a string - see `proto/bobby/hermeneutic/aggregator/aggregator.proto`
and `bobby::hermeneutic::symbol::BookId` in `symbol/symbol.hpp` for the
equivalent C++ type), gets an initial snapshot, then every subsequent change
as it happens. One instance serves any number of books on a single port —
each gets two independent `SymbolBook`s (an `AggregateOrderBook` plus its own
subscriber fan-out), one for perpetual/futures liquidity and one for spot.
Which venues feed which book is driven by a JSON subscription config (see
`apps/aggregator/subscriptions.example.json` and
`bobby/hermeneutic/ingestion/book_subscription.hpp` for the document shape);
its `"symbol"` field is the same `"BASE_QUOTE"` spelling (`"BTC_USDT"`) as the
`BookId` a client subscribes with - see `docs/ingestion_design.md`'s 第10節
第10項 for why the wire format moved from a concatenated string key to a
structured message, and its OKX section for why perp and spot aren't merged
into one book. It only wraps the book(s) and broadcasts to subscribers —
feeding real market data (`SymbolBook::apply_delta`/`apply_snapshot`/
`invalidate_venue`, reached via `AggregatorService::book(BookId)`) is up to
the caller.

```sh
cmake --build build-vcpkg --target hermeneutic_aggregator_service
# [address] <subscription-config.json> - the config path is required.
./build-vcpkg/hermeneutic_aggregator_service 0.0.0.0:50051 apps/aggregator/subscriptions.example.json
```

`hermeneutic_aggregator_client` (`apps/aggregator/client_main.cpp`) is a
minimal example client for the service above - not a throwaway (unlike this
project's earlier live-verification programs, see `docs/ingestion_design.md`),
kept around as a starting point for consuming `AggregatorService`'s output
and for manually poking at a running instance. It subscribes one or more
books (one thread per book) and publishes a chosen view of the order book to
stdout on every update; `volume-bands`/`price-bands` check `SubscribeL2Diff`'s
own `book_seq` contiguity guarantee themselves, printing a `GAP` line if that
contract is ever violated. Each book argument is the same human-readable
`"BASE_QUOTE.SPOT"`/`"BASE_QUOTE.PERP"` spelling `bobby::hermeneutic::symbol::
to_string(BookId)` produces - parsed back into a `BookId` once, at this
program's own argv boundary (`parse_book_id`), never as a concatenated string
past that point:

```sh
cmake --build build-vcpkg --target hermeneutic_aggregator_client
# <address> <bbo|volume-bands|price-bands> <duration_seconds> <book1> [book2 ...]
# duration_seconds <= 0 runs until interrupted or the server ends the stream.
#   bbo:          subscribes to SubscribeBbo, prints best bid/ask
#   volume-bands: subscribes to SubscribeL2Diff, prints the VWAP needed to
#                 fill 1M/5M/10M/25M/50M+ notional on each side
#   price-bands:  subscribes to SubscribeL2Diff, prints depth within
#                 50/100/200/500/1000+ bps of BBO on each side
./build-vcpkg/hermeneutic_aggregator_client 0.0.0.0:50051 bbo 60 BTC_USDT.SPOT BTC_USDT.PERP
./build-vcpkg/hermeneutic_aggregator_client 0.0.0.0:50051 volume-bands 60 BTC_USDT.SPOT
./build-vcpkg/hermeneutic_aggregator_client 0.0.0.0:50051 price-bands 60 BTC_USDT.SPOT
```

`hermeneutic_aggregator_service_test` exercises it over a real (in-process)
gRPC connection; it only builds when both `HERMENEUTIC_BUILD_SERVICE` and
`HERMENEUTIC_BUILD_TESTS` are `ON`:

```sh
cmake -B build-vcpkg-full -S . \
  -DCMAKE_TOOLCHAIN_FILE=$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake \
  -DHERMENEUTIC_BUILD_SERVICE=ON -DHERMENEUTIC_BUILD_TESTS=ON
cmake --build build-vcpkg-full --target hermeneutic_aggregator_service_test
./build-vcpkg-full/hermeneutic_aggregator_service_test
```

## Running with Docker

`docker/Dockerfile` is one multi-stage file with two final targets,
`service` and `client`, sharing a single `builder` stage - this project has
exactly two executables (`hermeneutic_aggregator_service`,
`hermeneutic_aggregator_client`) and both need the same vcpkg-built
gRPC/protobuf/Boost toolchain described above, so building that once and
copying both binaries out avoids paying for it twice. The builder stage
mounts vcpkg's binary cache (`~/.cache/vcpkg/archives`, the same path the
host build above uses) as a BuildKit cache mount rather than baking it into
a layer, so only the first build (or a `vcpkg.json`/baseline change) pays
for compiling gRPC/protobuf/abseil from source; ordinary source-code changes
reuse the cache even though they invalidate the `COPY . .` layer. A second,
separate cache mount for `ccache` covers what the vcpkg cache doesn't -
this project's own source (proto-generated code, `server_main.cpp`,
`client_main.cpp`) and simdjson (fetched via `FetchContent`, so it's
compiled through the same `CMAKE_CXX_COMPILER_LAUNCHER=ccache` setting) -
so those don't fully recompile on every build either, even though nothing
here persists the `build-vcpkg/` directory itself across builds. Requires
Docker with BuildKit (the default since Docker 23) and Compose v2.

```sh
docker compose build
docker compose up aggregator-service          # ctrl-C to stop
```

This serves the three-venue `BTC_USDT` spot+perp example config
(`apps/aggregator/subscriptions.example.json`, bind-mounted in by
`docker-compose.yml` - the image itself bakes in no config) on
`localhost:50051`, matching the manual invocation in "Aggregator service"
above. To ingest a different set of books/venues, point the compose volume
at your own config instead of editing the image:

```yaml
volumes:
  - ./path/to/your-subscriptions.json:/etc/hermeneutic/subscriptions.json:ro
```

`aggregator-service` runs as a non-root, home-less system user (see
"Technical decisions" below), so make sure your own config is
world-readable (`chmod 644 your-subscriptions.json`) - a file you created
with a stricter umask, or that Docker Desktop's host/VM file sharing maps
to a UID the container's user doesn't match, otherwise fails to read with
a permission error at startup that has nothing to do with the JSON's
actual content. The bundled example works out of the box because it's
already world-readable in the repo.

`hermeneutic_aggregator_client` is available as three optional `client`
Compose profile services - `aggregator-client-bbo`,
`aggregator-client-volume-bands`, `aggregator-client-price-bands`, one per
view the client supports, sharing everything but their `command:` via the
`x-client-base` anchor - since it's a diagnostic tool, not part of "the
service", `docker compose up` alone starts none of them. `--profile client`
starts all three alongside `aggregator-service`; a single service name
after `up` starts only that one.

```sh
docker compose --profile client up
```

To watch all four containers' stdout, either stay in that same foreground
terminal - Compose interleaves every container's output there, each line
prefixed with its service name - or run detached and follow each client
separately:

```sh
docker compose --profile client up -d
docker compose logs -f aggregator-client-bbo
docker compose logs -f aggregator-client-volume-bands
docker compose logs -f aggregator-client-price-bands
```

`docker compose logs -f` with no service name follows all of them at once
(equivalent to the foreground case above); dropping `-f` dumps what a
service has printed so far without continuing to follow it.

Tear down with the same profile you started with -
`docker compose --profile client down` - or Compose only stops/removes the
default-profile `aggregator-service`; the three client containers are left
behind (already exited, since losing their stream when the service
disappears counts as "the server ends the stream" - see "Aggregator
service" above - not removed, since `down` never considered them part of
this invocation).

Technical decisions worth calling out:

- **Runtime image is separate from the builder.** The final `service`/
  `client` images start `FROM` a small `runtime-base` stage
  (`ubuntu:24.04` + `ca-certificates` + `libssl3`), not the builder - they
  never carry vcpkg, the build toolchain, or the ~gigabytes of intermediate
  build artifacts that stage produces. `ca-certificates` is there because
  both binaries make real outbound TLS connections (the service to
  exchange WebSocket/REST endpoints; the client only if pointed at a
  TLS-terminated gRPC endpoint).
- **OpenSSL is the one dynamically-linked third-party dependency.**
  vcpkg's default Linux triplet links gRPC/protobuf/abseil/Boost
  statically, but OpenSSL is resolved via system `find_package(OpenSSL
  REQUIRED)` (see `CMakeLists.txt`), not vcpkg.json, and is linked as a
  shared library - hence `libssl3` (not `libssl-dev`) in the runtime image
  alongside the statically-linked binaries.
- **Tests are excluded from the image** (`HERMENEUTIC_BUILD_TESTS=OFF`):
  googletest and the test binaries aren't part of either runtime image and
  only add build time; run `hermeneutic_tests` (and the other
  `gtest_discover_tests` targets) on the host or in CI instead.
- **The image bakes in no subscription config**, matching
  `server_main.cpp`'s own required-CLI-argument, no-built-in-default
  design: a bare `docker run` with nothing mounted at
  `/etc/hermeneutic/subscriptions.json` exits immediately with the
  binary's own usage error rather than silently serving an example
  config. `docker-compose.yml` supplies that mount, it isn't a fallback
  the image provides on its own.
- **The client's default command lives in one place** (`docker/Dockerfile`'s
  `CMD` for the `client` target); `docker-compose.yml` doesn't repeat it,
  so there's no risk of the two drifting apart, as two independent
  argument lists eventually would.
- **`aggregator-service` has a Compose healthcheck** (a plain TCP connect
  to :50051, not a real gRPC health check - just enough to know the
  listener is up), and `aggregator-client` depends on it being healthy
  rather than merely started. `client_main.cpp`'s `SubscribeBbo` call
  doesn't set `wait_for_ready`, so without this the client can race the
  service's startup and fail fast with `UNAVAILABLE` instead of connecting.
- **Both containers run as a non-root user** (`hermeneutic`, created in
  `runtime-base`) rather than the default root, since neither binary needs
  elevated privileges at runtime.
- **Graceful shutdown is a known gap, not something this Docker setup
  adds or hides.** `server_main.cpp` has no `SIGTERM`/`SIGINT` handler and
  nothing calls `grpc::Server::Shutdown()` yet (see that file's own
  comment on `server->Wait()` for why the shutdown-watchdog thread already
  anticipates this); `docker compose down`/`docker stop` therefore hard-kill
  the process exactly as an unhandled `SIGTERM` would on the host, skipping
  `runner.stop_all()`'s clean drain of in-flight venue sessions and
  subscriber streams. Wiring that up is an application-level change
  outside this Docker work's scope, not a container-runtime one.

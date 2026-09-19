# hermeneutic

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

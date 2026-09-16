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
example, alongside its hand-written service in `service/aggregator_service.hpp`.

### Aggregator service

`hermeneutic_aggregator_service` (`service/aggregator_main.cpp`) streams the
aggregated L2 order book to subscribers: a client calls `Subscribe` with the
symbol it wants, gets an initial snapshot, then every subsequent change as it
happens. One instance serves any number of symbols on a single port — each
gets its own `SymbolBook` (an `AggregateOrderBook` plus its own subscriber
fan-out), routed by `SubscribeRequest.symbol`. It only wraps the book(s) and
broadcasts to subscribers — feeding real market data
(`SymbolBook::apply_delta`/`apply_snapshot`/`invalidate_venue`, reached via
`AggregatorService::book(symbol)`) is up to the caller.

```sh
cmake --build build-vcpkg --target hermeneutic_aggregator_service
./build-vcpkg/hermeneutic_aggregator_service 0.0.0.0:50051 BTCUSDT,ETHUSDT
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

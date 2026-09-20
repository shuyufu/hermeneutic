# syntax=docker/dockerfile:1
#
# One Dockerfile, two runnable images (`service` and `client`), sharing a
# single `builder` stage. This project has exactly two executables
# (hermeneutic_aggregator_service and hermeneutic_aggregator_client, see
# CMakeLists.txt) and both need the same gRPC/protobuf/Boost/OpenSSL build
# via vcpkg (see README.md's "Building the gRPC-based service targets"
# section) - building that twice in two separate Dockerfiles would double
# the slowest part of the build for no benefit, since neither final image
# needs the other's binary.
#
# Build with BuildKit (the default for `docker build`/`docker compose build`
# since Docker 23): the `--mount=type=cache` below persists vcpkg's binary
# cache across builds, so only the first build (or a vcpkg.json/baseline
# change) pays for compiling gRPC/protobuf/abseil from source.

FROM ubuntu:24.04 AS builder

# build-essential/cmake/ninja-build: the C++23 toolchain (CMakeLists.txt
# requires CMake >= 3.28; Ubuntu 24.04 ships 3.28.3). gcc-14/g++-14: the
# codebase uses std::expected (e.g. book_subscription.hpp,
# server_main.cpp's `parsed.error()`), which libstdc++ only completed
# under GCC 14 - Ubuntu 24.04's *default* g++ is GCC 13, so it's installed
# and selected explicitly via CC/CXX below rather than relying on the
# image default. No `|| gcc-13` fallback here (unlike the OpenSSL runtime
# package below): GCC 13 and 14 aren't interchangeable for this codebase,
# so a silent fallback would trade a clear apt failure for a much more
# confusing compile failure deep in <expected> - if gcc-14 ever disappears
# from noble's archive, this should fail loudly here, not degrade quietly.
# git/curl/zip/unzip/tar/pkg-config are vcpkg's own bootstrap/build
# dependencies, not this project's. libssl-dev is a *system* dependency:
# unlike gRPC/boost, OpenSSL is resolved via find_package(OpenSSL REQUIRED)
# in CMakeLists.txt rather than vcpkg.json.
# DEBIAN_FRONTEND=noninteractive scoped to each apt-get RUN's own shell
# (not a persistent ENV, so it never leaks into runtime-base/service's
# shipped environment): without it, a future ubuntu:24.04 snapshot that
# happens to pull in a debconf-prompting package (tzdata is the classic
# case) would hang this RUN waiting on stdin instead of failing fast.
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y --no-install-recommends \
        build-essential gcc-14 g++-14 cmake ninja-build ccache git ca-certificates curl zip unzip tar \
        pkg-config python3 libssl-dev \
    && rm -rf /var/lib/apt/lists/*
ENV CC=gcc-14 CXX=g++-14

WORKDIR /src
# Just vcpkg.json for now (the rest of the source follows in the COPY . .
# below): needed here already to pin the vcpkg *tool* itself, not only the
# port versions it resolves.
COPY vcpkg.json ./

# Not --depth 1: vcpkg.json pins builtin-baseline to a specific commit in
# vcpkg's own history, and vcpkg resolves it by reading that commit's
# ports/versions files straight out of this local clone's object store - a
# shallow clone doesn't have those objects and would fail to resolve it.
#
# Checked out to that same builtin-baseline commit, not left at whatever
# vcpkg's default branch tip happens to be on build day: vcpkg.json's
# baseline is already this repo's single source of truth for "which
# vcpkg", so this reads it out of vcpkg.json with python3 (installed
# above) instead of hardcoding the same commit a second time - drifting
# vcpkg *tool* versions between builds could otherwise change manifest
# resolution or CLI behavior with no corresponding change in this repo.
ENV VCPKG_ROOT=/opt/vcpkg
RUN git clone https://github.com/microsoft/vcpkg.git "$VCPKG_ROOT" \
    && git -C "$VCPKG_ROOT" checkout "$(python3 -c 'import json; print(json.load(open("vcpkg.json"))["builtin-baseline"])')" \
    && "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics

COPY . .

# HERMENEUTIC_BUILD_TESTS=OFF: googletest and the test binaries add real
# build time and aren't part of either runtime image - run
# `hermeneutic_tests` on the host (see README.md's test policy) or in CI,
# not in this image. HERMENEUTIC_BUILD_INGESTION stays at its default ON:
# both executables need the real exchange feed parsers.
#
# Three independent caches, all mounted rather than baked into a layer
# (they're caches, not artifacts this image should ship, and mounting them
# lets them survive across source-code changes that invalidate the
# COPY-based layer above):
#   - vcpkg's own binary archive cache (~/.cache/vcpkg/archives, same path
#     the host-build instructions in README.md rely on) - covers every
#     vcpkg-built port (gRPC/protobuf/abseil/boost/zlib/...), which vcpkg
#     builds through its own internal toolchain invocation, bypassing
#     whatever this project's own CMAKE_CXX_COMPILER_LAUNCHER says.
#   - ccache - covers the recompiling vcpkg's cache doesn't: this
#     project's own translation units (the generated proto/gRPC .cc
#     files, server_main.cpp, client_main.cpp) and simdjson (fetched via
#     FetchContent, so it *is* compiled through our own compiler launcher
#     setting, unlike the vcpkg ports).
#   - FETCHCONTENT_BASE_DIR redirected to its own cache mount - ccache
#     only saves recompiling simdjson, not re-cloning it: CMakeLists.txt's
#     FetchContent_Declare(simdjson ...) otherwise clones it fresh from
#     GitHub into build-vcpkg/_deps on every build, since nothing else
#     here persists build-vcpkg/ itself across builds. This variable is
#     FetchContent's own documented override, so it takes effect without
#     touching CMakeLists.txt.
ENV CCACHE_DIR=/root/.cache/ccache
RUN --mount=type=cache,target=/root/.cache/vcpkg/archives \
    --mount=type=cache,target=/root/.cache/ccache \
    --mount=type=cache,target=/root/.cache/cmake-fetchcontent \
    cmake -S . -B build-vcpkg -G Ninja \
        -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_C_COMPILER_LAUNCHER=ccache \
        -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
        -DFETCHCONTENT_BASE_DIR=/root/.cache/cmake-fetchcontent \
        -DHERMENEUTIC_BUILD_SERVICE=ON \
        -DHERMENEUTIC_BUILD_TESTS=OFF \
    && cmake --build build-vcpkg \
        --target hermeneutic_aggregator_service hermeneutic_aggregator_client \
        -j"$(nproc)"

# Shared runtime base: both final images need the same handful of shared
# libraries. vcpkg's default Linux triplet links gRPC/protobuf/abseil/Boost
# statically, so the only things left dynamically linked are libc/libstdc++
# and OpenSSL (linked as a shared library per the system libssl-dev above);
# ca-certificates is needed because both binaries make real outbound TLS
# connections (the service to exchange WebSocket/REST endpoints, the client
# only if pointed at a TLS-terminated gRPC endpoint). Ubuntu 24.04's
# time_t64 transition renamed the real OpenSSL 3 runtime package to
# libssl3t64 while keeping libssl3 installable as a transitional package on
# most mirrors; the fallback covers mirrors where it isn't. Not bash: that's
# only the `service` stage's healthcheck below, not something `client` has
# any use for, so it's installed there instead of here.
FROM ubuntu:24.04 AS runtime-base
RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && (apt-get install -y --no-install-recommends ca-certificates libssl3 \
        || apt-get install -y --no-install-recommends ca-certificates libssl3t64) \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin hermeneutic

FROM runtime-base AS service
# Pinned explicitly, not left to however the base image happens to ship it:
# aggregator-service's healthcheck (docker-compose.yml) uses bash's own
# /dev/tcp redirection, which a slimmer/chiseled Ubuntu variant might drop.
# Scoped to this stage, not runtime-base, so the client image - which never
# execs a shell - doesn't carry it.
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y --no-install-recommends bash \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/build-vcpkg/hermeneutic_aggregator_service /usr/local/bin/hermeneutic_aggregator_service
USER hermeneutic
EXPOSE 50051
ENTRYPOINT ["/usr/local/bin/hermeneutic_aggregator_service"]
# No config baked into the image at /etc/hermeneutic/subscriptions.json -
# matching server_main.cpp's own required-file, no-built-in-default design
# (see the "Technical decisions" section of README.md): CMD only documents
# the conventional mount point, it doesn't provide a working default. Run
# without mounting something there and the binary exits immediately with
# its own usage error, same as it would on the host - it does not silently
# fall back to any example config.
CMD ["0.0.0.0:50051", "/etc/hermeneutic/subscriptions.json"]

FROM runtime-base AS client
COPY --from=builder /src/build-vcpkg/hermeneutic_aggregator_client /usr/local/bin/hermeneutic_aggregator_client
USER hermeneutic
ENTRYPOINT ["/usr/local/bin/hermeneutic_aggregator_client"]
# "aggregator-service" only resolves on Compose's own network (see
# docker-compose.yml); running this image standalone (`docker run` with no
# compose network) needs an explicit address override.
CMD ["aggregator-service:50051", "bbo", "60", "BTC_USDT.SPOT", "BTC_USDT.PERP"]

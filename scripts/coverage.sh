#!/usr/bin/env bash
# Drives the full source-coverage cycle: configure with -DHERMENEUTIC_COVERAGE=ON,
# build every test binary, run each one directly (not through ctest - see below),
# merge the resulting profiles, and print a per-file report scoped to this
# project's own headers/sources.
#
# Deliberately runs each test *binary* once instead of `ctest --test-dir
# build-coverage`: gtest_discover_tests() registers one ctest case per gtest
# TEST(), so a plain ctest run launches the same binary hundreds of times. A
# fixed LLVM_PROFILE_FILE across those launches means each run overwrites the
# last one's profile, so the "coverage" would really just be whichever test
# case happened to run last. Running each binary once (no --gtest_filter) is
# both correct and far faster.
#
# Two scopes:
#   - default: the full vcpkg build (-DHERMENEUTIC_BUILD_SERVICE=ON), so
#     net/ingestion/aggregator-service coverage is included. Needs VCPKG_ROOT
#     (defaults to ~/vcpkg if unset - see docs' own vcpkg setup convention).
#   - --no-service: the vcpkg-free build from the README
#     (-DHERMENEUTIC_BUILD_SERVICE=OFF). Faster, but net/** and the gRPC/
#     Boost/OpenSSL-gated half of ingestion/ (venue_session.hpp,
#     ingestion_runner.hpp) plus the aggregator service/client aren't built
#     at all - ingestion/book_subscription.hpp stays in scope either way,
#     since it only needs HERMENEUTIC_BUILD_INGESTION (on by default). This
#     script prints the exact scope as a banner rather than let a missing
#     row be mistaken for "untested".

set -euo pipefail

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SOURCE_ROOT"

BUILD_DIR="build-coverage"
WITH_SERVICE=1
EMIT_HTML=0
JOBS="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"

for arg in "$@"; do
    case "$arg" in
        --no-service) WITH_SERVICE=0 ;;
        --html) EMIT_HTML=1 ;;
        -h|--help)
            echo "Usage: $0 [--no-service] [--html]"
            echo "  --no-service  vcpkg-free build (skips net/, venue_session.hpp/ingestion_runner.hpp, and the aggregator service/client)"
            echo "  --html        also emit an HTML report under $BUILD_DIR/coverage-html"
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

if ! xcrun --find llvm-profdata >/dev/null 2>&1 || ! xcrun --find llvm-cov >/dev/null 2>&1; then
    echo "error: llvm-profdata/llvm-cov not found via xcrun (needs Xcode command line tools)" >&2
    exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
    echo "error: python3 not found (needed to parse ctest's --show-only=json-v1 output)" >&2
    exit 1
fi
# Resolved via `xcrun --find`, the same lookup used for llvm-profdata/
# llvm-cov above, rather than `command -v`: on a machine with Homebrew LLVM
# ahead of the Xcode/CLT toolchain on PATH, `command -v clang++` and `xcrun
# --find llvm-cov` would silently resolve to two different LLVM versions,
# and a raw-profile format mismatch between the compiler that instruments
# and the tool that reads the result can misparse rather than error.
CLANG="$(xcrun --find clang)" || { echo "error: clang not found via xcrun" >&2; exit 1; }
CLANGXX="$(xcrun --find clang++)" || { echo "error: clang++ not found via xcrun" >&2; exit 1; }

# HERMENEUTIC_BUILD_INGESTION is left at its CMakeLists default (ON) by
# every scope below: passing it explicitly here, every run, means a
# build-coverage/ that somehow got reconfigured with it OFF (e.g. a manual
# debugging invocation) self-heals on the next scripts/coverage.sh run
# without needing to be part of the staleness check below - plain option()
# BOOL cache variables like this one, HERMENEUTIC_BUILD_SERVICE, and
# CMAKE_BUILD_TYPE all take a new -D value on any reconfigure, no wipe
# needed (verified: `cmake -B dir -DOPT=ON` then `cmake -B dir -DOPT=OFF`
# on the same dir does flip it - CMake doesn't lock option() cache vars).
CONFIGURE_ARGS=(
    -B "$BUILD_DIR" -S .
    -DCMAKE_C_COMPILER="$CLANG"
    -DCMAKE_CXX_COMPILER="$CLANGXX"
    -DCMAKE_BUILD_TYPE=Debug
    -DHERMENEUTIC_COVERAGE=ON
    -DHERMENEUTIC_BUILD_TESTS=ON
    -DHERMENEUTIC_BUILD_INGESTION=ON
)

# Shared by both vcpkg-missing error messages below, so the "what
# --no-service actually skips" description only needs updating in one
# place if the HERMENEUTIC_BUILD_INGESTION/BUILD_SERVICE gating ever changes.
NO_SERVICE_HINT="pass --no-service for the smaller vcpkg-free build that skips net/ and the gRPC/Boost/OpenSSL-gated half of ingestion/ - book_subscription.hpp stays in scope either way"

TOOLCHAIN=""
if [[ "$WITH_SERVICE" -eq 1 ]]; then
    VCPKG_ROOT="${VCPKG_ROOT:-$HOME/vcpkg}"
    if [[ ! -d "$VCPKG_ROOT" ]]; then
        echo "error: VCPKG_ROOT ($VCPKG_ROOT) is not a directory (set VCPKG_ROOT, or $NO_SERVICE_HINT)" >&2
        exit 1
    fi
    # Normalize away a trailing slash or a relative VCPKG_ROOT so the
    # staleness check below (a plain string compare against what CMake
    # persisted last time) doesn't false-positive on a cosmetic difference.
    VCPKG_ROOT="$(cd "$VCPKG_ROOT" && pwd)"
    TOOLCHAIN="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
    if [[ ! -f "$TOOLCHAIN" ]]; then
        echo "error: no vcpkg toolchain at $TOOLCHAIN (set VCPKG_ROOT, or $NO_SERVICE_HINT)" >&2
        exit 1
    fi
    CONFIGURE_ARGS+=(-DCMAKE_TOOLCHAIN_FILE="$TOOLCHAIN" -DHERMENEUTIC_BUILD_SERVICE=ON)
    SCOPE_NOTE="full build (HERMENEUTIC_BUILD_SERVICE=ON): includes net/, ingestion/, and the aggregator service/client tests"
else
    CONFIGURE_ARGS+=(-DHERMENEUTIC_BUILD_SERVICE=OFF)
    SCOPE_NOTE="vcpkg-free build (HERMENEUTIC_BUILD_SERVICE=OFF): net/** and the gRPC/Boost/OpenSSL-gated half of ingestion/ (venue_session.hpp, ingestion_runner.hpp) plus the aggregator service/client are NOT built, so they will not appear in the report at all - that is a scope gap, not 0% coverage. ingestion/book_subscription.hpp stays in scope either way (it only needs HERMENEUTIC_BUILD_INGESTION, on by default). Pass no flags (or drop --no-service) for the full picture."
fi

echo "== coverage scope: $SCOPE_NOTE"

# Wipe the build dir only when a *sticky* CMake setting differs from what's
# already configured there. CMAKE_TOOLCHAIN_FILE and CMAKE_<LANG>_COMPILER
# are the two settings CMake locks in after a directory's first configure -
# every other flag CONFIGURE_ARGS passes (HERMENEUTIC_BUILD_SERVICE/
# INGESTION/COVERAGE, CMAKE_BUILD_TYPE) is a plain option()/cache variable
# that a reconfigure updates freely, so only these two need to gate the
# wipe. Getting this wrong is exactly the bug commit f59d2dd fixed for
# CMAKE_TOOLCHAIN_FILE alone; checking the compiler too closes the same gap
# for a build-coverage/ that was ever pointed at a non-Clang compiler.
# Reusing a same-scope cache (the common case when iterating) skips
# re-fetching googletest/simdjson and re-running vcpkg install.
CACHE="$BUILD_DIR/CMakeCache.txt"
NEEDS_FRESH=1
if [[ -f "$CACHE" ]]; then
    # CMake's own cache-variable type for these varies (CMAKE_TOOLCHAIN_FILE
    # is FILEPATH; CMAKE_C_COMPILER/CMAKE_CXX_COMPILER, despite conceptually
    # being one too, are persisted as STRING) - match the type generically
    # rather than hardcode one, or a mismatch here silently defeats cache
    # reuse entirely (every run mismatches on empty vs. non-empty and
    # wipes/rebuilds from scratch).
    CACHED_TOOLCHAIN="$(grep -o 'CMAKE_TOOLCHAIN_FILE:[A-Z]*=.*' "$CACHE" | cut -d= -f2- || true)"
    CACHED_CC="$(grep -o 'CMAKE_C_COMPILER:[A-Z]*=.*' "$CACHE" | cut -d= -f2- || true)"
    CACHED_CXX="$(grep -o 'CMAKE_CXX_COMPILER:[A-Z]*=.*' "$CACHE" | cut -d= -f2- || true)"
    if [[ "$CACHED_TOOLCHAIN" == "$TOOLCHAIN" && "$CACHED_CC" == "$CLANG" && "$CACHED_CXX" == "$CLANGXX" ]]; then
        NEEDS_FRESH=0
    fi
fi
if [[ "$NEEDS_FRESH" -eq 1 ]]; then
    rm -rf "$BUILD_DIR"
fi
echo "== configuring $BUILD_DIR"
cmake "${CONFIGURE_ARGS[@]}"

echo "== building"
cmake --build "$BUILD_DIR" -j "$JOBS"

PROFILE_DIR="$BUILD_DIR/coverage-profiles"
rm -rf "$PROFILE_DIR"
mkdir -p "$PROFILE_DIR"

# Ask ctest which binaries it would run, rather than assuming a layout
# (RUNTIME_OUTPUT_DIRECTORY isn't set in this project's CMakeLists, but a
# vcpkg-provided package can still set it project-wide) - then run each
# unique binary exactly once, directly, with no --gtest_filter.
#
# Captured into plain variables with explicit `||` checks, not piped
# straight into `< <(...)`: a command's exit status inside a process
# substitution is invisible to `set -e` and to the while-loop reading it,
# so a real ctest/python3 failure there would silently look like "zero
# tests found" instead of surfacing its actual error.
CTEST_JSON="$(ctest --test-dir "$BUILD_DIR" --show-only=json-v1)" ||
    { echo "error: 'ctest --show-only=json-v1' failed in $BUILD_DIR" >&2; exit 1; }
PARSED_BINARIES="$(printf '%s' "$CTEST_JSON" | python3 -c '
import json, sys
data = json.load(sys.stdin)
seen = {}
for t in data["tests"]:
    cmd = t["command"]
    seen[cmd[0]] = True
for path in seen:
    print(path)
')" || { echo "error: parsing ctest's test list with python3 failed" >&2; exit 1; }

# `mapfile`/`readarray` need bash >= 4; macOS ships bash 3.2, so read into
# the array line-by-line instead.
TEST_BINARIES=()
while IFS= read -r line; do
    [[ -n "$line" ]] && TEST_BINARIES+=("$line")
done <<< "$PARSED_BINARIES"

if [[ "${#TEST_BINARIES[@]}" -eq 0 ]]; then
    echo "error: ctest found no tests in $BUILD_DIR" >&2
    exit 1
fi

# gtest_discover_tests() registers a "<target>_NOT_BUILT" ctest entry with a
# bogus command when a binary is missing at discovery time - silently
# skipping it here would drop a whole tier's coverage from the report while
# the rest of the table still looks plausible, so this must abort, not warn.
for bin in "${TEST_BINARIES[@]}"; do
    if [[ ! -x "$bin" ]]; then
        echo "error: ctest reports a non-executable test command (build incomplete?): $bin" >&2
        exit 1
    fi
done

PROFILE_DIR="$(cd "$PROFILE_DIR" && pwd)"

# Run every binary in parallel, not sequentially: each already gets its own
# LLVM_PROFILE_FILE prefix (plus %p for its own pid), and each test binary
# is its own process (network tests bind ephemeral port 0, not a fixed
# port), so nothing here shares mutable state across binaries. With one of
# them (venue_session_test) alone taking ~50s of real-timer waits, running
# the full suite sequentially made total wall time the *sum* of every
# binary's runtime instead of roughly the slowest one - verified against
# this project's timing-sensitive reconnect/idle-timeout tests before
# landing this: 5 consecutive runs, zero test failures. The coverage TOTAL
# itself wobbled by 1 region between runs (90.68% vs. 90.60%), which is a
# race already inherent to a backoff/reconnect branch's own timing, not a
# parallelism-introduced test flake - every run still passed. Output is
# captured per binary and printed after that binary finishes (in launch
# order) rather than streamed live, since interleaving N processes' stdout
# directly would be unreadable.
echo "== running ${#TEST_BINARIES[@]} test binaries in parallel"
PIDS=()
NAMES=()
# Log files are keyed "$i-$name.log" (the loop index), not just "$name.log":
# RUNTIME_OUTPUT_DIRECTORY isn't set anywhere in this project's CMakeLists
# today (see the comment above), but nothing guarantees that stays true, and
# two binaries sharing a basename from different output directories would
# otherwise silently interleave/corrupt each other's captured log.
for i in "${!TEST_BINARIES[@]}"; do
    bin="${TEST_BINARIES[$i]}"
    name="$(basename "$bin")"
    NAMES+=("$name")
    # gtest_discover_tests() sets each ctest case's WORKING_DIRECTORY to the
    # build root (every target here is declared in the top-level
    # CMakeLists.txt), so a test that reads a fixture by a build-relative
    # path only works run from there - match that instead of running from
    # $SOURCE_ROOT.
    (cd "$BUILD_DIR" && LLVM_PROFILE_FILE="$PROFILE_DIR/$name-%p.profraw" "$bin") \
        > "$PROFILE_DIR/$i-$name.log" 2>&1 &
    PIDS+=("$!")
done

TEST_FAILED=0
FAILED_NAMES=()
for i in "${!PIDS[@]}"; do
    name="${NAMES[$i]}"
    echo "-- $name"
    if ! wait "${PIDS[$i]}"; then
        TEST_FAILED=1
        FAILED_NAMES+=("$name")
    fi
    cat "$PROFILE_DIR/$i-$name.log"
done

echo "== merging profiles"
# nullglob so a total wipeout (every binary crashed before its profiling
# runtime flushed anything) hits this script's own clear error instead of
# `set -e` killing the run on llvm-profdata's raw "No such file or
# directory" for the literal, unmatched glob - which skipped the
# "one or more test binaries failed" diagnostic below entirely.
shopt -s nullglob
PROFRAW_FILES=("$PROFILE_DIR"/*.profraw)
shopt -u nullglob
if [[ "${#PROFRAW_FILES[@]}" -eq 0 ]]; then
    echo "error: no .profraw files were produced - did every test binary crash before writing a profile?" >&2
    exit 1
fi
# --failure-mode=warn: a binary killed mid-write (crash, OOM, disk full)
# can leave one truncated .profraw next to otherwise-valid ones from every
# other binary - the default mode fails the whole merge (and therefore the
# whole report) over that one file, discarding good data the TEST_FAILED/
# FAILED_NAMES handling above was written to still report on.
xcrun llvm-profdata merge -sparse --failure-mode=warn "${PROFRAW_FILES[@]}" -o "$PROFILE_DIR/merged.profdata"

# Restrict the report to this project's own headers/sources - not tests/,
# not googletest/simdjson under _deps/, not generated *.pb.h/*.pb.cc, and
# not libc++/system headers that happen to pick up coverage mapping for
# templates instantiated in an instrumented TU.
SOURCES=()
while IFS= read -r line; do
    [[ -n "$line" ]] && SOURCES+=("$line")
done < <(
    find "$SOURCE_ROOT/include" "$SOURCE_ROOT/apps" \
        \( -name '*.hpp' -o -name '*.cpp' \) -type f
)

OBJECT_ARGS=()
for bin in "${TEST_BINARIES[@]:1}"; do
    OBJECT_ARGS+=(-object "$bin")
done

echo
echo "== coverage scope: $SCOPE_NOTE"
echo
# "${OBJECT_ARGS[@]}" alone throws "unbound variable" under bash 3.2's
# `set -u` when OBJECT_ARGS is empty (exactly one test binary discovered) -
# a real bash 3.2 quirk (empty arrays are, for nounset's purposes, treated
# as unset), not reachable today since this project always registers 8+
# gtest binaries, but latent for any future build that reduces to one. The
# `${arr[@]+"${arr[@]}"}` idiom below expands to nothing instead of erroring
# when the array is empty.
xcrun llvm-cov report "${TEST_BINARIES[0]}" ${OBJECT_ARGS[@]+"${OBJECT_ARGS[@]}"} \
    -instr-profile="$PROFILE_DIR/merged.profdata" \
    "${SOURCES[@]}"

if [[ "$EMIT_HTML" -eq 1 ]]; then
    HTML_DIR="$BUILD_DIR/coverage-html"
    # llvm-cov show only (re)writes pages for sources in the current
    # SOURCES list, so without clearing this first, a header removed or
    # renamed since the last --html run would leave its stale page (with
    # outdated numbers) sitting on disk and still openable.
    rm -rf "$HTML_DIR"
    echo "== writing HTML report to $HTML_DIR"
    xcrun llvm-cov show "${TEST_BINARIES[0]}" ${OBJECT_ARGS[@]+"${OBJECT_ARGS[@]}"} \
        -instr-profile="$PROFILE_DIR/merged.profdata" \
        -format=html -output-dir="$HTML_DIR" \
        "${SOURCES[@]}"
fi

if [[ "$TEST_FAILED" -ne 0 ]]; then
    echo "error: these test binaries failed (coverage above reflects a failing run):" \
         "${FAILED_NAMES[*]}" >&2
    exit 1
fi

#!/usr/bin/env bash
# Drives the full coverage cycle: configure with -DHERMENEUTIC_COVERAGE=ON,
# build every test binary, run each one directly (not through ctest - see
# below), merge the resulting profiles, and print a per-file report scoped
# to this project's own headers/sources.
#
# Two toolchains:
#   - default: Clang's source-based coverage (llvm-profdata/llvm-cov).
#   - --gcc: GCC's gcov coverage (gcov, reported via gcovr). This project's
#     usual dev machine has no real GCC (macOS ships /usr/bin/g++ as an
#     Apple Clang shim - see the compiler-identity check below), so this
#     was verified in an ubuntu:24.04 container with gcc-14/g++-14/gcovr
#     installed (matching docker/Dockerfile's own toolchain - see its
#     ENV CC=gcc-14 CXX=g++-14): `--no-service` passed cleanly (all 227+
#     gtest suites green, gcovr TOTAL 1383/1553 lines = 89%, --html
#     produced per-file pages, and a same-args rerun reused the CMake
#     cache and reproduced the identical TOTAL - confirming the .gcda
#     wipe below actually prevents cross-run accumulation). Not run
#     against this project's own CI (there isn't one yet) or against the
#     --with-service/vcpkg-linked scope. Needs gcovr (`pip install gcovr`
#     or your package manager's gcovr).
#
# Deliberately runs each test *binary* once instead of `ctest --test-dir
# <build dir>`: gtest_discover_tests() registers one ctest case per gtest
# TEST(), so a plain ctest run launches the same binary hundreds of times.
# Under Clang, a fixed LLVM_PROFILE_FILE across those launches means each
# run overwrites the last one's profile; under GCC, each run instead
# *accumulates* into the same .gcda counters. Either way "coverage" would
# stop meaning "every test ran once" - so this script always runs each
# binary once (no --gtest_filter), which is both correct and far faster.
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

# Configure/build/run/report are driven by the named presets in
# CMakePresets.json (coverage-clang[-service], coverage-gcc[-service]) -
# one binaryDir per compiler-kind/scope combination. The presets reference
# $env{CC}/$env{CXX}/$env{VCPKG_ROOT}, so this script's job for those is
# just: resolve the right binary/identity-check it/export it, not build a
# -D args array. Every configure passes --fresh rather than detect
# whether the cached compiler/toolchain still matches what was just
# resolved (a real bug this script used to carry piecemeal, fixed by
# commit f59d2dd for CMAKE_TOOLCHAIN_FILE alone, then generalized and
# re-broken more than once across this file's history) - see the
# --fresh call site's own comment for why reconfiguring unconditionally
# is cheap enough here to not need that detection at all.

set -euo pipefail

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$SOURCE_ROOT"

# This project's own coverage-scope allowlist (not tests/, not _deps/
# googletest/simdjson, not generated *.pb.h/*.pb.cc) - a single source of
# truth both report paths below build their own tool-specific filter from
# (an explicit find'd file list for Clang/llvm-cov, gcovr --filter regexes
# for GCC), rather than each hardcoding "include" and "apps" separately.
SCOPE_ROOTS=("$SOURCE_ROOT/include" "$SOURCE_ROOT/apps")

WITH_SERVICE=1
EMIT_HTML=0
COMPILER_KIND="clang"
JOBS="$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo 4)"

for arg in "$@"; do
    case "$arg" in
        --no-service) WITH_SERVICE=0 ;;
        --html) EMIT_HTML=1 ;;
        --gcc) COMPILER_KIND="gcc" ;;
        -h|--help)
            echo "Usage: $0 [--no-service] [--html] [--gcc]"
            echo "  --no-service  vcpkg-free build (skips net/, venue_session.hpp/ingestion_runner.hpp, and the aggregator service/client)"
            echo "  --html        also emit an HTML report under build-coverage-<clang|gcc>[-service]/coverage-html"
            echo "  --gcc         use GCC's gcov coverage (via gcovr) instead of Clang's source-based coverage"
            exit 0
            ;;
        *)
            echo "Unknown argument: $arg" >&2
            exit 1
            ;;
    esac
done

if ! command -v python3 >/dev/null 2>&1; then
    echo "error: python3 not found (needed to parse ctest's --show-only=json-v1 output)" >&2
    exit 1
fi

GCOV_BIN=""
if [[ "$COMPILER_KIND" == "clang" ]]; then
    if ! xcrun --find llvm-profdata >/dev/null 2>&1 || ! xcrun --find llvm-cov >/dev/null 2>&1; then
        echo "error: llvm-profdata/llvm-cov not found via xcrun (needs Xcode command line tools)" >&2
        exit 1
    fi
    # Resolved via `xcrun --find`, the same lookup used for llvm-profdata/
    # llvm-cov above, rather than `command -v`: on a machine with Homebrew
    # LLVM ahead of the Xcode/CLT toolchain on PATH, `command -v clang++`
    # and `xcrun --find llvm-cov` would silently resolve to two different
    # LLVM versions, and a raw-profile format mismatch between the
    # compiler that instruments and the tool that reads the result can
    # misparse rather than error.
    CC_BIN="$(xcrun --find clang)" || { echo "error: clang not found via xcrun" >&2; exit 1; }
    CXX_BIN="$(xcrun --find clang++)" || { echo "error: clang++ not found via xcrun" >&2; exit 1; }
else
    # CXX lets the caller point at a specific GCC (e.g. `CXX=g++-14
    # ./scripts/coverage.sh --gcc`) since the real-GCC binary name isn't
    # portable: plain `g++` on Linux, typically `g++-<ver>` on Homebrew
    # macOS (where unversioned `g++` is Apple Clang wearing a GCC name -
    # see the identity check below).
    CXX_BIN="$(command -v "${CXX:-g++}")" || { echo "error: ${CXX:-g++} not found (set CXX, e.g. CXX=g++-14)" >&2; exit 1; }
    # macOS ships /usr/bin/gcc and /usr/bin/g++ as Apple Clang shims (no
    # real GCC installed by default), so "found a binary named g++" does
    # not mean "found GCC" - a real Clang build under this branch would
    # silently instrument with --coverage (a flag Clang also accepts) but
    # emit no .gcda files gcov/gcovr can read. Gate on --version output,
    # the same discriminator used pre-configure; CMAKE_CXX_COMPILER_ID is
    # checked again after configure below as a second, authoritative gate.
    if ! "$CXX_BIN" --version 2>&1 | grep -qi "Free Software Foundation"; then
        echo "error: $CXX_BIN does not look like real GCC (no 'Free Software Foundation' in --version output - on macOS, /usr/bin/g++ is usually Apple Clang; install GCC and set CXX=g++-<ver>)" >&2
        exit 1
    fi
    # CC deliberately isn't taken from the environment the way CXX is:
    # this project's C compiler only matters for matching the C++ one
    # (there's no separate C-only coverage path - though CMAKE_C_COMPILER
    # is genuinely consulted here despite `project(hermeneutic CXX)`
    # never listing C: googletest's own CMakeLists.txt pulls in
    # find_package(Threads), which enables/probes C transitively), so
    # CC_BIN is *derived* from CXX_BIN's own name/directory (g++-14 ->
    # gcc-14 next to it) rather than independently resolved from
    # ${CC:-gcc} - the latter would let a caller who only set CXX=g++-14
    # silently end up with /usr/bin/gcc (Apple Clang) as CC_BIN on macOS,
    # a mixed toolchain the CMAKE_CXX_COMPILER_ID gate below wouldn't
    # catch (it only inspects the C++ compiler). gcov must similarly be
    # the exact one built alongside this g++ - a mismatched gcov either
    # refuses to parse the .gcno/.gcda format or misreports counts.
    #
    # Both are found via the same sibling-binary strategy: substitute the
    # "g++" substring in CXX_BIN's own basename (e.g. g++-14 -> gcc-14 or
    # gcov-14) and prefer that file if it exists next to CXX_BIN, since
    # only a same-directory sibling is guaranteed to match the exact GCC
    # installation CXX_BIN came from. Falls back to a plain PATH lookup
    # only when the substitution was a genuine no-op (CXX_BIN's basename
    # doesn't literally contain "g++" - e.g. CXX=c++, or a custom-named
    # wrapper) or the derived sibling doesn't exist; checking for the
    # no-op case explicitly matters because an unguarded substitution
    # that doesn't change the string would otherwise have this "sibling"
    # check trivially match CXX_BIN's own path (already confirmed
    # executable) and silently hand back the C++ compiler as CC_BIN/
    # GCOV_BIN instead of failing or falling back.
    CXX_BASENAME="$(basename "$CXX_BIN")"
    CXX_DIR="$(dirname "$CXX_BIN")"
    find_gcc_sibling() {
        local replacement="$1" path_fallback_name="$2" candidate="${CXX_BASENAME/g++/$1}"
        if [[ "$candidate" != "$CXX_BASENAME" && -x "$CXX_DIR/$candidate" ]]; then
            echo "$CXX_DIR/$candidate"
            return 0
        fi
        command -v "$path_fallback_name"
    }
    CC_BIN="$(find_gcc_sibling gcc gcc)" || { echo "error: no gcc found matching $CXX_BIN (tried substituting g++->gcc in $CXX_BASENAME, and PATH)" >&2; exit 1; }
    GCOV_BIN="$(find_gcc_sibling gcov gcov)" || { echo "error: no gcov found matching $CXX_BIN (tried substituting g++->gcov in $CXX_BASENAME, and PATH)" >&2; exit 1; }
    if ! command -v gcovr >/dev/null 2>&1; then
        echo "error: gcovr not found (needed for the --gcc report/--html output; pip install gcovr)" >&2
        exit 1
    fi
fi

# CC/CXX are how CMakePresets.json's coverage-clang/coverage-gcc presets
# (both `"CMAKE_C_COMPILER": "$env{CC}"` / `"CMAKE_CXX_COMPILER":
# "$env{CXX}"`) pick up the machine-specific binaries resolved above -
# presets are static JSON and can't run `xcrun --find` or derive a
# matching gcc-<ver> themselves, so this export is the handoff point.
export CC="$CC_BIN" CXX="$CXX_BIN"

# Shared by both vcpkg-missing error messages below, so the "what
# --no-service actually skips" description only needs updating in one
# place if the HERMENEUTIC_BUILD_INGESTION/BUILD_SERVICE gating ever changes.
NO_SERVICE_HINT="pass --no-service for the smaller vcpkg-free build that skips net/ and the gRPC/Boost/OpenSSL-gated half of ingestion/ - book_subscription.hpp stays in scope either way"

PRESET="coverage-${COMPILER_KIND}"
if [[ "$WITH_SERVICE" -eq 1 ]]; then
    VCPKG_ROOT="${VCPKG_ROOT:-$HOME/vcpkg}"
    if [[ ! -d "$VCPKG_ROOT" ]]; then
        echo "error: VCPKG_ROOT ($VCPKG_ROOT) is not a directory (set VCPKG_ROOT, or $NO_SERVICE_HINT)" >&2
        exit 1
    fi
    VCPKG_ROOT="$(cd "$VCPKG_ROOT" && pwd)"
    TOOLCHAIN="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
    if [[ ! -f "$TOOLCHAIN" ]]; then
        echo "error: no vcpkg toolchain at $TOOLCHAIN (set VCPKG_ROOT, or $NO_SERVICE_HINT)" >&2
        exit 1
    fi
    export VCPKG_ROOT
    PRESET="${PRESET}-service"
    SCOPE_NOTE="full build (HERMENEUTIC_BUILD_SERVICE=ON): includes net/, ingestion/, and the aggregator service/client tests"
else
    SCOPE_NOTE="vcpkg-free build (HERMENEUTIC_BUILD_SERVICE=OFF): net/** and the gRPC/Boost/OpenSSL-gated half of ingestion/ (venue_session.hpp, ingestion_runner.hpp) plus the aggregator service/client are NOT built, so they will not appear in the report at all - that is a scope gap, not 0% coverage. ingestion/book_subscription.hpp stays in scope either way (it only needs HERMENEUTIC_BUILD_INGESTION, on by default). Pass no flags (or drop --no-service) for the full picture."
fi

# Read BUILD_DIR out of CMakePresets.json itself (walking `inherits` up
# to whichever preset in the chain actually sets binaryDir) rather than
# reconstructing the same "build-coverage-<kind>[-service]" string by hand
# here - a hand-duplicated string would silently point ctest/profile-merge/
# report at the wrong (or a never-configured) directory if a preset's
# binaryDir is ever renamed in CMakePresets.json without this string being
# updated to match.
BUILD_DIR="$(python3 -c '
import json, sys
name = sys.argv[1]
presets = {p["name"]: p for p in json.load(open("CMakePresets.json"))["configurePresets"]}
def resolve(n, seen):
    if n in seen:
        sys.exit(f"error: inherits cycle in CMakePresets.json at preset {n}")
    seen.add(n)
    p = presets[n]
    if "binaryDir" in p:
        return p["binaryDir"]
    inherits = p.get("inherits", [])
    for parent in ([inherits] if isinstance(inherits, str) else inherits):
        r = resolve(parent, seen)
        if r is not None:
            return r
    return None
bd = resolve(name, set())
if bd is None:
    sys.exit(f"error: preset {name} has no resolvable binaryDir in CMakePresets.json")
print(bd.replace("${sourceDir}", "."))
' "$PRESET")" || exit 1

# CMake cache-locks CMAKE_C_COMPILER/CMAKE_CXX_COMPILER (and
# CMAKE_TOOLCHAIN_FILE/toolchainFile) on a build tree's *first* configure,
# so a plain reconfigure of a reused binaryDir can silently keep building
# against a stale compiler or vcpkg checkout even though CC_BIN/CXX_BIN/
# VCPKG_ROOT just resolved to something different this run (CXX=g++-13
# then later CXX=g++-14 against the same build-coverage-gcc, say). Rather
# than detect that staleness (comparing cached vs. requested values,
# wiping on mismatch - what this block used to do, and what the
# pre-presets version of this script did too), `--fresh` sidesteps the
# question entirely by always reconfiguring from a clean CMake cache -
# needs CMake >=3.24, well under this project's >=3.28 floor.
#
# `--fresh` does remove <binaryDir>/CMakeCache.txt and
# <binaryDir>/CMakeFiles/ (CMake's own documented behavior), and this
# project's own compiled objects live under exactly that path
# (CMakeFiles/hermeneutic_tests.dir/...), so they - unlike vcpkg's
# vcpkg_installed/, an entirely separate directory `--fresh` never
# touches - do NOT survive a reconfigure and get rebuilt from scratch
# every run (verified: 25 -> 7 .o files under CMakeFiles/ after --fresh,
# then 18 "Building CXX object" lines on the next `cmake --build`).
# FetchContent'd googletest/simdjson happen to survive anyway: each is
# add_subdirectory()'d into its own _deps/<name>-build/CMakeFiles/,
# physically outside the top-level CMakeFiles/ this flag clears - not
# because `--fresh` treats them specially. So the real cost of
# unconditional --fresh is "recompile this project's own coverage-
# instrumented sources every run", not zero - it's just small enough at
# this project's current size to fold into the total run times already
# verified end-to-end for this script (~10-25s for --no-service, ~2-3.5
# min for the default vcpkg-linked build - no "-service" flag exists;
# --no-service's absence is what selects it - the latter dominated by
# test execution, including venue_session_test's real-timer waits, not
# by recompiling this project's own sources, and nowhere near vcpkg/
# gRPC's own from-scratch build time).
echo "== coverage scope: $SCOPE_NOTE"
echo "== configuring $BUILD_DIR (preset: $PRESET)"
cmake --preset="$PRESET" --fresh

# Authoritative check, on top of the pre-configure --version grep above:
# trust what CMake itself resolved CXX_BIN to, not just the path we asked
# for. Catches a wrapper/symlink that resolves to a different compiler
# than its own --version banner claimed, or a stale cache from a
# differently-flavored compiler that the staleness check above didn't
# treat as stale for some other reason. CMAKE_CXX_COMPILER_ID isn't a
# CMakeCache.txt entry (unlike CMAKE_C_COMPILER/CMAKE_CXX_COMPILER above) -
# CMake writes it into CMakeFiles/<cmake-version>/CMakeCXXCompiler.cmake
# instead, so it's read from there. The exact <cmake-version> subdirectory
# is read out of CMakeCache.txt's own CMAKE_CACHE_*_VERSION entries
# (written by the `cmake --preset` invocation just above) rather than
# globbed with `find ... | head -n1`: a long-lived, reused binaryDir
# (the whole point of one directory per combination) can accumulate a
# second CMakeFiles/<version>/ after a local CMake upgrade, and a glob's
# result order isn't sorted or mtime-ordered, so `head -n1` could pick
# the stale prior version's file instead of the one this configure run
# just wrote.
CMAKE_CACHE_VERSION="$(awk -F= '
    /^CMAKE_CACHE_MAJOR_VERSION:INTERNAL=/ { major=$2 }
    /^CMAKE_CACHE_MINOR_VERSION:INTERNAL=/ { minor=$2 }
    /^CMAKE_CACHE_PATCH_VERSION:INTERNAL=/ { patch=$2 }
    END { print major "." minor "." patch }
' "$BUILD_DIR/CMakeCache.txt")"
COMPILER_ID_FILE="$BUILD_DIR/CMakeFiles/$CMAKE_CACHE_VERSION/CMakeCXXCompiler.cmake"
# Failure to find/parse this file is "couldn't verify", not "wrong compiler
# family" - conflating the two would abort an otherwise-good configure (a
# CMake version that relocates this file, or a generator that hasn't
# populated it yet) with a misleading "not GNU"/"not Clang" message that
# points at the wrong problem.
if [[ ! -f "$COMPILER_ID_FILE" ]]; then
    echo "error: expected $COMPILER_ID_FILE (from CMakeCache.txt's own CMAKE_CACHE_*_VERSION) but it doesn't exist - could not verify the configured compiler's identity" >&2
    exit 1
fi
CACHED_COMPILER_ID="$(grep -o 'CMAKE_CXX_COMPILER_ID "[^"]*"' "$COMPILER_ID_FILE" | cut -d'"' -f2 || true)"
if [[ -z "$CACHED_COMPILER_ID" ]]; then
    echo "error: could not parse CMAKE_CXX_COMPILER_ID out of $COMPILER_ID_FILE" >&2
    exit 1
fi
if [[ "$COMPILER_KIND" == "gcc" && "$CACHED_COMPILER_ID" != "GNU" ]]; then
    echo "error: configured compiler resolved to CMAKE_CXX_COMPILER_ID=$CACHED_COMPILER_ID, not GNU (--gcc was requested with CXX=$CXX_BIN)" >&2
    exit 1
# Substring match (*Clang*), not exact equality, to stay consistent with
# CMakeLists.txt's own `CMAKE_CXX_COMPILER_ID MATCHES "Clang"` gate - an
# exact-equality check here could reject an ID CMakeLists.txt just
# accepted (e.g. a vendor/cross Clang variant CMake reports under some
# other "*Clang*" spelling), failing this script after a configure that
# actually succeeded with instrumentation correctly applied.
elif [[ "$COMPILER_KIND" == "clang" && "$CACHED_COMPILER_ID" != *Clang* ]]; then
    echo "error: configured compiler resolved to CMAKE_CXX_COMPILER_ID=$CACHED_COMPILER_ID, not a Clang variant" >&2
    exit 1
fi

echo "== building"
cmake --build "$BUILD_DIR" -j "$JOBS"

if [[ "$COMPILER_KIND" == "gcc" ]]; then
    # Unlike Clang's LLVM_PROFILE_FILE (a fresh path per run, keyed by
    # %p), GCC writes .gcda counters next to each translation unit's .o
    # and *accumulates* into them across runs of the same binary rather
    # than overwriting. A stale .gcda from a previous scripts/coverage.sh
    # invocation (or from `cmake --build` alone re-running a test target)
    # would silently inflate this run's counts, so wipe before running
    # anything - the gcov analogue of the PROFILE_DIR wipe below.
    find "$BUILD_DIR" -name '*.gcda' -delete
fi

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

# Run every binary in parallel, not sequentially: under Clang each already
# gets its own LLVM_PROFILE_FILE prefix (plus %p for its own pid); under
# GCC, each binary's .gcda files live next to *that binary's own* object
# files (a separate CMakeFiles/<target>.dir/ per target), so two different
# binaries running concurrently don't share a .gcda path either - only two
# concurrent runs of the *same* binary would collide, which this script
# never does (each TEST_BINARIES entry is launched exactly once). Each
# test binary is also its own process (network tests bind ephemeral port
# 0, not a fixed port), so nothing here shares mutable state across
# binaries. With one of them (venue_session_test) alone taking ~50s of
# real-timer waits, running the full suite sequentially made total wall
# time the *sum* of every binary's runtime instead of roughly the slowest
# one - verified, on the Clang path, against this project's
# timing-sensitive reconnect/idle-timeout tests before landing this: 5
# consecutive runs, zero test failures, coverage TOTAL wobbling by 1
# region between runs (90.68% vs. 90.60%, a race already inherent to a
# backoff/reconnect branch's own timing, not a parallelism-introduced test
# flake - every run still passed). The GCC/gcov path reuses the same
# parallel-launch code by construction; the --no-service scope (no
# venue_session_test, so this specific timing-sensitive suite wasn't
# re-exercised there) was verified clean under GCC too - see the --gcc
# header comment. Output is captured per binary and printed after that
# binary finishes (in launch order) rather than streamed live, since
# interleaving N processes' stdout directly would be unreadable.
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

HTML_DIR="$BUILD_DIR/coverage-html"

if [[ "$COMPILER_KIND" == "clang" ]]; then
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

    # Restrict the report to SCOPE_ROOTS (this project's own headers/
    # sources) - not libc++/system headers that happen to pick up
    # coverage mapping for templates instantiated in an instrumented TU.
    SOURCES=()
    while IFS= read -r line; do
        [[ -n "$line" ]] && SOURCES+=("$line")
    done < <(
        find "${SCOPE_ROOTS[@]}" \( -name '*.hpp' -o -name '*.cpp' \) -type f
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
    # Three things in this report that look like bugs but aren't:
    #   - `warning: N functions have mismatched data`: this project is
    #     almost entirely header-only, so the same inline function gets
    #     compiled slightly differently across separate test binaries'
    #     translation units, and llvm-cov flags (but doesn't fail on) that
    #     mismatch when merging profiles from multiple binaries.
    #   - llvm-cov only reports on template specializations actually
    #     instantiated by the tests, e.g. BasicFixedPoint<9> (this
    #     project's Notional) showing coverage while other widths don't -
    #     that reflects which specializations ran, not a hole in the report.
    #   - A header with no coverage mapping at all (nothing in it compiles
    #     to an instrumented region - a pure alias/traits header, or one no
    #     instrumented test binary happens to include, like
    #     apps/aggregator/client_main.cpp/server_main.cpp, which only build
    #     into the non-test service/client executables) is silently missing
    #     from the table entirely, not listed at 0%. Compare the table's row
    #     count against `find include apps -name '*.hpp' -o -name '*.cpp'`
    #     if a file's absence needs explaining.
    xcrun llvm-cov report "${TEST_BINARIES[0]}" ${OBJECT_ARGS[@]+"${OBJECT_ARGS[@]}"} \
        -instr-profile="$PROFILE_DIR/merged.profdata" \
        "${SOURCES[@]}"

    if [[ "$EMIT_HTML" -eq 1 ]]; then
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
else
    # gcovr reads .gcno/.gcda directly (no separate merge step - each
    # binary's counters already live under $BUILD_DIR, one directory per
    # CMake target, so gcovr just walks the tree); this count is the gcov
    # analogue of the .profraw check above, since a gcovr run over zero
    # .gcda files reports "0 files" rather than erroring, which would
    # otherwise look like a clean empty pass instead of "every binary
    # crashed before flushing coverage".
    GCDA_COUNT="$(find "$BUILD_DIR" -name '*.gcda' | wc -l | tr -d ' ')"
    if [[ "$GCDA_COUNT" -eq 0 ]]; then
        echo "error: no .gcda files were produced - did every test binary crash before writing coverage?" >&2
        exit 1
    fi

    # Same SCOPE_ROOTS allowlist as the Clang path above, expressed as
    # gcovr --filter regexes (matched against each source's absolute
    # path) instead of an explicit file list - gcovr's --filter is a
    # regex, so each root is regex-escaped via Python's own re.escape
    # (SOURCE_ROOT is an absolute path that can itself contain
    # regex-special characters, e.g. a literal "." in a directory
    # component - as it does in this very checkout under
    # .claude/worktrees/ - which would otherwise silently over-match
    # "any character" instead of a literal dot).
    GCOVR_FILTERS=()
    for root in "${SCOPE_ROOTS[@]}"; do
        ESCAPED_ROOT="$(python3 -c 'import re, sys; print(re.escape(sys.argv[1]))' "$root")"
        GCOVR_FILTERS+=(--filter "${ESCAPED_ROOT}/.*")
    done

    echo
    echo "== coverage scope: $SCOPE_NOTE"
    echo
    # One gcovr invocation for both the terminal table and (with --html)
    # the HTML report, not two: both formats read the exact same
    # .gcno/.gcda tree under $BUILD_DIR, so a second invocation would
    # just re-parse identical coverage data from scratch for no
    # difference in output (unlike the Clang side, where `llvm-cov
    # report` and `llvm-cov show` are genuinely different operations
    # against the same already-merged merged.profdata - there's no
    # separate merge step here to amortize a second parse against).
    # `--txt -` means "the usual text table, to stdout" (gcovr defaults
    # to that anyway with no format flags at all, but stays implicit once
    # --html is also requested, so it must be requested explicitly here
    # to keep printing the table when --html is passed).
    GCOVR_ARGS=(--root "$SOURCE_ROOT" --gcov-executable "$GCOV_BIN" "${GCOVR_FILTERS[@]}" --txt -)
    if [[ "$EMIT_HTML" -eq 1 ]]; then
        # gcovr only (re)writes pages for sources currently in scope, so
        # without clearing this first, a header removed or renamed since
        # the last --html run would leave its stale page (with outdated
        # numbers) sitting on disk and still openable.
        rm -rf "$HTML_DIR"
        mkdir -p "$HTML_DIR"
        echo "== also writing HTML report to $HTML_DIR"
        GCOVR_ARGS+=(--html --html-details -o "$HTML_DIR/index.html")
    fi
    gcovr "${GCOVR_ARGS[@]}" "$BUILD_DIR"
fi

if [[ "$TEST_FAILED" -ne 0 ]]; then
    echo "error: these test binaries failed (coverage above reflects a failing run):" \
         "${FAILED_NAMES[*]}" >&2
    exit 1
fi

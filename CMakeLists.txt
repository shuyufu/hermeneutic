cmake_minimum_required(VERSION 3.28)
project(hermeneutic CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Target layout mirrors include/bobby/hermeneutic/<tier>/ and tracks the
# actual dependency graph, not just the directory names: core/book/
# exchange_policy are always available (zero third-party dependency beyond
# each other); exchange (real Feed parsers) needs simdjson
# (HERMENEUTIC_BUILD_INGESTION); net/service/ingestion need gRPC/Boost/
# OpenSSL (HERMENEUTIC_BUILD_SERVICE). Every target is an INTERFACE library
# (this project is header-only outside the generated proto/gRPC code and
# the aggregator executable) exposed under a hermeneutic::<tier> ALIAS -
# `::` is reserved for ALIAS/IMPORTED targets in CMake, so the real
# add_library() name stays the plain hermeneutic_<tier> spelling and only
# the ALIAS carries the namespaced name consumers link against.

add_library(hermeneutic_core INTERFACE)
target_include_directories(hermeneutic_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
add_library(hermeneutic::core ALIAS hermeneutic_core)

add_library(hermeneutic_book INTERFACE)
target_link_libraries(hermeneutic_book INTERFACE hermeneutic::core)
add_library(hermeneutic::book ALIAS hermeneutic_book)

# This project's own symbol/venue naming rules (BASE_QUOTE splitting, book
# keys, per-venue native symbol formats - see symbol.hpp's own class
# comment): zero dependency beyond the standard library, so any future
# consumer beyond apps/aggregator/server_main.cpp's ingestion config - a client
# config, a REST surface - can depend on this alone. Links hermeneutic::core
# purely for the include/ root, the same as hermeneutic_net does below -
# not a real dependency on anything core actually defines.
add_library(hermeneutic_symbol INTERFACE)
target_link_libraries(hermeneutic_symbol INTERFACE hermeneutic::core)
add_library(hermeneutic::symbol ALIAS hermeneutic_symbol)

# The per-venue *SequencePolicy structs (exchange/<venue>/*_sequence_policy.hpp)
# are deliberately split out from their venue's Feed header: a Policy only
# needs SymbolSync's own DepthUpdate/SnapshotMessage types, never simdjson,
# and VenueSessionTest/IngestionRunnerTest drive a fake Feed against a real
# Policy specifically to avoid linking simdjson into those tests - see
# tests/ingestion/venue_session_test.cpp's FakeHttpRequestSpec comment.
add_library(hermeneutic_exchange_policy INTERFACE)
target_link_libraries(hermeneutic_exchange_policy INTERFACE hermeneutic::book)
add_library(hermeneutic::exchange_policy ALIAS hermeneutic_exchange_policy)

option(HERMENEUTIC_BUILD_TESTS "Build unit tests" ON)

if(HERMENEUTIC_BUILD_TESTS)
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG v1.15.2
    )
    # Keep googletest's runtime library selection consistent with this project on MSVC.
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    enable_testing()
    include(GoogleTest)

    add_executable(hermeneutic_tests
        tests/core/fixed_point_test.cpp
        tests/core/notional_test.cpp
        tests/book/l2_order_book_test.cpp
        tests/book/aggregate_order_book_test.cpp
        tests/book/price_bands_test.cpp
        tests/book/volume_bands_test.cpp
        tests/book/symbol_sync_test.cpp
        tests/symbol/symbol_test.cpp
    )
    target_link_libraries(hermeneutic_tests PRIVATE
        hermeneutic::book hermeneutic::exchange_policy hermeneutic::symbol GTest::gtest_main)
    gtest_discover_tests(hermeneutic_tests)
endif()

option(HERMENEUTIC_BUILD_INGESTION "Build market-data ingestion feed parsers (needs no vcpkg toolchain)" ON)

if(HERMENEUTIC_BUILD_INGESTION)
    # Fetched directly (like googletest above), not via vcpkg: simdjson's
    # vcpkg port requires a system pkg-config, which this project has no
    # other reason to depend on, and simdjson officially supports being
    # brought in this way.
    include(FetchContent)
    FetchContent_Declare(
        simdjson
        GIT_REPOSITORY https://github.com/simdjson/simdjson.git
        GIT_TAG v4.6.11
        GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(simdjson)

    add_library(hermeneutic_exchange INTERFACE)
    target_link_libraries(hermeneutic_exchange INTERFACE hermeneutic::book simdjson)
    add_library(hermeneutic::exchange ALIAS hermeneutic_exchange)

    if(HERMENEUTIC_BUILD_TESTS)
        add_executable(hermeneutic_binance_futures_feed_test tests/exchange/binance/binance_futures_feed_test.cpp)
        target_link_libraries(hermeneutic_binance_futures_feed_test PRIVATE
            hermeneutic::exchange GTest::gtest_main)
        gtest_discover_tests(hermeneutic_binance_futures_feed_test)

        add_executable(hermeneutic_binance_spot_feed_test tests/exchange/binance/binance_spot_feed_test.cpp)
        target_link_libraries(hermeneutic_binance_spot_feed_test PRIVATE
            hermeneutic::exchange GTest::gtest_main)
        gtest_discover_tests(hermeneutic_binance_spot_feed_test)

        add_executable(hermeneutic_bybit_linear_feed_test tests/exchange/bybit/bybit_linear_feed_test.cpp)
        target_link_libraries(hermeneutic_bybit_linear_feed_test PRIVATE
            hermeneutic::exchange GTest::gtest_main)
        gtest_discover_tests(hermeneutic_bybit_linear_feed_test)

        add_executable(hermeneutic_bybit_spot_feed_test tests/exchange/bybit/bybit_spot_feed_test.cpp)
        target_link_libraries(hermeneutic_bybit_spot_feed_test PRIVATE
            hermeneutic::exchange GTest::gtest_main)
        gtest_discover_tests(hermeneutic_bybit_spot_feed_test)

        add_executable(hermeneutic_okx_feed_test tests/exchange/okx/okx_feed_test.cpp)
        target_link_libraries(hermeneutic_okx_feed_test PRIVATE
            hermeneutic::exchange GTest::gtest_main)
        gtest_discover_tests(hermeneutic_okx_feed_test)

        # book_subscription.hpp's JSON parsing needs simdjson, so - like the
        # feed tests above - it's gated on HERMENEUTIC_BUILD_INGESTION rather
        # than living in the always-built hermeneutic_tests binary.
        add_executable(hermeneutic_book_subscription_test tests/ingestion/book_subscription_test.cpp)
        target_link_libraries(hermeneutic_book_subscription_test PRIVATE
            hermeneutic::exchange hermeneutic::symbol GTest::gtest_main)
        gtest_discover_tests(hermeneutic_book_subscription_test)
    endif()
endif()

option(HERMENEUTIC_BUILD_SERVICE "Build gRPC service targets (requires vcpkg toolchain, see README)" OFF)

if(HERMENEUTIC_BUILD_SERVICE)
    find_package(Protobuf CONFIG REQUIRED)
    find_package(gRPC CONFIG REQUIRED)
    find_package(boost_asio CONFIG REQUIRED)
    find_package(boost_beast CONFIG REQUIRED)
    find_package(OpenSSL REQUIRED)

    # net_traits.hpp/http_client.hpp/websocket_connection.hpp never touch
    # Price/Size or any book type, but every header in this project
    # #includes its neighbors via the repo-root-relative
    # "bobby/hermeneutic/..." spelling rather than a relative path, so this
    # target still needs hermeneutic::core purely for the include/ root -
    # not a real dependency on anything core actually defines.
    add_library(hermeneutic_net INTERFACE)
    target_link_libraries(hermeneutic_net INTERFACE
        hermeneutic::core Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto)
    add_library(hermeneutic::net ALIAS hermeneutic_net)

    set(HERMENEUTIC_PROTO_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/proto)
    set(HERMENEUTIC_PROTO_GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
    file(MAKE_DIRECTORY ${HERMENEUTIC_PROTO_GEN_DIR})

    # Picks up every proto/**/*.proto automatically. A .proto's directory
    # should mirror its `package` (e.g. proto/bobby/hermeneutic/aggregator/aggregator.proto
    # for `package bobby.hermeneutic.aggregator`) so generated/ mirrors it too and
    # same-named messages/services in different packages can't collide.
    # CONFIGURE_DEPENDS re-globs on `cmake --build`, so adding, removing, or
    # moving a .proto needs no manual CMakeLists.txt change.
    file(GLOB_RECURSE HERMENEUTIC_PROTO_FILES CONFIGURE_DEPENDS
        ${HERMENEUTIC_PROTO_ROOT}/*.proto)

    set(HERMENEUTIC_PROTO_GENERATED_SRCS "")
    foreach(proto_file ${HERMENEUTIC_PROTO_FILES})
        file(RELATIVE_PATH proto_rel ${HERMENEUTIC_PROTO_ROOT} ${proto_file})
        get_filename_component(proto_rel_dir ${proto_rel} DIRECTORY)
        get_filename_component(proto_name ${proto_rel} NAME_WE)
        set(out_dir ${HERMENEUTIC_PROTO_GEN_DIR}/${proto_rel_dir})

        set(proto_srcs ${out_dir}/${proto_name}.pb.cc)
        set(proto_hdrs ${out_dir}/${proto_name}.pb.h)
        set(grpc_srcs ${out_dir}/${proto_name}.grpc.pb.cc)
        set(grpc_hdrs ${out_dir}/${proto_name}.grpc.pb.h)

        add_custom_command(
            OUTPUT ${proto_srcs} ${proto_hdrs} ${grpc_srcs} ${grpc_hdrs}
            COMMAND ${CMAKE_COMMAND} -E make_directory ${out_dir}
            COMMAND protobuf::protoc
            ARGS --cpp_out=${HERMENEUTIC_PROTO_GEN_DIR}
                 --grpc_out=${HERMENEUTIC_PROTO_GEN_DIR}
                 -I ${HERMENEUTIC_PROTO_ROOT}
                 --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
                 ${proto_file}
            DEPENDS ${proto_file}
            COMMENT "Generating gRPC/protobuf code for ${proto_rel}"
        )
        list(APPEND HERMENEUTIC_PROTO_GENERATED_SRCS ${proto_srcs} ${grpc_srcs})
    endforeach()

    # All generated protobuf/gRPC code, one library for every proto/**/*.proto.
    add_library(hermeneutic_proto STATIC ${HERMENEUTIC_PROTO_GENERATED_SRCS})
    target_include_directories(hermeneutic_proto PUBLIC ${HERMENEUTIC_PROTO_GEN_DIR})
    target_link_libraries(hermeneutic_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
    add_library(hermeneutic::proto ALIAS hermeneutic_proto)

    # venue_session.hpp/ingestion_runner.hpp: the I/O driver that feeds a
    # venue's WebSocket stream into a Book (SymbolRegistry<Book>) -
    # depends on net (its own transport) and book (VenueSession only ever
    # calls apply_snapshot/apply_batch/invalidate_venue on Book, all three
    # on AggregateOrderBook's own surface - see venue_session.hpp's
    # SymbolRegistry comment). No dependency on proto/gRPC at all: unlike
    # an earlier revision, this header never mentions
    # aggregator::SymbolBook by name.
    add_library(hermeneutic_ingestion INTERFACE)
    target_link_libraries(hermeneutic_ingestion INTERFACE hermeneutic::net hermeneutic::book)
    add_library(hermeneutic::ingestion ALIAS hermeneutic_ingestion)

    # apps/aggregator/aggregator_service.hpp (SymbolBook/AggregatorService)
    # is app-private, not a library header: it lives beside the one binary
    # that owns it rather than under include/, and nothing under
    # include/bobby/hermeneutic/ depends on it any more now that
    # VenueSession is templated on Book instead of hardcoding SymbolBook.
    # Its few consumers below link hermeneutic::book + hermeneutic::proto
    # directly and add the repo root as an include directory (so its own
    # "apps/aggregator/aggregator_service.hpp" spelling resolves) rather
    # than reintroducing a hermeneutic::service target that would
    # misleadingly suggest this header is part of the library.
    add_executable(hermeneutic_aggregator_service apps/aggregator/server_main.cpp)
    target_include_directories(hermeneutic_aggregator_service PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(hermeneutic_aggregator_service PRIVATE
        hermeneutic::ingestion hermeneutic::exchange hermeneutic::exchange_policy hermeneutic::symbol
        hermeneutic::proto)

    # Example/diagnostic client for the service above - see
    # apps/aggregator/client_main.cpp's own comment for why this one isn't
    # a throwaway like this project's other live-verification programs.
    add_executable(hermeneutic_aggregator_client apps/aggregator/client_main.cpp)
    target_link_libraries(hermeneutic_aggregator_client PRIVATE hermeneutic::proto)

    if(HERMENEUTIC_BUILD_TESTS)
        add_executable(hermeneutic_aggregator_service_test tests/apps/aggregator/aggregator_service_test.cpp)
        target_include_directories(hermeneutic_aggregator_service_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(hermeneutic_aggregator_service_test PRIVATE
            hermeneutic::book hermeneutic::proto GTest::gtest_main)
        gtest_discover_tests(hermeneutic_aggregator_service_test)

        add_executable(hermeneutic_websocket_connection_test tests/net/websocket_connection_test.cpp)
        target_link_libraries(hermeneutic_websocket_connection_test PRIVATE
            hermeneutic::net GTest::gtest_main)
        gtest_discover_tests(hermeneutic_websocket_connection_test)

        add_executable(hermeneutic_http_client_test tests/net/http_client_test.cpp)
        target_link_libraries(hermeneutic_http_client_test PRIVATE
            hermeneutic::net GTest::gtest_main)
        gtest_discover_tests(hermeneutic_http_client_test)

        # Unlike hermeneutic_ingestion itself, this test file deliberately
        # does reach for aggregator_service.hpp directly - several cases
        # spin up a real grpc::Server and assert on the L2Update stream a
        # real SymbolBook actually produces, not just VenueSession's own
        # mechanics. A test is free to cross tiers like this; the library
        # header it exercises (venue_session.hpp) no longer has to.
        add_executable(hermeneutic_venue_session_test tests/ingestion/venue_session_test.cpp)
        target_include_directories(hermeneutic_venue_session_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(hermeneutic_venue_session_test PRIVATE
            hermeneutic::ingestion hermeneutic::exchange_policy hermeneutic::proto GTest::gtest_main)
        gtest_discover_tests(hermeneutic_venue_session_test)

        # By contrast, this test never registers a book at all (see
        # MinimalFakeFeed's comment) - it stays off proto/gRPC entirely by
        # using plain AggregateOrderBook as its Book, proving the seam
        # actually decouples IngestionRunner's own plumbing from SymbolBook.
        add_executable(hermeneutic_ingestion_runner_test tests/ingestion/ingestion_runner_test.cpp)
        target_link_libraries(hermeneutic_ingestion_runner_test PRIVATE
            hermeneutic::ingestion hermeneutic::exchange_policy GTest::gtest_main)
        gtest_discover_tests(hermeneutic_ingestion_runner_test)
    endif()
endif()

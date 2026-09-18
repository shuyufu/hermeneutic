cmake_minimum_required(VERSION 3.28)
project(hermeneutic CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_library(hermeneutic INTERFACE)
target_include_directories(hermeneutic INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

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
        tests/fixed_point_test.cpp
        tests/order_book_test.cpp
        tests/aggregate_order_book_test.cpp
        tests/notional_test.cpp
        tests/volume_bands_test.cpp
        tests/price_bands_test.cpp
        tests/symbol_sync_test.cpp
    )
    target_link_libraries(hermeneutic_tests PRIVATE hermeneutic GTest::gtest_main)
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

    if(HERMENEUTIC_BUILD_TESTS)
        add_executable(hermeneutic_binance_futures_feed_test tests/binance_futures_feed_test.cpp)
        target_include_directories(hermeneutic_binance_futures_feed_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/service)
        target_link_libraries(hermeneutic_binance_futures_feed_test PRIVATE
            hermeneutic simdjson GTest::gtest_main)
        gtest_discover_tests(hermeneutic_binance_futures_feed_test)

        add_executable(hermeneutic_binance_spot_feed_test tests/binance_spot_feed_test.cpp)
        target_include_directories(hermeneutic_binance_spot_feed_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/service)
        target_link_libraries(hermeneutic_binance_spot_feed_test PRIVATE
            hermeneutic simdjson GTest::gtest_main)
        gtest_discover_tests(hermeneutic_binance_spot_feed_test)
    endif()
endif()

option(HERMENEUTIC_BUILD_SERVICE "Build gRPC service targets (requires vcpkg toolchain, see README)" OFF)

if(HERMENEUTIC_BUILD_SERVICE)
    find_package(Protobuf CONFIG REQUIRED)
    find_package(gRPC CONFIG REQUIRED)
    find_package(boost_asio CONFIG REQUIRED)
    find_package(boost_beast CONFIG REQUIRED)
    find_package(OpenSSL REQUIRED)

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

    add_executable(hermeneutic_aggregator_service service/aggregator_main.cpp)
    target_link_libraries(hermeneutic_aggregator_service PRIVATE
        hermeneutic_proto hermeneutic simdjson Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto)

    if(HERMENEUTIC_BUILD_TESTS)
        add_executable(hermeneutic_aggregator_service_test tests/aggregator_service_test.cpp)
        target_include_directories(hermeneutic_aggregator_service_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/service)
        target_link_libraries(hermeneutic_aggregator_service_test PRIVATE
            hermeneutic_proto hermeneutic GTest::gtest_main)
        gtest_discover_tests(hermeneutic_aggregator_service_test)

        add_executable(hermeneutic_websocket_connection_test tests/websocket_connection_test.cpp)
        target_include_directories(hermeneutic_websocket_connection_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/service)
        target_link_libraries(hermeneutic_websocket_connection_test PRIVATE
            hermeneutic Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto GTest::gtest_main)
        gtest_discover_tests(hermeneutic_websocket_connection_test)

        add_executable(hermeneutic_http_client_test tests/http_client_test.cpp)
        target_include_directories(hermeneutic_http_client_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/service)
        target_link_libraries(hermeneutic_http_client_test PRIVATE
            hermeneutic Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto GTest::gtest_main)
        gtest_discover_tests(hermeneutic_http_client_test)

        add_executable(hermeneutic_venue_session_test tests/venue_session_test.cpp)
        target_include_directories(hermeneutic_venue_session_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/service)
        target_link_libraries(hermeneutic_venue_session_test PRIVATE
            hermeneutic_proto hermeneutic Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto
            GTest::gtest_main)
        gtest_discover_tests(hermeneutic_venue_session_test)

        add_executable(hermeneutic_ingestion_runner_test tests/ingestion_runner_test.cpp)
        target_include_directories(hermeneutic_ingestion_runner_test PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/service)
        target_link_libraries(hermeneutic_ingestion_runner_test PRIVATE
            hermeneutic_proto hermeneutic Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto
            GTest::gtest_main)
        gtest_discover_tests(hermeneutic_ingestion_runner_test)
    endif()
endif()

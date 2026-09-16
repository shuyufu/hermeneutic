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
    )
    target_link_libraries(hermeneutic_tests PRIVATE hermeneutic GTest::gtest_main)
    gtest_discover_tests(hermeneutic_tests)
endif()

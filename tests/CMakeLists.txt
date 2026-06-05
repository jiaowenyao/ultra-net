cmake_minimum_required(VERSION 3.20)

# Find GTest
find_package(GTest QUIET)
if(NOT GTest_FOUND)
    # Try to find GTest from system
    find_path(GTEST_INCLUDE_DIR NAMES gtest/gtest.h)
    find_library(GTEST_LIBRARY NAMES gtest)
    find_library(GTEST_MAIN_LIBRARY NAMES gtest_main)
    if(GTEST_INCLUDE_DIR AND GTEST_LIBRARY)
        set(GTest_FOUND TRUE)
        set(GTest_INCLUDE_DIR ${GTEST_INCLUDE_DIR})
        set(GTest_LIBRARY ${GTEST_LIBRARY})
        set(GTest_MAIN_LIBRARY ${GTEST_MAIN_LIBRARY})
    endif()
endif()

if(GTest_FOUND)
    message(STATUS "GTest found: include=${GTest_INCLUDE_DIR}")
else()
    message(STATUS "GTest not found, tests will use simple assert")
endif()

# Compile tests
set(TEST_SOURCES
    init.cc
    pool_test.cc
    error_test.cc
    mpsc_test.cc
    dns_test.cc
    udp_test.cc
    channel_test.cc
    metrics_test.cc
    log_test.cc
    trace_test.cc
    tcp_socket_test.cc
    when_all_test.cc
    when_any_test.cc
    config_test.cc
    shutdown_test.cc
    circuit_breaker_test.cc
    retry_test.cc
    connection_pool_test.cc
    http_test.cc
    websocket_test.cc
    dns_srv_test.cc
    service_discovery_test.cc
    proxy_test.cc
)

foreach(test_src ${TEST_SOURCES})
    get_filename_component(test_name ${test_src} NAME_WE)
    add_executable(${test_name} ${test_src})
    target_include_directories(${test_name} PRIVATE ${CMAKE_SOURCE_DIR}/src ${CMAKE_SOURCE_DIR})
    target_link_libraries(${test_name}
        ultranet
        pthread
    )
    if(GTest_FOUND)
        target_include_directories(${test_name} PRIVATE ${GTest_INCLUDE_DIR})
    endif()
    add_test(NAME ${test_name} COMMAND ${test_name})
endforeach()

message(STATUS "Tests configured: ${TEST_SOURCES}")

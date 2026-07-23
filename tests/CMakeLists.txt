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
    log_test.cc
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

# Actor framework test

add_executable(actor_test actor_test.cc)
target_link_libraries(actor_test PRIVATE ultranet pthread)


add_executable(actor_v2_test actor_v2_test.cc)
target_link_libraries(actor_v2_test PRIVATE ultranet pthread)


add_executable(actor_cluster_test actor_cluster_test.cc)
target_link_libraries(actor_cluster_test PRIVATE ultranet pthread)


add_executable(actor_message_test actor_message_test.cc)
target_link_libraries(actor_message_test PRIVATE ultranet pthread)

add_executable(actor_mailbox_test actor_mailbox_test.cc)
target_link_libraries(actor_mailbox_test PRIVATE ultranet pthread)

add_executable(actor_serialization_test actor_serialization_test.cc)
target_link_libraries(actor_serialization_test PRIVATE ultranet pthread)

add_executable(actor_system_integration_test actor_system_integration_test.cc)
target_link_libraries(actor_system_integration_test PRIVATE ultranet pthread)

add_executable(actor_cluster_unit_test actor_cluster_test.cc)
target_link_libraries(actor_cluster_unit_test PRIVATE ultranet pthread)

add_executable(core_mpsc_test core_mpsc_test.cc)
target_link_libraries(core_mpsc_test PRIVATE ultranet pthread)

add_executable(core_pool_test core_pool_test.cc)
target_link_libraries(core_pool_test PRIVATE ultranet pthread)

add_executable(actor_robustness_test actor_robustness_test.cc)
target_link_libraries(actor_robustness_test PRIVATE ultranet pthread)

add_executable(actor_bench_test actor_bench_test.cc)
target_link_libraries(actor_bench_test PRIVATE ultranet pthread)

# GTest-based actor tests
add_executable(actor_gtest actor_gtest.cc)
target_include_directories(actor_gtest PRIVATE ${GTest_INCLUDE_DIR})
target_link_libraries(actor_gtest PRIVATE ultranet pthread gtest gtest_main)

# Actor stress test
add_executable(actor_stress_test actor_stress_test.cc)
target_link_libraries(actor_stress_test PRIVATE ultranet pthread)

add_executable(ws_bench_server ws_bench_server.cc)
target_link_libraries(ws_bench_server PRIVATE ultranet pthread)

add_executable(ws_bench_client ws_bench_client.cc)
target_link_libraries(ws_bench_client PRIVATE ultranet pthread)

add_executable(ws_echo_bench ws_echo_bench.cc)
target_link_libraries(ws_echo_bench ultranet pthread)

cmake_minimum_required(VERSION 3.20)

message(STATUS "Configuring examples...")

# Example: Simple echo server
add_executable(echo_server echo_server.cc)
target_link_libraries(echo_server ultranet pthread)

# Example: Simple client
add_executable(echo_client echo_client.cc)
target_link_libraries(echo_client ultranet pthread)

# Example: HTTP server
add_executable(http_server http_server.cc)
target_link_libraries(http_server ultranet pthread)

# Stress test
add_executable(stress_test stress_test.cc)
target_link_libraries(stress_test ultranet pthread)

message(STATUS "Examples configured")

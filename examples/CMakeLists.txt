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

# WebSocket echo server
add_executable(ws_echo_server ws_echo_server.cc)
target_link_libraries(ws_echo_server ultranet pthread)

# WebSocket echo client
add_executable(ws_echo_client ws_echo_client.cc)
target_link_libraries(ws_echo_client ultranet pthread)

# WebSocket stress test
add_executable(ws_stress_test ws_stress_test.cc)
target_link_libraries(ws_stress_test ultranet pthread)

message(STATUS "Examples configured")


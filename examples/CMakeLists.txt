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

# --- Documentation Examples (source in docs/examples/) ---
# Each demonstrates a specific API usage pattern with detailed comments.

# TCP echo server/client (docs examples)
add_executable(tcp_echo_server ../docs/examples/tcp_echo_server.cc)
target_link_libraries(tcp_echo_server ultranet pthread)

add_executable(tcp_echo_client ../docs/examples/tcp_echo_client.cc)
target_link_libraries(tcp_echo_client ultranet pthread)

# UDP echo server/client (docs examples)
add_executable(udp_echo_server ../docs/examples/udp_echo_server.cc)
target_link_libraries(udp_echo_server ultranet pthread)

add_executable(udp_echo_client ../docs/examples/udp_echo_client.cc)
target_link_libraries(udp_echo_client ultranet pthread)

# HTTP server (docs example)
add_executable(http_server_example ../docs/examples/http_server.cc)
target_link_libraries(http_server_example ultranet pthread)

# WebSocket echo server/client (docs examples)
add_executable(websocket_echo_server ../docs/examples/websocket_echo_server.cc)
target_link_libraries(websocket_echo_server ultranet pthread)

add_executable(websocket_echo_client ../docs/examples/websocket_echo_client.cc)
target_link_libraries(websocket_echo_client ultranet pthread)

# Actor framework examples (docs examples)
add_executable(actor_hello ../docs/examples/actor_hello.cc)
target_link_libraries(actor_hello ultranet pthread)

add_executable(actor_multi ../docs/examples/actor_multi.cc)
target_link_libraries(actor_multi ultranet pthread)

add_executable(actor_ps ../docs/examples/actor_ps.cc)
target_link_libraries(actor_ps ultranet pthread)

add_executable(actor_train ../docs/examples/actor_train.cc)
target_link_libraries(actor_train ultranet pthread)

add_executable(actor_async ../docs/examples/actor_async.cc)
target_link_libraries(actor_async ultranet pthread)

add_executable(actor_server ../docs/examples/actor_server.cc)
target_link_libraries(actor_server ultranet pthread)

add_executable(actor_client ../docs/examples/actor_client.cc)
target_link_libraries(actor_client ultranet pthread)

message(STATUS "Examples configured")



add_executable(actor_v2_demo ../docs/examples/actor_v2_demo.cc)
target_link_libraries(actor_v2_demo ultranet pthread)


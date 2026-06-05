cmake_minimum_required(VERSION 3.20)

add_executable(backend echo_server.cc)
target_link_libraries(backend ultranet pthread)

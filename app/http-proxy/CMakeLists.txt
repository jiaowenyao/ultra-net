cmake_minimum_required(VERSION 3.20)

add_executable(http-proxy main.cc)
target_link_libraries(http-proxy ultranet pthread)

cmake_minimum_required(VERSION 3.20)

add_executable(redis-proxy main.cc)
target_link_libraries(redis-proxy ultranet pthread)

cmake_minimum_required(VERSION 3.20)

message(STATUS "Configuring apps...")

add_subdirectory(redis-proxy)
add_subdirectory(http-proxy)
add_subdirectory(backend)

message(STATUS "Apps configured")

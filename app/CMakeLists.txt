cmake_minimum_required(VERSION 3.20)

message(STATUS "Configuring apps...")

add_subdirectory(redis-proxy)
add_subdirectory(http-proxy)
add_subdirectory(backend)
add_subdirectory(data-collector)
add_subdirectory(dist-bench)

# Dashboard requires Qt5 (optional — not required for core library).
find_package(Qt5 COMPONENTS Core Widgets Network QUIET)
if(Qt5_FOUND)
    message(STATUS "Qt5 found — building training-dashboard")
    add_subdirectory(training-dashboard)
else()
    message(STATUS "Qt5 not found — skipping training-dashboard")
endif()

message(STATUS "Apps configured")

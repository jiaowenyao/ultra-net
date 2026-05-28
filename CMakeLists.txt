cmake_minimum_required(VERSION 3.20)

project(ultranet VERSION 0.1.0 LANGUAGES CXX)

# C++20 standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Build options
option(BUILD_SHARED_LIBS "Build shared libraries" OFF)
option(ULTRANET_BUILD_TESTS "Build tests" ON)
option(ULTRANET_BUILD_EXAMPLES "Build examples" ON)

# Export compile commands for IDE support
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Output directories
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Find liburing
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(liburing REQUIRED liburing>=2.5)
else()
    # Fallback to manual find
    find_path(liburing_INCLUDE_DIR NAMES liburing.h)
    find_library(liburing_LIBRARY NAMES uring)
    if(liburing_INCLUDE_DIR AND liburing_LIBRARY)
        set(liburing_FOUND TRUE)
        set(liburing_INCLUDE_DIRS ${liburing_INCLUDE_DIR})
        set(liburing_LIBRARIES ${liburing_LIBRARY})
    endif()
endif()

if(NOT liburing_FOUND)
    message(FATAL_ERROR "liburing not found. Please install liburing-dev")
endif()

# Create ultranet library
set(ULTRANET_SOURCES
    src/buffer/buffer.cc
)

# Headers that users need (exposed via include/ultranet/)
set(ULTRANET_PUBLIC_HEADERS
    src/scheduler.h
    src/execution_context.hpp
    include/ultranet/coroutine/task.hpp
    include/ultranet/coroutine/thread_pool.hpp
    include/ultranet/coroutine/queue.hpp
    include/ultranet/coroutine/unified_task.hpp
    include/ultranet/io/io_engine.hpp
    include/ultranet/io/io_awaitable.hpp
    include/ultranet/io/timer.hpp
    include/ultranet/io/io_callback.hpp
    include/ultranet/buffer/buffer.h
    include/ultranet/net/socket.hpp
    include/ultranet/net/listen.hpp
    include/ultranet/net/accept.hpp
    include/ultranet/net/read.hpp
    include/ultranet/net/write.hpp
    include/ultranet/net/connect.hpp
    include/ultranet/net/close.hpp
)

add_library(ultranet ${ULTRANET_SOURCES} ${ULTRANET_PUBLIC_HEADERS})

target_include_directories(ultranet
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/src>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${liburing_INCLUDE_DIRS}
)

target_link_libraries(ultranet
    PUBLIC
        ${liburing_LIBRARIES}
)

target_compile_definitions(ultranet
    PUBLIC
        ULTRANET_VERSION_MAJOR=${PROJECT_VERSION_MAJOR}
        ULTRANET_VERSION_MINOR=${PROJECT_VERSION_MINOR}
        ULTRANET_VERSION_PATCH=${PROJECT_VERSION_PATCH}
)

# Installation
include(GNUInstallDirs)

install(TARGETS ultranet
    EXPORT ultranetTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

# Install public headers (from src/ to include/ultranet/)
install(FILES
        ultranet/coroutine/task.hpp
        src/scheduler.h
        src/execution_context.hpp
        ultranet/coroutine/thread_pool.hpp
        ultranet/coroutine/queue.hpp
        ultranet/coroutine/unified_task.hpp
        ultranet/io/io_engine.hpp
        ultranet/io/io_awaitable.hpp
        ultranet/io/timer.hpp
        ultranet/io/io_callback.hpp
        ultranet/buffer/buffer.h
        ultranet/net/socket.hpp
        ultranet/net/listen.hpp
        ultranet/net/accept.hpp
        ultranet/net/read.hpp
        ultranet/net/write.hpp
        ultranet/net/connect.hpp
        ultranet/net/close.hpp
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ultranet
)

# Install wrapper header
install(FILES
        include/ultranet/ultranet.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ultranet
)

# CMake config files for find_package
include(CMakePackageConfigHelpers)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/ultranet-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/ultranet-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_DATADIR}/cmake/ultranet
)

write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/ultranetConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)

install(EXPORT ultranetTargets
    FILE ultranetTargets.cmake
    NAMESPACE ultranet::
    DESTINATION ${CMAKE_INSTALL_DATADIR}/cmake/ultranet
)

install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/ultranet-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/ultranetConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_DATADIR}/cmake/ultranet
)

# Also install the cmake module
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/Findliburing.cmake
    DESTINATION ${CMAKE_INSTALL_DATADIR}/cmake/ultranet
)

# Subdirectories
if(ULTRANET_BUILD_TESTS)
    add_subdirectory(tests)
endif()

if(ULTRANET_BUILD_EXAMPLES)
    add_subdirectory(examples)
endif()

# Print configuration summary
message(STATUS "")
message(STATUS "ultranet ${PROJECT_VERSION}")
message(STATUS "---")
message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "Build shared libs: ${BUILD_SHARED_LIBS}")
message(STATUS "Build tests: ${ULTRANET_BUILD_TESTS}")
message(STATUS "Build examples: ${ULTRANET_BUILD_EXAMPLES}")
message(STATUS "liburing include: ${liburing_INCLUDE_DIRS}")
message(STATUS "liburing library: ${liburing_LIBRARIES}")
message(STATUS "")

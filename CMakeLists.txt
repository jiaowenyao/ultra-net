cmake_minimum_required(VERSION 3.18)

project(co_net)

set(CMAKE_CXX_STANDARD 20)

set(EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib)


## subdirectory
add_subdirectory(test)
# add_subdirectory(co)


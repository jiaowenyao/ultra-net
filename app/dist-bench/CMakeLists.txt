add_executable(dist-bench main.cc)
target_link_libraries(dist-bench PRIVATE ultranet pthread)


add_executable(init init.cc)

add_executable(schedule_test schedule_test.cc)
target_link_libraries(schedule_test ultra-net)

add_executable(io_uring_test io_uring_test.cc)
target_link_libraries(io_uring_test ultra-net)



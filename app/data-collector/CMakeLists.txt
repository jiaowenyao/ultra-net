# Data Collector — reliable UDP video frame transfer over io_uring

add_executable(data-collector-sender sender.cc)
target_link_libraries(data-collector-sender PRIVATE ultranet)

add_executable(data-collector-receiver receiver.cc)
target_link_libraries(data-collector-receiver PRIVATE ultranet)

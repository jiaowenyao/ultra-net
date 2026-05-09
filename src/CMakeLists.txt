
add_library(
    ultra-net
    ./buffer/buffer.cc
)

target_link_libraries(
    ultra-net PUBLIC
    uring
)


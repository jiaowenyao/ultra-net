
add_library(
    ultra-net
    ./io/buffer.cc
)

target_link_libraries(
    ultra-net PUBLIC
    uring
)


#include <iostream>

#include "async/task.hpp"
#include "runtime/thread_pool.hpp"

using ynet::async::Task;

Task<int> get_num() {
    co_return 42;
}

Task<void> message() {
    std::cout << "message" << std::endl;

    int num = co_await get_num();

    std::cout << "num=" << num << std::endl;
}

Task<void> process() {
    co_await message();
}


int main() {
    std::cout << "hello world" << std::endl;

    Task<void> process_task = process();

    process_task.handle().resume();

    return 0;
}


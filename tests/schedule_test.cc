#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/coroutine/task.hpp"
#include <thread>
#include <chrono>


ynet::async::Task<int> get_num() {
    static std::atomic<int> num = 0;
    ++num;
    co_return num;
}

ynet::async::Task<void> print_num() {
    int num = co_await get_num();
    std::cout << "thread=" << std::this_thread::get_id() << ", num=" << num << std::endl;
    std::cout.flush();
}

ynet::async::Task<void> task() {
    int cnt = 5;
    for (int i = 0; i < cnt; ++i) {
        co_await print_num();
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

ynet::async::Task<void> server() {

    co_await task();
    co_await task();

}

int main() {

    ynet::async::scheduling::WorkStealingThreadPool pool(4);

    ynet::async::ExecutionContext::Scope scope(&pool);

    auto task1 = server();
    auto task2 = server();

    // task.handle().resume();

    pool.submit(task1.release());
    pool.submit(task2.release());

    pool.wait_all();

    return 0;
}


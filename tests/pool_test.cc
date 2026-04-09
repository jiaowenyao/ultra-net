// tests/pool_test.cc - Thread pool test without sockets
#include "async/task.hpp"
#include "async/io/io_context.hpp"
#include "async/scheduling/thread_pool.hpp"
#include <iostream>
#include <atomic>
#include <chrono>

using namespace ynet::async;
using namespace ynet::async::io;

std::atomic<int> g_counter{0};

Task<void> simple_task(int id) {
    for (int i = 0; i < 10; ++i) {
        g_counter.fetch_add(1, std::memory_order_relaxed);
    }
    co_return;
}

int main() {
    std::cout << "=== Thread Pool Test ===" << std::endl;

    scheduling::WorkStealingThreadPool pool(4);
    ExecutionContext::Scope scope(&pool);

    g_counter = 0;
    const int num_tasks = 20;

    for (int i = 0; i < num_tasks; ++i) {
        auto task = simple_task(i);
        pool.submit(task.task());
    }

    pool.wait_all();

    std::cout << "Counter: " << g_counter.load() << " (expected: " << num_tasks * 10 << ")" << std::endl;

    return (g_counter.load() == num_tasks * 10) ? 0 : 1;
}
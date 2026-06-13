#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include "ultranet/buffer/buffer.h"
#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/coroutine/unified_task.hpp"
#include "ultranet/coroutine/task.hpp"

// Thread pool and unified_task comprehensive tests.

using namespace ynet::async::scheduling;

static int g_passed = 0, g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── UnifiedTask tests ──────────────────────────────────────────────────

using ynet::async::Task;

void test_unified_task_coroutine() {
    T("unified_task coroutine handle");
    bool called = false;
    auto make_task = [](bool* c) -> Task<void> {
        *c = true;
        co_return;
    };
    auto t = make_task(&called);
    auto h = t.release();
    UnifiedTask ut(h);
    ut();
    CHECK(called, "coroutine executed");
    PASS();
}

void test_unified_task_function() {
    T("unified_task std::function");
    bool called = false;
    UnifiedTask ut([&called]{ called = true; });
    ut();
    CHECK(called, "function executed");
    PASS();
}

void test_unified_task_empty() {
    T("unified_task empty is safe");
    UnifiedTask ut;
    CHECK(!static_cast<bool>(ut), "empty is falsy");
    PASS();
}

// ── Thread pool tests ──────────────────────────────────────────────────

void test_pool_create_destroy() {
    T("pool create/destroy");
    WorkStealingThreadPool pool(2);
    CHECK(pool.num_threads() == 2, "thread count");
    PASS();
}

void test_pool_submit_function() {
    T("pool submit_function");
    WorkStealingThreadPool pool(2);
    std::atomic<int> counter{0};
    for (int i = 0; i < 100; ++i) {
        pool.submit_function([&]{ counter.fetch_add(1); });
    }
    pool.wait_all();
    CHECK(counter.load() == 100, "all 100 executed");
    PASS();
}

void test_pool_submit_coroutine() {
    T("pool submit_coroutine");
    WorkStealingThreadPool pool(2);
    std::atomic<int> val{0};
    auto coro = [](std::atomic<int>* v) -> Task<void> {
        v->store(42);
        co_return;
    };
    auto task = coro(&val);
    pool.submit(task.release());
    pool.wait_all();
    CHECK(val.load() == 42, "coroutine executed");
    PASS();
}

void test_pool_active_tasks() {
    T("pool active task tracking");
    WorkStealingThreadPool pool(2);
    CHECK(pool.active_tasks() == 0, "zero initially");
    std::atomic<int> c{0};
    for (int i = 0; i < 10; ++i) {
        pool.submit_function([&]() {
            c++;
        });
    }
    pool.wait_all();
    CHECK(pool.active_tasks() == 0, "zero after wait");
    CHECK(c.load() == 10, "all done");
    PASS();
}

void test_pool_pending_tasks() {
    T("pool pending tasks query");
    WorkStealingThreadPool pool(2);
    pool.submit_function([]{});
    CHECK(pool.pending_tasks() >= 0, "pending query works");
    pool.wait_all();
    PASS();
}

void test_pool_is_current_thread() {
    T("pool is_current_thread");
    WorkStealingThreadPool pool(1);
    CHECK(!pool.is_current_thread(), "main is not worker");
    PASS();
}

void test_pool_name() {
    T("pool name");
    WorkStealingThreadPool pool(1);
    CHECK(std::string(pool.name()) == "WorkStealingThreadPool", "name");
    PASS();
}

void test_pool_worker_count() {
    T("pool worker count");
    WorkStealingThreadPool pool(4);
    CHECK(pool.worker_count() == 4, "4 workers");
    pool.wait_all();
    PASS();
}

int main() {
    std::cout << "=== Thread Pool Tests ===" << std::endl;
    test_unified_task_coroutine();
    test_unified_task_function();
    test_unified_task_empty();
    test_pool_create_destroy();
    test_pool_submit_function();
    test_pool_submit_coroutine();
    test_pool_active_tasks();
    test_pool_pending_tasks();
    test_pool_is_current_thread();
    test_pool_name();
    test_pool_worker_count();
    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

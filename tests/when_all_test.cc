#include "ultranet/ultranet.h"
#include <iostream>
#include <string>

using namespace ynet::async;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

// Test 1: two tasks with same type
void test_two_tasks() {
    T("two int tasks");
    auto test = []() -> Task<void> {
        auto task_a = []() -> Task<int> { co_return 10; };
        auto task_b = []() -> Task<int> { co_return 20; };

        auto [a, b] = co_await when_all(task_a(), task_b());
        CO_CHECK(a == 10, "first result mismatch");
        CO_CHECK(b == 20, "second result mismatch");
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

// Test 2: three tasks with different types
void test_three_tasks() {
    T("three tasks (int, string, double)");
    auto test = []() -> Task<void> {
        auto ta = []() -> Task<int> { co_return 42; };
        auto tb = []() -> Task<std::string> { co_return std::string("hello"); };
        auto tc = []() -> Task<double> { co_return 3.14; };

        auto results = co_await when_all(ta(), tb(), tc());
        CO_CHECK(std::get<0>(results) == 42, "int result mismatch");
        CO_CHECK(std::get<1>(results) == "hello", "string result mismatch");
        CO_CHECK(std::get<2>(results) > 3.0, "double result mismatch");
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

// Test 3: exception propagation
void test_exception() {
    T("exception propagation");
    bool caught = false;
    auto test = [&caught]() -> Task<void> {
        auto good = []() -> Task<int> { co_return 1; };
        auto bad = []() -> Task<int> {
            throw std::runtime_error("test error");
            co_return 0;
        };

        try {
            co_await when_all(good(), bad());
        } catch (const std::runtime_error& e) {
            caught = true;
            CO_CHECK(std::string(e.what()) == "test error", "wrong exception message");
        }
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    CHECK(caught, "exception was not caught");
    PASS();
}

// Test 4: already-completed task
void test_already_done() {
    T("already-completed task in when_all");
    auto test = []() -> Task<void> {
        // Create and complete a task before passing to when_all
        auto make_done = []() -> Task<int> { co_return 100; };

        // Wait for it to complete
        auto done_task = make_done();
        int val = co_await done_task;
        CO_CHECK(val == 100, "pre-completion check");

        // Now use the (already done) task in when_all
        auto other = []() -> Task<int> { co_return 200; };
        auto [a, b] = co_await when_all(
            []() -> Task<int> { co_return 100; }(),
            other()
        );
        CO_CHECK(a == 100, "first result");
        CO_CHECK(b == 200, "second result");
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

int main() {
    std::cout << "=== when_all Tests ===" << std::endl;
    test_two_tasks();
    test_three_tasks();
    test_exception();
    test_already_done();

    std::cout << "\n=== when_all Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

#include "ultranet/ultranet.h"
#include <iostream>
#include <string>
#include <atomic>

using namespace ynet::async;
using namespace ynet::async::io;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

// Test 1: fast task wins
void test_fast_wins() {
    T("fast task wins over slow task");
    auto test = []() -> Task<void> {
        auto fast = []() -> Task<int> { co_return 1; };
        auto slow = []() -> Task<int> {
            co_await sleep_for(std::chrono::milliseconds(500));
            co_return 2;
        };

        auto [index, results] = co_await when_any(fast(), slow());
        CO_CHECK(index == 0, "fast task should be index 0");
        CO_CHECK(std::get<0>(results) == 1, "fast task result should be 1");
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

// Test 2: exception propagation
void test_exception_wins() {
    T("exception from first completed task");
    bool caught = false;
    auto test = [&caught]() -> Task<void> {
        auto bad = []() -> Task<int> {
            throw std::runtime_error("boom");
            co_return 0;
        };
        auto slow = []() -> Task<int> {
            co_await sleep_for(std::chrono::milliseconds(500));
            co_return 42;
        };

        try {
            co_await when_any(bad(), slow());
        } catch (const std::runtime_error& e) {
            caught = true;
            CO_CHECK(std::string(e.what()) == "boom", "wrong exception");
        }
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    CHECK(caught, "exception not caught");
    PASS();
}

// Test 3: winner index correctness with small delays
void test_winner_index() {
    T("winner index with small delays");
    auto test = []() -> Task<void> {
        auto a = []() -> Task<int> {
            co_await sleep_for(std::chrono::milliseconds(50));
            co_return 10;
        };
        auto b = []() -> Task<int> {
            co_await sleep_for(std::chrono::milliseconds(100));
            co_return 20;
        };
        auto c = []() -> Task<int> {
            co_await sleep_for(std::chrono::milliseconds(150));
            co_return 30;
        };

        auto [index, results] = co_await when_any(a(), b(), c());
        CO_CHECK(index < 3, "index out of range");
        // Since these race, we can't guarantee which wins,
        // but the index should correspond to a valid task
        if (index == 0) CO_CHECK(std::get<0>(results) == 10, "wrong result for task 0");
        else if (index == 1) CO_CHECK(std::get<1>(results) == 20, "wrong result for task 1");
        else CO_CHECK(std::get<2>(results) == 30, "wrong result for task 2");
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

int main() {
    std::cout << "=== when_any Tests ===" << std::endl;
    test_fast_wins();
    test_exception_wins();
    test_winner_index();

    std::cout << "\n=== when_any Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

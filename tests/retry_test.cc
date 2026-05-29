#include "ultranet/ultranet.h"
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::io;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

void test_delay_computation() {
    T("exponential backoff delay computation");
    ExponentialBackoff policy(std::chrono::milliseconds(100),
                              std::chrono::milliseconds(5000), 3, 0.0);

    auto d0 = policy.delay_for(0);
    CHECK(d0.count() >= 90 && d0.count() <= 110, "attempt 0 ~100ms");

    auto d1 = policy.delay_for(1);
    CHECK(d1.count() >= 180 && d1.count() <= 220, "attempt 1 ~200ms");

    auto d2 = policy.delay_for(2);
    CHECK(d2.count() >= 360 && d2.count() <= 440, "attempt 2 ~400ms");
    PASS();
}

void test_success_on_first_attempt() {
    T("success on first attempt (no retry)");
    auto test = []() -> Task<void> {
        auto result = co_await with_retry(
            []() -> Task<int> { co_return 42; },
            ExponentialBackoff{},
            [](const std::error_code&) { return false; }
        );
        CO_CHECK(result == 42, "correct result");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

void test_retry_on_failure() {
    T("retry on failure then success");
    auto test = []() -> Task<void> {
        int calls = 0;
        auto result = co_await with_retry(
            [&calls]() -> Task<int> {
                ++calls;
                if (calls < 3) throw std::system_error(make_io_error(ECONNREFUSED));
                co_return 100;
            },
            ExponentialBackoff{std::chrono::milliseconds(10),
                               std::chrono::milliseconds(50), 5, 0.0},
            [](const std::error_code& ec) { return is_retryable(ec); }
        );
        CO_CHECK(result == 100, "got result after retries");
        CO_CHECK(calls == 3, "called 3 times");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

void test_non_retryable_throws() {
    T("non-retryable error throws immediately");
    bool caught = false;
    auto test = [&caught]() -> Task<void> {
        int calls = 0;
        try {
            co_await with_retry(
                [&calls]() -> Task<int> {
                    ++calls;
                    throw std::system_error(make_io_error(EPERM));
                },
                ExponentialBackoff{},
                [](const std::error_code& ec) { return is_retryable(ec); }
            );
        } catch (const std::system_error&) {
            caught = true;
        }
        CO_CHECK(calls == 1, "called once");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    CHECK(caught, "exception caught");
    PASS();
}

void test_max_retries_exceeded() {
    T("max retries exceeded throws");
    bool caught = false;
    auto test = [&caught]() -> Task<void> {
        try {
            co_await with_retry(
                []() -> Task<int> {
                    throw std::system_error(make_io_error(ECONNREFUSED));
                },
                ExponentialBackoff{std::chrono::milliseconds(10),
                                   std::chrono::milliseconds(50), 2, 0.0},
                [](const std::error_code&) { return true; }
            );
        } catch (const std::system_error&) {
            caught = true;
        }
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    CHECK(caught, "exception caught after max retries");
    PASS();
}

int main() {
    std::cout << "=== Retry Tests ===" << std::endl;
    test_delay_computation();
    test_success_on_first_attempt();
    test_retry_on_failure();
    test_non_retryable_throws();
    test_max_retries_exceeded();

    std::cout << "\n=== Retry Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

#include "ultranet/ultranet.h"
#include <iostream>
#include <thread>

using namespace ynet::async;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

void test_initial_state() {
    T("initial state is Closed");
    CircuitBreaker cb;
    CHECK(cb.state() == CircuitState::Closed, "should be Closed");
    PASS();
}

void test_closed_allows_request() {
    T("Closed allows request");
    CircuitBreaker cb;
    CHECK(cb.try_acquire(), "should allow");
    PASS();
}

void test_transition_to_open() {
    T("transitions to Open after threshold failures");
    CircuitBreaker cb({.failure_threshold = 3, .open_timeout = std::chrono::seconds(10)});
    cb.on_failure();
    cb.on_failure();
    CHECK(cb.state() == CircuitState::Closed, "still Closed after 2 failures");
    cb.on_failure();
    CHECK(cb.state() == CircuitState::Open, "Open after 3 failures");
    PASS();
}

void test_open_fast_fails() {
    T("Open fast-fails requests");
    CircuitBreaker cb({.failure_threshold = 1, .open_timeout = std::chrono::seconds(10)});
    cb.on_failure();
    CHECK(cb.state() == CircuitState::Open, "now Open");
    CHECK(!cb.try_acquire(), "should fast-fail");
    PASS();
}

void test_half_open_after_timeout() {
    T("transitions to HalfOpen after timeout");
    CircuitBreaker cb({.failure_threshold = 1, .open_timeout = std::chrono::milliseconds(1)});
    cb.on_failure();
    CHECK(cb.state() == CircuitState::Open, "now Open");
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK(cb.try_acquire(), "should allow in HalfOpen");
    CHECK(cb.state() == CircuitState::HalfOpen, "now HalfOpen");
    PASS();
}

void test_half_open_success_goes_closed() {
    T("HalfOpen success transitions to Closed");
    CircuitBreaker cb({.failure_threshold = 1, .open_timeout = std::chrono::milliseconds(1)});
    cb.on_failure();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    cb.try_acquire();
    CHECK(cb.state() == CircuitState::HalfOpen, "HalfOpen");
    cb.on_success();
    CHECK(cb.state() == CircuitState::Closed, "Closed after success");
    PASS();
}

void test_half_open_failure_goes_open() {
    T("HalfOpen failure goes back to Open");
    CircuitBreaker cb({.failure_threshold = 1, .open_timeout = std::chrono::milliseconds(1)});
    cb.on_failure();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    cb.try_acquire();
    CHECK(cb.state() == CircuitState::HalfOpen, "HalfOpen");
    cb.on_failure();
    CHECK(cb.state() == CircuitState::Open, "back to Open");
    PASS();
}

void test_reset() {
    T("reset() clears state");
    CircuitBreaker cb({.failure_threshold = 1, .open_timeout = std::chrono::seconds(10)});
    cb.on_failure();
    CHECK(cb.state() == CircuitState::Open, "Open");
    cb.reset();
    CHECK(cb.state() == CircuitState::Closed, "Closed after reset");
    CHECK(cb.try_acquire(), "allows after reset");
    PASS();
}

void test_success_resets_failure_count() {
    T("success resets failure count in Closed");
    CircuitBreaker cb({.failure_threshold = 3, .open_timeout = std::chrono::seconds(10)});
    cb.on_failure();
    cb.on_failure();
    cb.on_success();
    cb.on_failure();
    CHECK(cb.state() == CircuitState::Closed, "still Closed (count reset)");
    PASS();
}

void test_stats() {
    T("stats tracking");
    CircuitBreaker cb({.failure_threshold = 5, .open_timeout = std::chrono::seconds(10)});
    cb.on_success();
    cb.on_failure();
    auto s = cb.snapshot();
    CHECK(s.total_successes == 1, "success count");
    CHECK(s.failure_count == 1, "failure count");
    PASS();
}

int main() {
    std::cout << "=== CircuitBreaker Tests ===" << std::endl;
    test_initial_state();
    test_closed_allows_request();
    test_transition_to_open();
    test_open_fast_fails();
    test_half_open_after_timeout();
    test_half_open_success_goes_closed();
    test_half_open_failure_goes_open();
    test_reset();
    test_success_resets_failure_count();
    test_stats();

    std::cout << "\n=== CircuitBreaker Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

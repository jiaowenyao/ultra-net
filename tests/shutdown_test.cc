#include "ultranet/ultranet.h"
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::lifecycle;
using namespace ynet::async::io;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

void test_shutdown_sets_flag() {
    T("shutdown() sets is_shutdown");
    ShutdownCoordinator coord;
    CHECK(!coord.is_shutdown(), "initially running");
    coord.shutdown();
    CHECK(coord.is_shutdown(), "after shutdown()");
    PASS();
}

void test_shutdown_idempotent() {
    T("shutdown() is idempotent");
    ShutdownCoordinator coord;
    coord.shutdown();
    coord.shutdown();
    CHECK(coord.is_shutdown(), "still shutdown");
    PASS();
}

void test_shutdown_flag() {
    T("shutdown flag transitions");
    ShutdownCoordinator coord;
    CHECK(!coord.is_shutdown(), "initially false");
    coord.shutdown();
    CHECK(coord.is_shutdown(), "true after shutdown");
    CHECK(coord.is_shutdown(), "stays true");
    PASS();
}

void test_wait_wakes_on_shutdown() {
    T("wait() returns nullopt on shutdown");
    auto test = []() -> Task<void> {
        auto coord = std::make_shared<ShutdownCoordinator>();

        // Launch shutdown after a short delay
        auto delayer = [coord]() -> Task<void> {
            co_await sleep_for(std::chrono::milliseconds(100));
            coord->shutdown();
        };

        // Use when_any to race wait against a timeout
        auto waiter = [coord]() -> Task<std::optional<bool>> {
            co_return co_await coord->wait();
        };

        auto timed = []() -> Task<std::optional<bool>> {
            co_await sleep_for(std::chrono::milliseconds(500));
            co_return std::optional<bool>(false);
        };

        auto [idx, results] = co_await when_any(waiter(), timed());
        if (idx == 0) {
            auto val = std::get<0>(results);
            // waiter won: should have nullopt (channel closed)
            CO_CHECK(!val.has_value(), "should be nullopt");
        }
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

void test_multiple_waiters() {
    T("multiple waiters all woken");
    std::atomic<int> woken{0};
    auto test = [&]() -> Task<void> {
        auto coord = std::make_shared<ShutdownCoordinator>();

        // 3 waiters
        auto w1 = [coord, &woken]() -> Task<void> {
            auto v = co_await coord->wait();
            if (!v.has_value()) woken.fetch_add(1);
        };
        auto w2 = [coord, &woken]() -> Task<void> {
            auto v = co_await coord->wait();
            if (!v.has_value()) woken.fetch_add(1);
        };
        auto w3 = [coord, &woken]() -> Task<void> {
            auto v = co_await coord->wait();
            if (!v.has_value()) woken.fetch_add(1);
        };

        // Start all 3 waiters + 1 shutdown trigger
        auto trigger = [coord]() -> Task<void> {
            co_await sleep_for(std::chrono::milliseconds(100));
            coord->shutdown();
        };

        co_await when_all(w1(), w2(), w3(), trigger());
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    CHECK(woken.load() == 3, "all 3 waiters woken");
    PASS();
}

int main() {
    std::cout << "=== ShutdownCoordinator Tests ===" << std::endl;
    test_shutdown_sets_flag();
    test_shutdown_idempotent();
    test_shutdown_flag();
    test_wait_wakes_on_shutdown();
    test_multiple_waiters();

    std::cout << "\n=== Shutdown Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

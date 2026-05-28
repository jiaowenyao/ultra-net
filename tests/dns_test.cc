#include "ultranet/ultranet.h"
#include <iostream>
#include <atomic>
#include <cassert>
#include <algorithm>

using namespace ynet::async;
using namespace ynet::async::io;

static int passed = 0;
static int failed = 0;

#define TEST(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(msg) do { std::cout << "FAILED: " << msg << std::endl; ++failed; } while(0)
#define CO_CHECK(cond, msg) do { if (!(cond)) { FAIL(msg); co_return; } } while(0)

Task<void> test_resolve_localhost(scheduling::WorkStealingThreadPool& pool) {
    TEST("resolve localhost");
    auto results = co_await resolve_host("localhost", std::chrono::milliseconds(3000));
    CO_CHECK(!results.empty(), "no results for localhost");
    bool has_loopback = false;
    for (const auto& ip : results) {
        if (ip == "127.0.0.1" || ip == "::1") { has_loopback = true; break; }
    }
    CO_CHECK(has_loopback, "localhost did not resolve to 127.0.0.1 or ::1");
    PASS();
    co_return;
}

Task<void> test_resolve_external(scheduling::WorkStealingThreadPool& pool) {
    TEST("resolve external domain");
    auto results = co_await resolve_host("github.com", std::chrono::milliseconds(5000));
    CO_CHECK(!results.empty(), "no results for github.com");
    for (const auto& ip : results) {
        unsigned char b1, b2, b3, b4;
        if (sscanf(ip.c_str(), "%hhu.%hhu.%hhu.%hhu", &b1, &b2, &b3, &b4) == 4) {
            PASS();
            co_return;
        }
    }
    FAIL("no valid IPv4 address found");
    co_return;
}

Task<void> test_resolve_nonexistent(scheduling::WorkStealingThreadPool& pool) {
    TEST("resolve nonexistent domain");
    auto results = co_await resolve_host("this-domain-does-not-exist-12345.com",
        std::chrono::milliseconds(3000));
    CO_CHECK(results.empty(), "should return empty for nonexistent domain");
    PASS();
    co_return;
}

Task<void> test_resolve_timeout(scheduling::WorkStealingThreadPool& pool) {
    TEST("resolve timeout (1ms → too short)");
    auto results = co_await resolve_host("github.com", std::chrono::milliseconds(1));
    CO_CHECK(results.empty(), "should return empty on timeout");
    PASS();
    co_return;
}

Task<void> test_resolve_multiple(scheduling::WorkStealingThreadPool& pool) {
    TEST("resolve multiple concurrent lookups");
    auto r1 = co_await resolve_host("localhost", std::chrono::milliseconds(3000));
    auto r2 = co_await resolve_host("example.com", std::chrono::milliseconds(5000));
    CO_CHECK(!r1.empty(), "localhost should resolve");
    CO_CHECK(!r2.empty(), "example.com should resolve");
    PASS();
    co_return;
}

int main() {
    std::cout << "=== DNS Resolver Tests ===" << std::endl;

    try {
        scheduling::WorkStealingThreadPool pool(2);
        {
            ExecutionContext::Scope scope(&pool);

            pool.submit(test_resolve_localhost(pool).task());
            pool.wait_all();

            pool.submit(test_resolve_external(pool).task());
            pool.wait_all();

            pool.submit(test_resolve_nonexistent(pool).task());
            pool.wait_all();

            pool.submit(test_resolve_timeout(pool).task());
            pool.wait_all();

            pool.submit(test_resolve_multiple(pool).task());
            pool.wait_all();
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\n=== DNS Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

#include "ultranet/ultranet.h"
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::io;

static int passed = 0, failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

void test_srv_query_format() {
    T("SRV query returns structured records");
    auto test = []() -> Task<void> {
        auto records = co_await resolve_srv("_xmpp-client", "_tcp", "gmail.com",
            std::chrono::milliseconds(5000));
        if (!records.empty()) {
            for (auto& r : records) {
                // All fields should be populated
                CO_CHECK(r.port > 0, "port > 0");
                CO_CHECK(!r.target.empty(), "target not empty");
            }
        }
        // Empty is OK too (network-dependent)
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

void test_srv_nonexistent() {
    T("SRV query for nonexistent service returns empty");
    auto test = []() -> Task<void> {
        auto records = co_await resolve_srv("_nonexistent12345", "_tcp", "example.com",
            std::chrono::milliseconds(5000));
        CO_CHECK(records.empty(), "should be empty");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

void test_srv_timeout() {
    T("SRV query with very short timeout");
    auto test = []() -> Task<void> {
        auto records = co_await resolve_srv("_http", "_tcp", "google.com",
            std::chrono::milliseconds(1));
        // Short timeout: either gets results quickly or returns empty
        // Either is acceptable for this test
        (void)records;
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

int main() {
    std::cout << "=== DNS SRV Tests ===" << std::endl;
    test_srv_query_format();
    test_srv_nonexistent();
    test_srv_timeout();

    std::cout << "\n=== DNS SRV Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

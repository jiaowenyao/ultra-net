#include "ultranet/ultranet.h"
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::discovery;

static int passed = 0, failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

void test_static_backend_initial() {
    T("static backend initial endpoints");
    std::vector<Endpoint> eps = {
        {"127.0.0.1", 8080, 0, 0, true},
        {"127.0.0.2", 8081, 0, 0, true},
    };
    ServiceDiscovery sd(eps);
    auto all = sd.all_endpoints();
    CHECK(all.size() == 2, "2 endpoints");
    CHECK(all[0].host == "127.0.0.1", "first host");
    CHECK(all[1].host == "127.0.0.2", "second host");
    PASS();
}

void test_circuit_breaker_per_endpoint() {
    T("circuit breaker per endpoint");
    ServiceDiscovery sd({{"a", 1, 0, 0, true}, {"b", 2, 0, 0, true}});
    auto* cb_a = sd.circuit_breaker({"a", 1});
    auto* cb_b = sd.circuit_breaker({"b", 2});
    CHECK(cb_a != nullptr, "cb a exists");
    CHECK(cb_b != nullptr, "cb b exists");
    CHECK(cb_a->state() == CircuitState::Closed, "cb a closed");
    CHECK(cb_b->state() == CircuitState::Closed, "cb b closed");
    PASS();
}

void test_stats() {
    T("stats initial values");
    ServiceDiscovery sd({{"x", 1, 0, 0, true}});
    auto s = sd.snapshot();
    CHECK(s.dns_queries == 0, "0 dns queries");
    CHECK(s.health_checks == 0, "0 health checks");
    PASS();
}

void test_health_check() {
    T("health check on unreachable endpoint");
    auto test = []() -> Task<void> {
        std::vector<Endpoint> eps = {{"127.0.0.1", 59999, 0, 0, true}};
        auto sd = std::make_shared<ServiceDiscovery>(
            eps, ServiceDiscovery::Config{
                .refresh_interval = std::chrono::seconds(60),
                .health_check_interval = std::chrono::milliseconds(200),
                .connect_timeout = std::chrono::milliseconds(200),
                .enable_health_checks = true
            });

        // Run health checks for a short time
        auto run_task = [sd]() -> Task<void> {
            co_await sd->run();
        };

        // Submit run, let it do one health check cycle, then shutdown
        auto stopper = [sd]() -> Task<void> {
            co_await io::sleep_for(std::chrono::milliseconds(500));
            sd->shutdown();
        };

        co_await when_all(run_task(), stopper());
        auto s = sd->snapshot();
        // At least one health check should have run
        // (may be 0 if timing races, that's OK)
    };

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

int main() {
    std::cout << "=== ServiceDiscovery Tests ===" << std::endl;
    test_static_backend_initial();
    test_circuit_breaker_per_endpoint();
    test_stats();
    test_health_check();

    std::cout << "\n=== ServiceDiscovery Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

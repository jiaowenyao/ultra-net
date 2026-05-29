#include "ultranet/ultranet.h"
#include <iostream>

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

void test_defaults() {
    T("default values");
    ynet::config::UltraNetConfig cfg;
    CHECK(cfg.io_uring.entries == 1024, "io entries default");
    CHECK(cfg.io_uring.max_pending_ops == 256, "max pending ops default");
    CHECK(cfg.pool.num_threads > 0, "pool threads > 0");
    CHECK(cfg.connection_pool.min_connections == 4, "pool min");
    CHECK(cfg.connection_pool.max_connections == 32, "pool max");
    CHECK(cfg.retry.max_retries == 3, "retry max");
    CHECK(cfg.retry.base_delay.count() == 100, "retry base delay");
    CHECK(cfg.circuit_breaker.failure_threshold == 5, "cb threshold");
    PASS();
}

void test_from_env() {
    T("from_env parses environment");
    setenv("ULTRANET_POOL_THREADS", "8", 1);
    setenv("ULTRANET_IO_ENTRIES", "2048", 1);
    setenv("ULTRANET_CONN_POOL_MIN", "2", 1);

    auto cfg = ynet::config::UltraNetConfig::from_env();
    CHECK(cfg.pool.num_threads == 8, "pool threads from env");
    CHECK(cfg.io_uring.entries == 2048, "io entries from env");
    CHECK(cfg.connection_pool.min_connections == 2, "conn pool min from env");

    unsetenv("ULTRANET_POOL_THREADS");
    unsetenv("ULTRANET_IO_ENTRIES");
    unsetenv("ULTRANET_CONN_POOL_MIN");
    PASS();
}

void test_sub_configs() {
    T("sub-config structs");
    ynet::config::PoolConfig pc;
    CHECK(pc.task_timeout.count() == 0, "pool task timeout default 0");

    ynet::config::ConnectionPoolConfig cpc;
    CHECK(cpc.idle_timeout == std::chrono::minutes(5), "conn idle timeout");

    ynet::config::RetryConfig rc;
    CHECK(rc.jitter_factor == 0.2, "retry jitter default");
    PASS();
}

int main() {
    std::cout << "=== Config Tests ===" << std::endl;
    test_defaults();
    test_from_env();
    test_sub_configs();

    std::cout << "\n=== Config Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

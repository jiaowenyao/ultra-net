#pragma once

#include "ultranet/io/io_engine.hpp"
#include "ultranet/log/logger.hpp"
#include "ultranet/coroutine/task.hpp"
#include <chrono>
#include <thread>
#include <cstdlib>

namespace ynet::config {

struct PoolConfig {
    size_t num_threads = std::thread::hardware_concurrency();
    std::chrono::milliseconds task_timeout{0};
};

struct ConnectionPoolConfig {
    size_t min_connections = 4;
    size_t max_connections = 32;
    std::chrono::milliseconds idle_timeout = std::chrono::minutes(5);
    std::chrono::milliseconds max_lifetime = std::chrono::minutes(30);
    std::chrono::milliseconds health_check_interval = std::chrono::seconds(30);
    std::chrono::milliseconds connect_timeout = std::chrono::seconds(5);
};

struct LogConfig {
    log::Level level = log::Level::Info;
    bool enable_spdlog = true;
};

struct CircuitBreakerConfig {
    size_t failure_threshold = 5;
    std::chrono::milliseconds open_timeout = std::chrono::seconds(30);
};

struct RetryConfig {
    std::chrono::milliseconds base_delay{100};
    std::chrono::milliseconds max_delay{5000};
    size_t max_retries{3};
    double jitter_factor{0.2};
};

struct ServiceDiscoveryConfig {
    std::chrono::milliseconds refresh_interval{30000};
    std::chrono::milliseconds health_check_interval{10000};
    std::chrono::milliseconds connect_timeout{5000};
    size_t max_endpoints{32};
    bool enable_health_checks{true};
    size_t circuit_breaker_failures{3};
    std::chrono::milliseconds circuit_breaker_timeout{30000};
};

struct UltraNetConfig {
    ynet::async::io::IoUringEngineConfig io_uring;
    PoolConfig pool;
    ConnectionPoolConfig connection_pool;
    LogConfig log;
    CircuitBreakerConfig circuit_breaker;
    RetryConfig retry;
    ServiceDiscoveryConfig service_discovery;

    static UltraNetConfig from_env() {
        UltraNetConfig c;
        if (const char* v = std::getenv("ULTRANET_POOL_THREADS"))
            c.pool.num_threads = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_IO_ENTRIES"))
            c.io_uring.entries = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_MAX_PENDING_OPS"))
            c.io_uring.max_pending_ops = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_IO_BATCH_THRESHOLD"))
            c.io_uring.batch_threshold = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_CONN_POOL_MIN"))
            c.connection_pool.min_connections = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_CONN_POOL_MAX"))
            c.connection_pool.max_connections = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_CONN_POOL_CONNECT_TIMEOUT"))
            c.connection_pool.connect_timeout = std::chrono::milliseconds(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_CB_THRESHOLD"))
            c.circuit_breaker.failure_threshold = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_RETRY_MAX"))
            c.retry.max_retries = static_cast<size_t>(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_SD_REFRESH_MS"))
            c.service_discovery.refresh_interval = std::chrono::milliseconds(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_SD_HEALTH_MS"))
            c.service_discovery.health_check_interval = std::chrono::milliseconds(std::atoll(v));
        if (const char* v = std::getenv("ULTRANET_LOG_LEVEL")) {
            int lvl = std::atoi(v);
            if (lvl >= 0 && lvl <= 5) c.log.level = static_cast<log::Level>(lvl);
        }
        return c;
    }
};

} // namespace ynet::config

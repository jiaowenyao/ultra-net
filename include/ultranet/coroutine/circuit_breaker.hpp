#pragma once

#include "ultranet/config/config.hpp"
#include "ultranet/utils/noncopyable.h"
#include <atomic>
#include <chrono>

namespace ynet::async {

enum class CircuitState : uint8_t {
    Closed,
    Open,
    HalfOpen
};

class CircuitBreaker : ynet::utils::Noncopyable {
public:
    using Config = config::CircuitBreakerConfig;

    explicit CircuitBreaker(Config cfg = {}) noexcept : m_config(cfg) {}

    CircuitState state() const noexcept {
        return m_state.load(std::memory_order_acquire);
    }

    const Config& config() const noexcept { return m_config; }

    bool try_acquire() noexcept {
        auto st = m_state.load(std::memory_order_acquire);

        if (st == CircuitState::Closed) {
            return true;
        }

        if (st == CircuitState::Open) {
            auto last_fail_ns = m_last_failure_time_ns.load(std::memory_order_relaxed);
            auto now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
            auto open_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                m_config.open_timeout).count();

            if (last_fail_ns > 0 && (now_ns - last_fail_ns) >= open_ns) {
                if (m_state.compare_exchange_strong(st, CircuitState::HalfOpen,
                        std::memory_order_acq_rel)) {
                    m_half_open_used.store(0, std::memory_order_relaxed);
                    m_half_open_attempts.fetch_add(1, std::memory_order_relaxed);
                    return true;
                }
            }
            m_fast_fail_count.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        // HalfOpen
        size_t used = m_half_open_used.fetch_add(1, std::memory_order_relaxed);
        if (used < 1) {
            m_half_open_attempts.fetch_add(1, std::memory_order_relaxed);
            return true;
        }
        m_fast_fail_count.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    void on_success() noexcept {
        m_success_count.fetch_add(1, std::memory_order_relaxed);
        m_failure_count.store(0, std::memory_order_relaxed);

        auto st = m_state.load(std::memory_order_acquire);
        if (st == CircuitState::HalfOpen) {
            m_state.store(CircuitState::Closed, std::memory_order_release);
        }
    }

    void on_failure() noexcept {
        m_failure_count.fetch_add(1, std::memory_order_relaxed);

        auto st = m_state.load(std::memory_order_acquire);
        if (st == CircuitState::Closed) {
            size_t fc = m_failure_count.load(std::memory_order_relaxed);
            if (fc >= m_config.failure_threshold) {
                auto now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
                m_last_failure_time_ns.store(now_ns, std::memory_order_relaxed);
                m_state.store(CircuitState::Open, std::memory_order_release);
            }
        } else if (st == CircuitState::HalfOpen) {
            auto now_ns = std::chrono::steady_clock::now().time_since_epoch().count();
            m_last_failure_time_ns.store(now_ns, std::memory_order_relaxed);
            m_state.store(CircuitState::Open, std::memory_order_release);
        }
    }

    void reset() noexcept {
        m_state.store(CircuitState::Closed, std::memory_order_release);
        m_failure_count.store(0, std::memory_order_relaxed);
        m_last_failure_time_ns.store(0, std::memory_order_relaxed);
        m_half_open_used.store(0, std::memory_order_relaxed);
    }

    struct Stats {
        size_t total_successes{0};
        size_t total_failures{0};
        size_t consecutive_failures{0};
        size_t fast_fails{0};
        size_t half_open_attempts{0};
    };

    Stats snapshot() const noexcept {
        return {
            m_success_count.load(std::memory_order_relaxed),
            m_failure_count.load(std::memory_order_relaxed),
            m_failure_count.load(std::memory_order_relaxed),
            m_fast_fail_count.load(std::memory_order_relaxed),
            m_half_open_attempts.load(std::memory_order_relaxed)
        };
    }

private:
    Config m_config;
    std::atomic<CircuitState> m_state{CircuitState::Closed};
    std::atomic<size_t> m_failure_count{0};
    std::atomic<size_t> m_success_count{0};
    std::atomic<size_t> m_fast_fail_count{0};
    std::atomic<size_t> m_half_open_attempts{0};
    std::atomic<size_t> m_half_open_used{0};
    std::atomic<int64_t> m_last_failure_time_ns{0};
};

} // namespace ynet::async

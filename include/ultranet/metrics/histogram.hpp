#pragma once

#include <atomic>
#include <vector>
#include <string>
#include <algorithm>
#include <cmath>
#include <cstdint>

namespace ynet::metrics {

class Histogram {
public:
    Histogram(std::string name, std::string help,
              std::vector<double> buckets = default_buckets())
        : m_name(std::move(name)), m_help(std::move(help)), m_buckets(std::move(buckets))
        , m_counts(m_buckets.size() + 1) {}

    static std::vector<double> default_buckets() {
        return {0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0};
    }

    void observe(double value) noexcept {
        m_count.fetch_add(1, std::memory_order_relaxed);
        double old = m_sum.load(std::memory_order_relaxed);
        while (!m_sum.compare_exchange_weak(old, old + value,
                std::memory_order_relaxed, std::memory_order_relaxed)) {}

        size_t idx = 0;
        for (idx = 0; idx < m_buckets.size(); ++idx) {
            if (value <= m_buckets[idx]) break;
        }

        lock();
        m_counts[idx]++;
        unlock();
    }

    size_t count() const noexcept { return m_count.load(std::memory_order_relaxed); }
    double sum() const noexcept { return m_sum.load(std::memory_order_relaxed); }

    const std::string& name() const noexcept { return m_name; }
    const std::string& help() const noexcept { return m_help; }

    // 基于桶计数的线性插值百分位（无需存储全量样本）
    double p50() { return percentile(0.50); }
    double p90() { return percentile(0.90); }
    double p99() { return percentile(0.99); }
    double p999() { return percentile(0.999); }

    std::string to_prometheus() const {
        std::string result;
        result += "# HELP " + m_name + " " + m_help + "\n";
        result += "# TYPE " + m_name + " histogram\n";

        lock();
        auto counts = m_counts;
        unlock();

        int64_t cumulative = 0;
        for (size_t i = 0; i < m_buckets.size(); ++i) {
            cumulative += counts[i];
            result += m_name + "_bucket{le=\"" + format_bucket(m_buckets[i]) + "\"} "
                   + std::to_string(cumulative) + "\n";
        }
        cumulative += counts[m_buckets.size()];
        result += m_name + "_bucket{le=\"+Inf\"} " + std::to_string(cumulative) + "\n";
        result += m_name + "_count " + std::to_string(count()) + "\n";
        result += m_name + "_sum " + std::to_string(sum()) + "\n";
        return result;
    }

private:
    void lock() const noexcept {
        while (m_lock.test_and_set(std::memory_order_acquire)) {
            // spin
        }
    }

    void unlock() const noexcept {
        m_lock.clear(std::memory_order_release);
    }

    double percentile(double p) {
        auto total = count();
        if (total == 0) return 0;

        lock();
        auto counts = m_counts;
        unlock();

        double target = p * static_cast<double>(total);
        int64_t cumulative = 0;
        double prev_bound = 0;

        for (size_t i = 0; i < m_buckets.size(); ++i) {
            cumulative += counts[i];
            if (static_cast<double>(cumulative) >= target) {
                double ratio = (target - (cumulative - counts[i])) / static_cast<double>(counts[i]);
                if (counts[i] == 0) ratio = 0;
                return prev_bound + ratio * (m_buckets[i] - prev_bound);
            }
            prev_bound = m_buckets[i];
        }

        return prev_bound;
    }

    static std::string format_bucket(double v) {
        if (v >= 0.001 && v <= 10.0) {
            char buf[32];
            snprintf(buf, sizeof(buf), "%g", v);
            return buf;
        }
        return std::to_string(v);
    }

    std::string m_name;
    std::string m_help;
    std::vector<double> m_buckets;
    std::vector<int64_t> m_counts;
    std::atomic<size_t> m_count{0};
    std::atomic<double> m_sum{0};
    mutable std::atomic_flag m_lock = ATOMIC_FLAG_INIT;
};

} // namespace ynet::metrics

#pragma once

#include "counter.hpp"
#include "gauge.hpp"
#include "histogram.hpp"
#include <memory>
#include <unordered_map>
#include <atomic>

namespace ynet::metrics {

class MetricRegistry {
public:
    static MetricRegistry& instance() {
        static MetricRegistry reg;
        return reg;
    }

    Counter* counter(const std::string& name, const std::string& help = "") {
        return get_or_create<Counter>(m_counters, name, help);
    }

    Gauge* gauge(const std::string& name, const std::string& help = "") {
        return get_or_create<Gauge>(m_gauges, name, help);
    }

    Histogram* histogram(const std::string& name, const std::string& help = "",
                         std::vector<double> buckets = Histogram::default_buckets()) {
        lock();
        auto it = m_histograms.find(name);
        if (it != m_histograms.end()) {
            unlock();
            return it->second.get();
        }
        auto h = std::make_unique<Histogram>(name, help, std::move(buckets));
        auto* ptr = h.get();
        m_histograms[name] = std::move(h);
        unlock();
        return ptr;
    }

    std::string to_prometheus_text() const {
        std::string result;
        lock();
        for (const auto& [_, c] : m_counters) {
            result += "# HELP " + c->name() + " " + c->help() + "\n";
            result += "# TYPE " + c->name() + " counter\n";
            result += c->to_prometheus();
        }
        for (const auto& [_, g] : m_gauges) {
            result += "# HELP " + g->name() + " " + g->help() + "\n";
            result += "# TYPE " + g->name() + " gauge\n";
            result += g->to_prometheus();
        }
        for (const auto& [_, h] : m_histograms) {
            result += h->to_prometheus();
        }
        unlock();
        return result;
    }

    void reset() {
        lock();
        m_counters.clear();
        m_gauges.clear();
        m_histograms.clear();
        unlock();
    }

private:
    MetricRegistry() = default;

    void lock() const noexcept {
        while (m_lock.test_and_set(std::memory_order_acquire)) {}
    }

    void unlock() const noexcept {
        m_lock.clear(std::memory_order_release);
    }

    template <typename T>
    T* get_or_create(std::unordered_map<std::string, std::unique_ptr<T>>& map,
                     const std::string& name, const std::string& help) {
        lock();
        auto it = map.find(name);
        if (it != map.end()) {
            unlock();
            return it->second.get();
        }
        auto obj = std::make_unique<T>(name, help);
        auto* ptr = obj.get();
        map[name] = std::move(obj);
        unlock();
        return ptr;
    }

    mutable std::atomic_flag m_lock = ATOMIC_FLAG_INIT;
    std::unordered_map<std::string, std::unique_ptr<Counter>> m_counters;
    std::unordered_map<std::string, std::unique_ptr<Gauge>> m_gauges;
    std::unordered_map<std::string, std::unique_ptr<Histogram>> m_histograms;
};

} // namespace ynet::metrics

#pragma once

#include <atomic>
#include <string>
#include <cstdint>

namespace ynet::metrics {

class Counter {
public:
    explicit Counter(std::string name, std::string help = "")
        : m_name(std::move(name)), m_help(std::move(help)) {}

    void inc(int64_t delta = 1) noexcept { m_value.fetch_add(delta, std::memory_order_relaxed); }
    int64_t value() const noexcept { return m_value.load(std::memory_order_relaxed); }
    void reset() noexcept { m_value.store(0, std::memory_order_relaxed); }

    const std::string& name() const noexcept { return m_name; }
    const std::string& help() const noexcept { return m_help; }

    // Prometheus text format: name{labels} value
    std::string to_prometheus() const {
        return m_name + " " + std::to_string(value()) + "\n";
    }

private:
    std::string m_name;
    std::string m_help;
    std::atomic<int64_t> m_value{0};
};

} // namespace ynet::metrics

#pragma once

#include <atomic>
#include <string>
#include <cstdint>

namespace ynet::metrics {

class Gauge {
public:
    explicit Gauge(std::string name, std::string help = "")
        : m_name(std::move(name)), m_help(std::move(help)) {}

    void set(int64_t v) noexcept { m_value.store(v, std::memory_order_relaxed); }
    void inc(int64_t delta = 1) noexcept { m_value.fetch_add(delta, std::memory_order_relaxed); }
    void dec(int64_t delta = 1) noexcept { m_value.fetch_sub(delta, std::memory_order_relaxed); }
    int64_t value() const noexcept { return m_value.load(std::memory_order_relaxed); }

    const std::string& name() const noexcept { return m_name; }
    const std::string& help() const noexcept { return m_help; }

    std::string to_prometheus() const {
        return m_name + " " + std::to_string(value()) + "\n";
    }

private:
    std::string m_name;
    std::string m_help;
    std::atomic<int64_t> m_value{0};
};

} // namespace ynet::metrics

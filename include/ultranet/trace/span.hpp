#pragma once

#include <chrono>
#include <string>
#include <atomic>
#include <random>

namespace ynet::trace {

// 简易 trace/span ID 生成器
inline uint64_t generate_id() {
    static std::atomic<uint64_t> counter{0};
    static thread_local std::mt19937_64 rng(std::random_device{}());
    auto base = rng();
    auto seq = counter.fetch_add(1, std::memory_order_relaxed);
    return base ^ (seq << 32);
}

struct SpanContext {
    uint64_t trace_id{0};
    uint64_t span_id{0};
    uint64_t parent_span_id{0};
};

class Span {
public:
    Span(std::string name, SpanContext parent = {})
        : m_name(std::move(name))
        , m_start(std::chrono::steady_clock::now()) {
        m_ctx.span_id = generate_id();
        m_ctx.parent_span_id = parent.span_id;
        m_ctx.trace_id = parent.trace_id ? parent.trace_id : generate_id();
    }

    void stop() noexcept {
        if (!m_stopped) {
            m_stop = std::chrono::steady_clock::now();
            m_stopped = true;
        }
    }

    uint64_t trace_id() const noexcept { return m_ctx.trace_id; }
    uint64_t span_id() const noexcept { return m_ctx.span_id; }
    uint64_t parent_span_id() const noexcept { return m_ctx.parent_span_id; }
    const std::string& name() const noexcept { return m_name; }

    const SpanContext& context() const noexcept { return m_ctx; }

    // 延迟（微秒）
    int64_t latency_us() const noexcept {
        auto end = m_stopped ? m_stop : std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(end - m_start).count();
    }

    // 创建子 Span
    Span child(std::string name) const {
        return Span(std::move(name), m_ctx);
    }

private:
    std::string m_name;
    SpanContext m_ctx;
    std::chrono::steady_clock::time_point m_start;
    std::chrono::steady_clock::time_point m_stop;
    bool m_stopped{false};
};

} // namespace ynet::trace

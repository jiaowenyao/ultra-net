// mailbox — 无锁 MPSC 消息队列，带背压检测。
// 每个 actor 持有一个 mailbox；生产者推入 message_envelope，
// actor 的调度器批量排空。多生产者-单消费者（MPSC）模式。
#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <optional>
#include <thread>

#include "ultranet/actor/core/type_hash.h"
#include "ultranet/coroutine/mpsc_queue.hpp"

namespace ynet::actor {

using ynet::async::scheduling::MpscQueue;

// ── 消息信封 ──────────────────────────────────────────────────────────────
// 携带序列化的消息负载及其类型哈希。始终拥有自己的存储（数据在构造时拷贝）。

struct message_envelope {
    uint64_t             msg_type = 0;
    std::vector<uint8_t> data;

    // 从消息实例构造信封（memcpy 拷贝消息体）
    template <typename Msg>
    static message_envelope make(const Msg& msg) {
        message_envelope env;
        env.msg_type = actor_type_hash<Msg>();
        env.data.resize(sizeof(Msg));
        std::memcpy(env.data.data(), &msg, sizeof(Msg));
        return env;
    }
};

// ── Mailbox ───────────────────────────────────────────────────────────────
// 封装固定容量的 MpscQueue。多生产者，单消费者。
// 提供背压检测和批量排空辅助方法。

class mailbox {
public:
    static constexpr size_t k_default_capacity = 4096;
    static constexpr size_t k_backpressure_pct = 80;

    mailbox() = default;

    // 推入一条消息。队列满时返回 false（触发背压）。
    bool try_push(message_envelope env) {
        return m_queue.try_push(std::move(env));
    }

    // 弹出一条消息。队列空时返回 nullopt。
    std::optional<message_envelope> try_pop() {
        return m_queue.try_pop();
    }

    // 近似消息数量（用于监控和背压判断）
    size_t approximate_size() const {
        return m_queue.approximate_size();
    }

    bool empty() const {
        return m_queue.empty();
    }

    // 队列是否超过背压阈值（默认 80% 容量）
    bool is_backpressure() const {
        return approximate_size() > (k_default_capacity * k_backpressure_pct / 100);
    }

    // 批量排空最多 max_count 条消息，每条调用 handler 处理。
    // 返回实际处理的消息数。
    template <typename Func>
    size_t drain(Func&& handler, size_t max_count) {
        size_t count = 0;
        while (count < max_count) {
            auto env = try_pop();
            if (!env) {
                break;
            }
            handler(std::move(*env));
            ++count;
        }
        return count;
    }

    // 阻塞直到 push 成功。接收 const 引用，每次重试由 MpscQueue::try_push
    // 内部拷贝；失败时不消费原数据，调用方的 env 对象始终保持完整。
    void push_blocking(const message_envelope& env) {
        while (!m_queue.try_push(env)) {
            std::this_thread::yield();
        }
    }

private:
    MpscQueue<message_envelope, k_default_capacity> m_queue;
};

} // namespace ynet::actor

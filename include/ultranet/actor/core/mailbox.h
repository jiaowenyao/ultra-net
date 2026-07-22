// mailbox — lock-free MPSC message queue with backpressure detection.
// Each actor owns one mailbox; producers push message_envelope objects,
// the actor's scheduler drains them in batches.
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

// ── Message envelope ────────────────────────────────────────────────────
// Carries a serialised message payload tagged with its type hash.
// Always owns its storage (data is copied in).

struct message_envelope {
    uint64_t             msg_type = 0;
    std::vector<uint8_t> data;

    template <typename Msg>
    static message_envelope make(const Msg& msg) {
        message_envelope env;
        env.msg_type = actor_type_hash<Msg>();
        env.data.resize(sizeof(Msg));
        std::memcpy(env.data.data(), &msg, sizeof(Msg));
        return env;
    }
};

// ── Mailbox ─────────────────────────────────────────────────────────────
// Wraps a fixed-capacity MpscQueue.  Multi-producer, single consumer.
// Provides backpressure detection and batch-drain helpers.

class mailbox {
public:
    static constexpr size_t k_default_capacity = 4096;
    static constexpr size_t k_backpressure_pct = 80;

    mailbox() = default;

    // Push a message.  Returns false if the queue is full (backpressure).
    bool try_push(message_envelope env) {
        return m_queue.try_push(std::move(env));
    }

    // Pop one message.  Returns nullopt when empty.
    std::optional<message_envelope> try_pop() {
        return m_queue.try_pop();
    }

    // Approximate number of messages waiting.
    size_t approximate_size() const {
        return m_queue.approximate_size();
    }

    bool empty() const {
        return m_queue.empty();
    }

    // True when the queue is above the backpressure threshold.
    bool is_backpressure() const {
        return approximate_size() > (k_default_capacity * k_backpressure_pct / 100);
    }

    // Drain up to max_count messages, invoking handler for each.
    // Returns the number of messages actually processed.
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

    // Spin until a push succeeds.  Passes env by value each iteration;
    // MpscQueue::try_push does not consume the argument on failure,
    // so env remains intact across retries.
    void push_blocking(const message_envelope& env) {
        while (!m_queue.try_push(env)) {
            std::this_thread::yield();
        }
    }

private:
    MpscQueue<message_envelope, k_default_capacity> m_queue;
};

} // namespace ynet::actor

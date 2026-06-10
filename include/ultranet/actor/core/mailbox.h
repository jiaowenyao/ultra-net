// Actor mailbox — per-actor message queue.
#pragma once

#include <vector>
#include <atomic>
#include <optional>

#include "ultranet/coroutine/mpsc_queue.hpp"
#include "ultranet/actor/core/message.h"

namespace ynet::actor {

using ynet::async::scheduling::MpscQueue;

struct mailbox_config {
    size_t capacity = 4096;
};

class mailbox_set {
public:
    static constexpr size_t k_default_capacity = 4096;

    explicit mailbox_set(const std::vector<mailbox_config>& configs) {
        if (configs.empty()) {
            m_queues.emplace_back(new MpscQueue<message*, k_default_capacity>());
            return;
        }
        for (auto& cfg : configs) {
            m_queues.emplace_back(new MpscQueue<message*, k_default_capacity>());
        }
    }

    void push(size_t idx, message* msg) {
        while (!m_queues[idx]->try_push(msg)) {}
    }

    std::optional<message*> try_pop(size_t idx) {
        return m_queues[idx]->try_pop();
    }

    size_t count() const { return m_queues.size(); }

private:
    std::vector<std::unique_ptr<MpscQueue<message*, k_default_capacity>>> m_queues;
};

} // namespace ynet::actor

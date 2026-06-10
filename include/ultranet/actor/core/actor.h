// Actor<T> — turns any C++ class into a stateful async actor.
#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include <chrono>

#include "ultranet/buffer/buffer.h"
#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/actor/core/message.h"
#include "ultranet/actor/core/mailbox.h"

namespace ynet::actor {

using ynet::async::scheduling::WorkStealingThreadPool;

struct actor_config {
    size_t max_messages_per_activation = 256;
    std::vector<mailbox_config> mailbox_configs = {};
    std::string name;
};

// ── actor_system: owns the thread pool ──────────────────────────────────

class actor_system {
public:
    explicit actor_system(size_t num_threads = 4)
        : m_pool(num_threads) {}

    WorkStealingThreadPool& pool() { return m_pool; }

    void schedule(std::function<void()> fn) {
        m_pool.submit_function(std::move(fn));
    }

    void shutdown() { m_pool.wait_all(); }

    ~actor_system() {
        m_stopping.store(true, std::memory_order_release);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    bool is_stopping() const { return m_stopping.load(std::memory_order_acquire); }

    static actor_system* current() { return t_current; }
    static void set_current(actor_system* sys) { t_current = sys; }

private:
    WorkStealingThreadPool m_pool;
    std::atomic<bool> m_stopping{false};
    static thread_local actor_system* t_current;
};

inline thread_local actor_system* actor_system::t_current = nullptr;

// ── type_erased_actor ──────────────────────────────────────────────────

class type_erased_actor {
public:
    explicit type_erased_actor(actor_config cfg, actor_system* system)
        : m_config(std::move(cfg)), m_system(system) {}
    virtual ~type_erased_actor() = default;
    virtual void push_message(message* msg, size_t mailbox_idx = 0) = 0;

    const actor_config& config() const { return m_config; }
    actor_system* system() const { return m_system; }

protected:
    actor_config m_config;
    actor_system* m_system;
};

// ── actor<T> ───────────────────────────────────────────────────────────

template <typename T>
class actor : public type_erased_actor {
public:
    template <typename... Args>
    explicit actor(actor_system* system, actor_config cfg, Args&&... args)
        : type_erased_actor(std::move(cfg), system)
        , m_instance(std::make_unique<T>(std::forward<Args>(args)...))
        , m_mailboxes(this->m_config.mailbox_configs) {}

    T* get() { return m_instance.get(); }

    void push_message(message* msg, size_t mailbox_idx = 0) override {
        m_mailboxes.push(mailbox_idx, msg);
        m_pending.fetch_add(1, std::memory_order_release);
        try_activate();
    }

    void pull_and_run() {
        if (!m_instance) return;

        size_t executed = 0;
        size_t limit = std::min(m_config.max_messages_per_activation,
                                m_pending.load(std::memory_order_acquire));
        size_t n = m_mailboxes.count();
        bool limit_reached = false;

        for (size_t i = 0; i < n && !limit_reached; ++i) {
            while (auto msg = m_mailboxes.try_pop(i)) {
                (*msg)->run();
                delete *msg;
                ++executed;
                if (executed >= limit) [[unlikely]] { limit_reached = true; break; }
            }
        }

        m_pending.fetch_sub(executed, std::memory_order_release);
        m_activated.store(false, std::memory_order_seq_cst);
        if (m_pending.load(std::memory_order_acquire) > 0) try_activate();
    }

private:
    std::unique_ptr<T> m_instance;
    mailbox_set m_mailboxes;
    std::atomic<size_t> m_pending{0};
    std::atomic<bool> m_activated{false};

    void try_activate() {
        bool expected = false;
        if (m_activated.compare_exchange_strong(expected, true,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            m_system->schedule([this] { pull_and_run(); });
        }
    }
};

} // namespace ynet::actor

// actor<T> — CRTP base class for the actor framework.
// Inherit from this to turn your class into a distributed actor.
// All infrastructure (socket, scheduler, discovery) is handled internally.
#pragma once

#include <string>
#include <memory>
#include <functional>
#include <vector>
#include <cstring>
#include <atomic>
#include <unordered_map>

#include "ultranet/actor/core/actor_uri.h"
#include "ultranet/actor/core/type_hash.h"
#include "ultranet/actor/core/mailbox.h"
#include "ultranet/log/logger.hpp"

namespace ynet::actor {

class actor_system;
template <typename T> class actor_ref;

// ── Message handler type ────────────────────────────────────────────────

using message_handler_t = std::function<void(const void* data, size_t len)>;

// ── Base class for all actors ──────────────────────────────────────────

class actor_base {
public:
    actor_base() = default;
    virtual ~actor_base() = default;

    void set_uri(const actor_uri& u) { m_uri = u; }
    void set_system(actor_system* s) { m_system = s; }

    // Set the scheduling callback used by try_activate().
    // Called by actor_system during spawn() to wire up the thread pool.
    void set_schedule_fn(std::function<void(std::function<void()>)> fn) {
        m_schedule_fn = std::move(fn);
    }

    const actor_uri& uri() const { return m_uri; }
    actor_system* system() const { return m_system; }
    std::string name() const { return m_uri.name; }

    // ── Handler registration ───────────────────────────────────────────

    // Register a handler for a specific message type.
    // Usage: register_handler<my_msg>([](const my_msg& m) { ... });
    template <typename Msg>
    void register_handler(std::function<void(const Msg&)> handler) {
        uint64_t hash = actor_type_hash<Msg>();
        m_handlers[hash] = [handler = std::move(handler)](const void* data, size_t len) {
            if (len >= sizeof(Msg)) {
                const Msg* msg = static_cast<const Msg*>(data);
                handler(*msg);
            }
        };
    }

    // Deliver a message to this actor by type hash.
    // Called from pull_and_run() on the thread pool.
    // Wrapped in an error boundary: if a handler throws, the exception is
    // caught and logged, and the actor continues processing.
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) {
        auto it = m_handlers.find(msg_type);
        if (it != m_handlers.end()) {
            try {
                it->second(data, len);
            } catch (const std::exception& e) {
                // Log and continue — one bad handler shouldn't kill the actor.
                ULTRA_LOG_ERROR("[actor {}] handler exception for msg_type={}: {}",
                               m_uri.to_string(), msg_type, e.what());
            } catch (...) {
                ULTRA_LOG_ERROR("[actor {}] handler exception for msg_type={}: "
                               "unknown", m_uri.to_string(), msg_type);
            }
        } else {
            // Dead letter: no handler registered for this message type.
            ULTRA_LOG_WARN("[actor {}] dead letter: no handler for msg_type={} "
                          "(len={})", m_uri.to_string(), msg_type, len);
        }
    }

    // Check if a handler is registered for the given type.
    template <typename Msg>
    bool handles() const {
        return m_handlers.find(actor_type_hash<Msg>()) != m_handlers.end();
    }

    // ── Async mailbox interface ────────────────────────────────────────

    // Push a pre-built envelope into the mailbox and try to activate.
    // Called by local_actor_proxy::deliver() (and eventually by the
    // inbound network handler for remote messages).
    void push_envelope(message_envelope env);

    // Pull messages from the mailbox and dispatch them.
    // Runs on the thread pool.  Returns true if any messages were processed.
    bool pull_and_run();

    // Activate this actor: schedule pull_and_run() on the thread pool.
    // Uses CAS to guarantee only one execution instance at a time.
    void try_activate();

    // Accessors for the mailbox (used by proxies and integration tests).
    mailbox& get_mailbox() { return m_mailbox; }
    size_t pending() const { return m_pending.load(std::memory_order_acquire); }
    void set_max_per_activation(size_t n) { m_max_per_activation = n; }

    // Signal that the system is shutting down.  Producers should stop
    // pushing messages after this flag is set.  push_envelope() will
    // silently drop messages during shutdown to avoid use-after-free.
    void set_shutting_down(bool v) { m_shutting_down.store(v, std::memory_order_release); }
    bool is_shutting_down() const { return m_shutting_down.load(std::memory_order_acquire); }

    // Drain the mailbox (blocking).  Called during graceful shutdown
    // to process any remaining messages before the actor is destroyed.
    void drain_pending() {
        auto handler = [this](message_envelope env) {
            deliver(env.msg_type, env.data.data(), env.data.size());
        };
        m_mailbox.drain(handler, size_t(-1));  // drain all
        m_pending.store(0, std::memory_order_release);
    }

protected:
    actor_uri m_uri;
    actor_system* m_system = nullptr;

private:
    std::unordered_map<uint64_t, message_handler_t> m_handlers;
    mailbox m_mailbox;
    std::function<void(std::function<void()>)> m_schedule_fn;
    std::atomic<bool> m_activated{false};
    std::atomic<size_t> m_pending{0};
    std::atomic<bool> m_shutting_down{false};
    size_t m_max_per_activation = 64;
};

// ── Inline definitions (require actor_system forward decl) ─────────────

inline void actor_base::push_envelope(message_envelope env) {
    // During shutdown, silently drop messages to avoid use-after-free
    // (the thread pool may be gone by the time this message is processed).
    if (m_shutting_down.load(std::memory_order_acquire)) {
        return;
    }

    // Bounded spin-then-yield: try a few times before yielding the CPU.
    for (int attempt = 0; attempt < 100; ++attempt) {
        if (m_mailbox.try_push(env)) {
            m_pending.fetch_add(1, std::memory_order_release);
            try_activate();
            return;
        }
        if (attempt >= 10) {
            std::this_thread::yield();
        }
    }
    // Fallback: block until the push succeeds.
    // push_blocking takes a const ref; try_push copies on each retry
    // but does NOT consume on failure, so env stays intact.
    m_mailbox.push_blocking(env);
    m_pending.fetch_add(1, std::memory_order_release);
    try_activate();
}

inline bool actor_base::pull_and_run() {
    size_t limit = std::min(m_max_per_activation,
                            m_pending.load(std::memory_order_acquire));
    auto handler = [this](message_envelope env) {
        deliver(env.msg_type, env.data.data(), env.data.size());
    };
    size_t executed = m_mailbox.drain(handler, limit);
    if (executed > 0) {
        m_pending.fetch_sub(executed, std::memory_order_release);
    }
    m_activated.store(false, std::memory_order_release);
    // Self-reactivation: more messages arrived during processing.
    // Skip if shutting down — the pool may already be destroyed.
    if (!m_shutting_down.load(std::memory_order_acquire) &&
        m_pending.load(std::memory_order_acquire) > 0) {
        try_activate();
    }
    return executed > 0;
}

inline void actor_base::try_activate() {
    // Skip activation during shutdown to avoid use-after-free of the pool.
    if (m_shutting_down.load(std::memory_order_acquire)) {
        return;
    }
    bool expected = false;
    if (m_activated.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel)) {
        if (m_schedule_fn) {
            m_schedule_fn([this]() {
                pull_and_run();
            });
        }
    }
}

// ── CRTP actor base ────────────────────────────────────────────────────

template <typename Derived>
class actor : public actor_base {
public:
    using base_type = actor<Derived>;

    // Send a message to another actor via its actor_ref.
    template <typename Msg>
    void send_to(actor_ref<Derived>& target, const Msg& msg) {
        target.send(msg);
    }

protected:
    friend class actor_system;
};

} // namespace ynet::actor

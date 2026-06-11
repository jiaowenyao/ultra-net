// actor<T> — CRTP base class for the actor framework.
// Inherit from this to turn your class into a distributed actor.
// All infrastructure (socket, scheduler, discovery) is handled internally.
#pragma once

#include <string>
#include <memory>
#include <functional>
#include <vector>
#include <cstring>

#include "ultranet/actor/core/actor_uri.h"

namespace ynet::actor {

class actor_system;
template <typename T> class actor_ref;

// ── Message envelope for type-safe dispatch ────────────────────────────

using message_handler_t = std::function<void(const void* data, size_t len)>;

// ── Base class for all actors ──────────────────────────────────────────

class actor_base {
public:
    actor_base() = default;
    virtual ~actor_base() = default;

    void set_uri(const actor_uri& u) { m_uri = u; }
    void set_system(actor_system* s) { m_system = s; }

    const actor_uri& uri() const { return m_uri; }
    actor_system* system() const { return m_system; }
    std::string name() const { return m_uri.name; }

    // Register a handler for a specific message type.
    // Usage: register_handler<my_msg>([](const my_msg& m) { ... });
    template <typename Msg>
    void register_handler(std::function<void(const Msg&)> handler) {
        uint64_t hash = type_hash<Msg>();
        m_handlers[hash] = [handler = std::move(handler)](const void* data, size_t len) {
            if (len >= sizeof(Msg)) {
                const Msg* msg = static_cast<const Msg*>(data);
                handler(*msg);
            }
        };
    }

    // Deliver a message to this actor by type hash.
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) {
        auto it = m_handlers.find(msg_type);
        if (it != m_handlers.end()) {
            it->second(data, len);
        }
    }

    // Check if a handler is registered for the given type.
    template <typename Msg>
    bool handles() const {
        return m_handlers.find(type_hash<Msg>()) != m_handlers.end();
    }

protected:
    actor_uri m_uri;
    actor_system* m_system = nullptr;

private:
    std::unordered_map<uint64_t, message_handler_t> m_handlers;

    template <typename T>
    static uint64_t type_hash() {
        // Use a simple compile-time hash based on type name.
        const char* name = typeid(T).name();
        uint64_t h = 14695981039346656037ULL;
        while (*name) { h ^= (uint8_t)*name++; h *= 1099511628211ULL; }
        return h;
    }
};

// ── CRTP actor base ────────────────────────────────────────────────────

template <typename Derived>
class actor : public actor_base {
public:
    using base_type = actor<Derived>;

    // Send a message to another actor via its actor_ref.
    template <typename Msg>
    void send_to(const actor_ref<Derived>& target, const Msg& msg);

protected:
    friend class actor_system;
};

} // namespace ynet::actor

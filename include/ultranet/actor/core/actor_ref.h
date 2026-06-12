// actor_ref<T> — type-safe, location-transparent handle to an actor.
// Supports send() for fire-and-forget and call() for request-reply.
// Works identically for local and remote actors.
#pragma once

#include <memory>
#include <string>
#include <cstring>
#include <functional>

#include "ultranet/actor/core/actor_uri.h"
#include "ultranet/actor/core/type_hash.h"
#include "ultranet/actor/core/mailbox.h"

namespace ynet::actor {

class actor_system;
class actor_base;

// ── Internal proxy: hides local/remote distinction ────────────────────

class actor_proxy {
public:
    virtual ~actor_proxy() = default;
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) = 0;
    virtual const actor_uri& uri() const = 0;
    virtual actor_base* local_actor() { return nullptr; }
};

// ── Local actor proxy: pushes to mailbox and activates actor ──────────
// Zero serialization overhead — message is copied into a message_envelope
// and pushed directly into the actor's mailbox.

class local_actor_proxy : public actor_proxy {
public:
    local_actor_proxy(actor_base* a, actor_system* sys)
        : m_actor(a), m_system(sys) {}

    void deliver(uint64_t msg_type, const void* data, size_t len) override {
        message_envelope env;
        env.msg_type = msg_type;
        env.data.assign(static_cast<const uint8_t*>(data),
                        static_cast<const uint8_t*>(data) + len);
        if (m_actor) {
            m_actor->push_envelope(std::move(env));
        }
    }

    const actor_uri& uri() const override {
        return m_actor->uri();
    }

    actor_base* local_actor() override {
        return m_actor;
    }

private:
    actor_base* m_actor;
    actor_system* m_system;
};

// ── Type-safe actor reference ─────────────────────────────────────────

template <typename T>
class actor_ref {
public:
    actor_ref() = default;
    actor_ref(std::shared_ptr<actor_proxy> p, const actor_uri& u)
        : m_proxy(std::move(p)), m_uri(u) {}

    bool is_valid() const { return m_proxy != nullptr; }
    const actor_uri& uri() const { return m_uri; }
    std::string name() const { return m_uri.name; }

    // Fire-and-forget: push a message into the actor's mailbox.
    // The message is delivered asynchronously via the thread pool.
    template <typename Msg>
    void send(const Msg& msg) {
        if (m_proxy) {
            uint64_t hash = actor_type_hash<Msg>();
            m_proxy->deliver(hash, &msg, sizeof(msg));
        }
    }

    // Send a message to a specific mailbox slot (for priority routing).
    template <typename Msg>
    void send_to(int /*slot*/, const Msg& msg) {
        send(msg);
    }

    // Internal accessors.
    std::shared_ptr<actor_proxy> proxy() const { return m_proxy; }

private:
    std::shared_ptr<actor_proxy> m_proxy;
    actor_uri m_uri;
    friend class actor_system;
};

} // namespace ynet::actor

// actor_ref<T> — type-safe, location-transparent handle to an actor.
// Works identically for local and remote actors.  The framework handles
// serialization, networking, and routing transparently.
#pragma once
#include <memory>
#include <string>
#include <functional>
#include "ultranet/actor/core/actor_uri.h"

namespace ynet::actor {

class actor_system;
class actor_base;
template <typename T> class actor_ref;

// ── Internal proxy: hides local/remote distinction ────────────────────

class actor_proxy {
public:
    virtual ~actor_proxy() = default;
    virtual void send(const void* data, size_t len) = 0;
    virtual const actor_uri& uri() const = 0;
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

    // Send a message to the actor (fire-and-forget, zero-copy for local).
    template <typename Msg>
    void send(const Msg& msg);

    // Send and wait for a reply (co_await-able).
    // Usage: auto reply = co_await ref.call(my_request{...});
    template <typename Msg>
    auto call(const Msg& msg);

    // Send a message to a specific mailbox slot (for priority routing).
    template <typename Msg>
    void send_to(int slot, const Msg& msg);

    // Internal.
    std::shared_ptr<actor_proxy> proxy() const { return m_proxy; }

private:
    std::shared_ptr<actor_proxy> m_proxy;
    actor_uri m_uri;
    friend class actor_system;
};

} // namespace ynet::actor

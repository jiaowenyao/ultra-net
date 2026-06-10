// actor<T> — CRTP base class.  Inherit from this to make your class an actor.
// All infrastructure (socket, scheduler, discovery) is handled by the framework.
#pragma once
#include <string>
#include <memory>
#include <functional>
#include "ultranet/actor/core/actor_uri.h"

namespace ynet::actor {

// Forward declarations.
class actor_system;
template <typename T> class actor_ref;

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

    // Internal: deliver a serialized message to this actor.
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) {}

protected:
    actor_uri m_uri;
    actor_system* m_system = nullptr;
};

// ── CRTP actor base ────────────────────────────────────────────────────

template <typename Derived>
class actor : public actor_base {
public:
    using base_type = actor<Derived>;

    // Get a reference to this actor (for passing to others).
    actor_ref<Derived> ref();

protected:
    // Reply to the sender of the current message.  Only valid within
    // an on_message() handler.  The framework injects sender info.
    template <typename ReplyMsg>
    void reply(ReplyMsg&& msg);

    // Spawn a child actor (supervised).  The child will be restarted
    // according to the parent's supervision strategy.
    template <typename ChildType>
    actor_ref<ChildType> spawn_child(const std::string& name);

    friend class actor_system;
};

} // namespace ynet::actor

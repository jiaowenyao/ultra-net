// actor_ref<T> — a handle to a local or remote actor.
#pragma once

#include <cstdint>
#include <type_traits>

#include "ultranet/actor/core/actor.h"

namespace ynet::actor {

template <typename T>
class actor_ref {
public:
    actor_ref() = default;
    explicit actor_ref(uint64_t id, actor<T>* a) : m_id(id), m_actor(a) {}

    bool is_valid() const { return m_actor != nullptr; }
    uint64_t id() const { return m_id; }

    template <auto Method, typename... Args>
        requires std::is_member_function_pointer_v<decltype(Method)>
    void send(Args... args) {
        auto* msg = make_message<T, Method>(m_actor->get(),
                                             std::forward<Args>(args)...);
        m_actor->push_message(msg);
    }

private:
    uint64_t m_id{0};
    actor<T>* m_actor{nullptr};
};

} // namespace ynet::actor

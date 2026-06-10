// Actor message — type-erased method invocation.
#pragma once

#include <memory>
#include <functional>

namespace ynet::actor {

struct message {
    std::function<void()> m_fn;
    explicit message(std::function<void()> fn) : m_fn(std::move(fn)) {}
    void run() { if (m_fn) m_fn(); }
};

// Construct a message that calls t::method with the given args.
template <typename T, auto Method, typename... Args>
    requires std::is_member_function_pointer_v<decltype(Method)>
message* make_message(T* instance, Args&&... args) {
    return new message([instance, ... args = std::forward<Args>(args)]() mutable {
        (instance->*Method)(std::forward<Args>(args)...);
    });
}

} // namespace ynet::actor

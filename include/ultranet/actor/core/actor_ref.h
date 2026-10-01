// actor_ref<T> — 类型安全、位置透明的 actor 句柄。
// 支持 send()（fire-and-forget）和 try_send()（非阻塞）。
// 对本地和远端 actor 提供统一接口，调用方无需关心 actor 的物理位置。
#pragma once

#include <memory>
#include <string>
#include <cstring>
#include <functional>
#include <type_traits>
#include <optional>
#include <chrono>
#include <vector>
#include <thread>

#include "ultranet/actor/core/actor_uri.h"
#include "ultranet/actor/core/type_hash.h"
#include "ultranet/actor/core/mailbox.h"
#include "ultranet/actor/core/base_actor.h"

namespace ynet::actor {

class actor_system;
class actor_base;
class actor_proxy;

// 远端 send 的重试在 actor_system.h 里定义，那里 actor_system 才是完整类型。
bool remote_accept(actor_proxy* proxy, actor_system* sys,
                   uint64_t hash, const void* data, size_t len,
                   uint8_t flags, bool wait_for_room);

// ── 内部代理：隐藏本地/远端差异 ──────────────────────────────────────────

class actor_proxy {
public:
    virtual ~actor_proxy() = default;
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) = 0;
    // 远端收下路径。默认转给 deliver 并当作成功；remote_proxy 覆盖它。
    virtual bool try_deliver(uint64_t msg_type, const void* data, size_t len,
                             uint64_t msg_id, uint8_t flags) {
        (void)msg_id;
        (void)flags;
        deliver(msg_type, data, len);
        return true;
    }
    virtual const actor_uri& uri() const = 0;
    virtual actor_base* local_actor() { return nullptr; }
};

// ── 本地 actor 代理 ───────────────────────────────────────────────────────
// 零序列化开销：消息直接拷贝到 message_envelope 并推入 actor mailbox。

class local_actor_proxy : public actor_proxy {
public:
    local_actor_proxy(actor_base* a, actor_system* sys)
        : m_actor(a), m_system(sys) {}

    // 将消息拷贝到信封并推入目标 actor 的 mailbox
    void deliver(uint64_t msg_type, const void* data, size_t len) override {
        message_envelope env;
        env.msg_type = msg_type;
        env.assign_bytes(static_cast<const uint8_t*>(data), len);
        if (m_actor) {
            m_actor->push_envelope(std::move(env));
        }
    }

    const actor_uri& uri() const override {
        return m_actor->uri();
    }

    // 获取本地 actor 裸指针（用于测试和内部访问）
    actor_base* local_actor() override {
        return m_actor;
    }

private:
    actor_base* m_actor;
    actor_system* m_system;
};

// ── 类型安全的 actor 引用 ─────────────────────────────────────────────────

template <typename T>
class actor_ref {
public:
    actor_ref() = default;
    actor_ref(std::shared_ptr<actor_proxy> p, const actor_uri& u,
              actor_system* sys = nullptr)
        : m_proxy(std::move(p)), m_uri(u), m_system(sys) {}

    bool is_valid() const { return m_proxy != nullptr; }
    const actor_uri& uri() const { return m_uri; }
    std::string name() const { return m_uri.name; }

    // ── 消息发送 ─────────────────────────────────────────────────────────

    // 收下后返回 true。代理为空立刻 false。本地进入邮箱为 true。
    // 远端缓冲满时，wait 路径在 remote_accept_timeout_ms 内重试。
    template <typename Msg>
    bool send(const Msg& msg) {
        static_assert(serializable_msg<Msg> || std::is_trivially_copyable_v<Msg>,
            "Message must be trivially copyable or provide serialize()/deserialize()");
        if (!m_proxy) {
            return false;
        }
        uint64_t hash = actor_type_hash<Msg>();
        if (auto* local = m_proxy->local_actor()) {
            return local->push_envelope(message_envelope::make(msg));
        }
        if constexpr (serializable_msg<Msg>) {
            auto data = msg.serialize();
            return remote_accept(m_proxy.get(), m_system, hash,
                                 data.data(), data.size(), 0, true);
        } else {
            return remote_accept(m_proxy.get(), m_system, hash,
                                 &msg, sizeof(msg), 0, true);
        }
    }

    template <typename U, typename Msg>
    bool send_to(actor_ref<U>& target, const Msg& msg) {
        return target.send(msg);
    }

    template <typename Msg>
    bool try_send(const Msg& msg) {
        static_assert(serializable_msg<Msg> || std::is_trivially_copyable_v<Msg>,
            "Message must be trivially copyable or provide serialize()/deserialize()");
        if (!m_proxy) {
            return false;
        }
        auto* local = m_proxy->local_actor();
        if (!local) {
            uint64_t hash = actor_type_hash<Msg>();
            if constexpr (serializable_msg<Msg>) {
                auto data = msg.serialize();
                return remote_accept(m_proxy.get(), m_system, hash,
                                     data.data(), data.size(), 0, false);
            } else {
                return remote_accept(m_proxy.get(), m_system, hash,
                                     &msg, sizeof(msg), 0, false);
            }
        }
        return local->try_push_envelope(message_envelope::make(msg));
    }

    // 等到 reply，或超时返回 nullopt。定义在 actor_system.h 末尾。
    template <typename Reply, typename Msg>
    std::optional<Reply> ask(const Msg& msg, std::chrono::milliseconds timeout);

    // ── Actor 访问器 ─────────────────────────────────────────────────

    // 获取底层 actor 裸指针（需 static_cast 到具体类型）
    T* get() const {
        if (!m_proxy) {
            return nullptr;
        }
        actor_base* base = m_proxy->local_actor();
        if (!base) {
            return nullptr;
        }
        // 侵入式：动态类型就是 T。非侵入式：适配器不是 T，走 native_object。
        if constexpr (std::is_base_of_v<actor_base, T>) {
            return static_cast<T*>(base);
        } else {
            return static_cast<T*>(base->native_object());
        }
    }

    T* operator->() const { return get(); }

    std::shared_ptr<actor_proxy> proxy() const { return m_proxy; }

private:
    std::shared_ptr<actor_proxy> m_proxy;
    actor_uri m_uri;
    actor_system* m_system = nullptr;
    friend class actor_system;
};

} // namespace ynet::actor

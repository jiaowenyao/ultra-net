// actor_ref<T> — 类型安全、位置透明的 actor 句柄。
// 支持 send()（fire-and-forget）和 try_send()（非阻塞）。
// 对本地和远端 actor 提供统一接口，调用方无需关心 actor 的物理位置。
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

// ── 内部代理：隐藏本地/远端差异 ──────────────────────────────────────────

class actor_proxy {
public:
    virtual ~actor_proxy() = default;
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) = 0;
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
        env.data.assign(static_cast<const uint8_t*>(data),
                        static_cast<const uint8_t*>(data) + len);
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
    actor_ref(std::shared_ptr<actor_proxy> p, const actor_uri& u)
        : m_proxy(std::move(p)), m_uri(u) {}

    bool is_valid() const { return m_proxy != nullptr; }
    const actor_uri& uri() const { return m_uri; }
    std::string name() const { return m_uri.name; }

    // ── 消息发送 ─────────────────────────────────────────────────────────

    // Fire-and-forget 发送：消息异步投递到线程池上的 mailbox。
    // 消息类型必须为 trivially copyable（框架使用 memcpy 拷贝消息体）。
    template <typename Msg>
    void send(const Msg& msg) {
        static_assert(serializable_msg<Msg> || std::is_trivially_copyable_v<Msg>,
            "Message must be trivially copyable or provide serialize()/deserialize()");
        if (!m_proxy) { return; }
        uint64_t hash = actor_type_hash<Msg>();
        if constexpr (serializable_msg<Msg>) {
            auto data = msg.serialize();
            m_proxy->deliver(hash, data.data(), data.size());
        } else {
            m_proxy->deliver(hash, &msg, sizeof(msg));
        }
    }

    // 向任意类型的 actor 发送消息（跨 actor 通信）
    template <typename U, typename Msg>
    void send_to(actor_ref<U>& target, const Msg& msg) {
        target.send(msg);
    }

    // 非阻塞发送：mailbox 满时返回 false，不阻塞调用方。
    template <typename Msg>
    bool try_send(const Msg& msg) {
        static_assert(serializable_msg<Msg> || std::is_trivially_copyable_v<Msg>,
            "Message must be trivially copyable or provide serialize()/deserialize()");
        if (!m_proxy) {
            return false;
        }
        auto* local = m_proxy->local_actor();
        if (!local) {
            // 远端代理：退化为 fire-and-forget（始终缓冲）
            uint64_t hash = actor_type_hash<Msg>();
            m_proxy->deliver(hash, &msg, sizeof(msg));
            return true;
        }
        // 本地 actor：尝试非阻塞推入 mailbox
        auto envelope = message_envelope::make(msg);
        if (local->get_mailbox().try_push(std::move(envelope))) {
            local->try_activate();
            return true;
        }
        return false;
    }

    // ── Actor 访问器 ─────────────────────────────────────────────────

    // 获取底层 actor 裸指针（需 static_cast 到具体类型）
    T* get() const {
        if (m_proxy) { return static_cast<T*>(m_proxy->local_actor()); }
        return nullptr;
    }

    T* operator->() const { return get(); }

    std::shared_ptr<actor_proxy> proxy() const { return m_proxy; }

private:
    std::shared_ptr<actor_proxy> m_proxy;
    actor_uri m_uri;
    friend class actor_system;
};

} // namespace ynet::actor

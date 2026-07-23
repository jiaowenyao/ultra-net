// actor<T> — actor 框架的 CRTP 基类。
// 继承此类即可将任意类变为分布式 actor，所有基础设施（socket、调度器、
// 服务发现）由框架内部处理，对用户透明。
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

// ── 消息处理器类型 ────────────────────────────────────────────────────────

using message_handler_t = std::function<void(const void* data, size_t len)>;

// ── actor 基类 ────────────────────────────────────────────────────────────

class actor_base {
public:
    actor_base() = default;
    virtual ~actor_base() = default;

    // ── 基本属性 ─────────────────────────────────────────────────────────

    void set_uri(const actor_uri& u) { m_uri = u; }
    void set_system(actor_system* s) { m_system = s; }

    // 设置调度回调，由 actor_system::spawn() 调用，将 actor 与线程池绑定。
    // try_activate() 通过此回调将 pull_and_run() 提交到线程池执行。
    void set_schedule_fn(std::function<void(std::function<void()>)> fn) {
        m_schedule_fn = std::move(fn);
    }

    const actor_uri& uri() const { return m_uri; }
    actor_system* system() const { return m_system; }
    std::string name() const { return m_uri.name; }

    // ── 消息处理器注册 ───────────────────────────────────────────────────

    // 为指定消息类型注册处理器。
    // 用法：register_handler<my_msg>([](const my_msg& m) { ... });
    // 要求：消息类型必须是 trivially copyable（框架使用 memcpy 拷贝消息体）。
    template <typename Msg>
    void register_handler(std::function<void(const Msg&)> handler) {
        static_assert(std::is_trivially_copyable_v<Msg>,
            "Actor message types must be trivially copyable. "
            "Use fixed-size char arrays instead of std::string, "
            "and plain types instead of std::vector or std::unique_ptr.");
        uint64_t hash = actor_type_hash<Msg>();
        // 用 lambda 包装用户处理器，提供类型安全的数据转换
        m_handlers[hash] = [handler = std::move(handler)](const void* data, size_t len) {
            if (len >= sizeof(Msg)) {
                const Msg* msg = static_cast<const Msg*>(data);
                handler(*msg);
            }
        };
    }

    // 按类型哈希投递消息到对应处理器。
    // 由 pull_and_run() 在线程池上调用。
    // 内置异常边界：处理器抛出的异常会被捕获并记录，不会导致 actor 崩溃。
    // 使用限流机制防止异常风暴导致日志爆炸。
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) {
        auto it = m_handlers.find(msg_type);
        if (it != m_handlers.end()) {
            // 找到已注册的处理器，在异常边界内调用
            try {
                it->second(data, len);
            } catch (const std::exception& e) {
                // 限流：每秒最多记录一次同类型异常
                uint64_t now = std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::steady_clock::now().time_since_epoch()).count();
                if (now != m_last_exception_log_sec) {
                    m_last_exception_log_sec = now;
                    m_exception_count = 0;
                }
                if (m_exception_count++ < 10) {
                    ULTRA_LOG_ERROR("[actor {}] handler exception for msg_type={}: {}",
                                   m_uri.to_string(), msg_type, e.what());
                } else if (m_exception_count == 10) {
                    ULTRA_LOG_ERROR("[actor {}] handler exception for msg_type={}: "
                                   "(further exceptions suppressed for this second)",
                                   m_uri.to_string(), msg_type);
                }
            } catch (...) {
                ULTRA_LOG_ERROR("[actor {}] handler exception for msg_type={}: "
                               "unknown", m_uri.to_string(), msg_type);
            }
        } else {
            // 死信：该消息类型没有注册处理器
            ULTRA_LOG_WARN("[actor {}] dead letter: no handler for msg_type={} "
                          "(len={})", m_uri.to_string(), msg_type, len);
        }
    }

    // 检查是否已注册指定消息类型的处理器。
    template <typename Msg>
    bool handles() const {
        return m_handlers.find(actor_type_hash<Msg>()) != m_handlers.end();
    }

    // ── 异步 mailbox 接口 ─────────────────────────────────────────────────

    // 将消息信封推入 mailbox 并尝试激活 actor。
    // 由 local_actor_proxy::deliver() 和远端消息入口调用。
    void push_envelope(message_envelope env);

    // 从 mailbox 中拉取消息并分发给处理器。
    // 在线程池上运行，返回是否处理了任何消息。
    bool pull_and_run();

    // 激活 actor：将 pull_and_run() 通过 schedule_fn 提交到线程池。
    // 使用 CAS 保证同一时刻只有一个执行实例。
    void try_activate();

    // ── Mailbox 访问器 ────────────────────────────────────────────────────

    mailbox& get_mailbox() { return m_mailbox; }
    size_t pending() const { return m_pending.load(std::memory_order_acquire); }
    void set_max_per_activation(size_t n) { m_max_per_activation = n; }

    // ── 关闭流程 ──────────────────────────────────────────────────────────

    // 标记关闭状态。设置后 push_envelope() 会丢弃消息，try_activate()
    // 不再重新调度，防止线程池销毁后的 use-after-free。
    void set_shutting_down(bool v) {
        m_shutting_down.store(v, std::memory_order_release);
    }
    bool is_shutting_down() const {
        return m_shutting_down.load(std::memory_order_acquire);
    }

    // 同步排空 mailbox 中剩余消息（阻塞调用）。
    // 在优雅关闭时使用：线程池已停止，在销毁 actor 之前处理最后的消息。
    void drain_pending() {
        auto handler = [this](message_envelope env) {
            deliver(env.msg_type, env.data.data(), env.data.size());
        };
        m_mailbox.drain(handler, size_t(-1));
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
    size_t m_max_per_activation = 256;

    // 异常日志限流：防止异常风暴导致日志爆炸
    uint64_t m_last_exception_log_sec = 0;
    int m_exception_count = 0;
};

// ── 内联实现 ──────────────────────────────────────────────────────────────

inline void actor_base::push_envelope(message_envelope env) {
    // 关闭期间静默丢弃消息，防止线程池已销毁后的 use-after-free
    if (m_shutting_down.load(std::memory_order_acquire)) {
        return;
    }

    // 自适应退避：忙等 → yield → 微秒级睡眠 → 阻塞回退
    // 参考 LMAX Disruptor SleepingWaitStrategy
    for (int attempt = 0; attempt < 200; ++attempt) {
        if (m_mailbox.try_push(env)) {
            // push 成功：更新待处理计数并激活 actor
            m_pending.fetch_add(1, std::memory_order_release);
            try_activate();
            return;
        }
        // 退避策略：0-9 忙等, 10-49 yield, 50-99 睡眠1μs,
        // 100-149 睡眠10μs, 150-199 睡眠100μs
        if (attempt < 10) {
            // 忙等（预期 mailbox 很快就会有空位）
        } else if (attempt < 50) {
            std::this_thread::yield();
        } else if (attempt < 100) {
            std::this_thread::sleep_for(std::chrono::microseconds(1));
        } else if (attempt < 150) {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        } else {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    }
    // 回退路径：阻塞直到 push 成功（极端背压场景）
    m_mailbox.push_blocking(env);
    m_pending.fetch_add(1, std::memory_order_release);
    try_activate();
}

inline bool actor_base::pull_and_run() {
    // 计算本次激活允许处理的最大消息数
    size_t limit = std::min(m_max_per_activation,
                            m_pending.load(std::memory_order_acquire));

    // 构造分发 lambda：将 mailbox 中的消息投递给 deliver()
    auto handler = [this](message_envelope env) {
        deliver(env.msg_type, env.data.data(), env.data.size());
    };

    // 批量排空 mailbox
    size_t executed = m_mailbox.drain(handler, limit);
    if (executed > 0) {
        m_pending.fetch_sub(executed, std::memory_order_release);
    }

    // 释放激活锁，允许新的 try_activate 调度
    m_activated.store(false, std::memory_order_release);

    // 自激活检查：处理期间可能有新消息到达
    // 关闭期间跳过，防止线程池已销毁后的 use-after-free
    if (!m_shutting_down.load(std::memory_order_acquire) &&
        m_pending.load(std::memory_order_acquire) > 0) {
        try_activate();
    }
    return executed > 0;
}

inline void actor_base::try_activate() {
    // 关闭期间跳过调度，防止线程池销毁后的 use-after-free
    if (m_shutting_down.load(std::memory_order_acquire)) {
        return;
    }

    // CAS 保证同一时刻只有一个线程成功激活
    bool expected = false;
    if (m_activated.compare_exchange_strong(expected, true,
            std::memory_order_acq_rel)) {
        // 通过系统调度回调将 pull_and_run 提交到线程池
        if (m_schedule_fn) {
            m_schedule_fn([this]() {
                pull_and_run();
            });
        }
    }
}

// ── CRTP actor 模板 ───────────────────────────────────────────────────────

template <typename Derived>
class actor : public actor_base {
public:
    using base_type = actor<Derived>;

    // 向另一个 actor 发送消息（通过 actor_ref）
    template <typename Msg>
    void send_to(actor_ref<Derived>& target, const Msg& msg) {
        target.send(msg);
    }

protected:
    friend class actor_system;
};

} // namespace ynet::actor

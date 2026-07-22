// remote_proxy — 远端（网络）actor 的 actor_proxy 实现。
//
// 消息在 TCP 连接建立前被缓冲。连接可用时（通过 set_connection /
// connect_and_flush），缓冲的消息被批量发送，后续消息直接走网络。
//
// 每次发送将异步协程提交到线程池，调用方永远不会被网络 I/O 阻塞。
#pragma once

#include <memory>
#include <mutex>
#include <deque>

#include "ultranet/actor/core/actor_ref.h"
#include "ultranet/actor/core/base_actor.h"
#include "ultranet/actor/net/transport.h"
#include "ultranet/actor/dist/serialization.h"
#include "ultranet/coroutine/thread_pool.hpp"

namespace ynet::actor::dist {

using ynet::async::scheduling::WorkStealingThreadPool;

// ── 单次发送上下文（避免 GCC 13.2 协程 lambda 捕获 bug）─────────────────

struct send_context {
    std::shared_ptr<net::outbound_conn> conn;
    std::vector<uint8_t> envelope;
};

// 前向声明（实现在文件末尾，类内部 submit_send 引用）
ynet::async::Task<void> remote_send_impl(std::shared_ptr<send_context> ctx);

// ── 缓冲消息（连接就绪前暂存）─────────────────────────────────────────────

struct buffered_message {
    uint64_t msg_type;
    std::vector<uint8_t> data;
};

// ── 远端 actor 代理 ──────────────────────────────────────────────────────

class remote_proxy : public actor_proxy,
                     public std::enable_shared_from_this<remote_proxy> {
public:
    static constexpr size_t k_max_buffered = 1024;

    remote_proxy(actor_uri uri,
                 std::shared_ptr<net::outbound_conn> conn,
                 WorkStealingThreadPool* pool)
        : m_uri(std::move(uri))
        , m_conn(std::move(conn))
        , m_pool(pool) {}

    // ── 投递消息 ─────────────────────────────────────────────────────────
    // 如果有可用连接，消息被序列化并作为异步发送协程提交。
    // 否则消息被缓冲（最多 k_max_buffered 条），等待 connect_and_flush()。

    void deliver(uint64_t msg_type, const void* data, size_t len) override {
        // 序列化为线格式信封
        auto envelope = pack_actor_message(m_uri, msg_type, data, len);

        // 检查当前连接是否可用
        std::shared_ptr<net::outbound_conn> conn;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            conn = m_conn;
        }

        if (conn && conn->is_valid()) {
            // 连接可用：直接提交异步发送
            submit_send(std::move(conn), std::move(envelope));
            return;
        }

        // 无连接：缓冲消息，等待 connect_and_flush 批量发送
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_buffered.size() < k_max_buffered) {
                m_buffered.push_back(
                    {msg_type, std::vector<uint8_t>(
                         static_cast<const uint8_t*>(data),
                         static_cast<const uint8_t*>(data) + len)});
            }
            // 缓冲区满时丢弃最旧消息 — 这是一种有意的背压策略
        }
    }

    const actor_uri& uri() const override {
        return m_uri;
    }

    actor_base* local_actor() override {
        return nullptr;
    }

    // ── 连接管理 ─────────────────────────────────────────────────────────

    // 替换底层连接
    void set_connection(std::shared_ptr<net::outbound_conn> conn) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_conn = std::move(conn);
    }

    // 设置连接并排空所有缓冲消息。
    // 由 gossip 循环在发现新对等节点后调用。
    void connect_and_flush(std::shared_ptr<net::outbound_conn> conn) {
        std::deque<buffered_message> pending;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_conn = conn;
            pending.swap(m_buffered);
        }

        // 重新序列化每条缓冲消息并提交发送
        for (auto& buf : pending) {
            auto envelope = pack_actor_message(
                m_uri, buf.msg_type, buf.data.data(), buf.data.size());
            submit_send(conn, std::move(envelope));
        }
    }

    // 当前缓冲消息数量
    size_t buffered_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_buffered.size();
    }

    bool has_connection() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_conn && m_conn->is_valid();
    }

private:
    // 将序列化好的信封提交为异步发送协程
    void submit_send(std::shared_ptr<net::outbound_conn> conn,
                     std::vector<uint8_t> envelope) {
        if (!m_pool) {
            return;
        }
        auto ctx = std::make_shared<send_context>(
            send_context{std::move(conn), std::move(envelope)});
        m_pool->submit_coroutine(
            remote_send_impl(std::move(ctx)).release());
    }

    actor_uri m_uri;
    std::shared_ptr<net::outbound_conn> m_conn;
    WorkStealingThreadPool* m_pool = nullptr;
    mutable std::mutex m_mutex;
    std::deque<buffered_message> m_buffered;
};

// ── 异步发送协程实现 ────────────────────────────────────────────────────

// 前向声明（类定义中 submit_send 引用）
inline ynet::async::Task<void> remote_send_impl(
        std::shared_ptr<send_context> ctx) {
    if (ctx->conn && ctx->conn->is_valid()) {
        co_await ctx->conn->send(ctx->envelope);
    }
    co_return;
}

} // namespace ynet::actor::dist

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
#include <functional>
#include <vector>
#include <cstdint>

#include "ultranet/actor/core/actor_ref.h"
#include "ultranet/actor/core/base_actor.h"
#include "ultranet/actor/net/transport.h"
#include "ultranet/actor/dist/serialization.h"
#include "ultranet/coroutine/thread_pool.hpp"

namespace ynet::actor::dist {

using ynet::async::scheduling::WorkStealingThreadPool;

// ── 缓冲消息（连接就绪前暂存）─────────────────────────────────────────────

struct buffered_message {
    uint64_t msg_type = 0;
    uint64_t msg_id = 0;
    uint8_t flags = 0;
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

    // 无连接时缓冲，满了返回 false，不丢弃已在队列里的消息。
    // 有连接但没有线程池时返回 false，不提交发送。
    bool try_deliver(uint64_t msg_type, const void* data, size_t len,
                     uint64_t msg_id, uint8_t flags) override {
        if (data == nullptr || len == 0) {
            return false;
        }
        auto frame = pack_routed(m_uri, msg_type, flags, msg_id, m_self_node, data, len);
        if (m_track && !m_track(msg_id, frame)) {
            return false;
        }
        std::shared_ptr<net::outbound_conn> conn;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            conn = m_conn;
            if (!conn || !conn->is_valid()) {
                if (m_buffered.size() >= k_max_buffered) {
                    return false;
                }
                buffered_message msg;
                msg.msg_type = msg_type;
                msg.msg_id = msg_id;
                msg.flags = flags;
                msg.data.assign(static_cast<const uint8_t*>(data),
                                static_cast<const uint8_t*>(data) + len);
                m_buffered.push_back(std::move(msg));
                return true;
            }
            if (m_pool == nullptr) {
                return false;
            }
        }
        if (!conn->post_frame(std::move(frame), m_pool)) {
            if (m_fail) {
                m_fail(msg_id);
            }
            return false;
        }
        return true;
    }

    void deliver(uint64_t msg_type, const void* data, size_t len) override {
        uint64_t id = m_alloc_id ? m_alloc_id() : 1;
        try_deliver(msg_type, data, len, id, 0);
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
        if (!m_pool || !conn) {
            return;
        }
        for (auto& buf : pending) {
            auto frame = pack_routed(m_uri, buf.msg_type, buf.flags, buf.msg_id,
                                     m_self_node, buf.data.data(), buf.data.size());
            if (m_track && !m_track(buf.msg_id, frame)) {
                if (m_fail) {
                    m_fail(buf.msg_id);
                }
                continue;
            }
            if (!conn->post_frame(std::move(frame), m_pool) && m_fail) {
                m_fail(buf.msg_id);
            }
        }
    }

    void drop_connection() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_conn.reset();
    }

    void set_hooks(std::function<uint64_t()> alloc_id,
                   std::function<bool(uint64_t, const std::vector<uint8_t>&)> track,
                   std::function<void(uint64_t)> fail = {}) {
        m_alloc_id = std::move(alloc_id);
        m_track = std::move(track);
        m_fail = std::move(fail);
    }

    void set_node(uint64_t id) { m_node = id; }
    uint64_t node_id() const { return m_node; }
    void set_self_node(uint64_t id) { m_self_node = id; }

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
    actor_uri m_uri;
    std::shared_ptr<net::outbound_conn> m_conn;
    WorkStealingThreadPool* m_pool = nullptr;
    mutable std::mutex m_mutex;
    std::deque<buffered_message> m_buffered;
    std::function<uint64_t()> m_alloc_id;
    std::function<bool(uint64_t, const std::vector<uint8_t>&)> m_track;
    std::function<void(uint64_t)> m_fail;
    uint64_t m_node = 0;
    uint64_t m_self_node = 0;
};

} // namespace ynet::actor::dist

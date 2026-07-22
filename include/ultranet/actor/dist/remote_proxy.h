// remote_proxy — actor_proxy implementation for remote (network) actors.
//
// Messages are buffered until a TCP connection is established.  Once the
// connection is available (set via set_connection / connect_and_flush),
// buffered messages are flushed and subsequent sends go directly over
// the wire.
//
// Each send submits an async coroutine to the thread pool so the caller
// is never blocked on network I/O.
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

// ── Per-message send context (avoids GCC 13.2 coroutine lambda capture bug) ──

struct send_context {
    std::shared_ptr<net::outbound_conn> conn;
    std::vector<uint8_t> envelope;
};

// Forward declaration — defined after the class.
ynet::async::Task<void> remote_send_impl(std::shared_ptr<send_context> ctx);

// ── Buffered message (held until connection is ready) ──────────────────────

struct buffered_message {
    uint64_t msg_type;
    std::vector<uint8_t> data;
};

// ── Remote actor proxy ──────────────────────────────────────────────────────

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

    // ── Deliver a message ──────────────────────────────────────────────
    // If a connection is available, the message is serialized and submitted
    // as an async send coroutine.  Otherwise it is buffered (up to
    // k_max_buffered) and will be flushed when connect_and_flush() is
    // called.

    void deliver(uint64_t msg_type, const void* data, size_t len) override {
        // Serialize the envelope once.
        auto envelope = pack_actor_message(m_uri, msg_type, data, len);

        std::shared_ptr<net::outbound_conn> conn;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            conn = m_conn;
        }

        if (conn && conn->is_valid()) {
            submit_send(std::move(conn), std::move(envelope));
            return;
        }

        // No connection yet — buffer the message.
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_buffered.size() < k_max_buffered) {
                m_buffered.push_back(
                    {msg_type, std::vector<uint8_t>(
                         static_cast<const uint8_t*>(data),
                         static_cast<const uint8_t*>(data) + len)});
            }
            // If the buffer is full, the oldest messages are dropped.
            // This is intentional backpressure — remote actors must
            // establish connections promptly.
        }
    }

    const actor_uri& uri() const override {
        return m_uri;
    }

    actor_base* local_actor() override {
        return nullptr;
    }

    // ── Connection management ──────────────────────────────────────────

    void set_connection(std::shared_ptr<net::outbound_conn> conn) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_conn = std::move(conn);
    }

    // Set the connection and flush all buffered messages.
    void connect_and_flush(std::shared_ptr<net::outbound_conn> conn) {
        std::deque<buffered_message> pending;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_conn = conn;
            pending.swap(m_buffered);
        }

        // Re-serialize and send each buffered message.
        for (auto& buf : pending) {
            auto envelope = pack_actor_message(
                m_uri, buf.msg_type, buf.data.data(), buf.data.size());
            submit_send(conn, std::move(envelope));
        }
    }

    // Number of messages waiting for a connection.
    size_t buffered_count() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_buffered.size();
    }

    bool has_connection() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_conn && m_conn->is_valid();
    }

private:
    void submit_send(std::shared_ptr<net::outbound_conn> conn,
                     std::vector<uint8_t> envelope) {
        if (!m_pool) return;
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

// ── Async send coroutine implementation ────────────────────────────────────

inline ynet::async::Task<void> remote_send_impl(
        std::shared_ptr<send_context> ctx) {
    if (ctx->conn && ctx->conn->is_valid()) {
        co_await ctx->conn->send(ctx->envelope);
    }
    co_return;
}

} // namespace ynet::actor::dist

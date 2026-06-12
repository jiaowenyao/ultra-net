// remote_proxy — actor_proxy implementation for remote (network) actors.
// Serializes messages via pack_actor_message and sends over outbound_conn.
// Submits the send as an async coroutine to avoid blocking the caller.
#pragma once

#include <memory>
#include <mutex>

#include "ultranet/actor/core/actor_ref.h"
#include "ultranet/actor/core/base_actor.h"
#include "ultranet/actor/net/transport.h"
#include "ultranet/actor/dist/serialization.h"
#include "ultranet/coroutine/thread_pool.hpp"

namespace ynet::actor::dist {

using ynet::async::scheduling::WorkStealingThreadPool;

// ── Forward declarations ──────────────────────────────────────────────

struct send_context;
class remote_proxy;

ynet::async::Task<void> remote_send_impl(std::shared_ptr<send_context> ctx);

// ── Send context ──────────────────────────────────────────────────────
// Named struct (not lambda) — avoids GCC 13.2 coroutine lambda capture bug.

struct send_context {
    std::shared_ptr<remote_proxy> self;
    std::shared_ptr<net::outbound_conn> conn;
    std::vector<uint8_t> envelope;
};

// ── Remote actor proxy ──────────────────────────────────────────────────

class remote_proxy : public actor_proxy,
                     public std::enable_shared_from_this<remote_proxy> {
public:
    remote_proxy(actor_uri uri,
                 std::shared_ptr<net::outbound_conn> conn,
                 WorkStealingThreadPool* pool)
        : m_uri(std::move(uri))
        , m_conn(std::move(conn))
        , m_pool(pool) {}

    void deliver(uint64_t msg_type, const void* data, size_t len) override {
        auto envelope = pack_actor_message(m_uri, msg_type, data, len);

        std::shared_ptr<net::outbound_conn> conn;
        {
            std::lock_guard<std::mutex> lock(m_conn_mutex);
            conn = m_conn;
        }

        if (!conn || !conn->is_valid()) {
            return;
        }

        auto ctx = std::make_shared<send_context>(
            send_context{shared_from_this(), conn, std::move(envelope)});

        if (m_pool) {
            m_pool->submit_coroutine(
                remote_send_impl(std::move(ctx)).release());
        }
    }

    const actor_uri& uri() const override {
        return m_uri;
    }

    actor_base* local_actor() override {
        return nullptr;
    }

    void set_connection(std::shared_ptr<net::outbound_conn> conn) {
        std::lock_guard<std::mutex> lock(m_conn_mutex);
        m_conn = std::move(conn);
    }

private:
    actor_uri m_uri;
    std::shared_ptr<net::outbound_conn> m_conn;
    WorkStealingThreadPool* m_pool = nullptr;
    std::mutex m_conn_mutex;
};

// ── Async send coroutine implementation ────────────────────────────────

inline ynet::async::Task<void> remote_send_impl(
        std::shared_ptr<send_context> ctx) {
    if (ctx->conn && ctx->conn->is_valid()) {
        co_await ctx->conn->send(ctx->envelope);
    }
    co_return;
}

} // namespace ynet::actor::dist

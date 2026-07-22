// src/actor_system.cc — actor_system implementation.
// Moved out of the header to reduce inline bloat and improve compile times.

#include "ultranet/actor/system/actor_system.h"
#include "ultranet/log/logger.hpp"

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <random>

namespace ynet::actor {

using ynet::async::scheduling::WorkStealingThreadPool;
using ynet::async::lifecycle::ShutdownCoordinator;
using namespace std::chrono;

// ── Socket binding ─────────────────────────────────────────────────────────

std::pair<int, uint16_t> actor_system::bind_socket(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return {-1, 0};
    }

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        ::close(fd);
        return {-1, 0};
    }

    if (::listen(fd, 64) < 0) {
        ::close(fd);
        return {-1, 0};
    }

    sockaddr_in bound{};
    socklen_t bound_len = sizeof(bound);
    uint16_t actual = port;
    if (getsockname(fd, reinterpret_cast<sockaddr*>(&bound),
                    &bound_len) == 0) {
        actual = ntohs(bound.sin_port);
    }

    return {fd, actual};
}

// ── Node ID generation ─────────────────────────────────────────────────────

net::node_id_t actor_system::generate_node_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    uint64_t r = gen();
    const auto& name = m_cfg.node_name;
    uint64_t h = 14695981039346656037ULL;
    for (char c : name) {
        h ^= static_cast<uint8_t>(c);
        h *= 1099511628211ULL;
    }
    return h ^ r;
}

// ── Constructor / destructor ───────────────────────────────────────────────

actor_system::actor_system(const system_config& cfg) : m_cfg(cfg) {
    if (cfg.num_threads == 0) {
        m_cfg.num_threads = 4;
    }
    m_pool = std::make_unique<WorkStealingThreadPool>(m_cfg.num_threads);

    m_shutdown = std::make_unique<ShutdownCoordinator>();

    auto [listen_fd, actual_port] = bind_socket(m_cfg.listen_port);
    m_actual_port.store(actual_port, std::memory_order_release);

    if (listen_fd < 0) {
        ULTRA_LOG_WARN("[actor_system] failed to bind to port {} — "
                       "networking disabled; only local actors available",
                       m_cfg.listen_port);
        return;
    }

    auto self_id = generate_node_id();
    std::string self_addr = "127.0.0.1:" + std::to_string(actual_port);
    m_cluster = std::make_unique<net::cluster>(self_id, self_addr);

    for (const auto& seed : m_cfg.seed_nodes) {
        m_cluster->add_seed(seed);
    }

    auto msg_handler = [this](
        std::vector<uint8_t> payload) -> ynet::async::Task<void> {
        co_await handle_inbound_message(std::move(payload));
    };
    m_transport = std::make_unique<net::tcp_transport>(
        listen_fd, actual_port, std::move(msg_handler));

    auto serve_task = m_transport->serve(*m_shutdown);
    m_pool->submit_coroutine(serve_task.release());

    auto gossip_task = gossip_loop();
    m_pool->submit_coroutine(gossip_task.release());

    ULTRA_LOG_INFO("[actor_system] node={} id={} port={} threads={}",
                   m_cfg.node_name, self_id, actual_port, m_cfg.num_threads);
}

actor_system::~actor_system() {
    // Signal all actors to stop accepting new messages.
    for (auto& a : m_owned_actors) {
        a->set_shutting_down(true);
    }

    // Signal transport and gossip loops to stop.
    if (m_shutdown) {
        m_shutdown->shutdown();
    }

    // Wait for the thread pool to drain all in-flight tasks.
    // The pool's built-in draining (in worker_thread) processes any
    // remaining coroutine tasks including actor mailbox deliveries.
    m_pool.reset();

    // Actors are destroyed via m_owned_actors after pool join.
    // Any messages still in mailboxes at this point were in-flight
    // during shutdown and are safely dropped (the pool drained them).
}

// ── Inbound message handler ────────────────────────────────────────────────

ynet::async::Task<void> actor_system::handle_inbound_message(
        std::vector<uint8_t> payload) {
    if (payload.empty()) {
        co_return;
    }

    uint8_t type_byte = payload[0];

    if (type_byte == static_cast<uint8_t>(dist::message_type::gossip)) {
        if (m_cluster && payload.size() > 1) {
            m_cluster->apply_gossip(payload.data() + 1,
                                     payload.size() - 1);
        }
    } else if (type_byte == static_cast<uint8_t>(
                   dist::message_type::actor_message)) {
        actor_uri target_uri;
        uint64_t msg_type = 0;
        std::vector<uint8_t> msg_payload;
        if (dist::unpack_actor_message(payload.data(), payload.size(),
                                        target_uri, msg_type, msg_payload)) {
            std::shared_ptr<actor_proxy> proxy;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                auto it = m_registry.find(target_uri.to_string());
                if (it != m_registry.end()) {
                    proxy = it->second;
                }
            }
            if (proxy) {
                auto* local = proxy->local_actor();
                if (local) {
                    message_envelope env;
                    env.msg_type = msg_type;
                    env.data = std::move(msg_payload);
                    local->push_envelope(std::move(env));
                }
            }
        }
    } else if (type_byte == static_cast<uint8_t>(
                   dist::message_type::actor_location)) {
        actor_uri ann_uri;
        uint32_t ttl = 0;
        if (dist::unpack_actor_location(payload.data(), payload.size(),
                                         ann_uri, ttl)) {
            if (ttl > 1) {
                std::lock_guard<std::mutex> lock(m_remote_mutex);
                auto key = ann_uri.to_string();
                auto it = m_remote_actors.find(key);
                net::node_id_t src_node = 0;
                if (!ann_uri.node.empty() && ann_uri.node != "*") {
                    src_node = std::stoull(ann_uri.node);
                }
                if (it == m_remote_actors.end()) {
                    m_remote_actors[key] = {src_node, ann_uri};
                }
            }
        }
    }

    co_return;
}

// ── Gossip loop ────────────────────────────────────────────────────────────

ynet::async::Task<void> actor_system::gossip_loop() {
    if (!m_cluster || !m_transport) {
        co_return;
    }

    std::mt19937 gen(static_cast<unsigned>(
        std::chrono::steady_clock::now().time_since_epoch().count()));

    while (!m_shutdown->is_shutdown()) {
        auto live = m_cluster->live_nodes(m_cfg.node_timeout_ms);

        std::vector<net::node_info> peers;
        auto self_id = m_cluster->self_id();
        for (auto& n : live) {
            if (n.id != self_id) {
                peers.push_back(n);
            }
        }

        if (!peers.empty()) {
            std::shuffle(peers.begin(), peers.end(), gen);

            size_t fanout = std::min(peers.size(), size_t(3));
            for (size_t i = 0; i < fanout; ++i) {
                auto gossip_data = m_cluster->build_gossip();

                std::vector<uint8_t> payload;
                payload.push_back(
                    static_cast<uint8_t>(dist::message_type::gossip));
                payload.insert(payload.end(),
                               gossip_data.begin(), gossip_data.end());

                const auto& addr = peers[i].addr;
                auto colon = addr.rfind(':');
                if (colon != std::string::npos) {
                    std::string host = addr.substr(0, colon);
                    uint16_t port = static_cast<uint16_t>(
                        std::stoi(addr.substr(colon + 1)));

                    auto conn = co_await m_transport->connect(host, port);
                    if (conn && conn->is_valid()) {
                        co_await conn->send(payload);
                        co_await connect_to_node(peers[i].id, host, port);
                    }
                }
            }
        }

        co_await ynet::async::io::sleep_for(
            milliseconds(m_cfg.gossip_interval_ms));
    }
}

// ── Remote routing helpers ─────────────────────────────────────────────────

std::optional<net::node_id_t>
actor_system::resolve_actor_location(const actor_uri& uri) {
    std::lock_guard<std::mutex> lock(m_remote_mutex);
    auto it = m_remote_actors.find(uri.to_string());
    if (it != m_remote_actors.end()) {
        return it->second.node_id;
    }
    return std::nullopt;
}

std::shared_ptr<actor_proxy>
actor_system::get_or_create_remote_proxy(
        const actor_uri& uri, net::node_id_t node_id) {
    {
        std::lock_guard<std::mutex> lock(m_proxy_mutex);
        auto it = m_remote_proxies.find(node_id);
        if (it != m_remote_proxies.end()) {
            return it->second;
        }
    }

    std::shared_ptr<net::outbound_conn> conn;
    {
        std::lock_guard<std::mutex> lock(m_conn_mutex);
        auto it = m_connections.find(node_id);
        if (it != m_connections.end() && it->second->is_valid()) {
            conn = it->second;
        }
    }

    auto proxy = std::make_shared<dist::remote_proxy>(uri, conn, &pool());
    {
        std::lock_guard<std::mutex> lock(m_proxy_mutex);
        m_remote_proxies[node_id] = proxy;
    }
    return proxy;
}

void actor_system::publish_actor_location(const actor_uri& uri) {
    if (!m_cluster) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(m_remote_mutex);
        m_remote_actors[uri.to_string()] = {
            m_cluster->self_id(), uri
        };
    }
}

ynet::async::Task<void> actor_system::connect_to_node(
        net::node_id_t node_id, const std::string& host, uint16_t port) {
    if (!m_transport) {
        co_return;
    }

    {
        std::lock_guard<std::mutex> lock(m_conn_mutex);
        auto it = m_connections.find(node_id);
        if (it != m_connections.end() && it->second->is_valid()) {
            co_return;
        }
    }

    auto conn = co_await m_transport->connect(host, port);
    if (!conn || !conn->is_valid()) {
        co_return;
    }

    std::shared_ptr<dist::remote_proxy> proxy;
    {
        std::lock_guard<std::mutex> lock(m_proxy_mutex);
        auto it = m_remote_proxies.find(node_id);
        if (it != m_remote_proxies.end()) {
            proxy = std::dynamic_pointer_cast<dist::remote_proxy>(it->second);
        }
    }

    {
        std::lock_guard<std::mutex> lock(m_conn_mutex);
        m_connections[node_id] = conn;
    }

    if (proxy) {
        proxy->connect_and_flush(conn);
    }
}

} // namespace ynet::actor

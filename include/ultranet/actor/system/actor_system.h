// actor_system — the single entry point for the actor framework.
// Handles: thread pool, actor registry, discovery, transport.
// Users only need to create one actor_system and call spawn/find.
#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <functional>
#include <optional>

#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/lifecycle/shutdown.hpp"
#include "ultranet/actor/core/base_actor.h"
#include "ultranet/actor/core/actor_ref.h"
#include "ultranet/actor/core/actor_uri.h"
#include "ultranet/actor/net/cluster.h"
#include "ultranet/actor/net/transport.h"
#include "ultranet/actor/dist/serialization.h"
#include "ultranet/actor/dist/remote_proxy.h"

namespace ynet::actor {

using ynet::async::scheduling::WorkStealingThreadPool;
using ynet::async::lifecycle::ShutdownCoordinator;

// ── Actor system configuration ─────────────────────────────────────────

struct system_config {
    size_t num_threads = 4;
    uint16_t listen_port = 0;           // 0 = auto-assign
    std::string node_name = "default";
    std::vector<std::string> seed_nodes;
    size_t max_per_activation = 64;
    uint64_t gossip_interval_ms = 1000;
    uint64_t node_timeout_ms = 3000;
};

// ── Actor system ───────────────────────────────────────────────────────

class actor_system {
public:
    explicit actor_system(const system_config& cfg = {});
    ~actor_system();

    // ── Non-intrusive adapter: wraps any class into an actor ──────────

    template <typename T>
    class actor_adapter : public actor<actor_adapter<T>> {
        T m_instance;
    public:
        template <typename... Args>
        explicit actor_adapter(Args&&... args)
            : m_instance(std::forward<Args>(args)...) {}
        T* get() { return &m_instance; }
    };

    // ── Spawn a local actor (globally visible by type+name) ──────────

    template <typename T, typename... Args>
    actor_ref<T> spawn(const std::string& name, Args&&... args) {
        auto u = actor_uri::make_local(typeid(T).name(), name);

        if constexpr (std::is_base_of_v<actor<T>, T>) {
            auto* a = new T(std::forward<Args>(args)...);
            a->set_uri(u);
            a->set_system(this);
            a->set_schedule_fn([this](std::function<void()> fn) {
                schedule(std::move(fn));
            });
            a->set_max_per_activation(m_cfg.max_per_activation);
            auto proxy = std::make_shared<local_actor_proxy>(a, this);
            auto ref = actor_ref<T>(proxy, u);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_registry[u.to_string()] = ref.proxy();
                m_owned_actors.push_back(std::unique_ptr<actor_base>(a));
            }
            publish_actor_location(u);
            return ref;
        } else {
            using Adapted = actor_adapter<T>;
            auto* adapted = new Adapted(std::forward<Args>(args)...);
            adapted->set_uri(u);
            adapted->set_system(this);
            adapted->set_schedule_fn([this](std::function<void()> fn) {
                schedule(std::move(fn));
            });
            adapted->set_max_per_activation(m_cfg.max_per_activation);
            auto proxy = std::make_shared<local_actor_proxy>(adapted, this);
            auto ref = actor_ref<T>(proxy, u);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_registry[u.to_string()] = ref.proxy();
                m_owned_actors.push_back(std::unique_ptr<actor_base>(adapted));
            }
            publish_actor_location(u);
            return ref;
        }
    }

    // ── Find an actor by URI string ──────────────────────────────────

    template <typename T>
    actor_ref<T> find(const std::string& uri_str) {
        // 1. Check local registry.
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto it = m_registry.find(uri_str);
            if (it != m_registry.end()) {
                return actor_ref<T>(it->second,
                    actor_uri::make_local(typeid(T).name(), uri_str));
            }
        }

        // 2. Check remote routing table.
        if (m_cluster) {
            std::lock_guard<std::mutex> lock(m_remote_mutex);
            auto it = m_remote_actors.find(uri_str);
            if (it != m_remote_actors.end()) {
                auto proxy = get_or_create_remote_proxy(
                    it->second.uri, it->second.node_id);
                if (proxy) {
                    return actor_ref<T>(std::move(proxy), it->second.uri);
                }
            }
        }

        // 3. Not found.
        return actor_ref<T>();
    }

    // ── Schedule a function on the thread pool ───────────────────────

    void schedule(std::function<void()> fn) {
        m_pool->submit_function(std::move(fn));
    }

    // ── Block until shutdown ─────────────────────────────────────────

    void run() { m_pool->wait_all(); }
    void shutdown() {
        if (m_shutdown) {
            m_shutdown->shutdown();
        }
        m_pool->wait_all();
    }

    // Accessors.
    WorkStealingThreadPool& pool() { return *m_pool; }
    const system_config& config() const { return m_cfg; }
    uint16_t actual_port() const {
        return m_actual_port.load(std::memory_order_acquire);
    }
    void add_seed(const std::string& addr) {
        if (m_cluster) {
            m_cluster->add_seed(addr);
        }
    }

    // Register an external proxy (for remote actors discovered via gossip).
    void register_proxy(const std::string& uri_str,
                        std::shared_ptr<actor_proxy> p) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_registry[uri_str] = std::move(p);
    }

private:
    system_config m_cfg;
    std::unique_ptr<WorkStealingThreadPool> m_pool;
    std::unordered_map<std::string, std::shared_ptr<actor_proxy>> m_registry;
    std::vector<std::unique_ptr<actor_base>> m_owned_actors;
    std::mutex m_mutex;

    // ── Cluster + transport ──────────────────────────────────────────

    std::unique_ptr<net::cluster> m_cluster;
    std::unique_ptr<net::tcp_transport> m_transport;
    std::unique_ptr<ShutdownCoordinator> m_shutdown;
    std::atomic<uint16_t> m_actual_port{0};

    // Persistent outbound connections to remote nodes.
    std::unordered_map<net::node_id_t,
                       std::shared_ptr<net::outbound_conn>> m_connections;
    std::mutex m_conn_mutex;

    // Remote actor routing: uri_str → (node_id, actor_uri).
    struct remote_entry {
        net::node_id_t node_id;
        actor_uri uri;
    };
    std::unordered_map<std::string, remote_entry> m_remote_actors;
    std::mutex m_remote_mutex;

    // Per-node remote proxies: node_id → proxy.
    std::unordered_map<net::node_id_t,
                       std::shared_ptr<actor_proxy>> m_remote_proxies;
    std::mutex m_proxy_mutex;

    // ── Internal helpers ─────────────────────────────────────────────

    net::node_id_t generate_node_id();
    std::pair<int, uint16_t> bind_socket(uint16_t port);

    ynet::async::Task<void> handle_inbound_message(
        std::vector<uint8_t> payload);
    ynet::async::Task<void> gossip_loop();

    std::optional<net::node_id_t> resolve_actor_location(
        const actor_uri& uri);
    std::shared_ptr<actor_proxy> get_or_create_remote_proxy(
        const actor_uri& uri, net::node_id_t node_id);
    void publish_actor_location(const actor_uri& uri);
    ynet::async::Task<void> connect_to_node(
        net::node_id_t node_id, const std::string& host, uint16_t port);
};

} // namespace ynet::actor

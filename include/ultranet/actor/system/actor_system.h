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

#include "ultranet/buffer/buffer.h"
#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/actor/core/base_actor.h"
#include "ultranet/actor/core/actor_ref.h"
#include "ultranet/actor/core/actor_uri.h"

namespace ynet::actor {

using ynet::async::scheduling::WorkStealingThreadPool;

// ── Actor system configuration ─────────────────────────────────────────

struct system_config {
    size_t num_threads = 4;
    uint16_t listen_port = 0;   // 0 = auto-assign
    std::string node_name = "default";
    std::vector<std::string> seed_nodes;
};

// ── Local actor proxy (direct dispatch, no serialization) ─────────────

template <typename T>
class local_actor_proxy : public actor_proxy {
public:
    local_actor_proxy(actor_base* a, actor_system* sys) : m_actor(a), m_sys(sys) {}
    void send(const void* data, size_t len) override {
        m_actor->deliver(0, data, len);
    }
    const actor_uri& uri() const override { return m_actor->uri(); }
private:
    actor_base* m_actor;
    actor_system* m_sys;
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
    //
    // Supports two modes:
    //   1. Intrusive:  T inherits actor<T>
    //      auto ref = system.spawn<my_actor>("name", args...);
    //   2. Non-intrusive: T is a plain class (automatically wrapped)
    //      struct counter { void add(int x); };
    //      auto ref = system.spawn<counter>("cnt");

    template <typename T, typename... Args>
    actor_ref<T> spawn(const std::string& name, Args&&... args) {
        auto u = actor_uri::make_local(typeid(T).name(), name);

        if constexpr (std::is_base_of_v<actor<T>, T>) {
            // Mode 1: T inherits actor<T> — direct construction.
            auto* a = new T(std::forward<Args>(args)...);
            a->set_uri(u);
            a->set_system(this);
            auto proxy = std::make_shared<local_actor_proxy<T>>(a, this);
            auto ref = actor_ref<T>(proxy, u);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_registry[u.to_string()] = ref.proxy();
                m_owned_actors.push_back(std::unique_ptr<actor_base>(a));
            }
            return ref;
        } else {
            // Mode 2: Plain class — wrap in adapter.
            using Adapted = actor_adapter<T>;
            auto* adapted = new Adapted(std::forward<Args>(args)...);
            adapted->set_uri(u);
            adapted->set_system(this);
            auto proxy = std::make_shared<local_actor_proxy<Adapted>>(adapted, this);
            // actor_ref<T> holds a proxy; the caller accesses T* via adapter.
            auto ref = actor_ref<T>(proxy, u);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_registry[u.to_string()] = ref.proxy();
                m_owned_actors.push_back(std::unique_ptr<actor_base>(adapted));
            }
            return ref;
        }
    }

    // ── Find an actor by URI string ──────────────────────────────────

    template <typename T>
    actor_ref<T> find(const std::string& uri_str) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_registry.find(uri_str);
        if (it != m_registry.end()) {
            return actor_ref<T>(it->second, actor_uri::make_local(typeid(T).name(), uri_str));
        }
        // Remote lookup: would search cluster.
        return actor_ref<T>();
    }

    // ── Schedule a function on the thread pool ───────────────────────

    void schedule(std::function<void()> fn) {
        m_pool->submit_function(std::move(fn));
    }

    // ── Block until shutdown ─────────────────────────────────────────

    void run() { m_pool->wait_all(); }
    void shutdown() { m_pool->wait_all(); }

    // Accessors.
    WorkStealingThreadPool& pool() { return *m_pool; }
    const system_config& config() const { return m_cfg; }

    // Register an external proxy (for remote actors discovered via gossip).
    void register_proxy(const std::string& uri_str, std::shared_ptr<actor_proxy> p) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_registry[uri_str] = std::move(p);
    }

private:
    system_config m_cfg;
    std::unique_ptr<WorkStealingThreadPool> m_pool;
    std::unordered_map<std::string, std::shared_ptr<actor_proxy>> m_registry;
    std::vector<std::unique_ptr<actor_base>> m_owned_actors;
    std::mutex m_mutex;
};

inline actor_system::actor_system(const system_config& cfg) : m_cfg(cfg) {
    if (cfg.num_threads == 0) m_cfg.num_threads = 4;
    m_pool = std::make_unique<WorkStealingThreadPool>(m_cfg.num_threads);
}

inline actor_system::~actor_system() {
    // Wait for pending work before destroying actors.
    m_pool->wait_all();
}

} // namespace ynet::actor

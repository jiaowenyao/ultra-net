// actor_system — the single entry point for the actor framework.
// Handles: thread pool, actor registry, discovery, transport.
// Users only need to create one actor_system and call spawn/find.
#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>
#include <functional>
#include <optional>
#include <condition_variable>
#include <chrono>
#include <csignal>
#include <thread>

#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/lifecycle/shutdown.hpp"
#include "ultranet/actor/core/base_actor.h"
#include "ultranet/actor/core/actor_ref.h"
#include "ultranet/actor/core/actor_uri.h"
#include "ultranet/actor/net/cluster.h"
#include "ultranet/actor/net/transport.h"
#include "ultranet/actor/dist/serialization.h"
#include "ultranet/actor/dist/remote_proxy.h"
#include "ultranet/actor/dist/remote_log.h"
#include "ultranet/actor/dist/receiver_log.h"

namespace ynet::actor {

using ynet::async::scheduling::WorkStealingThreadPool;
using ynet::async::lifecycle::ShutdownCoordinator;

// ── Actor system configuration ─────────────────────────────────────────

struct system_config {
    size_t num_threads = 4;
    uint16_t listen_port = 0;           // 0 = auto-assign
    std::string node_name = "default";
    // 空则对外公布 127.0.0.1。跨主机或容器时填本机可被对端访问的地址，不含端口。
    std::string advertise_host;
    std::vector<std::string> seed_nodes;
    size_t max_per_activation = 256;
    uint64_t gossip_interval_ms = 1000;
    uint64_t node_timeout_ms = 3000;
    uint64_t remote_accept_timeout_ms = 5000;
    uint64_t remote_ack_timeout_ms = 5000;
    std::string remote_log_path;
    std::string receiver_log_path;
    size_t dedup_cap = 1048576;
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
        void* native_object() noexcept override { return &m_instance; }
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
            auto ref = actor_ref<T>(proxy, u, this);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_registry[u.to_string()] = ref.proxy();
                m_owned_actors.push_back(std::unique_ptr<actor_base>(a));
            }
            publish_actor_location(u);
            replay_receiver_for(u);
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
            auto ref = actor_ref<T>(proxy, u, this);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_registry[u.to_string()] = ref.proxy();
                m_owned_actors.push_back(std::unique_ptr<actor_base>(adapted));
            }
            publish_actor_location(u);
            replay_receiver_for(u);
            return ref;
        }
    }

    template <typename T, typename... Args>
    actor_ref<T> spawn_supervised(const std::string& name, supervisor sv,
                                  Args&&... args) {
        auto ref = spawn<T>(name, std::forward<Args>(args)...);
        if (ref.proxy() && ref.proxy()->local_actor()) {
            ref.proxy()->local_actor()->install_supervisor(sv);
        }
        return ref;
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
                    actor_uri::make_local(typeid(T).name(), uri_str), this);
            }
        }

        // 2. Check remote routing table.
        if (m_cluster) {
            std::lock_guard<std::mutex> lock(m_remote_mutex);
            // 精确匹配
            auto it = m_remote_actors.find(uri_str);
            if (it != m_remote_actors.end()) {
                auto proxy = get_or_create_remote_proxy(
                    it->second.uri, it->second.node_id);
                if (proxy) {
                    return actor_ref<T>(std::move(proxy), it->second.uri, this);
                }
            }
            // 通配匹配: 若 uri 中 node="*"，遍历全部远程 actor，
            // 按 type + name 匹配（忽略 node_id）
            if (uri_str.find("ultra://*/") == 0) {
                auto slash2 = uri_str.find('/', 10);
                if (slash2 != std::string::npos) {
                    std::string type_name = uri_str.substr(10,
                        slash2 - 10);
                    std::string actor_name = uri_str.substr(slash2 + 1);
                    for (auto& [key, entry] : m_remote_actors) {
                        auto rs1 = key.find('/', 8);
                        auto rs2 = key.find('/', rs1 + 1);
                        if (rs1 != std::string::npos &&
                            rs2 != std::string::npos) {
                            if (key.substr(rs1 + 1, rs2 - rs1 - 1) == type_name
                                && key.substr(rs2 + 1) == actor_name) {
                                auto proxy = get_or_create_remote_proxy(
                                    entry.uri, entry.node_id);
                                if (proxy) {
                                    return actor_ref<T>(std::move(proxy),
                                        entry.uri, this);
                                }
                            }
                        }
                    }
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

    uint64_t self_node_id() const {
        return m_cluster ? m_cluster->self_id() : 0;
    }

    std::string advertised_addr() {
        return m_cluster ? m_cluster->self_addr() : std::string();
    }

    std::vector<net::node_info> current_live_nodes() {
        if (!m_cluster) {
            return {};
        }
        return m_cluster->live_nodes(m_cfg.node_timeout_ms);
    }
    void add_seed(const std::string& addr) {
        if (m_cluster) {
            m_cluster->add_seed(addr);
        }
    }

    uint64_t alloc_msg_id() {
        return m_next_msg_id.fetch_add(1, std::memory_order_relaxed);
    }

    uint64_t resent_count() const {
        return m_resent.load(std::memory_order_relaxed);
    }

    uint64_t ack_timeout_resend_count() const {
        return m_ack_timeout_resend.load(std::memory_order_relaxed);
    }

    uint64_t unacked_count() const {
        std::lock_guard<std::mutex> lock(m_unacked_mu);
        return m_unacked.size();
    }

    uint64_t recover_attempt_count() const {
        return m_recover_attempts.load(std::memory_order_relaxed);
    }

    uint64_t stopped_recover_count() const {
        return m_stopped_recover.load(std::memory_order_relaxed);
    }

    uint64_t wire_skip_count() const {
        return m_wire_skips.load(std::memory_order_relaxed);
    }

    uint64_t dedup_overflow_count() const {
        return m_dedup_overflow.load(std::memory_order_relaxed);
    }

    uint64_t dedup_evict_count() const {
        return m_dedup_evict.load(std::memory_order_relaxed);
    }

    uint64_t receiver_log_rejected_count() const {
        return m_receiver_log_rejected.load(std::memory_order_relaxed);
    }

    uint64_t receiver_fsynced_data_count() const {
        if (!m_rcv) {
            return 0;
        }
        return m_rcv->fsynced_data_count();
    }

    uint64_t receiver_fsynced_handled_count() const {
        if (!m_rcv) {
            return 0;
        }
        return m_rcv->fsynced_handled_count();
    }

    int receiver_log_torn() const {
        if (!m_rcv) {
            return 0;
        }
        return m_rcv->torn();
    }

    void set_hold_handler(bool hold) {
        m_hold_handler.store(hold, std::memory_order_release);
    }

    void receiver_clear_queued(uint64_t sender, uint64_t msg_id) {
        if (m_rcv) {
            m_rcv->set_enqueued(sender, msg_id, false);
        }
    }

    void note_wire_skip() {
        m_wire_skips.fetch_add(1, std::memory_order_relaxed);
    }

    enum class dedup_view {
        absent,
        in_progress,
        completed,
    };

    enum class dedup_claim {
        fresh,
        completed,
        busy,
        overflow,
    };

    dedup_view peek_dedup(uint64_t sender_node_id, uint64_t msg_id);
    dedup_claim claim_remote(uint64_t sender_node_id, uint64_t msg_id);
    void finish_remote_delivery(uint64_t sender_node_id, uint64_t msg_id,
                                uint64_t reply_conn_id, deliver_result result);
    void post_remote_ack(uint64_t reply_conn_id, uint64_t msg_id);

    size_t drop_inbound_connections() {
        if (!m_transport) {
            return 0;
        }
        return m_transport->drop_inbound_connections();
    }

    void blackhole_accept_for(std::chrono::milliseconds dur) {
        if (m_transport) {
            m_transport->blackhole_accept_for(dur);
        }
    }

    void set_hold_routed(bool hold) {
        m_hold_routed.store(hold, std::memory_order_release);
    }

    void release_routed_after_drop() {
        drop_inbound_connections();
        if (m_transport) {
            m_transport->bump_accept_epoch();
            m_routed_min_epoch.store(m_transport->accept_epoch(),
                                     std::memory_order_release);
        }
        m_hold_routed.store(false, std::memory_order_release);
    }

    bool post_original_frame(const std::vector<uint8_t>& frame);

    bool dispatch_reply(uint64_t reply_conn_id, uint64_t correlation_id,
                        std::vector<uint8_t> payload);
    uint64_t prepare_ask();
    bool take_ask_payload(uint64_t id, std::chrono::milliseconds timeout,
                          std::vector<uint8_t>& out);
    void cancel_ask(uint64_t id);

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
    std::unique_ptr<remote_log> m_log;
    std::unique_ptr<receiver_log> m_rcv;
    std::atomic<uint64_t> m_receiver_log_rejected{0};
    std::atomic<bool> m_hold_handler{false};
    std::atomic<bool> m_hold_routed{false};
    std::atomic<uint64_t> m_routed_min_epoch{0};
    // 重放按日志里的目标节点拨种子时，避免每 20ms 再提交一次连接。
    std::atomic<bool> m_direct_dial{false};
    std::unique_ptr<ShutdownCoordinator> m_shutdown;
    std::atomic<uint16_t> m_actual_port{0};

    // Persistent outbound connections to remote nodes.
    struct pooled_conn {
        std::shared_ptr<net::outbound_conn> conn;
        std::string host;
        uint16_t port = 0;
    };
    std::unordered_map<net::node_id_t, pooled_conn> m_connections;
    std::mutex m_conn_mutex;

    struct unacked_item {
        std::vector<uint8_t> frame;
        std::chrono::steady_clock::time_point sent_at{};
        net::node_id_t node = 0;
        bool needs_resend = false;
    };
    mutable std::mutex m_unacked_mu;
    std::unordered_map<uint64_t, unacked_item> m_unacked;
    std::atomic<uint64_t> m_next_msg_id{1};
    std::atomic<uint64_t> m_resent{0};
    std::atomic<uint64_t> m_ack_timeout_resend{0};
    std::atomic<uint64_t> m_recover_attempts{0};
    std::atomic<uint64_t> m_stopped_recover{0};
    std::atomic<uint64_t> m_wire_skips{0};
    std::atomic<uint64_t> m_dedup_overflow{0};
    std::atomic<uint64_t> m_dedup_evict{0};
    struct dedup_key {
        uint64_t sender = 0;
        uint64_t msg_id = 0;
        bool operator==(const dedup_key& other) const {
            return sender == other.sender && msg_id == other.msg_id;
        }
    };
    struct dedup_key_hash {
        size_t operator()(const dedup_key& key) const noexcept {
            return static_cast<size_t>(key.sender ^ (key.msg_id * 0x9E3779B97F4A7C15ull));
        }
    };
    enum class dedup_state : uint8_t {
        in_progress,
        completed,
    };
    std::mutex m_dedup_mu;
    std::unordered_map<dedup_key, dedup_state, dedup_key_hash> m_dedup;
    std::mutex m_recover_mu;
    std::unordered_map<net::node_id_t, bool> m_recovering;
    std::unordered_map<net::node_id_t, bool> m_recover_again;

    struct ask_slot {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        std::vector<uint8_t> payload;
    };
    std::mutex m_ask_mu;
    std::unordered_map<uint64_t, std::shared_ptr<ask_slot>> m_asks;

    std::shared_ptr<ask_slot> begin_ask(uint64_t id);
    bool finish_ask(uint64_t id, std::vector<uint8_t> payload);

    // Remote actor routing: uri_str → (node_id, actor_uri).
    struct remote_entry {
        net::node_id_t node_id;
        actor_uri uri;
    };
    std::unordered_map<std::string, remote_entry> m_remote_actors;
    std::mutex m_remote_mutex;

    // 按 actor URI 字符串索引的远端代理（同节点多 actor 各自独立）。
    std::unordered_map<std::string, std::shared_ptr<actor_proxy>> m_remote_proxies;
    std::mutex m_proxy_mutex;

    // ── Internal helpers ─────────────────────────────────────────────
    // (implemented inline below)

    net::node_id_t generate_node_id();
    std::pair<int, uint16_t> bind_socket(uint16_t port);
    ynet::async::Task<void> handle_inbound_message(
        std::shared_ptr<net::inbound_conn> inbound, std::vector<uint8_t> payload);
    ynet::async::Task<void> gossip_loop();
    ynet::async::Task<void> ack_timeout_loop();
    ynet::async::Task<void> receiver_flush_loop();
    ynet::async::Task<void> interruptible_sleep(std::chrono::milliseconds total);
    void accept_logged_routed(const std::vector<uint8_t>& frame,
                              uint64_t sender, uint64_t msg_id,
                              uint64_t reply_conn);
    void apply_receiver_flush(receiver_log::flush_result result);
    void try_enqueue_receiver(uint64_t sender, uint64_t msg_id);
    void pump_receiver_waiting();
    void replay_receiver_for(const actor_uri& uri);
    bool push_routed_frame(const std::vector<uint8_t>& frame, uint64_t reply_conn);
    enum class dedup_room { push, already, full };
    dedup_room dedup_room_for(uint64_t sender, uint64_t msg_id);
    ynet::async::Task<void> read_pooled_outbound(
        std::shared_ptr<net::outbound_conn> conn, net::node_id_t node_id);
    ynet::async::Task<void> recover_node(
        net::node_id_t node_id, std::string host, uint16_t port);
    std::optional<net::node_id_t> resolve_actor_location(const actor_uri& uri);
    std::shared_ptr<actor_proxy> get_or_create_remote_proxy(
        const actor_uri& uri, net::node_id_t node_id);
    std::shared_ptr<actor_proxy> find_local_proxy(const actor_uri& uri);
    void publish_actor_location(const actor_uri& uri);
    ynet::async::Task<void> broadcast_actor_location(actor_uri uri);
    ynet::async::Task<void> connect_to_node(
        net::node_id_t node_id, const std::string& host, uint16_t port,
        std::shared_ptr<net::outbound_conn> existing_conn = nullptr);
    ynet::async::Task<void> dial_logged_destination(
        net::node_id_t node_id, std::string host, uint16_t port);
    void track_unacked(net::node_id_t node, uint64_t msg_id,
                       const std::vector<uint8_t>& frame);
    void note_ack(uint64_t msg_id);
    void note_send_failed(uint64_t msg_id);
    void resend_unacked(net::node_id_t node,
                        const std::shared_ptr<net::outbound_conn>& conn);
    void on_link_dead(net::node_id_t node);
    void bind_proxy_hooks(const std::shared_ptr<dist::remote_proxy>& proxy,
                          net::node_id_t node_id);
};

// ── Inline implementations ──────────────────────────────────────────────

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <cstring>
#include <random>

inline std::pair<int, uint16_t> actor_system::bind_socket(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { return {-1, 0}; }

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

inline net::node_id_t actor_system::generate_node_id() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    uint64_t r = gen();
    const auto& name = m_cfg.node_name;
    uint64_t h = 14695981039346656037ULL;
    for (char c : name) { h ^= static_cast<uint8_t>(c); h *= 1099511628211ULL; }
    return h ^ r;
}

inline actor_system::actor_system(const system_config& cfg) : m_cfg(cfg) {
    // 对端关闭后 write 必须返回 EPIPE，不能把进程杀掉。
    static std::once_flag sigpipe_once;
    std::call_once(sigpipe_once, []() {
        std::signal(SIGPIPE, SIG_IGN);
    });
    if (cfg.num_threads == 0) {
        m_cfg.num_threads = 4;
    }
    m_pool = std::make_unique<WorkStealingThreadPool>(m_cfg.num_threads);
    m_shutdown = std::make_unique<ShutdownCoordinator>();
    if (!m_cfg.remote_log_path.empty()) {
        m_log = std::make_unique<remote_log>(m_cfg.remote_log_path);
    }
    if (!m_cfg.receiver_log_path.empty()) {
        if (!m_cfg.remote_log_path.empty()
            && m_cfg.receiver_log_path == m_cfg.remote_log_path) {
            m_receiver_log_rejected.fetch_add(1, std::memory_order_relaxed);
        } else {
            m_rcv = std::make_unique<receiver_log>(m_cfg.receiver_log_path);
            auto flush_task = receiver_flush_loop();
            m_pool->submit_coroutine(flush_task.release());
        }
    }

    auto [listen_fd, actual_port] = bind_socket(m_cfg.listen_port);
    m_actual_port.store(actual_port, std::memory_order_release);

    if (listen_fd < 0) {
        ULTRA_LOG_WARN("[actor_system] failed to bind to port {} — "
                       "networking disabled; only local actors available",
                       m_cfg.listen_port);
        return;
    }

    auto self_id = generate_node_id();
    std::string host = m_cfg.advertise_host.empty() ? "127.0.0.1" : m_cfg.advertise_host;
    std::string self_addr = host + ":" + std::to_string(actual_port);
    m_cluster = std::make_unique<net::cluster>(self_id, self_addr);
    for (const auto& seed : m_cfg.seed_nodes) { m_cluster->add_seed(seed); }

    auto msg_handler = [this](std::shared_ptr<net::inbound_conn> inbound,
                              std::vector<uint8_t> payload) -> ynet::async::Task<void> {
        co_await handle_inbound_message(std::move(inbound), std::move(payload));
    };
    m_transport = std::make_unique<net::tcp_transport>(
        listen_fd, actual_port, std::move(msg_handler));

    auto serve_task = m_transport->serve(*m_shutdown);
    m_pool->submit_coroutine(serve_task.release());
    auto gossip_task = gossip_loop();
    m_pool->submit_coroutine(gossip_task.release());
    auto ack_task = ack_timeout_loop();
    m_pool->submit_coroutine(ack_task.release());

    ULTRA_LOG_INFO("[actor_system] node={} id={} port={} threads={}",
                   m_cfg.node_name, self_id, actual_port, m_cfg.num_threads);
}

inline actor_system::~actor_system() {
    for (auto& a : m_owned_actors) {
        if (a) {
            a->set_shutting_down(true);
        }
    }
    if (m_shutdown) {
        m_shutdown->shutdown();
    }
    if (m_transport) {
        m_transport->shutdown_listen();
    }
    // serve 可能在第一次 drop 之后又 accept 一条连接。循环关掉新连接，
    // 直到 serve / gossip / ack / 读写协程都结束，再拆线程池。
    if (m_pool) {
        for (int attempt = 0; attempt < 20; ++attempt) {
            if (m_transport) {
                m_transport->drop_inbound_connections();
            }
            {
                std::lock_guard<std::mutex> lock(m_conn_mutex);
                for (auto& [id, pooled] : m_connections) {
                    (void)id;
                    if (pooled.conn) {
                        pooled.conn->close();
                    }
                }
                m_connections.clear();
            }
            if (m_pool->wait_all_for(std::chrono::milliseconds(200))) {
                break;
            }
        }
        m_pool.reset();
    }
}

inline ynet::async::Task<void> actor_system::handle_inbound_message(
        std::shared_ptr<net::inbound_conn> inbound, std::vector<uint8_t> payload) {
    if (payload.empty()) { co_return; }
    uint8_t type_byte = payload[0];

    if (type_byte == static_cast<uint8_t>(dist::message_type::gossip)) {
        if (m_cluster && payload.size() > 1) {
            m_cluster->apply_gossip(payload.data() + 1, payload.size() - 1);
        }
    } else if (type_byte == static_cast<uint8_t>(dist::message_type::routed)) {
        if (m_hold_routed.load(std::memory_order_acquire)) {
            co_return;
        }
        if (inbound && inbound->accept_epoch < m_routed_min_epoch.load(std::memory_order_acquire)) {
            co_return;
        }
        actor_uri target_uri;
        uint8_t flags = 0;
        uint64_t msg_id = 0;
        uint64_t sender_node_id = 0;
        uint64_t msg_hash = 0;
        std::vector<uint8_t> msg_payload;
        if (!dist::unpack_routed(payload.data(), payload.size(),
                                 target_uri, flags, msg_id, sender_node_id,
                                 msg_hash, msg_payload)) {
            co_return;
        }
        if (m_rcv) {
            accept_logged_routed(payload, sender_node_id, msg_id,
                                 inbound ? inbound->id : 0);
            co_return;
        }
        auto proxy = find_local_proxy(target_uri);
        auto* local = proxy ? proxy->local_actor() : nullptr;
        if (!local) {
            co_return;
        }
        message_envelope env;
        env.msg_type = msg_hash;
        env.correlation_id = msg_id;
        env.sender_node_id = sender_node_id;
        env.flags = flags;
        env.reply_conn_id = inbound ? inbound->id : 0;
        env.assign_vector(std::move(msg_payload));
        auto seen = peek_dedup(sender_node_id, msg_id);
        if (seen == dedup_view::completed) {
            note_wire_skip();
            if (inbound && m_pool) {
                post_remote_ack(inbound->id, msg_id);
            }
            co_return;
        }
        if (seen == dedup_view::in_progress) {
            note_wire_skip();
            co_return;
        }
        local->push_envelope(std::move(env));
    } else if (type_byte == static_cast<uint8_t>(dist::message_type::reply)
               || type_byte == static_cast<uint8_t>(dist::message_type::ack)) {
        ULTRA_LOG_ERROR("[actor_system] inbound ignored type {}", type_byte);
    } else if (type_byte == static_cast<uint8_t>(dist::message_type::actor_message)) {
        actor_uri target_uri;
        uint64_t msg_type = 0;
        std::vector<uint8_t> msg_payload;
        if (dist::unpack_actor_message(payload.data(), payload.size(),
                                        target_uri, msg_type, msg_payload)) {
            auto proxy = find_local_proxy(target_uri);
            if (proxy) {
                auto* local = proxy->local_actor();
                if (local) {
                    message_envelope env;
                    env.msg_type = msg_type;
                    env.assign_vector(std::move(msg_payload));
                    local->push_envelope(std::move(env));
                }
            }
        }
    } else if (type_byte == static_cast<uint8_t>(dist::message_type::actor_location)) {
        actor_uri ann_uri;
        uint32_t ttl = 0;
        if (dist::unpack_actor_location(payload.data(), payload.size(), ann_uri, ttl)) {
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

inline ynet::async::Task<void> actor_system::gossip_loop() {
    if (!m_cluster || !m_transport) { co_return; }

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
                payload.push_back(static_cast<uint8_t>(dist::message_type::gossip));
                payload.insert(payload.end(), gossip_data.begin(), gossip_data.end());

                const auto& addr = peers[i].addr;
                auto colon = addr.rfind(':');
                if (colon != std::string::npos) {
                    try {
                        std::string host = addr.substr(0, colon);
                        uint16_t port = static_cast<uint16_t>(
                            std::stoi(addr.substr(colon + 1)));
                        // 建立连接 → 发送gossip → 复用为持久连接（避免重复建连）
                        // 对端已死时 connect 抛 system_error，不能因此结束整个循环
                        auto conn = co_await m_transport->connect(host, port);
                        if (conn && conn->is_valid()) {
                            co_await conn->send(payload);
                            co_await connect_to_node(peers[i].id, host, port, conn);
                        }
                    } catch (...) {
                    }
                }
            }
        } else if (!m_cluster->seeds().empty()) {
            // 尚无存活对等节点时，拨号种子以完成初始发现（一次性连接，不分配 node_id）
            for (const auto& seed : m_cluster->seeds()) {
                auto gossip_data = m_cluster->build_gossip();
                std::vector<uint8_t> payload;
                payload.push_back(static_cast<uint8_t>(dist::message_type::gossip));
                payload.insert(payload.end(), gossip_data.begin(), gossip_data.end());

                auto colon = seed.rfind(':');
                if (colon == std::string::npos) {
                    continue;
                }
                std::string host = seed.substr(0, colon);
                uint16_t port = 0;
                try {
                    port = static_cast<uint16_t>(
                        std::stoi(seed.substr(colon + 1)));
                } catch (...) {
                    continue;
                }
                try {
                    auto conn = co_await m_transport->connect(host, port);
                    if (conn && conn->is_valid()) {
                        co_await conn->send(payload);
                        // 一次性连接：成功也保留为 one-shot，不 invent node_id
                    }
                } catch (...) {
                    // 连接失败忽略，坏种子不得中断 gossip 循环
                }
            }
        }
        if (!peers.empty()) {
            std::vector<actor_uri> locals;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                for (auto& owned : m_owned_actors) {
                    if (owned) {
                        locals.push_back(owned->uri());
                    }
                }
            }
            for (const auto& local_uri : locals) {
                publish_actor_location(local_uri);
            }
        }
        co_await interruptible_sleep(
            std::chrono::milliseconds(m_cfg.gossip_interval_ms));
    }
}

inline std::optional<net::node_id_t>
actor_system::resolve_actor_location(const actor_uri& uri) {
    std::lock_guard<std::mutex> lock(m_remote_mutex);
    auto it = m_remote_actors.find(uri.to_string());
    if (it != m_remote_actors.end()) { return it->second.node_id; }
    return std::nullopt;
}

inline std::shared_ptr<actor_proxy>
actor_system::get_or_create_remote_proxy(const actor_uri& uri, net::node_id_t node_id) {
    const std::string key = uri.to_string();
    {
        std::lock_guard<std::mutex> lock(m_proxy_mutex);
        auto it = m_remote_proxies.find(key);
        if (it != m_remote_proxies.end()) {
            return it->second;
        }
    }
    // 出站连接仍按 node_id 共享
    std::shared_ptr<net::outbound_conn> conn;
    {
        std::lock_guard<std::mutex> lock(m_conn_mutex);
        auto it = m_connections.find(node_id);
        if (it != m_connections.end() && it->second.conn && it->second.conn->is_valid()) {
            conn = it->second.conn;
        }
    }
    auto proxy = std::make_shared<dist::remote_proxy>(uri, conn, &pool());
    bind_proxy_hooks(proxy, node_id);
    {
        std::lock_guard<std::mutex> lock(m_proxy_mutex);
        m_remote_proxies[key] = proxy;
    }
    return proxy;
}

inline void actor_system::publish_actor_location(const actor_uri& uri) {
    if (!m_cluster) {
        return;
    }
    actor_uri announced = uri;
    if (announced.node.empty() || announced.node == "*") {
        announced.node = std::to_string(m_cluster->self_id());
    }
    {
        std::lock_guard<std::mutex> lock(m_remote_mutex);
        m_remote_actors[announced.to_string()] = {m_cluster->self_id(), announced};
    }
    // 向当前存活对等节点广播位置通告（失败不致命）
    if (m_transport && m_pool) {
        auto task = broadcast_actor_location(announced);
        m_pool->submit_coroutine(task.release());
    }
}

inline ynet::async::Task<void>
actor_system::broadcast_actor_location(actor_uri uri) {
    if (!m_cluster || !m_transport) {
        co_return;
    }
    auto payload = dist::pack_actor_location(uri, 3);
    auto live = m_cluster->live_nodes(m_cfg.node_timeout_ms);
    auto self_id = m_cluster->self_id();
    for (const auto& n : live) {
        if (n.id == self_id) {
            continue;
        }
        const auto& addr = n.addr;
        auto colon = addr.rfind(':');
        if (colon == std::string::npos) {
            continue;
        }
        std::string host = addr.substr(0, colon);
        uint16_t port = 0;
        try {
            port = static_cast<uint16_t>(std::stoi(addr.substr(colon + 1)));
        } catch (...) {
            continue;
        }
        try {
            auto conn = co_await m_transport->connect(host, port);
            if (conn && conn->is_valid()) {
                co_await conn->send(payload);
            }
        } catch (...) {
            // 连接失败忽略
        }
    }
    co_return;
}

inline ynet::async::Task<void> actor_system::connect_to_node(
        net::node_id_t node_id, const std::string& host, uint16_t port,
        std::shared_ptr<net::outbound_conn> existing_conn) {
    if (!m_transport) {
        co_return;
    }

    // 复用已有连接（由 gossip_loop 传入，避免重复建连）
    auto conn = std::move(existing_conn);
        if (!conn || !conn->is_valid()) {
            {
                std::lock_guard<std::mutex> lock(m_conn_mutex);
                auto it = m_connections.find(node_id);
                if (it != m_connections.end() && it->second.conn
                    && it->second.conn->is_valid()) {
                    co_return;
                }
            }
            try {
                conn = co_await m_transport->connect(host, port);
            } catch (...) {
                co_return;
            }
            if (!conn || !conn->is_valid()) {
                co_return;
            }
        } else {
            std::lock_guard<std::mutex> lock(m_conn_mutex);
            auto it = m_connections.find(node_id);
            if (it != m_connections.end() && it->second.conn
                && it->second.conn->is_valid()) {
                co_return;
            }
        }

        conn->on_dead = [this, node_id]() {
            on_link_dead(node_id);
        };
        {
            std::lock_guard<std::mutex> lock(m_conn_mutex);
            auto it = m_connections.find(node_id);
            if (it != m_connections.end() && it->second.conn
                && it->second.conn->is_valid()) {
                co_return;
            }
            m_connections[node_id] = pooled_conn{conn, host, port};
        }

        if (m_pool) {
            m_pool->submit_coroutine(
                read_pooled_outbound(conn, node_id).release());
        }

        std::vector<std::shared_ptr<dist::remote_proxy>> to_flush;
        {
            std::lock_guard<std::mutex> lock(m_proxy_mutex);
            const std::string node_str = std::to_string(node_id);
            for (auto& [key, p] : m_remote_proxies) {
                (void)key;
                if (!p) {
                    continue;
                }
                auto rp = std::dynamic_pointer_cast<dist::remote_proxy>(p);
                if (!rp) {
                    continue;
                }
                if (rp->node_id() == node_id || rp->uri().node == node_str) {
                    to_flush.push_back(std::move(rp));
                }
            }
        }
        for (auto& proxy : to_flush) {
            proxy->connect_and_flush(conn);
        }
        resend_unacked(node_id, conn);
        co_return;
}

inline ynet::async::Task<void> actor_system::dial_logged_destination(
        net::node_id_t node_id, std::string host, uint16_t port) {
    try {
        co_await connect_to_node(node_id, host, port, nullptr);
    } catch (...) {
    }
    m_direct_dial.store(false, std::memory_order_release);
    co_return;
}

inline void actor_system::bind_proxy_hooks(
        const std::shared_ptr<dist::remote_proxy>& proxy, net::node_id_t node_id) {
    if (!proxy) {
        return;
    }
    proxy->set_node(node_id);
    proxy->set_self_node(self_node_id());
    proxy->set_hooks(
        [this]() { return alloc_msg_id(); },
        [this, node_id](uint64_t id, const std::vector<uint8_t>& frame) {
            if (m_log && !m_log->append_data(id, frame)) {
                return false;
            }
            track_unacked(node_id, id, frame);
            return true;
        },
        [this](uint64_t id) {
            note_send_failed(id);
        });
}

inline void actor_system::track_unacked(
        net::node_id_t node, uint64_t msg_id, const std::vector<uint8_t>& frame) {
    std::lock_guard<std::mutex> lock(m_unacked_mu);
    unacked_item item;
    item.frame = frame;
    item.sent_at = std::chrono::steady_clock::now();
    item.node = node;
    item.needs_resend = false;
    m_unacked[msg_id] = std::move(item);
}

inline void actor_system::note_ack(uint64_t msg_id) {
    if (m_log && !m_log->append_ack(msg_id)) {
        return;
    }
    std::lock_guard<std::mutex> lock(m_unacked_mu);
    m_unacked.erase(msg_id);
}

inline void actor_system::note_send_failed(uint64_t msg_id) {
    std::lock_guard<std::mutex> lock(m_unacked_mu);
    auto it = m_unacked.find(msg_id);
    if (it == m_unacked.end()) {
        return;
    }
    it->second.needs_resend = true;
}

inline void actor_system::resend_unacked(
        net::node_id_t node, const std::shared_ptr<net::outbound_conn>& conn) {
    if (!conn || !m_pool) {
        return;
    }
    struct pending_frame {
        uint64_t id = 0;
        std::vector<uint8_t> frame;
    };
    std::vector<pending_frame> frames;
    {
        std::lock_guard<std::mutex> lock(m_unacked_mu);
        for (auto& [id, item] : m_unacked) {
            if (item.node == node && item.needs_resend) {
                frames.push_back(pending_frame{id, item.frame});
            }
        }
    }
    uint64_t sent = 0;
    for (auto& item : frames) {
        if (!conn->post_frame(item.frame, m_pool.get())) {
            continue;
        }
        std::lock_guard<std::mutex> lock(m_unacked_mu);
        auto it = m_unacked.find(item.id);
        if (it == m_unacked.end()) {
            continue;
        }
        it->second.needs_resend = false;
        it->second.sent_at = std::chrono::steady_clock::now();
        sent++;
    }
    if (sent > 0) {
        m_resent.fetch_add(sent, std::memory_order_relaxed);
    }
}

inline void actor_system::on_link_dead(net::node_id_t node) {
    if (m_shutdown && m_shutdown->is_shutdown()) {
        return;
    }
    std::string host;
    uint16_t port = 0;
    bool have_addr = false;
    {
        std::lock_guard<std::mutex> lock(m_conn_mutex);
        auto it = m_connections.find(node);
        if (it != m_connections.end()) {
            host = it->second.host;
            port = it->second.port;
            have_addr = true;
            m_connections.erase(it);
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_unacked_mu);
        for (auto& [id, item] : m_unacked) {
            (void)id;
            if (item.node == node) {
                item.needs_resend = true;
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_proxy_mutex);
        for (auto& [key, proxy] : m_remote_proxies) {
            (void)key;
            auto remote = std::dynamic_pointer_cast<dist::remote_proxy>(proxy);
            if (remote && remote->node_id() == node) {
                remote->drop_connection();
            }
        }
    }
    bool start = false;
    {
        std::lock_guard<std::mutex> lock(m_recover_mu);
        if (!have_addr && !m_recovering[node]) {
            return;
        }
        if (m_recovering[node]) {
            m_recover_again[node] = true;
        } else {
            m_recovering[node] = true;
            m_recover_again[node] = false;
            start = have_addr;
        }
    }
    if (start && m_pool) {
        m_pool->submit_coroutine(recover_node(node, host, port).release());
    }
}

inline ynet::async::Task<void> actor_system::recover_node(
        net::node_id_t node_id, std::string host, uint16_t port) {
    for (;;) {
        co_await interruptible_sleep(std::chrono::milliseconds(50));
        if (m_shutdown && m_shutdown->is_shutdown()) {
            break;
        }
        bool still_live = false;
        if (m_cluster) {
            for (const auto& node : m_cluster->live_nodes(m_cfg.node_timeout_ms)) {
                if (node.id == node_id) {
                    still_live = true;
                    break;
                }
            }
        }
        if (!still_live) {
            m_stopped_recover.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(m_recover_mu);
            m_recovering[node_id] = false;
            m_recover_again[node_id] = false;
            co_return;
        }
        try {
            m_recover_attempts.fetch_add(1, std::memory_order_relaxed);
            co_await connect_to_node(node_id, host, port, nullptr);
        } catch (...) {
            // 连接被拒绝时协程不能在这里结束，否则节点还在存活表里也不会再试。
        }
        std::shared_ptr<net::outbound_conn> conn;
        {
            std::lock_guard<std::mutex> lock(m_conn_mutex);
            auto it = m_connections.find(node_id);
            if (it != m_connections.end()) {
                conn = it->second.conn;
            }
        }
        if (conn && conn->is_valid()) {
            resend_unacked(node_id, conn);
        }
        bool again = false;
        bool pending = false;
        {
            std::lock_guard<std::mutex> lock(m_recover_mu);
            again = m_recover_again[node_id];
            if (again) {
                m_recover_again[node_id] = false;
            }
        }
        {
            std::lock_guard<std::mutex> lock(m_unacked_mu);
            for (const auto& [id, item] : m_unacked) {
                (void)id;
                if (item.node == node_id && item.needs_resend) {
                    pending = true;
                    break;
                }
            }
        }
        if (m_shutdown && m_shutdown->is_shutdown()) {
            break;
        }
        if (again || !conn || !conn->is_valid() || pending) {
            continue;
        }
        std::lock_guard<std::mutex> lock(m_recover_mu);
        if (m_recover_again[node_id]) {
            m_recover_again[node_id] = false;
            continue;
        }
        m_recovering[node_id] = false;
        break;
    }
    {
        std::lock_guard<std::mutex> lock(m_recover_mu);
        if (m_shutdown && m_shutdown->is_shutdown()) {
            m_recovering[node_id] = false;
            m_recover_again[node_id] = false;
        }
    }
    co_return;
}

inline ynet::async::Task<void> actor_system::read_pooled_outbound(
        std::shared_ptr<net::outbound_conn> conn, net::node_id_t node_id) {
    (void)node_id;
    while (conn && conn->is_valid() && m_shutdown && !m_shutdown->is_shutdown()) {
        auto payload = co_await net::read_frame(conn->m_sock.fd());
        if (!payload.ok) {
            break;
        }
        if (payload.bytes.empty()) {
            continue;
        }
        uint8_t type_byte = payload.bytes[0];
        if (type_byte == static_cast<uint8_t>(dist::message_type::ack)) {
            uint64_t msg_id = 0;
            if (dist::unpack_ack(payload.bytes.data(), payload.bytes.size(), msg_id)) {
                note_ack(msg_id);
            }
        } else if (type_byte == static_cast<uint8_t>(dist::message_type::reply)) {
            uint64_t msg_id = 0;
            std::vector<uint8_t> body;
            if (dist::unpack_reply(payload.bytes.data(), payload.bytes.size(), msg_id, body)) {
                finish_ask(msg_id, std::move(body));
            }
        } else {
            ULTRA_LOG_ERROR("[actor_system] pooled outbound ignored type {}",
                            type_byte);
        }
    }
    if (conn) {
        conn->notify_dead();
    }
    co_return;
}

inline ynet::async::Task<void> actor_system::receiver_flush_loop() {
    while (m_shutdown && !m_shutdown->is_shutdown()) {
        co_await interruptible_sleep(std::chrono::milliseconds(2));
        if (!m_rcv) {
            co_return;
        }
        apply_receiver_flush(m_rcv->flush_ready());
    }
    co_return;
}

inline actor_system::dedup_room actor_system::dedup_room_for(
        uint64_t sender, uint64_t msg_id) {
    std::lock_guard<std::mutex> lock(m_dedup_mu);
    if (m_dedup.find(dedup_key{sender, msg_id}) != m_dedup.end()) {
        return dedup_room::already;
    }
    if (m_dedup.size() < m_cfg.dedup_cap) {
        return dedup_room::push;
    }
    for (const auto& entry : m_dedup) {
        if (entry.second == dedup_state::completed) {
            return dedup_room::push;
        }
    }
    return dedup_room::full;
}

inline bool actor_system::push_routed_frame(const std::vector<uint8_t>& frame,
                                            uint64_t reply_conn) {
    actor_uri uri;
    uint8_t flags = 0;
    uint64_t msg_id = 0;
    uint64_t sender = 0;
    uint64_t msg_hash = 0;
    std::vector<uint8_t> payload;
    if (!dist::unpack_routed(frame.data(), frame.size(), uri, flags, msg_id,
                             sender, msg_hash, payload)) {
        return false;
    }
    auto proxy = find_local_proxy(uri);
    auto* local = proxy ? proxy->local_actor() : nullptr;
    if (!local) {
        return false;
    }
    message_envelope env;
    env.msg_type = msg_hash;
    env.correlation_id = msg_id;
    env.sender_node_id = sender;
    env.flags = flags;
    env.reply_conn_id = reply_conn;
    env.assign_vector(std::move(payload));
    return local->push_envelope(std::move(env));
}

inline void actor_system::try_enqueue_receiver(uint64_t sender, uint64_t msg_id) {
    if (!m_rcv || m_hold_handler.load(std::memory_order_acquire)) {
        return;
    }
    if (m_rcv->executed(sender, msg_id) || !m_rcv->persisted(sender, msg_id)) {
        return;
    }
    if (m_rcv->enqueued(sender, msg_id)) {
        return;
    }
    auto room = dedup_room_for(sender, msg_id);
    if (room == dedup_room::full) {
        return;
    }
    if (room == dedup_room::already) {
        m_rcv->set_enqueued(sender, msg_id, true);
        return;
    }
    auto frame = m_rcv->copy_frame(sender, msg_id);
    if (frame.empty()) {
        return;
    }
    m_rcv->set_enqueued(sender, msg_id, true);
    if (!push_routed_frame(frame, m_rcv->reply_conn(sender, msg_id))) {
        m_rcv->set_enqueued(sender, msg_id, false);
    }
}

inline void actor_system::pump_receiver_waiting() {
    if (!m_rcv || m_hold_handler.load(std::memory_order_acquire)) {
        return;
    }
    auto keys = m_rcv->waiting_keys();
    for (const auto& id : keys) {
        try_enqueue_receiver(id.sender, id.msg_id);
    }
}

inline void actor_system::apply_receiver_flush(receiver_log::flush_result result) {
    if (!result.ok) {
        return;
    }
    if (!m_hold_handler.load(std::memory_order_acquire)) {
        for (const auto& id : result.data_keys) {
            try_enqueue_receiver(id.sender, id.msg_id);
        }
    }
    for (const auto& ack : result.acks) {
        post_remote_ack(ack.reply_conn_id, ack.msg_id);
    }
    pump_receiver_waiting();
}

inline void actor_system::accept_logged_routed(const std::vector<uint8_t>& frame,
                                               uint64_t sender, uint64_t msg_id,
                                               uint64_t reply_conn) {
    if (!m_rcv) {
        return;
    }
    if (m_rcv->executed(sender, msg_id)) {
        note_wire_skip();
        if (m_rcv->handled_durable(sender, msg_id)) {
            post_remote_ack(reply_conn, msg_id);
        } else {
            m_rcv->buffer_handled(sender, msg_id, reply_conn);
            apply_receiver_flush(m_rcv->flush_ready());
        }
        return;
    }
    if (m_rcv->has_data(sender, msg_id)) {
        if (!m_hold_handler.load(std::memory_order_acquire)
            && m_rcv->persisted(sender, msg_id)
            && !m_rcv->enqueued(sender, msg_id)) {
            try_enqueue_receiver(sender, msg_id);
        }
        return;
    }
    m_rcv->buffer_data(sender, msg_id, frame, reply_conn);
    apply_receiver_flush(m_rcv->flush_ready());
}

inline void actor_system::replay_receiver_for(const actor_uri& uri) {
    if (!m_rcv || m_hold_handler.load(std::memory_order_acquire)) {
        return;
    }
    auto items = m_rcv->take_for_actor(uri.type, uri.name);
    for (auto& item : items) {
        if (!push_routed_frame(item.frame, 0)) {
            m_rcv->set_enqueued(item.sender, item.msg_id, false);
        }
    }
}

inline ynet::async::Task<void> actor_system::interruptible_sleep(
        std::chrono::milliseconds total) {
    auto left = total;
    while (left.count() > 0) {
        if (!m_shutdown || m_shutdown->is_shutdown()) {
            co_return;
        }
        auto slice = left > std::chrono::milliseconds(50)
            ? std::chrono::milliseconds(50)
            : left;
        co_await ynet::async::io::sleep_for(slice);
        left -= slice;
    }
}

inline ynet::async::Task<void> actor_system::ack_timeout_loop() {
    while (m_shutdown && !m_shutdown->is_shutdown()) {
        co_await interruptible_sleep(std::chrono::milliseconds(200));
        auto limit = std::chrono::milliseconds(m_cfg.remote_ack_timeout_ms);
        auto now = std::chrono::steady_clock::now();
        struct due_item {
            net::node_id_t node;
            std::vector<uint8_t> frame;
        };
        std::vector<due_item> due;
        {
            std::lock_guard<std::mutex> lock(m_unacked_mu);
            for (auto& [id, item] : m_unacked) {
                (void)id;
                if (item.needs_resend) {
                    continue;
                }
                if (now - item.sent_at >= limit) {
                    due.push_back(due_item{item.node, item.frame});
                    item.sent_at = now;
                }
            }
        }
        if (due.empty()) {
            continue;
        }
        m_ack_timeout_resend.fetch_add(due.size(), std::memory_order_relaxed);
        for (auto& item : due) {
            std::shared_ptr<net::outbound_conn> conn;
            {
                std::lock_guard<std::mutex> lock(m_conn_mutex);
                auto it = m_connections.find(item.node);
                if (it != m_connections.end()) {
                    conn = it->second.conn;
                }
            }
            if (conn && conn->is_valid() && m_pool) {
                conn->post_frame(std::move(item.frame), m_pool.get());
            }
        }
    }
    co_return;
}

inline std::shared_ptr<actor_proxy> actor_system::find_local_proxy(const actor_uri& uri) {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto it = m_registry.find(uri.to_string());
    if (it != m_registry.end()) {
        return it->second;
    }
    for (auto& [key, proxy] : m_registry) {
        (void)key;
        if (!proxy) {
            continue;
        }
        const actor_uri& have = proxy->uri();
        if (have.type == uri.type && have.name == uri.name) {
            return proxy;
        }
    }
    return nullptr;
}

inline std::shared_ptr<actor_system::ask_slot> actor_system::begin_ask(uint64_t id) {
    auto slot = std::make_shared<ask_slot>();
    std::lock_guard<std::mutex> lock(m_ask_mu);
    m_asks[id] = slot;
    return slot;
}

inline void actor_system::cancel_ask(uint64_t id) {
    std::lock_guard<std::mutex> lock(m_ask_mu);
    m_asks.erase(id);
}

inline bool actor_system::finish_ask(uint64_t id, std::vector<uint8_t> payload) {
    std::shared_ptr<ask_slot> slot;
    {
        std::lock_guard<std::mutex> lock(m_ask_mu);
        auto it = m_asks.find(id);
        if (it == m_asks.end()) {
            return false;
        }
        slot = it->second;
        m_asks.erase(it);
    }
    {
        std::lock_guard<std::mutex> lock(slot->mu);
        slot->payload = std::move(payload);
        slot->done = true;
    }
    slot->cv.notify_one();
    return true;
}

inline uint64_t actor_system::prepare_ask() {
    uint64_t id = alloc_msg_id();
    begin_ask(id);
    return id;
}

inline bool actor_system::take_ask_payload(
        uint64_t id, std::chrono::milliseconds timeout, std::vector<uint8_t>& out) {
    std::shared_ptr<ask_slot> slot;
    {
        std::lock_guard<std::mutex> lock(m_ask_mu);
        auto it = m_asks.find(id);
        if (it == m_asks.end()) {
            return false;
        }
        slot = it->second;
    }
    std::unique_lock<std::mutex> lock(slot->mu);
    if (!slot->cv.wait_for(lock, timeout, [&slot]() { return slot->done; })) {
        lock.unlock();
        cancel_ask(id);
        return false;
    }
    out = std::move(slot->payload);
    return true;
}

inline actor_system::dedup_view actor_system::peek_dedup(
        uint64_t sender_node_id, uint64_t msg_id) {
    std::lock_guard<std::mutex> lock(m_dedup_mu);
    auto it = m_dedup.find(dedup_key{sender_node_id, msg_id});
    if (it == m_dedup.end()) {
        return dedup_view::absent;
    }
    if (it->second == dedup_state::completed) {
        return dedup_view::completed;
    }
    return dedup_view::in_progress;
}

inline actor_system::dedup_claim actor_system::claim_remote(
        uint64_t sender_node_id, uint64_t msg_id) {
    bool log_executed = false;
    bool log_handled = false;
    if (m_rcv) {
        log_executed = m_rcv->executed(sender_node_id, msg_id);
        log_handled = m_rcv->handled_durable(sender_node_id, msg_id);
    }
    std::lock_guard<std::mutex> lock(m_dedup_mu);
    dedup_key key{sender_node_id, msg_id};
    auto it = m_dedup.find(key);
    if (it != m_dedup.end()) {
        if (it->second == dedup_state::completed) {
            return dedup_claim::completed;
        }
        return dedup_claim::busy;
    }
    if (log_executed && log_handled) {
        return dedup_claim::completed;
    }
    if (log_executed) {
        return dedup_claim::busy;
    }
    if (m_dedup.size() >= m_cfg.dedup_cap) {
        bool found = false;
        dedup_key victim{};
        for (const auto& entry : m_dedup) {
            if (entry.second != dedup_state::completed) {
                continue;
            }
            if (!found
                || entry.first.msg_id < victim.msg_id
                || (entry.first.msg_id == victim.msg_id
                    && entry.first.sender < victim.sender)) {
                found = true;
                victim = entry.first;
            }
        }
        if (!found) {
            m_dedup_overflow.fetch_add(1, std::memory_order_relaxed);
            return dedup_claim::overflow;
        }
        m_dedup.erase(victim);
        m_dedup_evict.fetch_add(1, std::memory_order_relaxed);
    }
    m_dedup.emplace(key, dedup_state::in_progress);
    return dedup_claim::fresh;
}

inline void actor_system::post_remote_ack(uint64_t reply_conn_id, uint64_t msg_id) {
    if (reply_conn_id == 0 || !m_transport || !m_pool) {
        return;
    }
    m_transport->post_inbound(reply_conn_id, dist::pack_ack(msg_id), m_pool.get());
}

inline void actor_system::finish_remote_delivery(
        uint64_t sender_node_id, uint64_t msg_id, uint64_t reply_conn_id,
        deliver_result result) {
    bool ack = false;
    {
        std::lock_guard<std::mutex> lock(m_dedup_mu);
        dedup_key key{sender_node_id, msg_id};
        auto it = m_dedup.find(key);
        if (result == deliver_result::threw) {
            if (it != m_dedup.end()) {
                m_dedup.erase(it);
            }
        } else {
            if (it != m_dedup.end()) {
                it->second = dedup_state::completed;
            } else if (m_dedup.size() < m_cfg.dedup_cap) {
                m_dedup.emplace(key, dedup_state::completed);
            }
            ack = true;
        }
    }
    if (result == deliver_result::threw) {
        if (m_rcv) {
            m_rcv->drop_key(sender_node_id, msg_id);
        }
        return;
    }
    if (m_rcv) {
        m_rcv->mark_executed(sender_node_id, msg_id);
        if (!m_rcv->handled_durable(sender_node_id, msg_id)) {
            m_rcv->buffer_handled(sender_node_id, msg_id, reply_conn_id);
            apply_receiver_flush(m_rcv->flush_ready());
        } else if (ack) {
            post_remote_ack(reply_conn_id, msg_id);
        }
        pump_receiver_waiting();
        return;
    }
    if (ack) {
        post_remote_ack(reply_conn_id, msg_id);
    }
}

inline void actor_base::dispatch_envelope(message_envelope env) {
    struct clear_tls {
        ~clear_tls() { g_deliver_tls = {}; }
    } guard;
    g_deliver_tls.correlation_id = env.correlation_id;
    g_deliver_tls.reply_conn_id = env.reply_conn_id;
    g_deliver_tls.flags = env.flags;
    if (env.correlation_id == 0 || m_system == nullptr) {
        deliver(env.msg_type, env.bytes(), env.size());
        return;
    }
    auto claim = m_system->claim_remote(env.sender_node_id, env.correlation_id);
    if (claim == actor_system::dedup_claim::completed) {
        m_system->note_wire_skip();
        m_system->post_remote_ack(env.reply_conn_id, env.correlation_id);
        return;
    }
    if (claim == actor_system::dedup_claim::busy) {
        m_system->note_wire_skip();
        return;
    }
    if (claim == actor_system::dedup_claim::overflow) {
        m_system->receiver_clear_queued(env.sender_node_id, env.correlation_id);
        return;
    }
    auto result = deliver(env.msg_type, env.bytes(), env.size());
    m_system->finish_remote_delivery(
        env.sender_node_id, env.correlation_id, env.reply_conn_id, result);
}

inline bool actor_system::dispatch_reply(
        uint64_t reply_conn_id, uint64_t correlation_id, std::vector<uint8_t> payload) {
    if (correlation_id == 0 || payload.empty()) {
        return false;
    }
    if (reply_conn_id == 0) {
        return finish_ask(correlation_id, std::move(payload));
    }
    if (!m_transport || !m_pool) {
        return false;
    }
    auto frame = dist::pack_reply(correlation_id, payload.data(), payload.size());
    return m_transport->post_inbound(reply_conn_id, std::move(frame), m_pool.get());
}

inline bool actor_system::post_original_frame(const std::vector<uint8_t>& frame) {
    actor_uri uri;
    uint8_t flags = 0;
    uint64_t msg_id = 0;
    uint64_t sender_node_id = 0;
    uint64_t msg_hash = 0;
    std::vector<uint8_t> payload;
    if (!dist::unpack_routed(frame.data(), frame.size(), uri, flags, msg_id,
                             sender_node_id, msg_hash, payload)) {
        return false;
    }
    (void)sender_node_id;
    (void)msg_hash;
    (void)flags;
    (void)uri;
    std::shared_ptr<net::outbound_conn> conn;
    net::node_id_t node = 0;
    {
        std::lock_guard<std::mutex> lock(m_conn_mutex);
        for (auto& [id, pooled] : m_connections) {
            if (pooled.conn && pooled.conn->is_valid()) {
                conn = pooled.conn;
                node = id;
                break;
            }
        }
    }
    if (!conn || !m_pool) {
        // 种子拨号不进入连接池。日志帧里的 node 就是接收方，直接拨种子。
        if (m_pool && !m_cfg.seed_nodes.empty() && uri.node != "*") {
            net::node_id_t dest = 0;
            try {
                dest = static_cast<net::node_id_t>(std::stoull(uri.node));
            } catch (...) {
                dest = 0;
            }
            const auto& seed = m_cfg.seed_nodes.front();
            auto colon = seed.rfind(':');
            if (dest != 0 && colon != std::string::npos) {
                bool start = false;
                if (!m_direct_dial.exchange(true, std::memory_order_acq_rel)) {
                    start = true;
                }
                if (start) {
                    std::string host = seed.substr(0, colon);
                    uint16_t port = 0;
                    try {
                        port = static_cast<uint16_t>(
                            std::stoi(seed.substr(colon + 1)));
                    } catch (...) {
                        m_direct_dial.store(false, std::memory_order_release);
                        return false;
                    }
                    m_pool->submit_coroutine(
                        dial_logged_destination(dest, std::move(host), port).release());
                }
            }
        }
        return false;
    }
    auto copy = frame;
    if (!conn->post_frame(std::move(copy), m_pool.get())) {
        return false;
    }
    track_unacked(node, msg_id, frame);
    return true;
}

inline bool remote_accept(actor_proxy* proxy, actor_system* sys,
                          uint64_t hash, const void* data, size_t len,
                          uint8_t flags, bool wait_for_room) {
    if (proxy == nullptr || data == nullptr || len == 0) {
        return false;
    }
    uint64_t id = sys ? sys->alloc_msg_id() : 1;
    if (proxy->try_deliver(hash, data, len, id, flags)) {
        return true;
    }
    if (!wait_for_room || sys == nullptr) {
        return false;
    }
    auto deadline = std::chrono::steady_clock::now()
        + std::chrono::milliseconds(sys->config().remote_accept_timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (proxy->try_deliver(hash, data, len, id, flags)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    return false;
}

template <typename Reply>
bool actor_base::reply(const Reply& msg) {
    if (g_deliver_tls.correlation_id == 0 || (g_deliver_tls.flags & 0x01) == 0) {
        return false;
    }
    if (m_system == nullptr) {
        return false;
    }
    std::vector<uint8_t> bytes;
    if constexpr (serializable_msg<Reply>) {
        bytes = msg.serialize();
    } else {
        static_assert(std::is_trivially_copyable_v<Reply>,
                      "Reply must be trivially copyable or serializable");
        bytes.resize(sizeof(Reply));
        std::memcpy(bytes.data(), &msg, sizeof(Reply));
    }
    if (bytes.empty()) {
        return false;
    }
    return m_system->dispatch_reply(g_deliver_tls.reply_conn_id,
                                    g_deliver_tls.correlation_id,
                                    std::move(bytes));
}

template <typename T>
template <typename Reply, typename Msg>
std::optional<Reply> actor_ref<T>::ask(const Msg& msg, std::chrono::milliseconds timeout) {
    static_assert(serializable_msg<Msg> || std::is_trivially_copyable_v<Msg>,
                  "Message must be trivially copyable or provide serialize()/deserialize()");
    static_assert(serializable_msg<Reply> || std::is_trivially_copyable_v<Reply>,
                  "Reply must be trivially copyable or provide serialize()/deserialize()");
    if (!m_proxy || m_system == nullptr) {
        return std::nullopt;
    }
    uint64_t id = m_system->prepare_ask();
    bool accepted = false;
    uint64_t hash = actor_type_hash<Msg>();
    if (auto* local = m_proxy->local_actor()) {
        message_envelope env = message_envelope::make(msg);
        env.correlation_id = id;
        env.reply_conn_id = 0;
        env.flags = 0x01;
        accepted = local->push_envelope(std::move(env));
    } else if constexpr (serializable_msg<Msg>) {
        auto data = msg.serialize();
        accepted = m_proxy->try_deliver(hash, data.data(), data.size(), id, 0x01);
    } else {
        accepted = m_proxy->try_deliver(hash, &msg, sizeof(msg), id, 0x01);
    }
    if (!accepted) {
        m_system->cancel_ask(id);
        return std::nullopt;
    }
    std::vector<uint8_t> payload;
    if (!m_system->take_ask_payload(id, timeout, payload)) {
        return std::nullopt;
    }
    if constexpr (serializable_msg<Reply>) {
        return Reply::deserialize(payload.data(), payload.size());
    } else {
        if (payload.size() < sizeof(Reply)) {
            return std::nullopt;
        }
        Reply reply{};
        std::memcpy(&reply, payload.data(), sizeof(Reply));
        return reply;
    }
}

} // namespace ynet::actor

// metrics_exporter — embedded HTTP server exposing JSON metrics for the training dashboard.
//
// Runs alongside the PS node, serving GET endpoints at:
//   /api/v1/nodes    — node topology and status
//   /api/v1/training — training progress (steps, throughput, latency, checksum)
//   /api/v1/actors   — per-actor queue depth and message rate
//   /api/v1/network  — per-connection bandwidth stats
//
// Usage:
//   metrics_exporter exporter;
//   exporter.set_nodes_provider([&] { return build_nodes_json(); });
//   exporter.set_training_provider([&] { return build_training_json(); });
//   auto* sched = ExecutionContext::current();
//   sched->submit(exporter.serve(18080, shutdown).release());
#pragma once

#include <string>
#include <functional>
#include <vector>
#include <sstream>
#include <iomanip>
#include <ctime>
#include <cstdio>
#include <atomic>
#include <thread>
#include <poll.h>
#include <cstring>
#include <cerrno>

#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include "ultranet/net/http.hpp"
#include "ultranet/log/logger.hpp"

namespace ynet::actor {

using namespace ynet::async::net::http;

// ── JSON builder helpers (no library dependency) ──────────────────────────

namespace json_detail {

inline void escape(std::string& out, const std::string& s) {
    for (char c : s) {
        switch (c) {
            case '"':  out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;      break;
        }
    }
}

inline std::string timestamp_ms() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now.time_since_epoch()).count();
    return std::to_string(ms);
}

} // namespace json_detail

// ── Metrics exporter ──────────────────────────────────────────────────────

class metrics_exporter : public std::enable_shared_from_this<metrics_exporter> {
public:
    using json_provider = std::function<std::string()>;

    metrics_exporter() = default;
    ~metrics_exporter() { stop(); }

    // ── Setters for each metrics endpoint ──────────────────────────────

    void set_nodes_provider(json_provider fn)    { m_nodes_provider = std::move(fn); }
    void set_training_provider(json_provider fn)  { m_training_provider = std::move(fn); }
    void set_actors_provider(json_provider fn)    { m_actors_provider = std::move(fn); }
    void set_network_provider(json_provider fn)   { m_network_provider = std::move(fn); }

    // ── Snapshot mode ──────────────────────────────────────────────────
    // When providers capture references to stack variables that go out of
    // scope (e.g. after tcp_ps_main returns in keep_metrics_alive mode),
    // store the final state as plain strings so the HTTP endpoints serve
    // stable data even after the source variables are destroyed.

    void store_nodes_snapshot(std::string json)    { m_nodes_snapshot = std::move(json); }
    void store_training_snapshot(std::string json)  { m_training_snapshot = std::move(json); }
    void store_actors_snapshot(std::string json)    { m_actors_snapshot = std::move(json); }
    void store_network_snapshot(std::string json)   { m_network_snapshot = std::move(json); }

    // Replace all providers with snapshot-returning lambdas.  Call this
    // after store_*_snapshot() to switch from live providers to cached data.
    void use_snapshots() {
        if (!m_nodes_snapshot.empty())
            m_nodes_provider = [this] { return m_nodes_snapshot; };
        if (!m_training_snapshot.empty())
            m_training_provider = [this] { return m_training_snapshot; };
        if (!m_actors_snapshot.empty())
            m_actors_provider = [this] { return m_actors_snapshot; };
        if (!m_network_snapshot.empty())
            m_network_provider = [this] { return m_network_snapshot; };
    }

    // ── Query port after bind (0 = auto-assign) ────────────────────────

    int port() const { return m_port; }

    // ── Configuration ───────────────────────────────────────────────

    // Accept int to avoid GCC 13.2 uint16_t coroutine corruption.
    void set_port(int p) { m_serve_port = p; }

    // Signal the accept loop to exit, then join the server thread.
    void stop() {
        m_running.store(false, std::memory_order_release);
        if (m_thread.joinable()) {
            m_thread.join();
        }
    }

    // ── Start the HTTP server on a dedicated thread ──────────────────
    //
    // The metrics server runs on its own std::thread with plain blocking
    // I/O.  This completely isolates it from the io_uring / coroutine
    // thread pool, avoiding all resource contention and cross-thread
    // io_uring Accept failures seen in WSL2.

    void start() {
        int port = static_cast<int>(m_serve_port);

        int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd < 0) {
            ULTRA_LOG_ERROR("[metrics] socket() failed: {}", strerror(errno));
            return;
        }

        int opt = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in server_addr{};
        server_addr.sin_family      = AF_INET;
        server_addr.sin_port        = htons(port);
        server_addr.sin_addr.s_addr = INADDR_ANY;

        if (::bind(listen_fd, reinterpret_cast<sockaddr*>(&server_addr),
                   sizeof(server_addr)) < 0) {
            ULTRA_LOG_ERROR("[metrics] bind failed: {}", strerror(errno));
            ::close(listen_fd);
            return;
        }
        if (::listen(listen_fd, 16) < 0) {
            ULTRA_LOG_ERROR("[metrics] listen failed: {}", strerror(errno));
            ::close(listen_fd);
            return;
        }

        sockaddr_in bound_addr{};
        socklen_t addr_len = sizeof(bound_addr);
        if (getsockname(listen_fd, reinterpret_cast<sockaddr*>(&bound_addr),
                        &addr_len) == 0) {
            m_port = ntohs(bound_addr.sin_port);
        } else {
            m_port = port;
        }
        ULTRA_LOG_INFO("[metrics] exporter listening on :{}", m_port);

        m_listen_fd.store(listen_fd, std::memory_order_release);

        // Launch a dedicated thread for the accept loop.
        auto self = shared_from_this();
        m_thread = std::thread([self, listen_fd]() {
            while (self->m_running.load(std::memory_order_acquire)) {
                struct pollfd pfd;
                pfd.fd = listen_fd;
                pfd.events = POLLIN;
                int ret = ::poll(&pfd, 1, 200);  // 200ms timeout
                if (ret < 0 && errno != EINTR) {
                    break;
                }
                if (ret == 0) {
                    continue;  // timeout — check m_running
                }

                int client_fd = ::accept(listen_fd, nullptr, nullptr);
                if (client_fd < 0) {
                    if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                    break;
                }

                // Handle client synchronously on this thread.
                self->handle_client_sync(client_fd);
            }
            ::close(listen_fd);
            self->m_listen_fd.store(-1, std::memory_order_release);
        });

        ULTRA_LOG_INFO("[metrics] exporter started");
    }

    // ── Synchronous HTTP request handler (runs on metrics thread) ────

    void handle_client_sync(int client_fd) {
        char read_buf[8192];
        ssize_t n = ::recv(client_fd, read_buf, sizeof(read_buf) - 1, 0);
        if (n <= 0) {
            ::close(client_fd);
            return;
        }
        read_buf[n] = '\0';

        // Parse HTTP request.
        HttpRequest req;
        size_t consumed = req.parse(read_buf, static_cast<size_t>(n));
        if (consumed == 0) {
            send_response_sync(client_fd, 400, "Bad Request",
                               R"({"error":"failed to parse request"})");
            ::close(client_fd);
            return;
        }

        // Route to handler.
        std::string body;
        int status_code = 200;
        std::string status_msg = "OK";

        if (req.path == "/api/v1/nodes" && m_nodes_provider) {
            body = m_nodes_provider();
        } else if (req.path == "/api/v1/training" && m_training_provider) {
            body = m_training_provider();
        } else if (req.path == "/api/v1/actors" && m_actors_provider) {
            body = m_actors_provider();
        } else if (req.path == "/api/v1/network" && m_network_provider) {
            body = m_network_provider();
        } else if (req.path == "/health") {
            body = R"({"status":"ok"})";
        } else {
            status_code = 404;
            status_msg = "Not Found";
            body = R"({"error":"endpoint not found"})";
        }

        send_response_sync(client_fd, status_code, status_msg, body);
        ::close(client_fd);
    }

    void send_response_sync(int fd, int code, const std::string& msg,
                            const std::string& body) {
        HttpResponse resp;
        resp.status_code = code;
        resp.status_message = msg;
        resp.http_version = "HTTP/1.1";
        resp.body = body;
        resp.headers.push_back({"Content-Type", "application/json"});
        resp.headers.push_back({"Access-Control-Allow-Origin", "*"});
        resp.headers.push_back({"Connection", "close"});

        std::string data = resp.serialize();
        ::send(fd, data.data(), data.size(), MSG_NOSIGNAL);
    }

private:
    int m_port = 0;
    int m_serve_port = 0;
    std::atomic<bool> m_running{true};
    std::atomic<int> m_listen_fd{-1};
    std::thread m_thread;
    json_provider m_nodes_provider;
    // Cached snapshots for keep-alive mode (survive after source variables die).
    std::string m_nodes_snapshot;
    std::string m_training_snapshot;
    std::string m_actors_snapshot;
    std::string m_network_snapshot;
    json_provider m_training_provider;
    json_provider m_actors_provider;
    json_provider m_network_provider;

};

// ── Convenience: pre-built JSON providers for bench_metrics ───────────────

// Build a /api/v1/nodes JSON response for a PS + N workers.
// Each worker is identified by (host, port).
inline std::string build_nodes_json(
    const std::string& ps_host, uint16_t ps_port,
    const std::vector<std::pair<std::string, uint16_t>>& workers,
    const std::vector<bool>& worker_online)
{
    std::string json;
    json = "[\n";

    // PS node.
    json += "  {\"node_id\":\"ps-0\",\"address\":\"" + ps_host + ":" +
            std::to_string(ps_port) + "\",\"status\":\"online\",";
    json += "\"cpu_percent\":0.0,\"memory_kb\":0,\"uptime_seconds\":0},\n";

    // Worker nodes.
    for (size_t i = 0; i < workers.size(); ++i) {
        bool online = (i < worker_online.size()) ? worker_online[i] : true;
        json += "  {\"node_id\":\"worker-" + std::to_string(i) + "\",";
        json += "\"address\":\"" + workers[i].first + ":" +
                std::to_string(workers[i].second) + "\",";
        json += "\"status\":\"" + std::string(online ? "online" : "offline") + "\",";
        json += "\"cpu_percent\":0.0,\"memory_kb\":0,\"uptime_seconds\":0}";
        if (i + 1 < workers.size()) {
            json += ",";
        }
        json += "\n";
    }

    json += "]\n";
    return json;
}

// Build a /api/v1/training JSON response from a bench_metrics snapshot.
struct training_snapshot_data {
    int64_t  timestamp_ms = 0;
    int      total_steps   = 0;
    int64_t  total_bytes   = 0;
    double   throughput_mbps = 0.0;
    double   latency_p50_ms  = 0.0;
    double   latency_p99_ms  = 0.0;
    uint32_t checksum = 0;
    int      num_workers = 0;

    struct worker_data {
        int    id = 0;
        int    steps = 0;
        double avg_latency_ms = 0.0;
        double p99_latency_ms = 0.0;
    };
    std::vector<worker_data> workers;
};

inline std::string build_training_json(const training_snapshot_data& snap) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3);

    // Read timestamp from the snapshot or generate current.
    int64_t ts = snap.timestamp_ms;
    if (ts == 0) {
        ts = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }

    oss << "{\n";
    oss << "  \"timestamp_ms\": " << ts << ",\n";
    oss << "  \"total_steps\": " << snap.total_steps << ",\n";
    oss << "  \"total_bytes\": " << snap.total_bytes << ",\n";
    oss << "  \"throughput_mbps\": " << std::setprecision(2)
        << snap.throughput_mbps << ",\n";
    oss << "  \"latency_p50_ms\": " << snap.latency_p50_ms << ",\n";
    oss << "  \"latency_p99_ms\": " << snap.latency_p99_ms << ",\n";
    oss << "  \"checksum\": \"0x" << std::hex << snap.checksum << std::dec << "\",\n";
    oss << "  \"workers\": [\n";

    for (size_t i = 0; i < snap.workers.size(); ++i) {
        const auto& w = snap.workers[i];
        oss << "    {\"id\": " << w.id
            << ", \"steps\": " << w.steps
            << ", \"avg_latency_ms\": " << w.avg_latency_ms
            << ", \"p99_latency_ms\": " << w.p99_latency_ms << "}";
        if (i + 1 < snap.workers.size()) {
            oss << ",";
        }
        oss << "\n";
    }

    oss << "  ]\n";
    oss << "}\n";
    return oss.str();
}

// Build a /api/v1/actors JSON response from actor status data.
struct actor_status_data {
    std::string name;
    int         queue_size = 0;
    double      msg_rate_per_sec = 0.0;
    uint64_t    total_messages = 0;
    std::string state = "running";  // running, idle, blocked
};

inline std::string build_actors_json(
    const std::vector<actor_status_data>& actors)
{
    std::ostringstream oss;
    oss << "[\n";
    for (size_t i = 0; i < actors.size(); ++i) {
        const auto& a = actors[i];
        oss << "  {\"name\": \"" << a.name << "\""
            << ", \"queue_size\": " << a.queue_size
            << ", \"msg_rate_per_sec\": " << a.msg_rate_per_sec
            << ", \"total_messages\": " << a.total_messages
            << ", \"state\": \"" << a.state << "\"}";
        if (i + 1 < actors.size()) {
            oss << ",";
        }
        oss << "\n";
    }
    oss << "]\n";
    return oss.str();
}

// Build a /api/v1/network JSON response.
struct network_stats_data {
    std::string peer;
    int64_t     bytes_sent = 0;
    int64_t     bytes_recv = 0;
    double      send_rate_mbps = 0.0;
    double      recv_rate_mbps = 0.0;
    int         active_connections = 0;
};

inline std::string build_network_json(
    const std::vector<network_stats_data>& stats)
{
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << "[\n";
    for (size_t i = 0; i < stats.size(); ++i) {
        const auto& s = stats[i];
        oss << "  {\"peer\": \"" << s.peer << "\""
            << ", \"bytes_sent\": " << s.bytes_sent
            << ", \"bytes_recv\": " << s.bytes_recv
            << ", \"send_rate_mbps\": " << s.send_rate_mbps
            << ", \"recv_rate_mbps\": " << s.recv_rate_mbps
            << ", \"active_connections\": " << s.active_connections << "}";
        if (i + 1 < stats.size()) {
            oss << ",";
        }
        oss << "\n";
    }
    oss << "]\n";
    return oss.str();
}

} // namespace ynet::actor

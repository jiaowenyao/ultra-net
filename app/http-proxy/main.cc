// HTTP Reverse Proxy with connection pooling.
//
// Forwards HTTP requests to a backend server and relays responses back.
// Supports HTTP/1.1 keep-alive.
//
// Design:
//   - Shared backend connection pool — no per-client backend connections
//   - Per-request borrow/return cycle — backend connections held briefly
//   - SlidingBuffer avoids O(N) erase on every request
//   - PooledConnection RAII guard for leak-free return
//   - Error retry: on backend failure, invalidate and retry once
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 http-proxy
// Run:
//   ./bin/http-proxy [listen_port] [backend_host] [backend_port]
//   ULTRANET_POOL_THREADS=8 ULTRANET_CONN_POOL_MAX=64 ./bin/http-proxy 8080 127.0.0.1 9000

#include "ultranet/ultranet.h"

#include <iostream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <csignal>
#include <atomic>
#include <netinet/in.h>
#include <netinet/tcp.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

namespace {

constexpr size_t kDefaultMaxBuf = 256 * 1024;
constexpr size_t kDefaultMaxConns = 512;

// --- Runtime-configurable helpers ---

size_t get_max_buf() {
    if (const char* v = std::getenv("ULTRANET_PROXY_MAX_BUF"))
        return std::max<size_t>(static_cast<size_t>(std::atoll(v)), 4096);
    return kDefaultMaxBuf;
}

size_t get_max_conns_from_env() {
    if (const char* v = std::getenv("ULTRANET_PROXY_MAX_CONNS"))
        return static_cast<size_t>(std::atoll(v));
    return kDefaultMaxConns;
}

std::chrono::milliseconds get_backend_timeout() {
    if (const char* v = std::getenv("ULTRANET_PROXY_BACKEND_TIMEOUT_MS"))
        return std::chrono::milliseconds(std::atoll(v));
    return std::chrono::seconds(30);
}

int64_t get_content_length(const std::vector<http::Header>& headers) {
    for (const auto& h : headers) {
        std::string lower = h.name;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (lower == "content-length") return std::stoll(h.value);
    }
    return -1;
}

bool request_keepalive(const http::HttpRequest& req) {
    auto conn = req.header("connection");
    if (conn.empty()) return req.http_version == "HTTP/1.1";
    std::string lower(conn);
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (lower.find("close") != std::string::npos) return false;
    return req.http_version == "HTTP/1.1";
}

// --- SlidingBuffer ---
// Avoids O(N) std::vector::erase() churn.

struct SlidingBuffer {
    std::vector<uint8_t> data;
    size_t offset = 0;

    SlidingBuffer() {
        data.reserve(16384);
    }

    void append(const uint8_t* ptr, size_t len) {
        data.insert(data.end(), ptr, ptr + len);
    }

    const uint8_t* head() const { return data.data() + offset; }
    size_t remaining() const { return data.size() - offset; }

    void consume(size_t n) {
        offset += n;
        if (offset > data.size() / 2) {
            data.erase(data.begin(), data.begin() + offset);
            offset = 0;
        }
    }

    void reset() {
        data.clear();
        offset = 0;
    }
};

// --- PooledConnection ---
// RAII guard: returns the backend connection to the pool on destruction.

class PooledConnection {
public:
    PooledConnection() = default;

    ~PooledConnection() {
        if (m_pool && m_socket.is_valid()) {
            if (m_invalid) {
                m_pool->invalidate(std::move(m_socket));
            } else {
                m_pool->release(std::move(m_socket));
            }
        }
    }

    PooledConnection(PooledConnection&& other) noexcept
        : m_pool(other.m_pool), m_socket(std::move(other.m_socket)),
          m_invalid(other.m_invalid) {
        other.m_pool = nullptr;
    }

    PooledConnection& operator=(PooledConnection&& other) noexcept {
        if (this != &other) {
            // Destroy current held socket first.
            if (m_pool && m_socket.is_valid()) {
                if (m_invalid) m_pool->invalidate(std::move(m_socket));
                else m_pool->release(std::move(m_socket));
            }
            m_pool = other.m_pool;
            m_socket = std::move(other.m_socket);
            m_invalid = other.m_invalid;
            other.m_pool = nullptr;
        }
        return *this;
    }

    Task<bool> acquire(ConnectionPool& pool) {
        if (m_socket.is_valid()) co_return true;
        try {
            m_socket = co_await pool.acquire();
            m_pool = &pool;
            m_invalid = false;
            co_return true;
        } catch (const std::system_error&) {
            co_return false;
        }
    }

    TcpSocket& socket() { return m_socket; }
    int fd() const { return m_socket.fd(); }
    bool is_valid() const { return m_socket.is_valid(); }
    void mark_invalid() { m_invalid = true; }

private:
    ConnectionPool* m_pool = nullptr;
    TcpSocket m_socket;
    bool m_invalid = false;
};

// --- Proxy session ---

Task<void> proxy_session(int client_fd, ConnectionPool& pool,
                         ShutdownCoordinator& shutdown,
                         std::atomic<size_t>& active_conns,
                         size_t max_buf,
                         std::chrono::milliseconds backend_timeout) {
    // RAII connection tracker: decrements on scope exit.
    struct ConnTracker {
        std::atomic<size_t>& counter;
        bool armed = true;
        ~ConnTracker() { if (armed) counter.fetch_sub(1, std::memory_order_relaxed); }
    };
    ConnTracker tracker{active_conns};

    SlidingBuffer client_buf;
    SlidingBuffer backend_buf;
    uint8_t io_buf[16384];

    // ENOBUFS retry: cooperative backoff when io_uring is saturated.
    constexpr int MAX_ENOBUFS_RETRIES = 10;
    constexpr auto ENOBUFS_SLEEP = std::chrono::milliseconds(10);

    auto forward_one = [&](PooledConnection& conn,
                           size_t total_req) -> Task<bool> {
        // Forward request to backend.
        size_t written = 0;
        const uint8_t* req_data = client_buf.head();
        while (written < total_req) {
            auto w = co_await conn.socket().write(req_data + written,
                                                   total_req - written);
            if (w) {
                written += *w;
                continue;
            }
            if (w.error().value() == ENOBUFS) {
                co_await sleep_for(ENOBUFS_SLEEP);
                continue;
            }
            conn.mark_invalid();
            co_return false;
        }

        // Read response from backend.
        http::HttpResponse resp;
        size_t resp_consumed = 0;
        int enobufs = 0;
        while (resp_consumed == 0) {
            resp_consumed = resp.parse(
                reinterpret_cast<const char*>(backend_buf.head()),
                backend_buf.remaining());
            if (resp_consumed > 0) break;

            Read reader(conn.fd(), io_buf, sizeof(io_buf));
            reader.with_timeout(backend_timeout);
            auto n = co_await reader;
            if (n) {
                if (*n == 0) { conn.mark_invalid(); co_return false; }
                backend_buf.append(io_buf, *n);
                if (backend_buf.data.size() > max_buf) {
                    conn.mark_invalid();
                    co_return false;
                }
                continue;
            }
            if (n.error().value() == ENOBUFS && enobufs < MAX_ENOBUFS_RETRIES) {
                ++enobufs;
                co_await sleep_for(ENOBUFS_SLEEP);
                continue;
            }
            conn.mark_invalid();
            co_return false;
        }

        // Forward response to client.
        size_t fwd = 0;
        const uint8_t* resp_data = backend_buf.head();
        while (fwd < resp_consumed) {
            Write client_writer(client_fd, resp_data + fwd,
                                resp_consumed - fwd);
            auto w = co_await client_writer;
            if (w) { fwd += *w; continue; }
            if (w.error().value() == ENOBUFS) {
                co_await sleep_for(ENOBUFS_SLEEP);
                continue;
            }
            co_return false;
        }

        backend_buf.consume(resp_consumed);
        if (backend_buf.remaining() == 0) backend_buf.reset();

        if (!resp.is_keepalive()) conn.mark_invalid();
        co_return true;
    };

    while (!shutdown.is_shutdown()) {
        // --- Read request from client ---
        http::HttpRequest req;
        size_t consumed = 0;
        int enobufs = 0;
        while (consumed == 0) {
            Read reader(client_fd, io_buf, sizeof(io_buf));
            reader.with_timeout(std::chrono::seconds(10));
            auto n = co_await reader;
            if (n) {
                if (*n == 0) { co_await Close(client_fd); co_return; }
                client_buf.append(io_buf, *n);
                if (client_buf.data.size() > max_buf) {
                    co_await Close(client_fd);
                    co_return;
                }
                consumed = req.parse(
                    reinterpret_cast<const char*>(client_buf.head()),
                    client_buf.remaining());
                continue;
            }
            if (n.error().value() == ENOBUFS && enobufs < MAX_ENOBUFS_RETRIES) {
                ++enobufs;
                co_await sleep_for(ENOBUFS_SLEEP);
                continue;
            }
            co_await Close(client_fd);
            co_return;
        }

        // Wait for body bytes.
        int64_t body_len = get_content_length(req.headers);
        size_t total_req = consumed +
            (body_len > 0 ? static_cast<size_t>(body_len) : 0);

        enobufs = 0;
        while (client_buf.remaining() < total_req) {
            Read reader(client_fd, io_buf, sizeof(io_buf));
            reader.with_timeout(std::chrono::seconds(30));
            auto n = co_await reader;
            if (n) {
                if (*n == 0) { co_await Close(client_fd); co_return; }
                client_buf.append(io_buf, *n);
                if (client_buf.data.size() > max_buf) {
                    co_await Close(client_fd);
                    co_return;
                }
                continue;
            }
            if (n.error().value() == ENOBUFS && enobufs < MAX_ENOBUFS_RETRIES) {
                ++enobufs;
                co_await sleep_for(ENOBUFS_SLEEP);
                continue;
            }
            co_await Close(client_fd);
            co_return;
        }

        // --- Forward to backend via pool ---
        PooledConnection conn;
        if (!(co_await conn.acquire(pool))) {
            co_await Close(client_fd);
            co_return;
        }

        bool ok = co_await forward_one(conn, total_req);

        // On failure, retry once with a fresh connection.
        if (!ok) {
            backend_buf.reset();

            PooledConnection retry_conn;
            if (!(co_await retry_conn.acquire(pool))) {
                co_await Close(client_fd);
                co_return;
            }
            conn = std::move(retry_conn);

            ok = co_await forward_one(conn, total_req);
            if (!ok) {
                co_await Close(client_fd);
                co_return;
            }
        }

        client_buf.consume(total_req);
        if (client_buf.remaining() == 0) client_buf.reset();

        if (!request_keepalive(req)) break;
    }

    co_await Close(client_fd);
}

// --- Proxy server ---

Task<void> proxy_server(int listen_port, const std::string& backend_host,
                        uint16_t backend_port, ShutdownCoordinator& shutdown) {
    auto env_cfg = ynet::config::UltraNetConfig::from_env();
    const size_t max_conns = get_max_conns_from_env();
    const size_t max_buf = get_max_buf();
    const auto backend_timeout = get_backend_timeout();
    std::atomic<size_t> active_conns{0};

    ConnectionPool pool(env_cfg.connection_pool, backend_host, backend_port);

    // Pre-warm the pool with min_connections.
    std::cout << "Pre-warming pool with " << env_cfg.connection_pool.min_connections
              << " connections..." << std::endl;
    co_await pool.pre_warm();
    auto warmup_stats = pool.snapshot();
    std::cout << "Pool warmed: " << warmup_stats.total_connections
              << " connections" << std::endl;

    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int fd = *sock;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    co_await Listen(fd, 512);

    // Delay accept until client data arrives (reduces wakeups for SYN-only connections).
    int defer_accept = 30;  // seconds
    setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &defer_accept, sizeof(defer_accept));

    std::cout << "HTTP proxy listening on :" << listen_port
              << " -> " << backend_host << ":" << backend_port
              << " (max_pool_conns=" << env_cfg.connection_pool.max_connections
              << " max_client_conns=" << max_conns
              << " threads=" << env_cfg.pool.num_threads << ")" << std::endl;

    // Exponential backoff state for accept loop.
    constexpr auto BACKOFF_MIN = std::chrono::milliseconds(1);
    constexpr auto BACKOFF_MAX = std::chrono::milliseconds(100);
    auto backoff = BACKOFF_MIN;

    while (!shutdown.is_shutdown()) {
        // Backpressure: throttle accept when SQ ring is saturated.
        auto* engine = IoUringEngine::current();
        if (engine && engine->over_watermark()) {
            co_await sleep_for(backoff);
            backoff = std::min(backoff * 2, BACKOFF_MAX);
            continue;
        }

        // Backpressure: sleep when over connection limit (don't accept+close).
        size_t cur = active_conns.load(std::memory_order_relaxed);
        if (cur >= max_conns) {
            co_await sleep_for(backoff);
            backoff = std::min(backoff * 2, BACKOFF_MAX);
            continue;
        }

        Accept acceptor(fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ETIMEDOUT) continue;
            break;
        }

        backoff = BACKOFF_MIN;

        int client_fd = *client;

        // Latency: disable Nagle and enable quick ACK.
        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
        setsockopt(client_fd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));

        active_conns.fetch_add(1, std::memory_order_relaxed);

        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->submit(proxy_session(client_fd, pool, shutdown, active_conns,
                                       max_buf, backend_timeout).release());
        }
    }

    co_await Close(fd);

    auto stats = pool.snapshot();
    std::cout << "Proxy stopped. Pool: acquired=" << stats.acquired
              << " released=" << stats.released
              << " evicted=" << stats.evicted
              << " failed=" << stats.failed_creates
              << " total_conns=" << stats.total_connections << std::endl;
}

} // namespace

int main(int argc, char* argv[]) {
    std::signal(SIGPIPE, SIG_IGN);

    int listen_port = (argc > 1) ? std::atoi(argv[1]) : 8080;
    std::string backend_host = (argc > 2) ? argv[2] : "127.0.0.1";
    int backend_port = (argc > 3) ? std::atoi(argv[3]) : 9000;

    auto cfg = ynet::config::UltraNetConfig::from_env();

    return Launcher()
        .threads(cfg.pool.num_threads)
        .io_uring_config(cfg.io_uring)
        .run([=](ShutdownCoordinator& shutdown) -> Task<void> {
            co_await proxy_server(listen_port, backend_host,
                                  static_cast<uint16_t>(backend_port), shutdown);
        });
}

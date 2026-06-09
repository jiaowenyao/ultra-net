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
#include <optional>
#include <cerrno>
#include <climits>
#include <strings.h>
#include <cstdio>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

namespace {

// Reduced for low-resource servers (was 256KB).
constexpr size_t kDefaultMaxBuf = 128 * 1024;
constexpr size_t kDefaultMaxConns = 512;
constexpr size_t kIOBufSize = 8192;

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

std::chrono::milliseconds get_idle_timeout() {
    if (const char* v = std::getenv("ULTRANET_PROXY_IDLE_TIMEOUT_MS"))
        return std::chrono::milliseconds(std::atoll(v));
    return std::chrono::seconds(60);
}

size_t get_mem_min_mb() {
    if (const char* v = std::getenv("ULTRANET_PROXY_MEM_MIN_MB"))
        return static_cast<size_t>(std::atoll(v));
    return 128;
}

// Exception-free int64 parser. std::stoll throws — fatal in coroutine context.
std::optional<int64_t> safe_stoll(std::string_view sv) {
    if (sv.empty()) return std::nullopt;
    char buf[32];
    size_t len = sv.size() < sizeof(buf) - 1 ? sv.size() : sizeof(buf) - 1;
    std::memcpy(buf, sv.data(), len);
    buf[len] = '\0';
    char* end = nullptr;
    errno = 0;
    int64_t val = strtoll(buf, &end, 10);
    if (errno == ERANGE || end == buf || *end != '\0') return std::nullopt;
    return val;
}

// Fast case-insensitive comparison using uint64 word comparison.
// The |0x20 trick ORs bit 5 for all-lowercase ASCII reference strings
// (which HTTP header names are), providing correct case-insensitive
// matching with zero false positives and zero function-call overhead.
int64_t get_content_length(const std::vector<http::Header>& headers) {
    for (const auto& h : headers) {
        if (h.name.size() == 14) {
            uint64_t lo, hi, ref_lo, ref_hi;
            std::memcpy(&lo, h.name.data(), 8);
            std::memcpy(&hi, h.name.data() + 6, 8);
            std::memcpy(&ref_lo, "content-", 8);
            std::memcpy(&ref_hi, "t-length", 8);
            constexpr uint64_t kCM = 0x2020202020202020ULL;
            if (((lo | kCM) == (ref_lo | kCM)) &&
                ((hi | kCM) == (ref_hi | kCM))) {
                auto v = safe_stoll(h.value);
                return v.has_value() ? *v : -1;
            }
        }
    }
    return -1;
}

bool request_keepalive(const http::HttpRequest& req) {
    auto conn = req.header("connection");
    if (conn.empty()) return req.http_version == "HTTP/1.1";
    constexpr uint64_t kClose = 0x00000065736F6C63ULL;  // "close\0\0\0" LE
    constexpr uint64_t kCM = 0x2020202020202020ULL;
    for (size_t i = 0; i + 5 <= conn.size(); ++i) {
        uint64_t word = 0;
        size_t n = conn.size() - i;
        if (n > 8) n = 8;
        std::memcpy(&word, conn.data() + i, n);
        if ((word | kCM) == (kClose | kCM)) return false;
    }
    return req.http_version == "HTTP/1.1";
}

// --- SlidingBuffer ---
// Avoids O(N) std::vector::erase() churn.
// Lazy compaction: only erase when offset is large AND remaining data
// is much smaller than the consumed portion.

struct SlidingBuffer {
    std::vector<uint8_t> data;
    size_t offset = 0;

    SlidingBuffer() {
        data.reserve(4096);
    }

    void append(const uint8_t* ptr, size_t len) {
        // Geometric preallocation to avoid repeated reallocations during
        // streaming reads. 1.5x growth with a 4KB floor to handle the
        // zero-capacity edge case (0 * 1.5 = 0).  Saturates at needed
        // when the request is larger than the geometric projection.
        size_t needed = data.size() + len;
        if (needed > data.capacity()) {
            size_t grow = data.capacity() + data.capacity() / 2;
            if (grow < 4096) grow = 4096;
            size_t new_cap = needed > grow ? needed : grow;
            data.reserve(new_cap);
        }
        data.insert(data.end(), ptr, ptr + len);
    }

    const uint8_t* head() const { return data.data() + offset; }
    size_t remaining() const { return data.size() - offset; }

    void consume(size_t n) {
        offset += n;
        if (offset > data.size() / 2 && offset > remaining() * 4) {
            data.erase(data.begin(), data.begin() + offset);
            offset = 0;
        }
    }

    void reset() {
        // Release large buffers between keepalive cycles to bound RSS.
        if (data.capacity() > 131072) {
            std::vector<uint8_t>().swap(data);
        } else {
            data.clear();
        }
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

// --- MemoryGuard ---
// Reads /proc/meminfo periodically to detect memory pressure.
// When available memory drops below min_free_mb, the accept loop
// sleeps to prevent accepting new connections that would risk OOM.

struct MemoryGuard {
    size_t min_free_mb;
    std::chrono::steady_clock::time_point last_check;
    bool critical{false};
    static constexpr auto kCheckInterval = std::chrono::seconds(2);

    explicit MemoryGuard(size_t min_mb) : min_free_mb(min_mb) {}

    bool check() {
        auto now = std::chrono::steady_clock::now();
        if (now - last_check < kCheckInterval) return critical;
        last_check = now;

        auto avail = get_available_memory_mb();
        critical = avail.has_value() && *avail < min_free_mb;
        if (critical) {
            std::cerr << "[MemoryGuard] CRITICAL: " << *avail
                      << "MB available < " << min_free_mb << "MB min" << std::endl;
        }
        return critical;
    }

    static std::optional<size_t> get_available_memory_mb() {
        FILE* f = std::fopen("/proc/meminfo", "r");
        if (!f) return std::nullopt;
        char line[256];
        while (std::fgets(line, sizeof(line), f)) {
            if (strncmp(line, "MemAvailable:", 13) == 0) {
                long kb = 0;
                std::sscanf(line + 13, "%ld", &kb);
                std::fclose(f);
                return static_cast<size_t>(kb / 1024);
            }
        }
        std::fclose(f);
        return std::nullopt;
    }
};

// --- BackoffGate ---
// Global ENOBUFS retry coordination. When many coroutines retry
// simultaneously, the per-coroutine fixed 10ms sleep creates a
// retry storm.  BackoffGate scales backoff with retry_count /
// active_conns ratio, and decays when idle.

struct BackoffGate {
    std::atomic<size_t> retry_count{0};
    std::atomic<int64_t> backoff_us{0};  // 0 = no extra backoff

    static constexpr int64_t kBaseBackoffUs = 10'000;   // 10ms base
    static constexpr int64_t kMaxBackoffUs = 200'000;    // 200ms cap
    static constexpr int64_t kDecayUs = 5'000;            // decay 5ms per step

    void register_retry(size_t active_conns) {
        retry_count.fetch_add(1, std::memory_order_relaxed);
        size_t rc = retry_count.load(std::memory_order_relaxed);
        size_t ac = std::max<size_t>(active_conns, 1);
        int64_t target = static_cast<int64_t>(kBaseBackoffUs *
            (1 + static_cast<double>(rc) / ac));
        if (target > kMaxBackoffUs) target = kMaxBackoffUs;
        // Only increase, never decrease on register.
        int64_t cur = backoff_us.load(std::memory_order_relaxed);
        while (target > cur &&
               !backoff_us.compare_exchange_weak(cur, target, std::memory_order_relaxed));
    }

    void unregister_retry() {
        retry_count.fetch_sub(1, std::memory_order_relaxed);
    }

    // Called from accept loop when no retries are happening.
    void maybe_decay() {
        if (retry_count.load(std::memory_order_relaxed) > 0) return;
        int64_t cur = backoff_us.load(std::memory_order_relaxed);
        if (cur > 0) {
            int64_t next = std::max<int64_t>(0, cur - kDecayUs);
            backoff_us.compare_exchange_strong(cur, next, std::memory_order_relaxed);
        }
    }

    int64_t current_backoff_us() const {
        return backoff_us.load(std::memory_order_relaxed);
    }
};

// RAII guard that registers/unregisters with BackoffGate.
// IMPORTANT: armed defaults to false because the constructor does NOT call
// register_retry(). Callers must set armed=true after calling register_retry().
// The destructor unregisters only when armed, preventing atomic underflow
// on the success path (where no ENOBUFS occurred).
struct RetryGuard {
    BackoffGate& gate;
    bool armed = false;
    explicit RetryGuard(BackoffGate& g) : gate(g) {}
    ~RetryGuard() { if (armed) gate.unregister_retry(); }
    void dismiss() { armed = false; gate.unregister_retry(); }
};

// --- safe_close ---
// io_uring Close can fail (ENOBUFS, ring full). Fall back to ::close()
// to guarantee the fd is released.
//
// Graceful TCP shutdown: calling ::shutdown(fd, SHUT_RDWR) before close
// initiates the FIN handshake. Without this, close() on a socket with
// unread data sends RST, causing "socket read errors" on the peer (wrk).
// The shutdown syscall is non-blocking and always succeeds or is harmless
// on an already-broken fd.

Task<void> safe_close(int fd) {
    if (fd < 0) co_return;
    ::shutdown(fd, SHUT_RDWR);
    auto result = co_await Close(fd);
    if (!result) {
        ::close(fd);
    }
}

// --- LinkedWriteThenRead ---
// Submits write+read SQEs chained with IOSQE_IO_LINK, saving one CQE
// round-trip per request when the entire write fits in one TCP send.

class NullResubmitOp : public IoOperationBase {
public:
    void resubmit() override {}
    void cancel() override {}
};
inline NullResubmitOp s_null_resubmit_op{};

struct LinkedWriteThenRead {
    int m_fd;
    const uint8_t* m_wbuf; size_t m_wlen;
    uint8_t* m_rbuf; size_t m_rlen;
    IoCallback m_cb{};

    LinkedWriteThenRead(int fd, const uint8_t* wbuf, size_t wlen,
                        uint8_t* rbuf, size_t rlen)
        : m_fd(fd), m_wbuf(wbuf), m_wlen(wlen), m_rbuf(rbuf), m_rlen(rlen) {
        m_cb.m_operation = &s_null_resubmit_op;
    }

    bool await_ready() noexcept { return m_cb.m_completed; }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        auto* eng = IoUringEngine::current();
        if (!eng || eng->over_watermark()) {
            m_cb.m_result = -ENOBUFS; m_cb.m_completed = true; h.resume(); return;
        }
        io_uring_sqe *sqe_w = eng->get_sqe(), *sqe_r = eng->get_sqe();
        if (!sqe_w || !sqe_r) {
            m_cb.m_result = -ENOBUFS; m_cb.m_completed = true; h.resume(); return;
        }
        sqe_w->flags |= IOSQE_IO_LINK;
        io_uring_prep_write(sqe_w, m_fd, m_wbuf, m_wlen, 0);
        io_uring_sqe_set_data(sqe_w, nullptr);
        io_uring_prep_recv(sqe_r, m_fd, m_rbuf, m_rlen, 0);
        io_uring_sqe_set_data(sqe_r, &m_cb);
        m_cb.m_handle = h;
        eng->increment_pending(); eng->increment_pending();
        eng->increment_pending_ops();
        eng->submit_now();
    }

    IoResult<size_t> await_resume() noexcept {
        if (m_cb.m_result < 0)
            return std::unexpected(make_io_error(m_cb.m_result));
        return static_cast<size_t>(m_cb.m_result);
    }
};

// --- Proxy session ---

Task<void> proxy_session(int client_fd, ConnectionPool& pool,
                         ShutdownCoordinator& shutdown,
                         std::atomic<size_t>& active_conns,
                         size_t max_buf,
                         std::chrono::milliseconds backend_timeout,
                         std::chrono::milliseconds idle_timeout,
                         BackoffGate& backoff_gate) {
    // RAII connection tracker: decrements on scope exit.
    struct ConnTracker {
        std::atomic<size_t>& counter;
        bool armed = true;
        ~ConnTracker() { if (armed) counter.fetch_sub(1, std::memory_order_relaxed); }
    };
    ConnTracker tracker{active_conns};

    SlidingBuffer client_buf;
    SlidingBuffer backend_buf;
    uint8_t io_buf[kIOBufSize];

    // ENOBUFS retry: cooperative backoff coordinated by BackoffGate.
    constexpr int MAX_ENOBUFS_RETRIES = 10;

    auto enobufs_sleep = [&](RetryGuard& guard) -> Task<void> {
        int64_t us = backoff_gate.current_backoff_us();
        if (us <= 0) us = 10'000;  // fallback 10ms
        co_await sleep_for(std::chrono::microseconds(us));
        // Re-register to keep backoff active across retries.
        guard.dismiss();
        backoff_gate.register_retry(active_conns.load(std::memory_order_relaxed));
        guard.armed = true;
    };

    auto enable_quickack = [&]() {
        int opt = 1;
        setsockopt(client_fd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));
    };

    auto forward_one = [&](PooledConnection& conn,
                           size_t total_req) -> Task<bool> {
        size_t written = 0;
        const uint8_t* req_data = client_buf.head();

        bool linked_ok = false;
        if (total_req > 0 && total_req <= kIOBufSize) {
            LinkedWriteThenRead linked(conn.fd(), req_data, total_req,
                                       io_buf, sizeof(io_buf));
            auto n = co_await linked;
            if (n && *n > 0) {
                written = total_req;
                backend_buf.append(io_buf, static_cast<size_t>(*n));
                linked_ok = true;
            } else if (n.error().value() != ENOBUFS) {
                conn.mark_invalid(); co_return false;
            }
        }

        if (!linked_ok) {
        while (written < total_req) {
            auto w = co_await conn.socket().write(req_data + written,
                                                   total_req - written);
            if (w) {
                written += *w;
                continue;
            }
            if (w.error().value() == ENOBUFS) {
                RetryGuard guard(backoff_gate);
                backoff_gate.register_retry(active_conns.load(std::memory_order_relaxed));
                guard.armed = true;
                for (int retry = 0; retry < MAX_ENOBUFS_RETRIES; ++retry) {
                    co_await enobufs_sleep(guard);
                    auto rw = co_await conn.socket().write(req_data + written,
                                                            total_req - written);
                    if (rw) { written += *rw; break; }
                    if (rw.error().value() != ENOBUFS) {
                        conn.mark_invalid();
                        co_return false;
                    }
                }
                if (written >= total_req) break;
                conn.mark_invalid();
                co_return false;
            }
            conn.mark_invalid();
            co_return false;
        }
        }  // !linked_ok

        // Read response from backend.
        http::HttpResponse resp;
        size_t resp_consumed = 0;
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
            if (n.error().value() == ENOBUFS) {
                RetryGuard guard(backoff_gate);
                backoff_gate.register_retry(active_conns.load(std::memory_order_relaxed));
                guard.armed = true;
                int enobufs = 0;
                while (enobufs < MAX_ENOBUFS_RETRIES) {
                    ++enobufs;
                    co_await enobufs_sleep(guard);
                    Read retry_reader(conn.fd(), io_buf, sizeof(io_buf));
                    retry_reader.with_timeout(backend_timeout);
                    auto rn = co_await retry_reader;
                    if (rn && *rn > 0) {
                        backend_buf.append(io_buf, *rn);
                        if (backend_buf.data.size() > max_buf) {
                            conn.mark_invalid();
                            co_return false;
                        }
                        break;
                    }
                    if (rn.error().value() != ENOBUFS) {
                        conn.mark_invalid();
                        co_return false;
                    }
                }
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
                RetryGuard guard(backoff_gate);
                backoff_gate.register_retry(active_conns.load(std::memory_order_relaxed));
                guard.armed = true;
                for (int retry = 0; retry < MAX_ENOBUFS_RETRIES; ++retry) {
                    co_await enobufs_sleep(guard);
                    Write retry_writer(client_fd, resp_data + fwd,
                                       resp_consumed - fwd);
                    auto rw = co_await retry_writer;
                    if (rw) { fwd += *rw; break; }
                    if (rw.error().value() != ENOBUFS) co_return false;
                }
                if (fwd >= resp_consumed) break;
                co_return false;
            }
            co_return false;
        }

        backend_buf.consume(resp_consumed);
        if (backend_buf.remaining() == 0) backend_buf.reset();

        if (!resp.is_keepalive()) conn.mark_invalid();
        co_return true;
    };

    // Keepalive read timeout: short for first request, longer for idle.
    bool is_first_request = true;

    while (!shutdown.is_shutdown()) {
        // --- Read request from client ---
        http::HttpRequest req;
        size_t consumed = 0;
        {
            RetryGuard guard(backoff_gate);
            while (consumed == 0) {
                Read reader(client_fd, io_buf, sizeof(io_buf));
                // Staged timeout: 10s for first request, configurable for idle keepalive.
                if (is_first_request) {
                    reader.with_timeout(std::chrono::seconds(10));
                } else {
                    reader.with_timeout(idle_timeout);
                }
                auto n = co_await reader;
                if (n) {
                    if (*n == 0) { co_await safe_close(client_fd); co_return; }
                    // Re-enable QUICKACK after each read — it's one-shot.
                    enable_quickack();
                    client_buf.append(io_buf, *n);
                    if (client_buf.data.size() > max_buf) {
                        co_await safe_close(client_fd);
                        co_return;
                    }
                    consumed = req.parse(
                        reinterpret_cast<const char*>(client_buf.head()),
                        client_buf.remaining());
                    continue;
                }
                if (n.error().value() == ENOBUFS) {
                    backoff_gate.register_retry(active_conns.load(std::memory_order_relaxed));
                    guard.armed = true;
                    int enobufs = 0;
                    while (enobufs < MAX_ENOBUFS_RETRIES) {
                        ++enobufs;
                        co_await enobufs_sleep(guard);
                        Read retry_reader(client_fd, io_buf, sizeof(io_buf));
                        if (is_first_request) {
                            retry_reader.with_timeout(std::chrono::seconds(10));
                        } else {
                            retry_reader.with_timeout(idle_timeout);
                        }
                        auto rn = co_await retry_reader;
                        if (rn && *rn > 0) {
                            enable_quickack();
                            client_buf.append(io_buf, *rn);
                            if (client_buf.data.size() > max_buf) {
                                co_await safe_close(client_fd);
                                co_return;
                            }
                            consumed = req.parse(
                                reinterpret_cast<const char*>(client_buf.head()),
                                client_buf.remaining());
                            break;
                        }
                        if (rn.error().value() != ENOBUFS) {
                            co_await safe_close(client_fd);
                            co_return;
                        }
                    }
                    guard.dismiss();
                    continue;
                }
                // ETIMEDOUT or other error — close and release.
                co_await safe_close(client_fd);
                co_return;
            }
        }

        is_first_request = false;

        // Wait for body bytes.
        int64_t body_len = get_content_length(req.headers);
        size_t total_req = consumed +
            (body_len > 0 ? static_cast<size_t>(body_len) : 0);

        {
            RetryGuard guard(backoff_gate);
            while (client_buf.remaining() < total_req) {
                Read reader(client_fd, io_buf, sizeof(io_buf));
                reader.with_timeout(std::chrono::seconds(30));
                auto n = co_await reader;
                if (n) {
                    if (*n == 0) { co_await safe_close(client_fd); co_return; }
                    enable_quickack();
                    client_buf.append(io_buf, *n);
                    if (client_buf.data.size() > max_buf) {
                        co_await safe_close(client_fd);
                        co_return;
                    }
                    continue;
                }
                if (n.error().value() == ENOBUFS) {
                    backoff_gate.register_retry(active_conns.load(std::memory_order_relaxed));
                    guard.armed = true;
                    int enobufs = 0;
                    while (enobufs < MAX_ENOBUFS_RETRIES) {
                        ++enobufs;
                        co_await enobufs_sleep(guard);
                        Read retry_reader(client_fd, io_buf, sizeof(io_buf));
                        retry_reader.with_timeout(std::chrono::seconds(30));
                        auto rn = co_await retry_reader;
                        if (rn && *rn > 0) {
                            enable_quickack();
                            client_buf.append(io_buf, *rn);
                            if (client_buf.data.size() > max_buf) {
                                co_await safe_close(client_fd);
                                co_return;
                            }
                            break;
                        }
                        if (rn.error().value() != ENOBUFS) {
                            co_await safe_close(client_fd);
                            co_return;
                        }
                    }
                    guard.dismiss();
                    continue;
                }
                co_await safe_close(client_fd);
                co_return;
            }
        }

        // --- Forward to backend via pool ---
        PooledConnection conn;
        if (!(co_await conn.acquire(pool))) {
            co_await safe_close(client_fd);
            co_return;
        }

        bool ok = co_await forward_one(conn, total_req);

        // On failure, retry once with a fresh connection.
        if (!ok) {
            backend_buf.reset();

            PooledConnection retry_conn;
            if (!(co_await retry_conn.acquire(pool))) {
                co_await safe_close(client_fd);
                co_return;
            }
            conn = std::move(retry_conn);

            ok = co_await forward_one(conn, total_req);
            if (!ok) {
                co_await safe_close(client_fd);
                co_return;
            }
        }

        client_buf.consume(total_req);
        if (client_buf.remaining() == 0) client_buf.reset();

        if (!request_keepalive(req)) break;
    }

    co_await safe_close(client_fd);
}

// --- Proxy server ---

Task<void> proxy_server(int listen_port, const std::string& backend_host,
                        uint16_t backend_port, ShutdownCoordinator& shutdown) {
    auto env_cfg = ynet::config::UltraNetConfig::from_env();
    const size_t max_conns = get_max_conns_from_env();
    const size_t max_buf = get_max_buf();
    const auto backend_timeout = get_backend_timeout();
    const auto idle_timeout = get_idle_timeout();
    std::atomic<size_t> active_conns{0};

    MemoryGuard memory_guard(get_mem_min_mb());
    BackoffGate backoff_gate;

    ConnectionPool pool(env_cfg.connection_pool, backend_host, backend_port);

    std::cout << "Pre-warming pool with " << env_cfg.connection_pool.min_connections
              << " connections..." << std::endl;
    co_await pool.pre_warm();
    auto warmup_stats = pool.snapshot();
    std::cout << "Pool warmed: " << warmup_stats.total_connections
              << " connections" << std::endl;

    // Standard single-socket accept (SO_REUSEPORT requires per-thread
    // accept coroutines which need deeper scheduler support — deferred
    // to a future release).

    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int fd = *sock;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    int sndbuf = 16384, rcvbuf = 16384;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    co_await Listen(fd, 512);

    int defer_accept = 30;
    setsockopt(fd, IPPROTO_TCP, TCP_DEFER_ACCEPT, &defer_accept, sizeof(defer_accept));

    std::cout << "HTTP proxy listening on :" << listen_port
              << " -> " << backend_host << ":" << backend_port
              << " (max_pool_conns=" << env_cfg.connection_pool.max_connections
              << " max_client_conns=" << max_conns
              << " threads=" << env_cfg.pool.num_threads
              << " max_buf=" << max_buf
              << " idle_timeout_ms=" << idle_timeout.count()
              << " mem_min_mb=" << memory_guard.min_free_mb << ")" << std::endl;

    constexpr auto BACKOFF_MIN = std::chrono::milliseconds(1);
    constexpr auto BACKOFF_MAX = std::chrono::milliseconds(100);
    auto backoff = BACKOFF_MIN;

    while (!shutdown.is_shutdown()) {
        auto* engine = IoUringEngine::current();

        if (engine && engine->has_cq_overflow()) {
            std::cerr << "[CQE_OVERFLOW] CQ ring overflow detected — backing off"
                      << std::endl;
            co_await sleep_for(std::chrono::milliseconds(100));
            backoff = BACKOFF_MAX;
            continue;
        }

        backoff_gate.maybe_decay();

        if (engine && engine->over_watermark()) {
            co_await sleep_for(backoff);
            backoff = std::min(backoff * 2, BACKOFF_MAX);
            continue;
        }

        size_t cur = active_conns.load(std::memory_order_relaxed);
        if (cur >= max_conns) {
            co_await sleep_for(backoff);
            backoff = std::min(backoff * 2, BACKOFF_MAX);
            continue;
        }

        if (memory_guard.check()) {
            co_await sleep_for(std::chrono::milliseconds(100));
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

        setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
        setsockopt(client_fd, IPPROTO_TCP, TCP_QUICKACK, &opt, sizeof(opt));
        setsockopt(client_fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        setsockopt(client_fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

        active_conns.fetch_add(1, std::memory_order_relaxed);

        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->submit(proxy_session(client_fd, pool, shutdown, active_conns,
                                       max_buf, backend_timeout, idle_timeout,
                                       backoff_gate).release());
        }
    }

    co_await safe_close(fd);

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

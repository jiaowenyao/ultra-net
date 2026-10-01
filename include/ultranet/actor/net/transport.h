// io_uring TCP 传输层 — 对用户完全透明。
// 负责连接管理、帧协议、消息分发和 TCP 性能调优。
// 由 actor_system 内部使用，第三方开发者无需直接接触。
#pragma once

#include <unordered_map>
#include <string>
#include <functional>
#include <memory>
#include <mutex>
#include <deque>
#include <atomic>
#include <optional>
#include <vector>
#include <chrono>
#include <cstring>

#include "ultranet/ultranet.h"
#include "ultranet/lifecycle/shutdown.hpp"
#include "ultranet/log/logger.hpp"
#include "ultranet/coroutine/execution_context.hpp"

namespace ynet::actor::net {

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

using msg_handler_t = std::function<Task<void>(
    std::shared_ptr<struct inbound_conn>, std::vector<uint8_t>)>;

// ── TCP 性能调优 ──────────────────────────────────────────────────────────
// 对每个 accept 和 connect 的 socket 自动调用。设置较大的收发缓冲区
// 并启用 TCP_NODELAY 以降低延迟。

inline void apply_tcp_tuning(int fd) {
    int buf_size = 256 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

// ── 预分配接收缓冲区 ──────────────────────────────────────────────────────
// 在连接处理期间复用，避免每条消息的堆分配。

struct recv_buffer {
    static constexpr size_t k_default_size = 256 * 1024;
    std::unique_ptr<uint8_t[]> data;

    recv_buffer() : data(std::make_unique<uint8_t[]>(k_default_size)) {}
    explicit recv_buffer(size_t size) : data(std::make_unique<uint8_t[]>(size)) {}
};

// ── 出站连接 ──────────────────────────────────────────────────────────────
// 封装到对等节点的 TCP 连接，提供带长度前缀的帧协议发送。

inline ynet::async::Task<bool> write_bytes(int fd, const uint8_t* data, size_t n);

inline ynet::async::Task<bool> write_frame(
    int fd, const std::vector<uint8_t>& payload);

inline void append_prefixed_frame(std::vector<uint8_t>& out,
                                  const std::vector<uint8_t>& frame) {
    uint32_t length = static_cast<uint32_t>(frame.size());
    const auto* len_bytes = reinterpret_cast<const uint8_t*>(&length);
    out.insert(out.end(), len_bytes, len_bytes + sizeof(length));
    out.insert(out.end(), frame.begin(), frame.end());
}

// ── 出站连接 ──────────────────────────────────────────────────────────────
// 封装到对等节点的 TCP 连接，提供带长度前缀的帧协议发送。

class outbound_conn : public std::enable_shared_from_this<outbound_conn> {
public:
    TcpSocket m_sock;
    bool m_valid = true;
    std::function<void()> on_dead;

    explicit outbound_conn(TcpSocket s) : m_sock(std::move(s)) {}

    // 直接写一帧。只给还没进入写队列的短连接用（gossip 拨号）。
    Task<bool> send(const std::vector<uint8_t>& payload) {
        bool ok = co_await write_frame(m_sock.fd(), payload);
        if (!ok) {
            notify_dead();
        }
        co_return ok;
    }

    // 同一条连接上只有一个写协程，避免多帧交错。
    // 成功入队返回 true。连接已死或没有调度器时返回 false，调用方必须重发。
    bool post_frame(std::vector<uint8_t> frame, ynet::async::Scheduler* sched) {
        bool start = false;
        {
            std::lock_guard<std::mutex> lock(m_mu);
            if (!m_valid) {
                return false;
            }
            m_frames.push_back(std::move(frame));
            if (!m_writing) {
                m_writing = true;
                start = true;
            }
        }
        if (!start) {
            return true;
        }
        if (sched == nullptr) {
            sched = ynet::async::ExecutionContext::current();
        }
        if (sched == nullptr) {
            std::lock_guard<std::mutex> lock(m_mu);
            m_writing = false;
            m_valid = false;
            m_frames.clear();
            return false;
        }
        auto keep = shared_from_this();
        sched->submit(write_loop(keep).release());
        return true;
    }

    bool is_valid() const { return m_valid && m_sock.is_valid(); }

    void notify_dead() {
        if (m_dead.exchange(true)) {
            return;
        }
        m_valid = false;
        if (on_dead) {
            on_dead();
        }
    }

    void close() {
        m_valid = false;
        m_sock.close_fd();
    }

private:
    std::mutex m_mu;
    std::deque<std::vector<uint8_t>> m_frames;
    bool m_writing = false;
    std::atomic<bool> m_dead{false};

    Task<void> write_loop(std::shared_ptr<outbound_conn> self) {
        for (;;) {
            std::vector<uint8_t> batch;
            {
                std::lock_guard<std::mutex> lock(self->m_mu);
                if (self->m_frames.empty() || !self->m_valid) {
                    self->m_writing = false;
                    co_return;
                }
                while (!self->m_frames.empty()) {
                    append_prefixed_frame(batch, self->m_frames.front());
                    self->m_frames.pop_front();
                }
            }
            bool ok = co_await write_bytes(
                self->m_sock.fd(), batch.data(), batch.size());
            if (!ok) {
                {
                    std::lock_guard<std::mutex> lock(self->m_mu);
                    self->m_frames.clear();
                    self->m_writing = false;
                }
                self->notify_dead();
                co_return;
            }
        }
    }
};

// ── 已接受的连接（可写，供 0x06 / 0x05 回写）──────────────────────────────

struct inbound_conn : std::enable_shared_from_this<inbound_conn> {
    uint64_t id = 0;
    uint64_t accept_epoch = 0;
    int fd = -1;
    std::atomic<bool> open{true};
    std::mutex mu;
    std::deque<std::vector<uint8_t>> frames;
    bool writing = false;

    void close_now() {
        int to_close = -1;
        {
            std::lock_guard<std::mutex> lock(mu);
            open.store(false);
            to_close = fd;
            fd = -1;
        }
        if (to_close >= 0) {
            ::shutdown(to_close, SHUT_RDWR);
            ::close(to_close);
        }
    }

    int current_fd() {
        std::lock_guard<std::mutex> lock(mu);
        return fd;
    }

    void post_frame(std::vector<uint8_t> frame, ynet::async::Scheduler* sched) {
        bool start = false;
        {
            std::lock_guard<std::mutex> lock(mu);
            if (!open.load() || fd < 0) {
                return;
            }
            frames.push_back(std::move(frame));
            if (!writing) {
                writing = true;
                start = true;
            }
        }
        if (!start) {
            return;
        }
        if (sched == nullptr) {
            sched = ynet::async::ExecutionContext::current();
        }
        if (sched == nullptr) {
            std::lock_guard<std::mutex> lock(mu);
            writing = false;
            return;
        }
        auto keep = shared_from_this();
        sched->submit(write_loop(keep).release());
    }

    Task<void> write_loop(std::shared_ptr<inbound_conn> self) {
        for (;;) {
            std::vector<uint8_t> batch;
            int write_fd = -1;
            {
                std::lock_guard<std::mutex> lock(self->mu);
                if (self->frames.empty() || !self->open.load() || self->fd < 0) {
                    self->writing = false;
                    co_return;
                }
                write_fd = self->fd;
                while (!self->frames.empty()) {
                    append_prefixed_frame(batch, self->frames.front());
                    self->frames.pop_front();
                }
            }
            bool ok = co_await write_bytes(write_fd, batch.data(), batch.size());
            if (!ok) {
                std::lock_guard<std::mutex> lock(self->mu);
                self->writing = false;
                co_return;
            }
        }
    }
};

inline Task<bool> write_bytes(int fd, const uint8_t* data, size_t n) {
    if (fd < 0) {
        co_return false;
    }
    size_t offset = 0;
    while (offset < n) {
        auto chunk_result = co_await Write(fd, data + offset, n - offset);
        if (!chunk_result || *chunk_result == 0) {
            co_return false;
        }
        offset += *chunk_result;
    }
    co_return true;
}

inline Task<bool> write_frame(int fd, const std::vector<uint8_t>& payload) {
    if (fd < 0) {
        co_return false;
    }
    uint32_t length = static_cast<uint32_t>(payload.size());
    std::vector<uint8_t> bytes(sizeof(length) + payload.size());
    std::memcpy(bytes.data(), &length, sizeof(length));
    if (!payload.empty()) {
        std::memcpy(bytes.data() + sizeof(length), payload.data(), payload.size());
    }
    co_return co_await write_bytes(fd, bytes.data(), bytes.size());
}

// Task 的 promise 把返回值放进 optional。co_return nullopt 会把外层 optional 清空，
// result() 接着断言失败。失败用 ok=false 表示。
struct frame_read {
    bool ok = false;
    std::vector<uint8_t> bytes;
};

inline Task<frame_read> read_frame(int fd) {
    if (fd < 0) {
        co_return frame_read{};
    }
    uint32_t message_length = 0;
    size_t len_offset = 0;
    uint8_t* len_ptr = reinterpret_cast<uint8_t*>(&message_length);
    while (len_offset < sizeof(message_length)) {
        Read length_reader(fd, len_ptr + len_offset,
                           sizeof(message_length) - len_offset);
        length_reader.with_timeout(std::chrono::seconds(30));
        auto read_result = co_await length_reader;
        if (!read_result || *read_result == 0) {
            co_return frame_read{};
        }
        len_offset += *read_result;
    }
    if (message_length == 0 || message_length > recv_buffer::k_default_size) {
        co_return frame_read{};
    }
    std::vector<uint8_t> payload(message_length);
    size_t offset = 0;
    while (offset < message_length) {
        Read payload_reader(fd, payload.data() + offset, message_length - offset);
        payload_reader.with_timeout(std::chrono::seconds(10));
        auto chunk_result = co_await payload_reader;
        if (!chunk_result || *chunk_result == 0) {
            co_return frame_read{};
        }
        offset += *chunk_result;
    }
    co_return frame_read{true, std::move(payload)};
}

// ── TCP 传输层 ────────────────────────────────────────────────────────────

class tcp_transport {
public:
    // 构造方式一：绑定到指定端口（独立传输层使用）
    tcp_transport(uint16_t port, msg_handler_t handler)
        : m_port(port)
        , m_handler(std::move(handler)) {}

    // 构造方式二：使用已绑定的监听 fd（actor_system 使用，
    // 它在启动 serve() 之前同步完成绑定）
    tcp_transport(int listen_fd, uint16_t actual_port, msg_handler_t handler)
        : m_listen_fd(listen_fd)
        , m_port(actual_port)
        , m_owns_fd(false)
        , m_handler(std::move(handler)) {}

    uint16_t port() const { return m_port; }
    uint16_t actual_port() const { return m_port; }

    // 让正在进行的 accept 马上返回。不关闭 fd，serve 退出时自己关。
    void shutdown_listen() {
        if (m_listen_fd >= 0) {
            ::shutdown(m_listen_fd, SHUT_RDWR);
        }
    }

    // 关闭已接受的连接，不关闭监听 fd。返回本次关闭的个数。
    size_t drop_inbound_connections() {
        std::vector<std::shared_ptr<inbound_conn>> doomed;
        {
            std::lock_guard<std::mutex> lock(m_inbound_mu);
            for (auto& [id, conn] : m_inbounds) {
                (void)id;
                if (conn) {
                    doomed.push_back(conn);
                }
            }
            m_inbounds.clear();
        }
        size_t closed = 0;
        for (auto& conn : doomed) {
            if (conn->open.load()) {
                conn->close_now();
                closed++;
            }
        }
        return closed;
    }

    // 这段时间里仍然 accept，但立刻关掉新 fd，不交给 handle_connection。
    void blackhole_accept_for(std::chrono::milliseconds dur) {
        auto until = std::chrono::steady_clock::now() + dur;
        m_blackhole_until_ns.store(
            std::chrono::duration_cast<std::chrono::nanoseconds>(
                until.time_since_epoch()).count(),
            std::memory_order_release);
    }

    uint64_t accept_epoch() const {
        return m_accept_epoch.load(std::memory_order_acquire);
    }

    void bump_accept_epoch() {
        m_accept_epoch.fetch_add(1, std::memory_order_acq_rel);
    }

    bool post_inbound(uint64_t id, std::vector<uint8_t> frame,
                      ynet::async::Scheduler* sched) {
        std::shared_ptr<inbound_conn> conn;
        {
            std::lock_guard<std::mutex> lock(m_inbound_mu);
            auto it = m_inbounds.find(id);
            if (it == m_inbounds.end()) {
                return false;
            }
            conn = it->second;
        }
        if (!conn) {
            return false;
        }
        conn->post_frame(std::move(frame), sched);
        return true;
    }

    // ── Accept 循环（协程）──────────────────────────────────────────────
    // 在 io_uring 上异步 accept 新连接，为每个连接提交 handle_connection
    // 协程到线程池。循环在 shutdown 信号触发后退出。

    Task<void> serve(ShutdownCoordinator& shutdown) {
        int listen_fd = m_listen_fd;

        // 若未预绑定（构造方式一），在此处绑定并监听
        if (listen_fd < 0) {
            auto sock_result = co_await Socket(AF_INET, SOCK_STREAM, 0);
            if (!sock_result) {
                co_return;
            }
            listen_fd = *sock_result;
            m_listen_fd = listen_fd;
            m_owns_fd = true;

            int opt = 1;
            setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

            sockaddr_in server_addr{};
            server_addr.sin_family      = AF_INET;
            server_addr.sin_port        = htons(m_port);
            server_addr.sin_addr.s_addr = INADDR_ANY;

            co_await Bind(listen_fd, reinterpret_cast<sockaddr*>(&server_addr),
                          sizeof(server_addr));
            co_await Listen(listen_fd, 64);

            // 记录实际绑定的端口（port=0 时的自动分配结果）
            sockaddr_in bound{};
            socklen_t bound_len = sizeof(bound);
            if (getsockname(listen_fd, reinterpret_cast<sockaddr*>(&bound),
                           &bound_len) == 0) {
                m_port = ntohs(bound.sin_port);
            }

            ULTRA_LOG_INFO("[transport] listening on :{}", m_port);
        } else {
            ULTRA_LOG_INFO("[transport] serving on pre-bound fd:{} port:{}",
                          listen_fd, m_port);
        }

        // 主循环：accept → 调优 → 提交连接处理协程
        while (!shutdown.is_shutdown()) {
            Accept acceptor(listen_fd);
            // 100ms 超时确保 shutdown 检查能及时响应
            acceptor.with_timeout(std::chrono::milliseconds(100));

            auto client_result = co_await acceptor;
            if (!client_result) {
                continue;  // 超时或错误，重新检查 shutdown 条件
            }

            int client_fd = *client_result;
            if (in_accept_blackhole()) {
                ::shutdown(client_fd, SHUT_RDWR);
                ::close(client_fd);
                continue;
            }
            apply_tcp_tuning(client_fd);

            auto inbound = std::make_shared<inbound_conn>();
            inbound->fd = client_fd;
            inbound->accept_epoch = m_accept_epoch.load(std::memory_order_acquire);
            {
                std::lock_guard<std::mutex> lock(m_inbound_mu);
                inbound->id = m_next_inbound_id++;
                m_inbounds[inbound->id] = inbound;
            }

            auto* scheduler = ExecutionContext::current();
            if (scheduler) {
                scheduler->submit(handle_connection(inbound).release());
            }
        }

        // 清理监听 socket
        if (m_owns_fd) {
            co_await Close(listen_fd);
        } else {
            ::close(listen_fd);
        }
    }

    // ── 出站连接 ─────────────────────────────────────────────────────────
    // 连接到远端节点，返回可用于发送消息的 outbound_conn。

    Task<std::shared_ptr<outbound_conn>> connect(
            const std::string& host, uint16_t port) {
        auto sock = co_await TcpSocket::connect(
            host, port, std::chrono::seconds(3));
        if (!sock.is_valid()) {
            co_return nullptr;
        }
        apply_tcp_tuning(sock.fd());
        co_return std::make_shared<outbound_conn>(std::move(sock));
    }

private:
    int m_listen_fd = -1;
    bool m_owns_fd = true;
    uint16_t m_port;
    msg_handler_t m_handler;
    std::mutex m_inbound_mu;
    std::unordered_map<uint64_t, std::shared_ptr<inbound_conn>> m_inbounds;
    uint64_t m_next_inbound_id = 1;
    std::atomic<int64_t> m_blackhole_until_ns{0};
    std::atomic<uint64_t> m_accept_epoch{0};

    bool in_accept_blackhole() const {
        int64_t until = m_blackhole_until_ns.load(std::memory_order_acquire);
        if (until == 0) {
            return false;
        }
        auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count();
        return now < until;
    }

    Task<void> handle_connection(std::shared_ptr<inbound_conn> inbound) {
        while (inbound && inbound->open.load()) {
            int fd = inbound->current_fd();
            if (fd < 0) {
                break;
            }
            auto payload = co_await read_frame(fd);
            if (!payload.ok) {
                break;
            }
            co_await m_handler(inbound, std::move(payload.bytes));
        }
        if (inbound) {
            uint64_t id = inbound->id;
            inbound->close_now();
            std::lock_guard<std::mutex> lock(m_inbound_mu);
            auto it = m_inbounds.find(id);
            if (it != m_inbounds.end() && it->second == inbound) {
                m_inbounds.erase(it);
            }
        }
        co_return;
    }
};

} // namespace ynet::actor::net

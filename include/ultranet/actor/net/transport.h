// io_uring TCP 传输层 — 对用户完全透明。
// 负责连接管理、帧协议、消息分发和 TCP 性能调优。
// 由 actor_system 内部使用，第三方开发者无需直接接触。
#pragma once

#include <unordered_map>
#include <string>
#include <functional>
#include <memory>

#include "ultranet/ultranet.h"
#include "ultranet/lifecycle/shutdown.hpp"

namespace ynet::actor::net {

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

using msg_handler_t = std::function<Task<void>(std::vector<uint8_t>)>;

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

class outbound_conn {
public:
    TcpSocket m_sock;
    bool m_valid = true;

    explicit outbound_conn(TcpSocket s) : m_sock(std::move(s)) {}

    // 发送带长度前缀的帧协议消息：[length:4][payload:length]
    Task<void> send(const std::vector<uint8_t>& payload) {
        uint32_t length = static_cast<uint32_t>(payload.size());

        // 1. 发送 4 字节长度前缀
        auto write_result = co_await m_sock.write(&length, sizeof(length));
        if (!write_result || *write_result != sizeof(length)) {
            m_valid = false;
            co_return;
        }

        // 2. 分块发送负载（处理部分写入）
        size_t offset = 0;
        while (offset < payload.size()) {
            auto chunk_result = co_await m_sock.write(
                payload.data() + offset, payload.size() - offset);
            if (!chunk_result || *chunk_result == 0) {
                m_valid = false;
                co_return;
            }
            offset += *chunk_result;
        }
    }

    bool is_valid() const { return m_valid && m_sock.is_valid(); }
    void close() { m_valid = false; m_sock.close(); }
};

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

            std::cout << "[transport] listening on :" << m_port << std::endl;
        } else {
            std::cout << "[transport] serving on pre-bound fd:" << listen_fd
                      << " port:" << m_port << std::endl;
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
            apply_tcp_tuning(client_fd);

            // 提交到当前线程池的调度器
            auto* scheduler = ExecutionContext::current();
            if (scheduler) {
                scheduler->submit(handle_connection(client_fd).release());
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

    // ── 入站连接处理（协程）──────────────────────────────────────────────
    // 从客户端 fd 读取长度前缀帧协议消息，调用 m_handler 分发。
    // 使用预分配缓冲区减少每条消息的堆分配。

    Task<void> handle_connection(int client_fd) {
        recv_buffer buf(recv_buffer::k_default_size);

        while (true) {
            // ── 1. 读取 4 字节长度前缀（处理短读）──────────────────────
            uint32_t message_length = 0;
            size_t len_offset = 0;
            uint8_t* len_ptr = reinterpret_cast<uint8_t*>(&message_length);
            while (len_offset < sizeof(message_length)) {
                Read length_reader(client_fd, len_ptr + len_offset,
                                   sizeof(message_length) - len_offset);
                length_reader.with_timeout(std::chrono::seconds(30));

                auto read_result = co_await length_reader;
                if (!read_result || *read_result == 0) {
                    co_await Close(client_fd);
                    co_return;
                }
                len_offset += *read_result;
            }

            // 合法性检查
            if (message_length == 0 || message_length > recv_buffer::k_default_size) {
                break;
            }

            // ── 2. 读取负载到预分配缓冲区（处理短读）──────────────────
            size_t offset = 0;
            uint8_t* dest = buf.data.get();
            while (offset < message_length) {
                Read payload_reader(client_fd, dest + offset,
                                    message_length - offset);
                payload_reader.with_timeout(std::chrono::seconds(10));

                auto chunk_result = co_await payload_reader;
                if (!chunk_result || *chunk_result == 0) {
                    co_await Close(client_fd);
                    co_return;
                }
                offset += *chunk_result;
            }

            // ── 3. 构造 vector 并分发给消息处理器 ──────────────────────
            // 注意：此处有一次拷贝（预分配缓冲区 → vector），
            // 后续可优化为使用 std::span 消除此拷贝。
            std::vector<uint8_t> payload(dest, dest + message_length);
            co_await m_handler(std::move(payload));
        }

        co_await Close(client_fd);
    }
};

} // namespace ynet::actor::net

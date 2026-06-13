// io_uring TCP transport — completely hidden from users.
// Handles connections, framing, message dispatch, and TCP tuning.
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

// Apply performance-oriented TCP socket options.
// Called automatically on every accepted and connected socket.
inline void apply_tcp_tuning(int fd) {
    int buf_size = 256 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

// Pre-allocated receive buffer for zero-allocation message handling.
// The actor framework uses this to avoid per-message heap allocations.
struct recv_buffer {
    static constexpr size_t k_default_size = 256 * 1024;
    std::unique_ptr<uint8_t[]> data;

    recv_buffer() : data(std::make_unique<uint8_t[]>(k_default_size)) {}
    explicit recv_buffer(size_t size) : data(std::make_unique<uint8_t[]>(size)) {}
};

// Outbound connection to a peer node.
class outbound_conn {
public:
    TcpSocket m_sock;
    bool m_valid = true;

    explicit outbound_conn(TcpSocket s) : m_sock(std::move(s)) {}

    Task<void> send(const std::vector<uint8_t>& payload) {
        uint32_t length = static_cast<uint32_t>(payload.size());
        auto write_result = co_await m_sock.write(&length, sizeof(length));
        if (!write_result || *write_result != sizeof(length)) {
            m_valid = false;
            co_return;
        }
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
    void close() { m_sock.close(); }
};

// TCP transport: listens for connections and dispatches framed messages.
class tcp_transport {
public:
    // Constructor: bind to a port (used by standalone transport).
    tcp_transport(uint16_t port, msg_handler_t handler)
        : m_port(port)
        , m_handler(std::move(handler)) {}

    // Constructor: use a pre-bound listen fd (used by actor_system
    // which handles binding synchronously before starting serve()).
    tcp_transport(int listen_fd, uint16_t actual_port, msg_handler_t handler)
        : m_listen_fd(listen_fd)
        , m_port(actual_port)
        , m_owns_fd(false)
        , m_handler(std::move(handler)) {}

    uint16_t port() const { return m_port; }
    uint16_t actual_port() const { return m_port; }

    // Start the accept loop.  Runs as a coroutine.
    // If m_listen_fd is already set (pre-bound constructor), use it directly.
    Task<void> serve(ShutdownCoordinator& shutdown) {
        int listen_fd = m_listen_fd;

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

            // Store the actual bound port.
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

        while (!shutdown.is_shutdown()) {
            Accept acceptor(listen_fd);
            acceptor.with_timeout(std::chrono::milliseconds(100));

            auto client_result = co_await acceptor;
            if (!client_result) {
                continue;
            }

            int client_fd = *client_result;
            apply_tcp_tuning(client_fd);

            auto* scheduler = ExecutionContext::current();
            if (scheduler) {
                scheduler->submit(handle_connection(client_fd).release());
            }
        }

        if (m_owns_fd) {
            co_await Close(listen_fd);
        } else {
            ::close(listen_fd);
        }
    }

    // Connect to a remote node.
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

    // Handle one inbound connection: read length-prefixed messages.
    // Uses a pre-allocated buffer to avoid per-message heap allocation.
    Task<void> handle_connection(int client_fd) {
        recv_buffer buf(recv_buffer::k_default_size);

        while (true) {
            // Read 4-byte length prefix (handle short reads).
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

            if (message_length == 0 || message_length > recv_buffer::k_default_size) {
                break;
            }

            // Read payload directly into pre-allocated buffer.
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

            // Wrap in vector for the handler (single allocation per message,
            // but data is already in the pre-allocated buffer — could be
            // further optimized with std::span in the handler signature).
            std::vector<uint8_t> payload(dest, dest + message_length);
            co_await m_handler(std::move(payload));
        }

        co_await Close(client_fd);
    }
};

} // namespace ynet::actor::net

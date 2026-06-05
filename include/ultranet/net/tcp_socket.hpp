#pragma once

#include "ultranet/coroutine/task.hpp"
#include "ultranet/io/io_awaitable.hpp"
#include "ultranet/net/socket.hpp"
#include "ultranet/net/connect.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/net/close.hpp"
#include "ultranet/net/dns.hpp"
#include "ultranet/utils/noncopyable.h"
#include <string>
#include <chrono>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>

namespace ynet::async::net {

class TcpSocket : ynet::utils::Noncopyable {
public:
    TcpSocket() noexcept = default;

    explicit TcpSocket(int fd) noexcept : m_fd(fd) {}

    ~TcpSocket() {
        if (m_fd >= 0) ::close(m_fd);
    }

    TcpSocket(TcpSocket&& other) noexcept
        : m_fd(other.m_fd) {
        other.m_fd = -1;
    }

    TcpSocket& operator=(TcpSocket&& other) noexcept {
        if (this != &other) {
            if (m_fd >= 0) ::close(m_fd);
            m_fd = other.m_fd;
            other.m_fd = -1;
        }
        return *this;
    }

    int fd() const noexcept { return m_fd; }
    bool is_valid() const noexcept { return m_fd >= 0; }
    explicit operator bool() const noexcept { return is_valid(); }

    io::Read read(void* buf, size_t count) { return io::Read(m_fd, buf, count); }
    io::Write write(const void* buf, size_t count) { return io::Write(m_fd, buf, count); }
    io::Close close() { return io::Close(m_fd); }

    int release() noexcept {
        int fd = m_fd;
        m_fd = -1;
        return fd;
    }

    static ynet::async::Task<TcpSocket> connect(
        const std::string& host, uint16_t port,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000));

private:
    int m_fd = -1;
};

inline ynet::async::Task<TcpSocket> TcpSocket::connect(
    const std::string& host, uint16_t port,
    std::chrono::milliseconds timeout) {

    std::vector<std::string> ips;

    // Check if host is already an IP address
    sockaddr_in check{};
    if (inet_pton(AF_INET, host.c_str(), &check.sin_addr) == 1) {
        ips.push_back(host);
    } else {
        ips = co_await io::resolve_host(host, timeout);
    }

    std::error_code last_error;

    for (const auto& ip : ips) {
        auto sock_result = co_await io::Socket(AF_INET, SOCK_STREAM, 0);
        if (!sock_result) {
            last_error = sock_result.error();
            continue;
        }
        int fd = *sock_result;

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

        auto conn_result = co_await io::Connect(fd, (sockaddr*)&addr, sizeof(addr));
        if (!conn_result) {
            last_error = conn_result.error();
            co_await io::Close(fd);
            continue;
        }

        int opt = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

        co_return TcpSocket(fd);
    }

    throw std::system_error(last_error.value() != 0 ? last_error
        : io::make_io_error(ENETUNREACH));
}

} // namespace ynet::async::net

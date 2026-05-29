#pragma once

#include "ultranet/net/tcp_socket.hpp"
#include "ultranet/coroutine/channel.hpp"
#include "ultranet/io/timer.hpp"
#include "ultranet/config/config.hpp"
#include <atomic>

namespace ynet::async::net {

using ConnectionPoolConfig = config::ConnectionPoolConfig;

class ConnectionPool : ynet::utils::Noncopyable {
public:
    struct Stats {
        size_t acquired{0};
        size_t released{0};
        size_t evicted{0};
        size_t failed_creates{0};
        size_t active() const noexcept { return acquired > released ? acquired - released : 0; }
        size_t idle() const noexcept { return 0; }
    };

    ConnectionPool(ConnectionPoolConfig cfg, std::string host, uint16_t port)
        : m_config(cfg), m_host(std::move(host)), m_port(port) {}

    ~ConnectionPool() {
        m_shutdown.store(true, std::memory_order_release);
        m_idle.close();
    }

    Task<TcpSocket> acquire() {
        if (m_shutdown.load(std::memory_order_acquire)) {
            throw std::system_error(io::make_io_error(ECANCELED), "connection pool is shut down");
        }

        // Try to get an idle connection first
        auto idle = co_await m_idle.read();
        if (idle.has_value()) {
            m_acquired.fetch_add(1, std::memory_order_relaxed);
            co_return std::move(*idle);
        }

        // Pool closed (shutting down) — idle channel returns nullopt
        throw std::system_error(io::make_io_error(ECANCELED), "connection pool is shut down");
    }

    void release(TcpSocket socket) {
        if (!socket.is_valid()) return;
        m_released.fetch_add(1, std::memory_order_relaxed);

        size_t active = m_released.load(std::memory_order_relaxed);
        (void)active;

        if (!m_idle.try_write(std::move(socket))) {
            // Pool is full, close the socket (destructor handles it)
        }
    }

    void invalidate(TcpSocket socket) {
        m_evicted.fetch_add(1, std::memory_order_relaxed);
        // socket destructor closes fd
    }

    Stats snapshot() const noexcept {
        return {
            m_acquired.load(std::memory_order_relaxed),
            m_released.load(std::memory_order_relaxed),
            m_evicted.load(std::memory_order_relaxed),
            m_failed_creates.load(std::memory_order_relaxed)
        };
    }

    bool is_shutdown() const noexcept {
        return m_shutdown.load(std::memory_order_acquire);
    }

    const std::string& host() const noexcept { return m_host; }
    uint16_t port() const noexcept { return m_port; }

private:
    ConnectionPoolConfig m_config;
    std::string m_host;
    uint16_t m_port;
    Channel<TcpSocket, 256> m_idle;
    std::atomic<bool> m_shutdown{false};
    std::atomic<size_t> m_acquired{0};
    std::atomic<size_t> m_released{0};
    std::atomic<size_t> m_evicted{0};
    std::atomic<size_t> m_failed_creates{0};
};

} // namespace ynet::async::net

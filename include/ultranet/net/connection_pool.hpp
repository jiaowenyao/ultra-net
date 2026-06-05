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
        size_t total_connections{0};

        size_t active() const noexcept {
            return total_connections > (acquired - released) ? total_connections - (acquired - released) : 0;
        }
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
            throw std::system_error(io::make_io_error(ECANCELED),
                                    "connection pool is shut down");
        }

        // 1. Try idle channel first (non-blocking)
        auto idle = m_idle.try_read();
        if (idle.has_value()) {
            m_acquired.fetch_add(1, std::memory_order_relaxed);
            co_return std::move(*idle);
        }

        // 2. Try to create a new connection (up to max_connections)
        size_t current = m_total_connections.load(std::memory_order_relaxed);
        while (current < m_config.max_connections) {
            if (m_total_connections.compare_exchange_weak(
                    current, current + 1, std::memory_order_relaxed)) {
                try {
                    auto sock = co_await TcpSocket::connect(
                        m_host, m_port, m_config.connect_timeout);
                    m_acquired.fetch_add(1, std::memory_order_relaxed);
                    co_return sock;
                } catch (...) {
                    m_total_connections.fetch_sub(1, std::memory_order_relaxed);
                    m_failed_creates.fetch_add(1, std::memory_order_relaxed);
                    throw;
                }
            }
        }

        // 3. Pool at capacity — block until a connection is returned
        idle = co_await m_idle.read();
        if (idle.has_value()) {
            m_acquired.fetch_add(1, std::memory_order_relaxed);
            co_return std::move(*idle);
        }
        throw std::system_error(io::make_io_error(ECANCELED),
                                "connection pool is shut down");
    }

    Task<void> pre_warm() {
        size_t target = m_config.min_connections;
        for (size_t i = 0; i < target; ++i) {
            size_t current = m_total_connections.load(std::memory_order_relaxed);
            if (current >= m_config.max_connections) break;
            if (!m_total_connections.compare_exchange_weak(
                    current, current + 1, std::memory_order_relaxed)) {
                --i;
                continue;
            }
            try {
                auto sock = co_await TcpSocket::connect(
                    m_host, m_port, m_config.connect_timeout);
                release(std::move(sock));
            } catch (const std::system_error&) {
                m_total_connections.fetch_sub(1, std::memory_order_relaxed);
                m_failed_creates.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    void release(TcpSocket socket) {
        if (!socket.is_valid()) return;
        m_released.fetch_add(1, std::memory_order_relaxed);

        if (!m_idle.try_write(std::move(socket))) {
            m_total_connections.fetch_sub(1, std::memory_order_relaxed);
        }
    }

    void invalidate(TcpSocket socket) {
        m_evicted.fetch_add(1, std::memory_order_relaxed);
        m_total_connections.fetch_sub(1, std::memory_order_relaxed);
    }

    Stats snapshot() const noexcept {
        return {
            m_acquired.load(std::memory_order_relaxed),
            m_released.load(std::memory_order_relaxed),
            m_evicted.load(std::memory_order_relaxed),
            m_failed_creates.load(std::memory_order_relaxed),
            m_total_connections.load(std::memory_order_relaxed)
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
    std::atomic<size_t> m_total_connections{0};
};

} // namespace ynet::async::net

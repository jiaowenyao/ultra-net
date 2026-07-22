#pragma once

#include "ultranet/coroutine/channel.hpp"
#include "ultranet/utils/noncopyable.h"
#include <atomic>
#include <csignal>

namespace ynet::async::lifecycle {

// Lightweight shutdown coordinator.
//
// Usage:
//   ShutdownCoordinator sd;
//   sd.install_signal_handlers();  // optional: Ctrl-C → shutdown
//
//   // In accept / processing loops:
//   while (!sd.is_shutdown()) { ... }
//
//   // To wait for shutdown asynchronously:
//   co_await sd.wait();
//
//   // To trigger shutdown:
//   sd.shutdown();

class ShutdownCoordinator : ynet::utils::Noncopyable {
public:
    ShutdownCoordinator() = default;

    ~ShutdownCoordinator() {
        shutdown();
        unregister_instance();
        if (m_signals_installed) {
            std::signal(SIGINT, SIG_DFL);
            std::signal(SIGTERM, SIG_DFL);
        }
    }

    void install_signal_handlers() {
        register_instance();
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        m_signals_installed = true;
    }

    void shutdown() noexcept {
        bool expected = false;
        if (m_shutdown.compare_exchange_strong(expected, true,
                std::memory_order_acq_rel)) {
            m_signal.close();
        }
    }

    bool is_shutdown() const noexcept {
        return m_shutdown.load(std::memory_order_acquire);
    }

    // Coroutine-compatible: suspends until shutdown is triggered.
    auto wait() noexcept { return m_signal.read(); }

private:
    Channel<bool, 1> m_signal;
    std::atomic<bool> m_shutdown{false};
    bool m_signals_installed{false};

    // Intrusive linked list for safe multi-instance signal handling.
    ShutdownCoordinator* m_next{nullptr};
    static inline std::atomic<ShutdownCoordinator*> s_head{nullptr};

    void register_instance() noexcept {
        m_next = s_head.load(std::memory_order_acquire);
        while (!s_head.compare_exchange_weak(m_next, this,
                std::memory_order_release, std::memory_order_relaxed)) {}
    }

    void unregister_instance() noexcept {
        auto* cur = s_head.load(std::memory_order_acquire);
        ShutdownCoordinator* prev = nullptr;
        while (cur) {
            if (cur == this) {
                auto* next = cur->m_next;
                if (prev) {
                    prev->m_next = next;
                } else {
                    s_head.store(next, std::memory_order_release);
                }
                return;
            }
            prev = cur;
            cur = cur->m_next;
        }
    }

    static void signal_handler(int) {
        for (auto* cur = s_head.load(std::memory_order_acquire);
             cur; cur = cur->m_next) {
            cur->shutdown();
        }
    }
};

} // namespace ynet::async::lifecycle

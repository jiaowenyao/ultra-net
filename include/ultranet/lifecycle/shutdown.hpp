#pragma once

#include "ultranet/coroutine/channel.hpp"
#include "ultranet/utils/noncopyable.h"
#include <atomic>
#include <csignal>
#include <optional>

namespace ynet::async::lifecycle {

enum class ShutdownPhase : uint8_t {
    Running = 0,
    Draining,
    Complete
};

class ShutdownCoordinator : ynet::utils::Noncopyable {
public:
    ShutdownCoordinator() = default;

    ~ShutdownCoordinator() {
        shutdown();
        if (m_signals_installed) {
            std::signal(SIGINT, SIG_DFL);
            std::signal(SIGTERM, SIG_DFL);
        }
        s_instance.store(nullptr, std::memory_order_release);
    }

    void install_signal_handlers() {
        s_instance.store(this, std::memory_order_release);
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        m_signals_installed = true;
    }

    void shutdown() noexcept {
        ShutdownPhase expected = ShutdownPhase::Running;
        if (m_phase.compare_exchange_strong(expected, ShutdownPhase::Draining,
                std::memory_order_acq_rel)) {
            m_signal.close();
        }
        // If already Draining or Complete, idempotent
    }

    bool is_shutdown() const noexcept {
        return m_phase.load(std::memory_order_acquire) != ShutdownPhase::Running;
    }

    ShutdownPhase phase() const noexcept {
        return m_phase.load(std::memory_order_acquire);
    }

    void advance_phase(ShutdownPhase p) noexcept {
        m_phase.store(p, std::memory_order_release);
    }

    auto wait() noexcept { return m_signal.read(); }

private:
    Channel<bool, 1> m_signal;
    std::atomic<ShutdownPhase> m_phase{ShutdownPhase::Running};
    bool m_signals_installed{false};
    static inline std::atomic<ShutdownCoordinator*> s_instance{nullptr};

    static void signal_handler(int) {
        if (auto* coord = s_instance.load(std::memory_order_acquire)) {
            coord->shutdown();
        }
    }
};

} // namespace ynet::async::lifecycle

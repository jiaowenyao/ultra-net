// ShutdownCoordinator — 轻量级关闭协调器。
//
// 核心只有一个 atomic<bool> 标志位。install_signal_handlers() 可选地
// 安装 SIGINT/SIGTERM 处理器来自动触发关闭。调用方通过 is_shutdown()
// 轮询检查关闭状态。
//
// 用法：
//   ShutdownCoordinator sd;
//   sd.install_signal_handlers();
//   while (!sd.is_shutdown()) { ... }
#pragma once

#include "ultranet/utils/noncopyable.h"
#include <atomic>
#include <csignal>

namespace ynet::async::lifecycle {

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

    // 安装信号处理器：Ctrl+C 自动触发 shutdown()
    void install_signal_handlers() {
        register_instance();
        std::signal(SIGINT, signal_handler);
        std::signal(SIGTERM, signal_handler);
        m_signals_installed = true;
    }

    // 触发关闭（CAS 保证幂等）
    void shutdown() noexcept {
        bool expected = false;
        m_shutdown.compare_exchange_strong(expected, true,
                std::memory_order_acq_rel);
    }

    bool is_shutdown() const noexcept {
        return m_shutdown.load(std::memory_order_acquire);
    }

private:
    std::atomic<bool> m_shutdown{false};
    bool m_signals_installed{false};

    // 侵入式链表：支持多个 ShutdownCoordinator 实例同时响应信号
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

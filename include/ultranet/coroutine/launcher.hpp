// Launcher — 轻量级应用启动器，封装 ultra-net 应用的样板代码：
// 信号处理、线程池创建、上下文绑定、任务提交和阻塞等待。
//
// 典型用法：
//   1. 零配置（自由函数）:
//        return launch([](ShutdownCoordinator& sd) -> Task<void> {
//            co_await my_server(8080, sd);
//        });
//
//   2. 流式配置:
//        return Launcher()
//            .threads(4)
//            .run([](ShutdownCoordinator& sd) -> Task<void> {
//                co_await my_server(8080, sd);
//            });
//
//   3. 每线程运行（SO_REUSEPORT 多线程 accept）:
//        return Launcher()
//            .threads(4)
//            .run_per_thread([](int tid, ShutdownCoordinator& sd) -> Task<void> {
//                co_await my_server(tid, sd);
//            });
#pragma once

#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/lifecycle/shutdown.hpp"
#include "ultranet/coroutine/execution_context.hpp"
#include "ultranet/log/logger.hpp"
#include <concepts>

namespace ynet::async {

class Launcher {
public:
    // 设置工作线程数（默认：hardware_concurrency）
    Launcher& threads(size_t n) noexcept {
        m_threads = n;
        return *this;
    }

    // 设置 io_uring 引擎配置
    Launcher& io_uring_config(const io::IoUringEngineConfig& cfg) noexcept {
        m_io_config = cfg;
        return *this;
    }

    // 运行接收 ShutdownCoordinator 的服务函数。
    // fn 接收 ShutdownCoordinator&，在 accept/处理循环中检查 is_shutdown()。
    template <typename F>
        requires std::is_invocable_r_v<Task<void>, F, lifecycle::ShutdownCoordinator&>
    int run(F&& fn) {
        lifecycle::ShutdownCoordinator shutdown;
        shutdown.install_signal_handlers();
        try {
            scheduling::WorkStealingThreadPool pool(m_threads, m_io_config);
            ExecutionContext::Scope scope(&pool);
            // 提交用户协程到线程池
            pool.submit(fn(shutdown).release());
            // 阻塞直到所有任务完成或收到关闭信号
            pool.wait_all();
        } catch (const std::exception& e) {
            ULTRA_LOG_CRITICAL("Fatal: {}", e.what());
            return 1;
        }
        return 0;
    }

    // 运行无需 ShutdownCoordinator 的函数（签名: () -> Task<void>）。
    // ShutdownCoordinator 由框架内部管理，用户无需感知。
    // 适用于使用高层封装（如 WsServer）的场景。
    template <typename F>
        requires std::is_invocable_r_v<Task<void>, F>
              && (!std::is_invocable_r_v<Task<void>, F, lifecycle::ShutdownCoordinator&>)
    int run(F&& fn) {
        lifecycle::ShutdownCoordinator shutdown;
        shutdown.install_signal_handlers();
        try {
            scheduling::WorkStealingThreadPool pool(m_threads, m_io_config);
            ExecutionContext::Scope scope(&pool);
            pool.submit(fn().release());
            pool.wait_all();
        } catch (const std::exception& e) {
            ULTRA_LOG_CRITICAL("Fatal: {}", e.what());
            return 1;
        }
        return 0;
    }

    // 在每个工作线程上运行一个 fn 实例（线程绑定）。
    // fn(thread_id, shutdown) → Task<void>，每个线程调用一次。
    // 适用于 SO_REUSEPORT 多线程 accept 和每线程状态管理。
    template <typename F>
        requires std::is_invocable_r_v<Task<void>, F, int, lifecycle::ShutdownCoordinator&>
    int run_per_thread(F&& fn) {
        lifecycle::ShutdownCoordinator shutdown;
        shutdown.install_signal_handlers();
        try {
            scheduling::WorkStealingThreadPool pool(m_threads, m_io_config);
            ExecutionContext::Scope scope(&pool);
            // 将每线程协程固定到对应的 worker 线程
            for (size_t i = 0; i < m_threads; ++i) {
                pool.submit_on_thread(i,
                    fn(static_cast<int>(i), shutdown).release());
            }
            pool.wait_all();
        } catch (const std::exception& e) {
            ULTRA_LOG_CRITICAL("Fatal: {}", e.what());
            return 1;
        }
        return 0;
    }

private:
    size_t m_threads = std::thread::hardware_concurrency();
    io::IoUringEngineConfig m_io_config{};
};

// 零配置入口。等价于 Launcher().run(fn)。
// 自动检测 fn 是否接受 ShutdownCoordinator& 参数。
template <typename F>
inline int launch(F&& fn) {
    return Launcher().run(
        [fn = std::forward<F>(fn)](lifecycle::ShutdownCoordinator& sd) mutable -> Task<void> {
            if constexpr (std::is_invocable_r_v<Task<void>, F, lifecycle::ShutdownCoordinator&>) {
                co_await fn(sd);
            } else {
                co_await fn();
            }
        });
}

} // namespace ynet::async

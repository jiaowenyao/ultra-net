#pragma once

#include "ultranet/coroutine/thread_pool.hpp"
#include "ultranet/lifecycle/shutdown.hpp"
#include "ultranet/coroutine/execution_context.hpp"
#include <concepts>
#include <iostream>

namespace ynet::async {

// Lightweight launcher that wraps the boilerplate of starting an ultra-net
// application: signal handling, thread pool creation, scope binding, task
// submission, and blocking until completion.
//
// Two common patterns:
//
//   1. Zero-config (free function):
//        int main() {
//            return launch([](ShutdownCoordinator& shutdown) -> Task<void> {
//                co_await my_server(8080, shutdown);
//            });
//        }
//
//   2. Fluent configuration:
//        int main() {
//            return Launcher()
//                .threads(4)
//                .run([](ShutdownCoordinator& shutdown) -> Task<void> {
//                    co_await my_server(8080, shutdown);
//                });
//        }
//
// The old explicit pattern still works for users who need full control:
//   ShutdownCoordinator + WorkStealingThreadPool + Scope + submit + wait_all

class Launcher {
public:
    // Set the number of worker threads (default: hardware_concurrency).
    Launcher& threads(size_t n) noexcept {
        m_threads = n;
        return *this;
    }

    // Set io_uring engine configuration.
    Launcher& io_uring_config(const io::IoUringEngineConfig& cfg) noexcept {
        m_io_config = cfg;
        return *this;
    }

    // Run a server function that participates in graceful shutdown.
    // The callable receives a ShutdownCoordinator& for checking is_shutdown()
    // in accept/processing loops.
    template <typename F>
        requires std::is_invocable_r_v<Task<void>, F, lifecycle::ShutdownCoordinator&>
    int run(F&& fn) {
        lifecycle::ShutdownCoordinator shutdown;
        shutdown.install_signal_handlers();
        try {
            scheduling::WorkStealingThreadPool pool(m_threads, m_io_config);
            ExecutionContext::Scope scope(&pool);
            pool.submit(fn(shutdown).release());
            pool.wait_all();
        } catch (const std::exception& e) {
            std::cerr << "Fatal: " << e.what() << std::endl;
            return 1;
        }
        return 0;
    }

    // Run one instance of fn on EACH worker thread, pinned to that thread.
    // fn(thread_id, shutdown) -> Task<void> is called once per thread.
    // This enables SO_REUSEPORT multi-threaded accept and per-thread state.
    template <typename F>
        requires std::is_invocable_r_v<Task<void>, F, int, lifecycle::ShutdownCoordinator&>
    int run_per_thread(F&& fn) {
        lifecycle::ShutdownCoordinator shutdown;
        shutdown.install_signal_handlers();
        try {
            scheduling::WorkStealingThreadPool pool(m_threads, m_io_config);
            ExecutionContext::Scope scope(&pool);
            for (size_t i = 0; i < m_threads; ++i) {
                pool.submit_on_thread(i,
                    fn(static_cast<int>(i), shutdown).release());
            }
            pool.wait_all();
        } catch (const std::exception& e) {
            std::cerr << "Fatal: " << e.what() << std::endl;
            return 1;
        }
        return 0;
    }

    // Run a function that does not need graceful shutdown (clients, one-shot
    // operations). Signal handlers are still installed so the process can be
    // terminated with Ctrl+C.
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
            std::cerr << "Fatal: " << e.what() << std::endl;
            return 1;
        }
        return 0;
    }

private:
    size_t m_threads = std::thread::hardware_concurrency();
    io::IoUringEngineConfig m_io_config{};
};

// Zero-configuration entry point.  Equivalent to Launcher().run(fn).
template <typename F>
inline int launch(F&& fn) {
    return Launcher().run(std::forward<F>(fn));
}

} // namespace ynet::async

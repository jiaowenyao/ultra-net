#pragma once

#include "scheduler.h"
#include "execution_context.hpp"
#include "task.hpp"
#include "ultranet/io/io_engine.hpp"
#include "ultranet/io/io_callback.hpp"
#include "queue.hpp"
#include "mpsc_queue.hpp"
#include <thread>
#include <future>
#include <random>
#include <iostream>
#include <atomic>
#include <sys/eventfd.h>
#include <unistd.h>


namespace ynet::async::scheduling {

class WorkStealingThreadPool final : public Scheduler {
public:
    explicit WorkStealingThreadPool(size_t num_threads = std::thread::hardware_concurrency())
        : m_stop(false)
        , m_active_tasks(0) {

        if (num_threads == 0) {
            num_threads = 1;
        }

        m_event_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (m_event_fd < 0) {
            throw std::system_error(errno, std::system_category(), "eventfd creation failed");
        }

        // 创建工作线程的本地队列
        m_local_queues.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            m_local_queues.emplace_back(
                std::make_unique<WorkStealingQueue<UnifiedTask>>()
            );
        }

        // 创建跨线程MPSC队列（取代全局锁队列）
        m_mpsc_queues.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            m_mpsc_queues.emplace_back(
                std::make_unique<MpscQueue<UnifiedTask>>()
            );
        }

        // 启动工作线程
        m_workers.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            m_workers.emplace_back([this, i] { worker_thread(i); });
        }
    }

    ~WorkStealingThreadPool() override {
        m_stop.store(true, std::memory_order_release);
        wake_workers(m_workers.size());
        for (auto& worker : m_workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        if (m_event_fd >= 0) {
            ::close(m_event_fd);
        }
    }

    WorkStealingThreadPool(const WorkStealingThreadPool&) = delete;
    WorkStealingThreadPool& operator=(const WorkStealingThreadPool&) = delete;

    void submit(std::coroutine_handle<> handle) override {
        submit_coroutine(handle);
    }

    void resubmit(std::coroutine_handle<> handle) override {
        resubmit_coroutine(handle);
    }

    bool is_current_thread() const override {
        return t_thread_local_state.pool == this;
    }

    const char* name() const noexcept override {
        return "WorkStealingThreadPool";
    }

    size_t worker_count() const noexcept override {
        return m_workers.size();
    }

    size_t pending_tasks() const noexcept override {
        size_t total = 0;
        for (const auto& q : m_mpsc_queues) {
            total += q->approximate_size();
        }
        for (const auto& queue : m_local_queues) {
            total += queue->size();
        }
        return total;
    }

    void increment_tasks() noexcept override {
        m_active_tasks.fetch_add(1, std::memory_order_relaxed);
    }

    void decrement_tasks() noexcept override {
        if (m_active_tasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(m_completion_mutex);
            m_completion_cv.notify_all();
        }
    }

    size_t total_submitted_ops() const noexcept override {
        return m_stats.submitted_ops.load(std::memory_order_relaxed);
    }

    size_t total_completed_ops() const noexcept override {
        return m_stats.completed_ops.load(std::memory_order_relaxed);
    }

    template <typename Func>
    void submit_function(Func&& func) {
        increment_tasks();
        UnifiedTask task([this, func = std::forward<Func>(func)]() mutable {
            func();
            decrement_tasks();
        });

        if (t_thread_local_state.pool == this &&
            t_thread_local_state.worker_id < m_local_queues.size()) {
            m_local_queues[t_thread_local_state.worker_id]->push(std::move(task));
        }
        else {
            enqueue_external(std::move(task));
        }
    }

    void submit_coroutine(std::coroutine_handle<> handle) {
        if (!handle || handle.done()) return;
        increment_tasks();
        UnifiedTask task(handle);
        enqueue_task(std::move(task));
    }

    void resubmit_coroutine(std::coroutine_handle<> handle) {
        if (!handle || handle.done()) return;
        UnifiedTask task(handle);
        enqueue_task(std::move(task));
    }

    void enqueue_task(UnifiedTask task) {
        if (t_thread_local_state.pool == this &&
            t_thread_local_state.worker_id < m_local_queues.size()) {
            m_local_queues[t_thread_local_state.worker_id]->push(std::move(task));
        }
        else {
            enqueue_external(std::move(task));
        }
    }

    void enqueue_external(UnifiedTask task) {
        size_t idx = m_next_worker.fetch_add(1, std::memory_order_relaxed) % m_workers.size();
        while (!m_mpsc_queues[idx]->try_push(std::move(task))) {
            idx = m_next_worker.fetch_add(1, std::memory_order_relaxed) % m_workers.size();
        }
        wake_workers();
    }

    template <typename T>
    void submit_task(Task<T>& task) {
        if (task.handle()) {
            task.handle().promise().m_creator_scheduler = this;
        }
        submit_coroutine(task.task());
    }

    template<typename F, typename... Args>
    auto submit_with_result(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {

        using ReturnType = std::invoke_result_t<F, Args...>;
        auto pkg = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        std::future<ReturnType> result = pkg->get_future();
        submit_function([pkg]() mutable { (*pkg)(); });
        return result;
    }

    void wait_all() {
        std::unique_lock<std::mutex> lock(m_completion_mutex);
        m_completion_cv.wait(lock, [this] {
            return m_active_tasks.load(std::memory_order_acquire) == 0;
        });
    }

    template <typename Rep, typename Period>
    bool wait_all_for(const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(m_completion_mutex);
        return m_completion_cv.wait_for(lock, timeout, [this] {
            return m_active_tasks.load(std::memory_order_acquire) == 0;
        });
    }

    static size_t current_worker_id() noexcept {
        return t_thread_local_state.worker_id;
    }

    static WorkStealingThreadPool* current_pool() noexcept {
        return t_thread_local_state.pool;
    }

    size_t num_threads() const noexcept { return m_workers.size(); }

    size_t active_tasks() const noexcept {
        return m_active_tasks.load(std::memory_order_relaxed);
    }

    void record_submit() noexcept { m_stats.submitted_ops.fetch_add(1, std::memory_order_relaxed); }
    void record_completion() noexcept { m_stats.completed_ops.fetch_add(1, std::memory_order_relaxed); }

private:
    struct Stats {
        std::atomic<size_t> submitted_ops{0};
        std::atomic<size_t> completed_ops{0};
    };

    struct WakeupCallback : io::IoOperationBase {
        void resubmit() override {}
        void cancel() override {}
    };

    void wake_workers(uint64_t count = 1) {
        uint64_t val = count;
        ::write(m_event_fd, &val, sizeof(val));
    }

    std::optional<UnifiedTask> try_get_local_task(size_t worker_id) {
        if (auto t = m_local_queues[worker_id]->pop()) {
            return t;
        }
        return std::nullopt;
    }

    bool try_steal_task(size_t thief_id, std::optional<UnifiedTask>& task) {
        size_t num = m_workers.size();
        if (num <= 1) {
            return false;
        }
        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<size_t> dist(0, num - 1);
        size_t start = dist(rng);
        for (size_t i = 0; i < num; ++i) {
            size_t victim = (start + i) % num;
            if (victim == thief_id) {
                continue;
            }
            if (auto t = m_local_queues[victim]->steal()) {
                task = std::move(*t);
                return true;
            }
        }
        return false;
    }

    bool try_get_from_mpsc(size_t worker_id, std::optional<UnifiedTask>& task) {
        if (auto t = m_mpsc_queues[worker_id]->try_pop()) {
            task = std::move(*t);
            return true;
        }
        return false;
    }

    void process_io_completions() {
        auto* ctx = io::IoUringEngine::current();
        if (!ctx) return;

        ctx->for_each_cqe([this, ctx](io_uring_cqe* cqe) {
            auto* callback = reinterpret_cast<io::IoCallback*>(io_uring_cqe_get_data(cqe));
            if (!callback) {
                return;
            }

            callback->m_result = cqe->res;
            callback->m_completed = true;

            if (callback == &m_wakeup_cb) {
                uint64_t val;
                ::read(m_event_fd, &val, sizeof(val));
                submit_eventfd_read();
                return;
            }

            if (cqe->res == -EAGAIN || cqe->res == -EINTR) {
                if (callback->m_operation) {
                    auto* op = static_cast<io::IoOperationBase*>(callback->m_operation);
                    op->resubmit();
                }
            } else if (callback->m_handle) {
                ctx->decrement_pending_ops();
                record_completion();
                resubmit_coroutine(callback->m_handle);
            }
        });
    }

    void submit_eventfd_read() {
        auto* ctx = io::IoUringEngine::current();
        if (!ctx) return;
        auto* sqe = ctx->get_sqe();
        if (sqe) {
            io_uring_prep_read(sqe, m_event_fd, &m_eventfd_buf, sizeof(m_eventfd_buf), 0);
            io_uring_sqe_set_data(sqe, &m_wakeup_cb);
            ctx->increment_pending();
            ctx->submit_now();
        }
    }

    void worker_thread(size_t worker_id) {
        t_thread_local_state.pool = this;
        t_thread_local_state.worker_id = worker_id;

        ExecutionContext::Scope context_scope(this);
        io::IoUringEngine::Scope io_uring_scope{};

        submit_eventfd_read();

        while (!m_stop.load(std::memory_order_acquire)) {
            process_io_completions();

            std::optional<UnifiedTask> task;
            if ((task = try_get_local_task(worker_id))) { (*task)(); continue; }
            if (try_steal_task(worker_id, task)) { (*task)(); continue; }
            if (try_get_from_mpsc(worker_id, task)) { (*task)(); continue; }

            wait_on_io();
        }

        while (auto t = m_local_queues[worker_id]->pop()) { (*t)(); }
        while (auto t = m_mpsc_queues[worker_id]->try_pop()) { (*t)(); }

        t_thread_local_state.pool = nullptr;
        t_thread_local_state.worker_id = static_cast<size_t>(-1);
    }

    void wait_on_io() {
        auto* ctx = io::IoUringEngine::current();
        if (!ctx) return;
        ctx->flush_submit();
        struct __kernel_timespec ts = {5, 0};
        io_uring_cqe* cqe = nullptr;
        ctx->wait_cqe(&cqe, 1, &ts);
    }

    std::vector<std::thread> m_workers;
    std::vector<std::unique_ptr<WorkStealingQueue<UnifiedTask>>> m_local_queues;
    std::vector<std::unique_ptr<MpscQueue<UnifiedTask>>> m_mpsc_queues;
    std::atomic<size_t> m_next_worker{0};
    std::atomic<bool> m_stop;
    std::mutex m_completion_mutex;
    std::condition_variable m_completion_cv;
    std::atomic<size_t> m_active_tasks;
    Stats m_stats{};
    int m_event_fd{-1};
    uint64_t m_eventfd_buf{0};
    io::IoCallback m_wakeup_cb{};

    struct ThreadLocalState {
        WorkStealingThreadPool* pool = nullptr;
        size_t worker_id = static_cast<size_t>(-1);
    };
    static thread_local ThreadLocalState t_thread_local_state;
};

inline thread_local WorkStealingThreadPool::ThreadLocalState WorkStealingThreadPool::t_thread_local_state{};

} // namespace ynet::async::scheduling

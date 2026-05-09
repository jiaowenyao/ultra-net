#pragma once

#include "src/scheduler.h"
#include "src/execution_context.hpp"
#include "ultranet/io/io_engine.hpp"
#include "ultranet/io/io_callback.hpp"
#include "queue.hpp"
#include <thread>
#include <future>
#include <queue>
#include <random>
#include <iostream>
#include <atomic>


namespace ynet::async::scheduling {

class WorkStealingThreadPool final : public Scheduler {
public:
    explicit WorkStealingThreadPool(size_t num_threads = std::thread::hardware_concurrency())
        : m_stop(false)
        , m_active_tasks(0) {

        if (num_threads == 0) {
            num_threads = 1;
        }

        // 创建工作线程的本地队列
        m_local_queues.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            m_local_queues.emplace_back(
                std::make_unique<WorkStealingQueue<UnifiedTask>>()
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

        {
            std::lock_guard<std::mutex> lock(m_global_mutex);
            m_global_cv.notify_all();
        }

        for (auto& worker : m_workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    WorkStealingThreadPool(const WorkStealingThreadPool&) = delete;
    WorkStealingThreadPool& operator=(const WorkStealingThreadPool&) = delete;

    // === Scheduler 接口实现 ===
    void submit(std::coroutine_handle<> handle) override {
        submit_coroutine(handle);
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
        {
            std::lock_guard<std::mutex> lock(m_global_mutex);
            total += m_global_queue.size();
        }

        for (const auto& queue : m_local_queues) {
            if (!queue->empty()) {
                ++total;
            }
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
    // === Scheduler 接口实现结束 ===

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
            std::lock_guard<std::mutex> lock(m_global_mutex);
            m_global_queue.push(std::move(task));
            m_global_cv.notify_one();
        }
    }

    void submit_coroutine(std::coroutine_handle<> handle) {
        if (!handle || handle.done()) {
            return;
        }

        increment_tasks();
        UnifiedTask task(handle);

        if (t_thread_local_state.pool == this &&
            t_thread_local_state.worker_id < m_local_queues.size()) {
            m_local_queues[t_thread_local_state.worker_id]->push(std::move(task));
        }
        else {
            std::lock_guard<std::mutex> lock(m_global_mutex);
            m_global_queue.push(std::move(task));
            m_global_cv.notify_one();
        }
    }

    template<typename F, typename... Args>
    auto submit_with_result(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {

        using ReturnType = std::invoke_result_t<F, Args...>;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        std::future<ReturnType> result = task->get_future();

        submit_function([task]() mutable {
            (*task)();
        });

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

    size_t num_threads() const noexcept {
        return m_workers.size();
    }

    size_t active_tasks() const noexcept {
        return m_active_tasks.load(std::memory_order_relaxed);
    }

    void record_submit() noexcept {
        m_stats.submitted_ops.fetch_add(1, std::memory_order_relaxed);
    }

    void record_completion() noexcept {
        m_stats.completed_ops.fetch_add(1, std::memory_order_relaxed);
    }

private:
    struct Stats {
        std::atomic<size_t> submitted_ops{0};
        std::atomic<size_t> completed_ops{0};
    };

    std::optional<UnifiedTask> try_get_local_task(size_t worker_id) {
        if (auto task = m_local_queues[worker_id]->pop()) {
            return task;
        }
        return std::nullopt;
    }

    bool try_steal_task(size_t thief_id, std::optional<UnifiedTask>& task) {
        size_t num_workers = m_workers.size();

        if (num_workers <= 1) {
            return false;
        }

        static thread_local std::mt19937 rng(std::random_device{}());
        std::uniform_int_distribution<size_t> dist(0, num_workers - 1);
        size_t start = dist(rng);

        for (size_t i = 0; i < num_workers; ++i) {
            size_t victim = (start + i) % num_workers;

            if (victim == thief_id) {
                continue;
            }

            if (auto opt_task = m_local_queues[victim]->steal()) {
                task = std::move(*opt_task);
                return true;
            }
        }

        return false;
    }

    bool try_get_from_global(std::optional<UnifiedTask>& task) {
        std::lock_guard<std::mutex> lock(m_global_mutex);
        if (!m_global_queue.empty()) {
            task = std::move(m_global_queue.front());
            m_global_queue.pop();
            return true;
        }
        return false;
    }

    void process_io_completions() {
        auto* ctx = io::IoUringEngine::current();
        if (!ctx) return;

        io_uring_cqe* cqe;
        unsigned head;
        unsigned processed = 0;
        io_uring* ring = ctx->get_ring();

        io_uring_for_each_cqe(ring, head, cqe) {
            ++processed;
            auto* callback = reinterpret_cast<io::IoCallback*>(io_uring_cqe_get_data(cqe));
            if (callback) {
                callback->m_result = cqe->res;
                callback->m_completed = true;

                // 处理 -EAGAIN / -EINTR：需要重新提交操作
                if (cqe->res == -EAGAIN || cqe->res == -EINTR) {
                    // 重新提交操作
                    if (callback->m_operation) {
                        auto* op = static_cast<io::IoOperationBase*>(callback->m_operation);
                        op->resubmit();
                    }
                    // 不恢复协程，等待重新完成
                } else if (callback->m_handle) {
                    record_completion();
                    submit_coroutine(callback->m_handle);
                }
            }
            io_uring_cqe_seen(ring, cqe);
        }
    }

    void worker_thread(size_t worker_id) {
        t_thread_local_state.pool = this;
        t_thread_local_state.worker_id = worker_id;

        ExecutionContext::Scope context_scope(this);
        io::IoUringEngine::Scope io_uring_scope{};

        while (!m_stop.load(std::memory_order_acquire)) {
            process_io_completions();

            std::optional<UnifiedTask> task;

            if ((task = try_get_local_task(worker_id))) {
                (*task)();
                continue;
            }

            if (try_steal_task(worker_id, task)) {
                (*task)();
                continue;
            }

            if (try_get_from_global(task)) {
                (*task)();
                continue;
            }

            wait_for_task();
        }

        while (auto opt_task = m_local_queues[worker_id]->pop()) {
            (*opt_task)();
        }

        t_thread_local_state.pool = nullptr;
        t_thread_local_state.worker_id = static_cast<size_t>(-1);
    }

    void wait_for_task() {
        std::unique_lock<std::mutex> lock(m_global_mutex);
        m_global_cv.wait_for(lock, std::chrono::milliseconds(10), [this] {
            return m_stop.load(std::memory_order_relaxed) ||
                   !m_global_queue.empty();
        });
    }

private:
    std::vector<std::thread> m_workers;
    std::vector<std::unique_ptr<WorkStealingQueue<UnifiedTask>>> m_local_queues;
    std::queue<UnifiedTask> m_global_queue;
    mutable std::mutex m_global_mutex;
    std::condition_variable m_global_cv;
    std::atomic<bool> m_stop;
    std::mutex m_completion_mutex;
    std::condition_variable m_completion_cv;
    std::atomic<size_t> m_active_tasks;
    Stats m_stats{};

    struct ThreadLocalState {
        WorkStealingThreadPool* pool = nullptr;
        size_t worker_id = static_cast<size_t>(-1);
    };
    static thread_local ThreadLocalState t_thread_local_state;
};

inline thread_local WorkStealingThreadPool::ThreadLocalState WorkStealingThreadPool::t_thread_local_state{};

} // namespace ynet::async::scheduling

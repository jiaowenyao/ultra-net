#pragma once

#include "async/scheduler.h"
#include "async/execution_context.hpp"
#include "queue.hpp"
#include <thread>
#include <future>
#include <queue>
#include <random>
#include <iostream>


namespace ynet::async::scheduling {

class WorkStealingThreadPool : public async::Scheduler {
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

    /**
     * @brief 析构函数
     *
     * 等待所有任务完成后关闭线程池
     */
    ~WorkStealingThreadPool() {
        // 设置停止标志
        m_stop.store(true, std::memory_order_release);

        // 唤醒所有等待的线程
        {
            std::lock_guard<std::mutex> lock(m_global_mutex);
            m_global_cv.notify_all();
        }

        // 等待所有工作线程结束
        for (auto& worker : m_workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    // 禁用拷贝
    WorkStealingThreadPool(const WorkStealingThreadPool&) = delete;
    WorkStealingThreadPool& operator=(const WorkStealingThreadPool&) = delete;

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
            // 估计一下
            if (!queue->empty()) {
                ++total;
            }
        }
        return total;
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
            std::lock_guard<std::mutex> lock(m_global_mutex);
            m_global_queue.push(std::move(task));
            m_global_cv.notify_one();
        }
    }

    void submit_coroutine(std::coroutine_handle<> handle) {
        if (!handle || handle.done()) {
            return;
        }

        // std::cout << "submit coroutine=" << handle.address()
        //           << ",is_done=" << handle.done()
        //           << ",tasks=" << (m_active_tasks.load() + 1) << std::endl;

        increment_tasks();
        // decrement_tasks在final_suspend中处理
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

    /**
     * @brief 提交任务到线程池
     * @tparam F 可调用对象类型
     * @tparam Args 参数类型
     * @param f 可调用对象
     * @param args 参数
     * @return std::future 用于获取任务结果
     *
     * 使用示例：
     * @code
     * auto future = pool.submit_with_result([]() { return 42; });
     * int result = future.get();  // 阻塞等待结果
     * @endcode
     */
    template<typename F, typename... Args>
    auto submit_with_result(F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>> {

        using ReturnType = std::invoke_result_t<F, Args...>;

        // 创建 packaged_task 来包装任务
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        // 获取 future
        std::future<ReturnType> result = task->get_future();

        submit_function([task]() mutable {
            (*task)();
        });

        return result;
    }

    /**
     * @brief 等待所有任务完成
     * 阻塞直到所有提交的任务都执行完毕
     */
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

    /**
     * @brief 获取线程池中的线程数量
     */
    size_t num_threads() const noexcept {
        return m_workers.size();
    }

    /**
     * @brief 获取活跃任务数量
     */
    size_t active_tasks() const noexcept {
        return m_active_tasks.load(std::memory_order_relaxed);
    }

protected:
    void increment_tasks() noexcept override {
        m_active_tasks.fetch_add(1, std::memory_order_relaxed);
    }

    void decrement_tasks() noexcept override {
        if (m_active_tasks.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard<std::mutex> lock(m_completion_mutex);
            m_completion_cv.notify_all();
        }
    }

private:
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

        // 遍历所有其他线程的队列
        for (size_t i = 0; i < num_workers; ++i) {
            size_t victim = (start + i) % num_workers;

            if (victim == thief_id) {
                continue;
            }

            // 尝试窃取
            if (auto opt_task = m_local_queues[victim]->steal()) {
                task = std::move(*opt_task);
                return true;
            }
        }

        return false;
    }

    /**
     * @brief 尝试从全局队列获取任务
     * @param task 输出参数，获取到的任务
     * @return 是否获取成功
     */
    bool try_get_from_global(std::optional<UnifiedTask>& task) {
        std::lock_guard<std::mutex> lock(m_global_mutex);
        if (!m_global_queue.empty()) {
            task = std::move(m_global_queue.front());
            m_global_queue.pop();
            return true;
        }
        return false;
    }


    void worker_thread(size_t worker_id) {
        // 设置当前线程的工作ID
        t_thread_local_state.pool = this;
        t_thread_local_state.worker_id = worker_id;

        // 设置执行上下文
        ExecutionContext::Scope context_scope(this);

        while (!m_stop.load(std::memory_order_acquire)) {
            std::optional<UnifiedTask> task;

            // 策略1：从本地队列获取任务
            if ((task = try_get_local_task(worker_id))) {
                (*task)();
                continue;
            }

            // 策略2：尝试窃取其他线程的任务
            if (try_steal_task(worker_id, task)) {
                (*task)();
                continue;
            }

            // 策略3：从全局队列获取任务
            if (try_get_from_global(task)) {
                (*task)();
                continue;
            }

            // 策略4：等待新任务
            wait_for_task();
        }

        // 线程结束前，处理剩余的本地任务
        while (auto opt_task = m_local_queues[worker_id]->pop()) {
            (*opt_task)();
        }

        // 清理线程局部状态
        t_thread_local_state.pool = nullptr;
        t_thread_local_state.worker_id = static_cast<size_t>(-1);
    }

    /**
     * @brief 等待新任务
     * 当没有任务可执行时，线程进入等待状态以节省CPU
     */
    void wait_for_task() {
        std::unique_lock<std::mutex> lock(m_global_mutex);
        // 使用带超时的等待，定期检查是否应该停止
        m_global_cv.wait_for(lock, std::chrono::milliseconds(10), [this] {
            return m_stop.load(std::memory_order_relaxed) ||
                   !m_global_queue.empty();
        });
    }

private:
    // 工作线程列表
    std::vector<std::thread> m_workers;
    // 每个工作线程的本地队列
    std::vector<std::unique_ptr<WorkStealingQueue<UnifiedTask>>> m_local_queues;
    // 全局任务队列（用于外部提交的任务）
    std::queue<UnifiedTask> m_global_queue;
    // 全局队列的互斥锁
    mutable std::mutex m_global_mutex;
    // 全局队列的条件变量
    std::condition_variable m_global_cv;
    // 停止标志（必须在 active_tasks_ 之前声明，因为构造函数初始化顺序）
    std::atomic<bool> m_stop;
    // 任务完成相关
    std::mutex m_completion_mutex;
    std::condition_variable m_completion_cv;
    std::atomic<size_t> m_active_tasks;


    // 线程局部存储
    struct ThreadLocalState {
        WorkStealingThreadPool* pool = nullptr;
        size_t worker_id = static_cast<size_t>(-1);
    };
    static thread_local ThreadLocalState t_thread_local_state;
};

// 线程局部变量定义（需要在cpp文件中或者使用inline）
inline thread_local WorkStealingThreadPool::ThreadLocalState WorkStealingThreadPool::t_thread_local_state{};






} // namespace ynet::async::scheduling



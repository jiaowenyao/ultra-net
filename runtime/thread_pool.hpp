#pragma once

#include "queue.hpp"
#include "async/context.h"

#include <thread>
#include <vector>
#include <functional>
#include <future>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <random>
#include <atomic>
#include <memory>
#include <type_traits>

namespace ynet::runtime {

class Task {
public:
    Task() = default;

    template<typename F>
    Task(F&& f) : m_impl(std::make_shared<TaskImpl<F>>(std::forward<F>(f))) {}

    void operator()() {
        if (m_impl) {
            m_impl->call();
        }
    }

    explicit operator bool() const {
        return m_impl != nullptr;
    }

private:
    struct TaskBase {
        virtual ~TaskBase() = default;
        virtual void call() = 0;
    };

    template<typename F>
    struct TaskImpl : TaskBase {
        F func;
        TaskImpl(F&& f) : func(std::forward<F>(f)) {}
        void call() override { func(); }
    };

    std::shared_ptr<TaskBase> m_impl;
};


class WorkStealingThreadPool : async::Scheduler {
public:
    explicit WorkStealingThreadPool(
        size_t num_threads = std::thread::hardware_concurrency())
        : m_stop(false)
        , m_active_tasks(0) {

        if (num_threads == 0) {
            num_threads = 1;
        }

        // 创建工作线程的本地队列
        m_local_queues.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            m_local_queues.emplace_back(
                std::make_unique<WorkStealingQueue<Task>>()
            );
        }

        // 启动工作线程
        m_workers.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i) {
            m_workers.emplace_back([this, i] {
                worker_thread(i);
            });
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
     * auto future = pool.submit([]() { return 42; });
     * int result = future.get();  // 阻塞等待结果
     * @endcode
     */
    template<typename F, typename... Args>
    auto submit(F&& f, Args&&... args)
        -> std::future<typename std::invoke_result<F, Args...>::type> {

        using ReturnType = typename std::invoke_result<F, Args...>::type;

        // 创建 packaged_task 来包装任务
        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
        );

        // 获取 future
        std::future<ReturnType> result = task->get_future();

        // 增加活跃任务计数
        m_active_tasks.fetch_add(1, std::memory_order_relaxed);

        // 将任务包装并提交
        Task wrapped_task([this, task]() {
            (*task)();
            // 任务完成，减少计数
            if (m_active_tasks.fetch_sub(1, std::memory_order_release) == 1) {
                // 最后一个任务完成，通知等待者
                std::lock_guard<std::mutex> lock(m_completion_mutex);
                m_completion_cv.notify_all();
            }
        });

        // 尝试直接放入当前线程的本地队列（如果是工作线程）
        int worker_id = get_current_worker_id();
        if (worker_id >= 0) {
            m_local_queues[worker_id]->push(std::move(wrapped_task));
        } else {
            // 外部线程提交，放入全局队列
            {
                std::lock_guard<std::mutex> lock(m_global_mutex);
                m_global_queue.push(std::move(wrapped_task));
            }
            // 唤醒一个等待的工作线程
            m_global_cv.notify_one();
        }

        return result;
    }

    /**
     * @brief 提交任务但不关心返回值
     * @tparam F 可调用对象类型
     * @param f 可调用对象
     *
     * 比 submit 更轻量，适合不需要返回值的场景
     */
    template<typename F>
    void execute(F&& f) {
        // 增加活跃任务计数
        m_active_tasks.fetch_add(1, std::memory_order_relaxed);

        Task task([this, func = std::forward<F>(f)]() mutable {
            func();
            if (m_active_tasks.fetch_sub(1, std::memory_order_release) == 1) {
                std::lock_guard<std::mutex> lock(m_completion_mutex);
                m_completion_cv.notify_all();
            }
        });

        int worker_id = get_current_worker_id();
        if (worker_id >= 0) {
            m_local_queues[worker_id]->push(std::move(task));
        } else {
            {
                std::lock_guard<std::mutex> lock(m_global_mutex);
                m_global_queue.push(std::move(task));
            }
            m_global_cv.notify_one();
        }
    }

    /**
     * @brief 等待所有任务完成
     *
     * 阻塞直到所有提交的任务都执行完毕
     */
    void wait_for_all() {
        std::unique_lock<std::mutex> lock(m_completion_mutex);
        m_completion_cv.wait(lock, [this] {
            return m_active_tasks.load(std::memory_order_acquire) == 0;
        });
    }

    /**
     * @brief 尝试帮助执行一个任务（工作帮助机制）
     * @return 是否成功执行了一个任务
     *
     * 这个方法用于在等待 future 时保持工作线程活跃。
     * 它会尝试从本地队列、其他线程或全局队列获取任务执行。
     *
     * 使用场景：
     * 1. fork-join 模式中，等待子任务时调用
     * 2. 避免工作线程在等待时空闲
     * 3. 防止死锁（所有线程都在等待时）
     */
    bool try_help_once() {
        int worker_id = get_current_worker_id();
        if (worker_id < 0) {
            // 非工作线程，无法帮助
            return false;
        }

        Task task;

        // 策略1：从本地队列获取
        if (auto opt_task = m_local_queues[worker_id]->pop()) {
            task = std::move(*opt_task);
        }
        // 策略2：尝试窃取
        else {
            std::mt19937 rng(static_cast<unsigned>(
                std::chrono::steady_clock::now().time_since_epoch().count()));
            size_t num_workers = m_workers.size();
            std::uniform_int_distribution<size_t> dist(0, num_workers - 1);
            size_t start = dist(rng);

            for (size_t i = 0; i < num_workers; ++i) {
                size_t victim = (start + i) % num_workers;
                if (static_cast<int>(victim) == worker_id) continue;

                if (auto opt_task = m_local_queues[victim]->steal()) {
                    task = std::move(*opt_task);
                    break;
                }
            }
        }

        // 策略3：从全局队列获取
        if (!task) {
            std::lock_guard<std::mutex> lock(m_global_mutex);
            if (!m_global_queue.empty()) {
                task = std::move(m_global_queue.front());
                m_global_queue.pop();
            }
        }

        // 执行任务
        if (task) {
            task();
            return true;
        }

        return false;
    }

    /**
     * @brief 在等待 future 时帮助执行任务
     * @tparam T future 的返回类型
     * @param fut 要等待的 future
     * @return future 的结果
     *
     * 这是解决 fork-join 死锁问题的关键方法。
     * 当工作线程等待一个子任务完成时，它会同时帮助执行其他任务，
     * 而不是简单地阻塞等待。这样可以避免所有线程都在等待的死锁情况。
     *
     * 注意：使用帮助深度限制来防止栈溢出
     */
    template<typename T>
    T help_while_waiting(std::future<T>& fut) {
        // 如果不是工作线程，直接等待
        if (get_current_worker_id() < 0) {
            return fut.get();
        }

        // 增加帮助深度
        ++t_current_help_depth;

        // 如果帮助深度过大，不再帮助，直接等待
        // 这可以防止递归任务导致的栈溢出
        if (t_current_help_depth > MAX_HELP_DEPTH) {
            --t_current_help_depth;
            return fut.get();
        }

        // 工作线程：边等待边帮助执行任务
        while (fut.wait_for(std::chrono::microseconds(0)) != std::future_status::ready) {
            if (!try_help_once()) {
                // 没有任务可执行，短暂休眠避免忙等待
                std::this_thread::yield();
            }
        }

        --t_current_help_depth;
        return fut.get();
    }

    /**
     * @brief help_while_waiting 的 void 特化版本
     */
    void help_while_waiting_void(std::future<void>& fut) {
        if (get_current_worker_id() < 0) {
            fut.get();
            return;
        }

        // 增加帮助深度
        ++t_current_help_depth;

        // 如果帮助深度过大，不再帮助，直接等待
        if (t_current_help_depth > MAX_HELP_DEPTH) {
            --t_current_help_depth;
            fut.get();
            return;
        }

        while (fut.wait_for(std::chrono::microseconds(0)) != std::future_status::ready) {
            if (!try_help_once()) {
                std::this_thread::yield();
            }
        }

        --t_current_help_depth;
        fut.get();
    }

    /**
     * @brief 等待所有任务完成（带超时）
     * @param timeout 超时时间
     * @return 如果任务全部完成返回 true，超时返回 false
     */
    template<typename Rep, typename Period>
    bool wait_for_all(const std::chrono::duration<Rep, Period>& timeout) {
        std::unique_lock<std::mutex> lock(m_completion_mutex);
        return m_completion_cv.wait_for(lock, timeout, [this] {
            return m_active_tasks.load(std::memory_order_acquire) == 0;
        });
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

    /**
     * @brief 获取当前工作线程的ID
     * @return 如果当前线程是工作线程，返回ID（0到numThreads-1）；否则返回-1
     *
     * 这个方法使用 thread_local 变量来存储线程ID
     */
    static int get_current_worker_id() {
        return t_current_worker_id;
    }

private:
    /**
     * @brief 工作线程主函数
     * @param id 工作线程ID
     *
     * 工作线程的执行逻辑：
     * 1. 首先尝试从本地队列获取任务
     * 2. 本地队列为空时，尝试窃取其他线程的任务
     * 3. 窃取失败时，尝试从全局队列获取任务
     * 4. 都没有任务时，等待新任务
     */
    void worker_thread(size_t id) {
        // 设置当前线程的工作ID
        t_current_worker_id = static_cast<int>(id);

        // 初始化随机数生成器（用于随机选择窃取目标）
        std::mt19937 rng(static_cast<unsigned>(id));

        while (!m_stop.load(std::memory_order_acquire)) {
            Task task;

            // 策略1：从本地队列获取任务
            if (auto opt_task = m_local_queues[id]->pop()) {
                task = std::move(*opt_task);
            }
            // 策略2：尝试窃取其他线程的任务
            else if (try_steal(id, rng, task)) {
                // 窃取成功
            }
            // 策略3：从全局队列获取任务
            else if (try_get_from_global(task)) {
                // 获取成功
            }
            // 策略4：等待新任务
            else {
                wait_for_task();
                continue;
            }

            // 执行任务
            if (task) {
                task();
            }
        }

        // 线程结束前，处理剩余的本地任务
        while (auto opt_task = m_local_queues[id]->pop()) {
            (*opt_task)();
        }
    }

    /**
     * @brief 尝试从其他线程窃取任务
     * @param id 当前线程ID
     * @param rng 随机数生成器
     * @param task 输出参数，窃取到的任务
     * @return 是否窃取成功
     *
     * 窃取策略：
     * 1. 从随机位置开始
     * 2. 遍历所有其他线程的队列
     * 3. 尝试窃取第一个非空队列的任务
     */
    bool try_steal(size_t id, std::mt19937& rng, Task& task) {
        size_t num_workers = m_workers.size();

        // 如果只有一个线程，没有可窃取的目标
        if (num_workers <= 1) {
            return false;
        }

        // 随机选择起始位置，避免所有线程都从同一个位置开始窃取
        std::uniform_int_distribution<size_t> dist(0, num_workers - 1);
        size_t start = dist(rng);

        // 遍历所有其他线程的队列
        for (size_t i = 0; i < num_workers; ++i) {
            size_t victim = (start + i) % num_workers;

            // 跳过自己
            if (victim == id) {
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
    bool try_get_from_global(Task& task) {
        std::lock_guard<std::mutex> lock(m_global_mutex);
        if (!m_global_queue.empty()) {
            task = std::move(m_global_queue.front());
            m_global_queue.pop();
            return true;
        }
        return false;
    }

    /**
     * @brief 等待新任务
     * 当没有任务可执行时，线程进入等待状态以节省CPU
     */
    void wait_for_task() {
        std::unique_lock<std::mutex> lock(m_global_mutex);
        // 使用带超时的等待，定期检查是否应该停止
        m_global_cv.wait_for(lock, std::chrono::milliseconds(1), [this] {
            return m_stop.load(std::memory_order_relaxed) ||
                   !m_global_queue.empty();
        });
    }

private:
    // 工作线程列表
    std::vector<std::thread> m_workers;
    // 每个工作线程的本地队列
    std::vector<std::unique_ptr<WorkStealingQueue<Task>>> m_local_queues;
    // 全局任务队列（用于外部提交的任务）
    std::queue<Task> m_global_queue;
    // 全局队列的互斥锁
    std::mutex m_global_mutex;
    // 全局队列的条件变量
    std::condition_variable m_global_cv;
    // 停止标志（必须在 active_tasks_ 之前声明，因为构造函数初始化顺序）
    std::atomic<bool> m_stop;
    // 任务完成相关
    std::mutex m_completion_mutex;
    std::condition_variable m_completion_cv;
    std::atomic<size_t> m_active_tasks;

    // 线程局部变量：当前线程的工作ID
    static thread_local int t_current_worker_id;

    // 线程局部变量：当前线程的帮助深度（用于防止栈溢出）
    static thread_local int t_current_help_depth;

    // 最大帮助深度（防止递归任务导致的栈溢出）
    static constexpr int MAX_HELP_DEPTH = 8;
};

// 线程局部变量定义（需要在cpp文件中或者使用inline）
inline thread_local int WorkStealingThreadPool::t_current_worker_id = -1;
inline thread_local int WorkStealingThreadPool::t_current_help_depth = 0;


/**
 * @brief 并行for循环
 *
 * 将一个范围 [begin, end) 分割成多个子任务并行执行
 *
 * @tparam Index 索引类型
 * @tparam Func 函数类型
 * @tparam GrainType 粒度类型（自动推导）
 * @param pool 线程池
 * @param begin 起始索引
 * @param end 结束索引
 * @param func 对每个索引执行的函数
 * @param grain_size 粒度（每个任务处理的元素数量）
 */
template<typename Index, typename Func, typename GrainType = Index>
void parallel_for(WorkStealingThreadPool& pool,
                  Index begin, Index end,
                  Func&& func,
                  GrainType grain_size_param = 1) {
    // 将 grain_size 转换为 Index 类型，确保类型一致
    Index grain_size = static_cast<Index>(grain_size_param);
    // 如果范围太小，直接顺序执行
    if (end - begin <= grain_size) {
        for (Index i = begin; i < end; ++i) {
            func(i);
        }
        return;
    }

    // 计算任务数量
    Index num_tasks = (end - begin + grain_size - 1) / grain_size;

    // 使用 shared_ptr 捕获 func，避免拷贝问题
    auto shared_func = std::make_shared<std::decay_t<Func>>(std::forward<Func>(func));

    // 提交任务
    std::vector<std::future<void>> futures;
    futures.reserve(num_tasks);

    for (Index task_id = 0; task_id < num_tasks; ++task_id) {
        Index task_begin = begin + task_id * grain_size;
        Index task_end = std::min(task_begin + grain_size, end);

        futures.push_back(pool.submit([shared_func, task_begin, task_end]() {
            for (Index i = task_begin; i < task_end; ++i) {
                (*shared_func)(i);
            }
        }));
    }

    // 等待所有任务完成（使用工作帮助机制）
    for (auto& f : futures) {
        pool.help_while_waiting_void(f);
    }
}


/**
 * @brief 递归任务分解辅助类
 * 用于实现分治算法的并行版本（如并行排序、并行归约等）
 * @tparam T 返回值类型
 */
template<typename T>
class RecursiveTask {
public:
    /**
     * @brief 构造函数
     * @param pool 线程池引用
     */
    explicit RecursiveTask(WorkStealingThreadPool& pool) : m_pool(pool) {}

    /**
     * @brief fork-join 模式执行
     * @tparam Func 任务函数类型
     * @param left_task 左子任务
     * @param right_task 右子任务
     * @return 两个子任务的结果对
     *
     * 注意：使用 helpWhileWaiting 来避免死锁。
     * 当等待右子任务时，当前线程会帮助执行其他任务。
     */
    template<typename LeftFunc, typename RightFunc>
    auto fork_join(LeftFunc&& left_task, RightFunc&& right_task)
        -> std::pair<
            typename std::invoke_result<LeftFunc>::type,
            typename std::invoke_result<RightFunc>::type
        > {

        // 提交右子任务到线程池
        auto right_future = m_pool.submit(std::forward<RightFunc>(right_task));

        // 当前线程执行左子任务
        auto left_result = left_task();

        // 等待右子任务完成（使用工作帮助机制避免死锁）
        auto right_result = m_pool.help_while_waiting(right_future);

        return std::make_pair(std::move(left_result), std::move(right_result));
    }

    /**
     * @brief fork-join 模式执行（void 特化）
     *
     * 使用工作帮助机制，在等待子任务时帮助执行其他任务，
     * 避免所有线程都阻塞等待导致的死锁问题。
     */
    template<typename LeftFunc, typename RightFunc>
    void fork_join_void(LeftFunc&& left_task, RightFunc&& right_task) {
        // 提交右子任务到线程池
        auto right_future = m_pool.submit(std::forward<RightFunc>(right_task));

        // 当前线程执行左子任务
        left_task();

        // 等待右子任务完成（使用工作帮助机制避免死锁）
        m_pool.help_while_waiting_void(right_future);
    }

private:
    WorkStealingThreadPool& m_pool;
};

} // namespace ynet::runtime


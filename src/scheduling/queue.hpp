#pragma once

#include "unified_task.hpp"
#include <atomic>
#include <memory>
#include <optional>
#include <vector>

namespace ynet::async::scheduling {

template<typename T>
class CircularBuffer {
public:
    /**
     * @param capacity 初始容量，必须是2的幂次方
     */
    explicit CircularBuffer(size_t capacity)
        : m_capacity(capacity)
        , m_mask(capacity - 1)
        , m_buffer(std::make_unique<T[]>(capacity)) {
    }

    size_t capacity() const noexcept {
        return m_capacity;
    }

    void put(int64_t index, T item) noexcept {
        m_buffer[index & m_mask] = std::move(item);
    }

    const T& get(int64_t index) const noexcept {
        return m_buffer[index & m_mask];
    }

    T get(int64_t index) noexcept {
        return std::move(m_buffer[index & m_mask]);
    }

    /**
     * @brief 扩容操作，创建一个容量翻倍的新缓冲区
     * @param bottom 当前队列的 bottom 索引
     * @param top 当前队列的 top 索引
     * @return 新的环形缓冲区指针
     *
     * 扩容步骤：
     * 1. 创建容量为原来2倍的新缓冲区
     * 2. 将旧缓冲区中 [top, bottom) 范围的任务复制到新缓冲区
     * 3. 返回新缓冲区
     */
    CircularBuffer* resize(int64_t bottom, int64_t top) {
        // 创建新的缓冲区，容量翻倍
        CircularBuffer* new_buffer = new CircularBuffer(m_capacity * 2);

        // 复制现有任务到新缓冲区
        // 注意：这里从 top 到 bottom，包含所有有效任务
        for (int64_t i = top; i < bottom; ++i) {
            new_buffer->put(i, get(i));
        }

        return new_buffer;
    }

private:
    size_t m_capacity;                    // 缓冲区容量
    size_t m_mask;                        // 用于快速取模的掩码 (capacity - 1)
    std::unique_ptr<T[]> m_buffer;        // 实际存储数组
};

/**
 * @brief 工作窃取队列
 * @tparam T 任务类型
 */
template<typename T>
class WorkStealingQueue {
public:
    // 默认初始容量（1024个任务槽位）
    static constexpr size_t DEFAULT_CAPACITY = 1024;

    // 最小容量（8个任务槽位）
    static constexpr size_t MIN_CAPACITY = 8;

    /**
     * @brief 构造函数
     * @param capacity 初始容量，会被向上取整到2的幂次方
     */
    explicit WorkStealingQueue(size_t capacity = DEFAULT_CAPACITY)
        : m_top(0)
        , m_bottom(0) {
        // 确保容量至少为最小值
        capacity = std::max(capacity, MIN_CAPACITY);

        // 将容量向上取整到2的幂次方
        // 例如：1000 -> 1024, 100 -> 128
        capacity = roundUpToPowerOfTwo(capacity);

        // 创建初始的环形缓冲区
        m_buffer.store(new CircularBuffer<T>(capacity), std::memory_order_relaxed);
    }

    /**
     * @brief 析构函数
     *
     * 释放所有分配的环形缓冲区内存
     */
    ~WorkStealingQueue() {
        // 删除当前缓冲区
        delete m_buffer.load(std::memory_order_relaxed);

        // 删除所有垃圾缓冲区（扩容时产生的旧缓冲区）
        for (auto* buf : m_garbage) {
            delete buf;
        }
    }

    // 禁用拷贝（工作窃取队列不应该被拷贝）
    WorkStealingQueue(const WorkStealingQueue&) = delete;
    WorkStealingQueue& operator=(const WorkStealingQueue&) = delete;

    /**
     * @brief 向队列中添加任务（只有拥有者线程调用）
     * @param item 要添加的任务
     *
     * 操作流程：
     * 1. 读取当前的 bottom 和 top
     * 2. 检查是否需要扩容
     * 3. 将任务放入 bottom 位置
     * 4. 递增 bottom
     *
     * 内存序分析：
     * - 对 top 使用 acquire 读取，确保看到窃取者的最新修改
     * - 对 bottom 使用 relaxed，因为只有拥有者线程修改 bottom
     * - 最后对 bottom 使用 release 写入，确保任务的写入对窃取者可见
     */
    void push(T item) {
        // 读取 bottom（只有拥有者修改，所以 relaxed 即可）
        int64_t b = m_bottom.load(std::memory_order_relaxed);

        // 读取 top（使用 acquire 确保看到窃取者的修改）
        int64_t t = m_top.load(std::memory_order_acquire);

        // 获取当前缓冲区
        CircularBuffer<T>* buf = m_buffer.load(std::memory_order_relaxed);

        // 检查是否需要扩容
        // 当前元素数量 = bottom - top
        // 如果元素数量 >= 容量 - 1，则需要扩容（留一个空位避免满和空的混淆）
        if (static_cast<size_t>(b - t) >= buf->capacity() - 1) {
            // 扩容
            buf = buf->resize(b, t);

            // 保存旧缓冲区到垃圾列表（稍后释放）
            // 不能立即释放，因为可能有窃取者正在读取
            m_garbage.push_back(m_buffer.load(std::memory_order_relaxed));

            // 更新缓冲区指针
            m_buffer.store(buf, std::memory_order_relaxed);
        }

        // 将任务放入 bottom 位置
        buf->put(b, std::move(item));

        // 这里需要一个写屏障，确保任务的写入发生在 bottom 递增之前
        std::atomic_thread_fence(std::memory_order_release);

        // 递增 bottom
        m_bottom.store(b + 1, std::memory_order_relaxed);
    }

    /**
     * @brief 从队列中取出任务（只有拥有者线程调用）
     * @return 如果队列非空，返回任务；否则返回 std::nullopt
     *
     * 操作流程：
     * 1. 递减 bottom
     * 2. 读取 top
     * 3. 如果队列为空（bottom <= top），恢复 bottom 并返回空
     * 4. 取出任务
     * 5. 如果只剩一个任务，需要与窃取者竞争
     *
     * 这是最复杂的操作，因为需要处理与 steal 的竞争：
     * - 当队列中只剩一个任务时，pop 和 steal 可能同时发生
     * - 使用 CAS 操作来解决这个竞争
     */
    std::optional<T> pop() {
        // 先递减 bottom（乐观地假设能取到任务）
        int64_t b = m_bottom.load(std::memory_order_relaxed) - 1;
        CircularBuffer<T>* buf = m_buffer.load(std::memory_order_relaxed);

        // 使用 seq_cst 保证与 steal 的正确交互
        m_bottom.store(b, std::memory_order_seq_cst);

        // 读取 top
        int64_t t = m_top.load(std::memory_order_seq_cst);

        // 判断队列状态
        if (t <= b) {
            // 队列非空，至少有一个任务
            T item = buf->get(b);

            if (t == b) {
                if (!m_top.compare_exchange_strong(
                        t, t + 1,
                        std::memory_order_seq_cst,
                        std::memory_order_relaxed)) {
                    m_bottom.store(b + 1, std::memory_order_relaxed);
                    return std::nullopt;
                }
                m_bottom.store(b + 1, std::memory_order_relaxed);
            }

            return item;
        } else {
            // 队列为空（bottom < top 是可能的，因为我们先递减了 bottom）
            // 恢复 bottom
            m_bottom.store(b + 1, std::memory_order_relaxed);
            return std::nullopt;
        }
    }

    /**
     * @brief 从队列中窃取任务（其他线程调用）
     * @return 如果队列非空，返回任务；否则返回 std::nullopt
     *
     * 操作流程：
     * 1. 读取 top
     * 2. 加载内存屏障
     * 3. 读取 bottom
     * 4. 如果队列为空，返回空
     * 5. 取出任务
     * 6. 使用 CAS 递增 top
     *
     * 窃取从 top 端进行，与 pop 从 bottom 端进行，减少了竞争
     */
    std::optional<T> steal() {
        // 读取 top
        int64_t t = m_top.load(std::memory_order_acquire);

        // 内存屏障，确保 top 在 bottom 之前读取
        // 这对于正确性很重要
        std::atomic_thread_fence(std::memory_order_seq_cst);

        // 读取 bottom
        int64_t b = m_bottom.load(std::memory_order_acquire);

        // 检查队列是否为空
        if (t >= b) {
            // 队列为空
            return std::nullopt;
        }

        // 队列非空，尝试窃取 top 位置的任务
        CircularBuffer<T>* buf = m_buffer.load(std::memory_order_consume);
        T item = buf->get(t);

        // 使用 CAS 尝试递增 top
        // 如果成功，说明我们成功窃取了任务
        // 如果失败，说明有其他窃取者或者拥有者已经修改了 top
        if (!m_top.compare_exchange_strong(
                t, t + 1,
                std::memory_order_seq_cst,
                std::memory_order_relaxed)) {
            // CAS 失败，重试
            // 注意：这里选择返回空而不是重试，让调用者决定是否重试
            return std::nullopt;
        }

        return item;
    }

    size_t size() const noexcept {
        int64_t b = m_bottom.load(std::memory_order_relaxed);
        int64_t t = m_top.load(std::memory_order_relaxed);
        return static_cast<size_t>(std::max(b - t, int64_t(0)));
    }

    bool empty() const noexcept {
        return size() == 0;
    }

    size_t capacity() const noexcept {
        return m_buffer.load(std::memory_order_relaxed)->capacity();
    }

private:
    static size_t roundUpToPowerOfTwo(size_t n) {
        if (n == 0) return 1;
        --n;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    alignas(64) std::atomic<int64_t> m_top;
    alignas(64) std::atomic<int64_t> m_bottom;
    std::atomic<CircularBuffer<T>*> m_buffer;
    std::vector<CircularBuffer<T>*> m_garbage;
};





} // namespace ynet::async::scheduling



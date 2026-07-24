#pragma once

#include <liburing.h>
#include <system_error>
#include <unordered_map>
#include <memory>
#include <atomic>

#include "ultranet/buffer/buffer.h"

namespace ynet::async::io {

inline constexpr size_t IOURING_DEFAULT_ENTRIES = 1024;
inline constexpr size_t BATCH_SUBMIT_THRESHOLD = 64;


class IoUringEngine;

struct IoUringEngineConfig {
    size_t entries = IOURING_DEFAULT_ENTRIES;
    uint32_t flags = 0;
    bool enable_sq_poll = false;                      // 是否启用SQ轮询
    uint32_t sq_poll_thread_cpu = 0;                  // SQ轮询线程CPU
    uint32_t sq_poll_thread_idle = 0;                 // SQ轮询线程空闲超时
    bool use_fixed_buffers = true;                    // 是否使用注册缓冲区
    size_t batch_threshold = BATCH_SUBMIT_THRESHOLD;  // 批量提交阈值
    size_t max_pending_ops = 4096;                    // 最大待处理操作数（背压阈值）
};


class IoUringEngine {
public:
    using Config = IoUringEngineConfig;

    static IoUringEngine* current() noexcept {
        return t_current_context;
    }

    static bool has_current() noexcept {
        return t_current_context != nullptr;
    }

    // 线程局部的引擎实例。由 Scope RAII 管理生命周期，
    // init_thread_local 分配，destroy_thread_local 释放。
    static void init_thread_local(const Config& config = Config{}) {
        if (!t_current_context) {
            t_current_context = new IoUringEngine(config);
        }
    }

    static void destroy_thread_local() {
        delete t_current_context;
        t_current_context = nullptr;
    }

    io_uring_sqe* get_sqe() noexcept {
        return io_uring_get_sqe(&m_ring);
    }

    int submit() noexcept {
        if (m_pending_sqes.load(std::memory_order_relaxed) == 0) {
            return 0;
        }
        int ret = io_uring_submit(&m_ring);
        if (ret > 0) {
            m_pending_sqes.fetch_sub(ret, std::memory_order_relaxed);
        }
        return ret;
    }

    int submit_now() noexcept {
        int ret = io_uring_submit(&m_ring);
        if (ret > 0) {
            m_pending_sqes.fetch_sub(ret, std::memory_order_relaxed);
        }
        return ret;
    }

    void flush_submit() noexcept {
        if (m_pending_sqes.load(std::memory_order_relaxed) > 0) {
            submit_now();
        }
    }

    void increment_pending() noexcept {
        m_pending_sqes.fetch_add(1, std::memory_order_relaxed);
    }

    // 检查是否应该提交
    bool should_submit() const noexcept {
        return m_pending_sqes.load(std::memory_order_relaxed) >= m_config.batch_threshold;
    }

    void increment_pending_ops() noexcept {
        m_pending_ops.fetch_add(1, std::memory_order_relaxed);
    }

    void decrement_pending_ops() noexcept {
        m_pending_ops.fetch_sub(1, std::memory_order_relaxed);
    }

    // Batched decrement — call once per N CQEs instead of once per CQE.
    void decrement_pending_ops_by(size_t count) noexcept {
        if (count > 0)
            m_pending_ops.fetch_sub(count, std::memory_order_relaxed);
    }

    bool over_watermark() const noexcept {
        return m_pending_ops.load(std::memory_order_relaxed) >= m_config.max_pending_ops;
    }

    size_t pending_op_count() const noexcept {
        return m_pending_ops.load(std::memory_order_relaxed);
    }

    // CQE overflow detection: when the CQ ring overflows under extreme load,
    // CQEs are silently dropped. This detection lets callers recover by
    // adjusting their pending_ops tracking.
    bool has_cq_overflow() const noexcept {
        return io_uring_cq_has_overflow(&m_ring);
    }

    // Heuristic recovery: halve pending_ops since we don't know exactly
    // how many CQEs were dropped. Recovery stabilises within 2 poll cycles.
    void adjust_pending_ops_on_overflow() noexcept {
        size_t cur = m_pending_ops.load(std::memory_order_relaxed);
        if (cur > 0) {
            m_pending_ops.store(cur / 2, std::memory_order_relaxed);
        }
    }

    int wait_cqe(io_uring_cqe** cqe, uint32_t wait_nr = 1,
                 struct __kernel_timespec* ts = nullptr) {
        return io_uring_wait_cqe_timeout(&m_ring, cqe, ts);
    }

    void cqe_seen(io_uring_cqe* cqe) noexcept {
        io_uring_cqe_seen(&m_ring, cqe);
    }

    template <typename Handler>
    size_t for_each_cqe(Handler&& handler) {
        io_uring_cqe* cqe;
        unsigned head;
        size_t count = 0;
        io_uring_for_each_cqe(&m_ring, head, cqe) {
            ++count;
            std::forward<Handler>(handler)(cqe);
        }
        if (count > 0) {
            io_uring_cq_advance(&m_ring, count);
        }
        return count;
    }

    // Process at most `max_cqes` CQEs, leaving the rest for the next poll.
    // Used by two-level CQE processing to interleave with coroutine execution.
    template <typename Handler>
    size_t for_each_cqe_n(size_t max_cqes, Handler&& handler) {
        if (max_cqes == 0) return for_each_cqe(std::forward<Handler>(handler));
        io_uring_cqe* cqe;
        unsigned head;
        size_t count = 0;
        io_uring_for_each_cqe(&m_ring, head, cqe) {
            if (count >= max_cqes) break;
            ++count;
            std::forward<Handler>(handler)(cqe);
        }
        if (count > 0) {
            io_uring_cq_advance(&m_ring, count);
        }
        return count;
    }

    io_uring* get_ring() noexcept { return &m_ring; }
    bool is_valid() const noexcept { return m_ring.ring_fd >= 0; }

    BufferGroup& register_buffer_group(unsigned gid, size_t entries = 1024,
                                        size_t buf_size = 4096) {
        auto it = m_buffer_groups.find(gid);
        if (it != m_buffer_groups.end()) {
            return *it->second;
        }
        auto group = std::make_unique<BufferGroup>(gid, entries, buf_size, &m_ring);
        auto* ptr = group.get();
        m_buffer_groups[gid] = std::move(group);
        return *ptr;
    }

    void* get_buffer(unsigned gid, unsigned bid) noexcept {
        auto it = m_buffer_groups.find(gid);
        if (it != m_buffer_groups.end()) {
            return it->second->get_buffer(bid);
        }
        return nullptr;
    }

private:
    explicit IoUringEngine(const Config& config)
        : m_config(config) {
        m_ring.ring_fd = -1;
        io_uring_params params{};
        params.flags = config.flags;
        // Double the CQ ring size to reduce overflow risk under load.
        params.flags |= IORING_SETUP_CQSIZE;
        params.cq_entries = config.entries * 2;
        if (config.enable_sq_poll) {
            params.flags |= IORING_SETUP_SQPOLL;
            if (config.sq_poll_thread_cpu > 0) {
                params.sq_thread_cpu = config.sq_poll_thread_cpu;
            }
            if (config.sq_poll_thread_idle > 0) {
                params.sq_thread_idle = config.sq_poll_thread_idle;
            }
        }
        int ret = io_uring_queue_init_params(config.entries, &m_ring, &params);
        if (ret < 0) {
            m_ring.ring_fd = -1;
            throw std::system_error(-ret, std::system_category(), "io_uring_queue_init_params failed");
        }
    }

    ~IoUringEngine() {
        if (m_ring.ring_fd >= 0) {
            io_uring_submit(&m_ring);
            m_buffer_groups.clear();
            io_uring_queue_exit(&m_ring);
        }
    }

    IoUringEngine(const IoUringEngine&) = delete;
    IoUringEngine& operator=(const IoUringEngine&) = delete;

    Config m_config;
    io_uring m_ring{};
    std::unordered_map<unsigned, std::unique_ptr<BufferGroup>> m_buffer_groups;
    std::atomic<size_t> m_pending_sqes{0};
    std::atomic<size_t> m_pending_ops{0};
    // 裸指针，生命周期由 init/destroy_thread_local 静态方法管理
    static thread_local IoUringEngine* t_current_context;
};

inline thread_local IoUringEngine* IoUringEngine::t_current_context = nullptr;

} // namespace ynet::async::io

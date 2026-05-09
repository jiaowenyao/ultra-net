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
class BufferGroup;

struct IoUringEngineConfig {
    size_t entries = IOURING_DEFAULT_ENTRIES;
    uint32_t flags = 0;
    bool enable_sq_poll = false;      // 是否启用SQ轮询
    uint32_t sq_poll_thread_cpu = 0;  // SQ轮询线程CPU
    uint32_t sq_poll_thread_idle = 0; // SQ轮询线程空闲超时
    bool use_fixed_buffers = true;    // 是否使用注册缓冲区
    size_t batch_threshold = BATCH_SUBMIT_THRESHOLD;  // 批量提交阈值
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

    static void init_thread_local(const Config& config = Config{}) {
        if (!t_current_context) {
            t_current_context = new IoUringEngine(config);
        }
    }

    static void destroy_thread_local() {
        if (t_current_context) {
            delete t_current_context;
            t_current_context = nullptr;
        }
    }

    // 作用域守卫
    class Scope {
    public:
        explicit Scope(const Config& config = Config{}) {
            init_thread_local(config);
        }

        ~Scope() {
            destroy_thread_local();
        }

        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

        Scope(Scope&&) = delete;
        Scope& operator=(Scope&&) = delete;
    };

    // 获取 SQE，不再自动设置 IOSQE_IO_LINK
    io_uring_sqe* get_sqe() noexcept {
        return io_uring_get_sqe(&m_ring);
    }

    // 批量提交
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

    // 强制立即提交
    int submit_now() noexcept {
        int ret = io_uring_submit(&m_ring);
        if (ret > 0) {
            m_pending_sqes.fetch_sub(ret, std::memory_order_relaxed);
        }
        return ret;
    }

    // 增加待提交计数
    void increment_pending() noexcept {
        m_pending_sqes.fetch_add(1, std::memory_order_relaxed);
    }

    // 检查是否应该提交
    bool should_submit() const noexcept {
        return m_pending_sqes.load(std::memory_order_relaxed) >= m_config.batch_threshold;
    }

    // 获取待提交数量
    size_t pending_sqes() const noexcept {
        return m_pending_sqes.load(std::memory_order_relaxed);
    }

    int wait_cqe(io_uring_cqe** cqe, uint32_t wait_nr = 1,
                 struct __kernel_timespec* ts = nullptr) {
        return io_uring_wait_cqe_timeout(&m_ring, cqe, ts);
    }

    void cqe_seen(io_uring_cqe* cqe) noexcept {
        io_uring_cqe_seen(&m_ring, cqe);
    }

    template <typename Callback>
    void for_each_cqe(Callback&& callback) {
        io_uring_cqe* cqe;
        unsigned head;
        unsigned count = 0;

        io_uring_for_each_cqe(&m_ring, head, cqe) {
            ++count;
            std::forward<Callback>(callback)(cqe);
        }

        if (count > 0) {
            io_uring_cq_advance(&m_ring, count);
        }
    }

    io_uring* get_ring() noexcept { return &m_ring; }
    bool is_valid() const noexcept { return m_ring.ring_fd >= 0; }

    // 注册Buffer Group
    BufferGroup& register_buffer_group(unsigned gid, size_t entries = 1024, size_t buf_size = 4096) {
        auto it = m_buffer_groups.find(gid);
        if (it != m_buffer_groups.end()) {
            return *it->second;
        }

        auto group = std::make_unique<BufferGroup>(gid, entries, buf_size);
        auto* ptr = group.get();
        m_buffer_groups[gid] = std::move(group);
        return *ptr;
    }

    // 获取buffer
    void* get_buffer(unsigned gid, unsigned bid) noexcept {
        auto it = m_buffer_groups.find(gid);
        if (it != m_buffer_groups.end()) {
            return it->second->get_buffer(bid);
        }
        return nullptr;
    }

    // 统计接口
    size_t total_submitted() const noexcept {
        return m_stats.submitted.load(std::memory_order_relaxed);
    }

    size_t total_completed() const noexcept {
        return m_stats.completed.load(std::memory_order_relaxed);
    }

    size_t peak_pending() const noexcept {
        return m_stats.peak_pending.load(std::memory_order_relaxed);
    }

    void record_submit(size_t count = 1) noexcept {
        m_stats.submitted.fetch_add(count, std::memory_order_relaxed);
    }

    void record_completion() noexcept {
        m_stats.completed.fetch_add(1, std::memory_order_relaxed);
    }

private:
    explicit IoUringEngine(const Config& config)
        : m_config(config) {
        io_uring_params params{};
        params.flags = config.flags;

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
            throw std::system_error(-ret, std::system_category(), "io_uring_queue_init_params failed");
        }
    }

    ~IoUringEngine() {
        // 确保提交所有待处理的 SQE
        if (m_ring.ring_fd >= 0) {
            io_uring_submit(&m_ring);
            io_uring_queue_exit(&m_ring);
        }
    }

    IoUringEngine(const IoUringEngine&) = delete;
    IoUringEngine& operator=(const IoUringEngine&) = delete;

    struct Stats {
        std::atomic<size_t> submitted{0};
        std::atomic<size_t> completed{0};
        std::atomic<size_t> peak_pending{0};
    };

    Config m_config;
    io_uring m_ring{};
    std::unordered_map<unsigned, std::unique_ptr<BufferGroup>> m_buffer_groups;
    std::atomic<size_t> m_pending_sqes{0};
    Stats m_stats{};
    static thread_local IoUringEngine* t_current_context;
};

inline thread_local IoUringEngine* IoUringEngine::t_current_context = nullptr;


} // namespace ynet::async::io

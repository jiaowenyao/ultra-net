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
    bool enable_sq_poll = false;                      // 是否启用SQ轮询
    uint32_t sq_poll_thread_cpu = 0;                  // SQ轮询线程CPU
    uint32_t sq_poll_thread_idle = 0;                 // SQ轮询线程空闲超时
    bool use_fixed_buffers = true;                    // 是否使用注册缓冲区
    size_t batch_threshold = BATCH_SUBMIT_THRESHOLD;  // 批量提交阈值
    size_t max_pending_ops = 256;                     // 最大待处理操作数（背压阈值）
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

    bool over_watermark() const noexcept {
        return m_pending_ops.load(std::memory_order_relaxed) >= m_config.max_pending_ops;
    }

    size_t pending_op_count() const noexcept {
        return m_pending_ops.load(std::memory_order_relaxed);
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

private:
    explicit IoUringEngine(const Config& config)
        : m_config(config) {
        m_ring.ring_fd = -1;
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
            m_ring.ring_fd = -1;
            throw std::system_error(-ret, std::system_category(), "io_uring_queue_init_params failed");
        }
    }

    ~IoUringEngine() {
        if (m_ring.ring_fd >= 0) {
            io_uring_submit(&m_ring);
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
    static thread_local IoUringEngine* t_current_context;
};

inline thread_local IoUringEngine* IoUringEngine::t_current_context = nullptr;

} // namespace ynet::async::io

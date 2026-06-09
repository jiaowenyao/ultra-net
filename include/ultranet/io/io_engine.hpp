#pragma once

#include <liburing.h>
#include <system_error>
#include <unordered_map>
#include <memory>
#include <atomic>
// BufferGroup forward-declared; full definition needed only by methods
// defined out-of-line in src/io_engine.cc (breaks circular dependency).

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

    BufferGroup& register_buffer_group(unsigned gid, size_t entries = 1024, size_t buf_size = 4096);
    void* get_buffer(unsigned gid, unsigned bid) noexcept;

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

    ~IoUringEngine();  // defined in src/io_engine.cc

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

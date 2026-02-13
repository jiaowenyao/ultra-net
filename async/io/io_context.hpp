#pragma once

#include <liburing.h>
#include <system_error>
#include <unordered_map>
#include <memory>
#include "buffer.h"

namespace ynet::async::io {

inline constexpr size_t IOURING_DEFAULT_ENTRIES = 1024;


class IoUringContext;
class BufferGroup;

struct IoUringContextConfig {
    size_t entries = IOURING_DEFAULT_ENTRIES;
    uint32_t flags = 0;
    bool enable_sq_poll = false;      // 是否启用SQ轮询
    uint32_t sq_poll_thread_cpu = 0;  // SQ轮询线程CPU
    uint32_t sq_poll_thread_idle = 0; // SQ轮询线程空闲超时
};


class IoUringContext {
public:
    using Config = IoUringContextConfig;

    static IoUringContext* current() noexcept {
        return t_current_context;
    }

    static bool has_current() noexcept {
        return t_current_context != nullptr;
    }

    static void init_thread_local(const Config& config = Config{}) {
        if (!t_current_context) {
            t_current_context = new IoUringContext(config);
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
        io_uring_sqe* sqe = io_uring_get_sqe(&m_ring);
        if (sqe) {
            sqe->flags |= IOSQE_IO_LINK;
        }
        return sqe;
    }

    int submit() noexcept {
        return io_uring_submit(&m_ring);
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

private:
    explicit IoUringContext(const Config& config) {
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

    ~IoUringContext() {
        if (m_ring.ring_fd >= 0) {
            io_uring_queue_exit(&m_ring);
        }
    }

    IoUringContext(const IoUringContext&) = delete;
    IoUringContext& operator=(const IoUringContext&) = delete;

private:
    io_uring m_ring{};
    std::unordered_map<unsigned, std::unique_ptr<BufferGroup>> m_buffer_groups;
    static thread_local IoUringContext* t_current_context;

};

inline thread_local IoUringContext* IoUringContext::t_current_context = nullptr;


} // namespace ynet::async::io


#pragma once
#include "ultranet/io/io_awaitable.hpp"
#include <sys/socket.h>

namespace ynet::async::io {

class Accept : public IoOperation<Accept> {
public:
    // === 父操作：真正的 multishot acceptor ===
    explicit Accept(int fd) noexcept
        : IoOperation<Accept>(
            [](io_uring_sqe* sqe, int f) {
                io_uring_prep_multishot_accept(sqe, f, nullptr, nullptr, 0);
                sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;  // 重要：只发错误 CQE
            },
            fd
        )
        , m_fd(fd)
        , m_is_parent(true) {
    }

    // === 子操作：轻量级等待器 ===
    Accept(int fd, Accept& parent) noexcept
        : IoOperation<Accept>()  // 重要：不调用基类构造函数！不申请 SQE！
        , m_fd(fd)
        , m_parent(&parent)
        , m_is_parent(false) {
        m_sqe = nullptr;  // 明确没有 SQE
    }

    // 禁用移动构造，避免混乱
    Accept(Accept&&) = delete;
    Accept& operator=(Accept&&) = delete;

    // 父操作才需要提交
    bool should_submit() const noexcept {
        return m_is_parent;
    }

    // 统一的 resume 接口
    IoResult<int> resume() noexcept {
        // 父操作：直接返回自己的结果
        if (m_callback.m_result < 0) {
            // 处理 -EAGAIN 的情况
            if (m_callback.m_result == -EAGAIN) {
                return std::unexpected(make_io_error(EAGAIN));
            }
            if (m_callback.m_result == -EINTR) {
                return std::unexpected(make_io_error(EINTR));
            }
            return std::unexpected(make_io_error(m_callback.m_result));
        }

        int client_fd = m_callback.m_result;

        // 重置，准备下一个连接
        m_callback.m_completed = false;
        m_callback.m_result = 0;

        return client_fd;
    }

    // 父操作：获取下一个连接（供子操作调用）
    IoResult<int> get_connection() noexcept {
        if (!m_is_parent) {
            return std::unexpected(make_io_error(EINVAL));
        }

        if (m_callback.m_result < 0) {
            return std::unexpected(make_io_error(m_callback.m_result));
        }

        int client_fd = m_callback.m_result;

        // 重置，准备下一个连接
        m_callback.m_completed = false;
        m_callback.m_result = 0;

        return client_fd;
    }

    // 父操作：取消 multishot
    void cancel() noexcept override {
        if (!m_is_parent) return;

        if (auto* sqe = IoUringEngine::current()->get_sqe()) {
            io_uring_prep_cancel(sqe, &m_callback, 0);
            io_uring_sqe_set_data(sqe, nullptr);
            IoUringEngine::current()->increment_pending();
        }

        m_callback.m_result = -ECANCELED;
        m_callback.m_completed = true;
        if (m_callback.m_handle) {
            m_callback.m_handle.resume();
        }
    }

    // 重新提交
    void resubmit() override {
        if (m_is_parent && m_sqe) {
            auto* ctx = IoUringEngine::current();
            auto* new_sqe = ctx->get_sqe();
            if (new_sqe) {
                *new_sqe = *m_sqe;
                io_uring_sqe_set_data(new_sqe, &m_callback);
                m_sqe = new_sqe;
                ctx->increment_pending();
            }
        }
    }

private:
    int m_fd;
    Accept* m_parent{nullptr};  // 只有子操作使用
    bool m_is_parent{true};     // 区分父/子
};


} // namespace ynet::net::op

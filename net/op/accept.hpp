#pragma once
#include "async/io/io_awaitable.hpp"
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
        if (!m_is_parent) {
            // 子操作：从父操作获取结果
            return m_parent->get_connection();
        }

        // 父操作：直接返回自己的结果
        if (m_callback.m_result < 0) {
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
            return std::unexpected(make_io_error(-EINVAL));
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
    bool cancel() noexcept override {
        if (!m_is_parent) return false;

        if (auto* sqe = IoUringContext::current()->get_sqe()) {
            io_uring_prep_cancel(sqe, &m_callback, 0);
            io_uring_sqe_set_data(sqe, nullptr);
            IoUringContext::current()->submit();
        }

        return IoOperation<Accept>::cancel();
    }

private:
    int m_fd;
    Accept* m_parent{nullptr};  // 只有子操作使用
    bool m_is_parent{true};     // 区分父/子
};


} // namespace ynet::net::op




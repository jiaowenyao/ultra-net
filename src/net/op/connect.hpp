// src/io/connect.hpp
#pragma once
#include "src/io/io_awaitable.hpp"

namespace ynet::async::io {

// Connect - 异步连接
class Connect : public IoOperation<Connect> {
public:
    Connect(int fd, const sockaddr* addr, socklen_t addrlen) noexcept
        : IoOperation<Connect>(
            [](io_uring_sqe* sqe, int f, const sockaddr* a, socklen_t l) {
                io_uring_prep_connect(sqe, f, a, l);
                sqe->flags |= IOSQE_FIXED_FILE;
            },
            fd, addr, addrlen
        )
        , m_fd(fd) {}

    IoResult<int> resume() noexcept {
        if (m_callback.m_result < 0) {
            // -EINPROGRESS 表示连接正在进行中（对于非阻塞 socket）
            // 这是正常的等待状态，不是错误
            if (m_callback.m_result == -EINPROGRESS) {
                // 对于 connect，-EINPROGRESS 实际上表示需要等待 socket 变为可写
                // 在 io_uring 中，这个操作会等待 POLLOUT
                // 但我们已经在等待中了，所以这是一个中间状态
                // 继续等待，不返回错误
                return std::unexpected(make_io_error(EAGAIN));
            }
            // -EAGAIN 表示需要再次调用
            if (m_callback.m_result == -EAGAIN) {
                return std::unexpected(make_io_error(EAGAIN));
            }
            // -EINTR 表示被信号中断，可以重试
            if (m_callback.m_result == -EINTR) {
                return std::unexpected(make_io_error(EINTR));
            }
            return std::unexpected(make_io_error(m_callback.m_result));
        }
        // 连接成功
        return m_fd;
    }

    // 重新提交
    void resubmit() override {
        if (m_sqe) {
            auto* ctx = IoUringContext::current();
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
};

} // namespace ynet::async::io
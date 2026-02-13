// async/io/connect.hpp
#pragma once
#include "async/io/io_awaitable.hpp"

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
            if (m_callback.m_result == -EINPROGRESS) {
                // 连接进行中，需要等待POLLOUT
                return std::unexpected(make_io_error(-EAGAIN));
            }
            return std::unexpected(make_io_error(m_callback.m_result));
        }
        return 0;
    }

private:
    int m_fd;
};

} // namespace ynet::async::io


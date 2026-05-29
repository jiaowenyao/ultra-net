#pragma once
#include "ultranet/io/io_awaitable.hpp"

namespace ynet::async::io {

class Connect : public IoOperation<Connect> {
public:
    Connect(int fd, const sockaddr* addr, socklen_t addrlen) noexcept
        : IoOperation<Connect>(
            [](io_uring_sqe* sqe, int f, const sockaddr* a, socklen_t l) {
                io_uring_prep_connect(sqe, f, a, l);
            },
            fd, addr, addrlen
        )
        , m_fd(fd) {}

    IoResult<int> resume() noexcept {
        if (m_callback.m_result < 0) {
            if (m_callback.m_result == -EAGAIN) {
                return std::unexpected(make_io_error(EAGAIN));
            }
            if (m_callback.m_result == -EINTR) {
                return std::unexpected(make_io_error(EINTR));
            }
            return std::unexpected(make_io_error(m_callback.m_result));
        }
        return m_fd;
    }

private:
    int m_fd;
};

} // namespace ynet::async::io

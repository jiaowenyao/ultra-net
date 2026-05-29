#pragma once
#include "ultranet/io/io_awaitable.hpp"
#include <sys/socket.h>

namespace ynet::async::io {

class Accept : public IoOperation<Accept> {
public:
    explicit Accept(int fd) noexcept
        : IoOperation<Accept>(
            [](io_uring_sqe* sqe, int f) {
                io_uring_prep_accept(sqe, f, nullptr, nullptr, 0);
            },
            fd
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
        return m_callback.m_result;
    }

private:
    int m_fd;
};

} // namespace ynet::async::io

#pragma once
#include "ultranet/io/io_awaitable.hpp"

namespace ynet::async::io {

class Shutdown : public IoOperation<Shutdown> {
public:
    Shutdown(int fd, int how) noexcept
        : IoOperation<Shutdown>(
            [](io_uring_sqe* sqe, int f, int h) {
                io_uring_prep_shutdown(sqe, f, h);
            },
            fd, how
        )
        , m_fd(fd) {}

    IoResult<int> resume() noexcept {
        if (m_callback.m_result < 0) {
            return std::unexpected(make_io_error(m_callback.m_result));
        }
        return m_fd;
    }

private:
    int m_fd;
};

} // namespace ynet::async::io

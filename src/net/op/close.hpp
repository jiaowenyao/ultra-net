#pragma once
#include "src/io/io_awaitable.hpp"

namespace ynet::async::io {

// Close - 异步关闭
class Close : public IoOperation<Close> {
public:
    explicit Close(int fd) noexcept
        : IoOperation<Close>(
            [](io_uring_sqe* sqe, int f) {
                io_uring_prep_close(sqe, f);
            },
            fd
        )
        , m_fd(fd) {}

    IoResult<int> resume() noexcept {
        if (m_callback.m_result < 0) {
            return std::unexpected(make_io_error(m_callback.m_result));
        }
        return 0;
    }

private:
    int m_fd;
};

} // namespace ynet::async::io

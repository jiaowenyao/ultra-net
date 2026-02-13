#pragma once
#include "async/io/io_awaitable.hpp"

namespace ynet::async::io {

// Listen是同步操作，封装成awaitable
class Listen {
public:
    Listen(int fd, int backlog) noexcept : m_fd(fd), m_backlog(backlog) {}

    bool await_ready() const noexcept {
        int ret = ::listen(m_fd, m_backlog);
        m_result = ret == 0 ? 0 : -errno;
        return true;
    }

    void await_suspend(std::coroutine_handle<>) noexcept {}

    IoResult<int> await_resume() const noexcept {
        if (m_result < 0) {
            return std::unexpected(make_io_error(m_result));
        }
        return m_result;
    }

private:
    int m_fd;
    int m_backlog;
    mutable int m_result{0};
};

} // namespace ynet::async::io

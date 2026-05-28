#pragma once
#include <sys/socket.h>
#include <cerrno>

namespace ynet::async::io {

class Bind {
public:
    Bind(int fd, const sockaddr* addr, socklen_t addrlen) noexcept
        : m_fd(fd), m_addr(addr), m_addrlen(addrlen) {}

    bool await_ready() const noexcept {
        int ret = ::bind(m_fd, m_addr, m_addrlen);
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
    const sockaddr* m_addr;
    socklen_t m_addrlen;
    mutable int m_result{0};
};

} // namespace ynet::async::io

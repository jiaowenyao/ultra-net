#pragma once
#include "ultranet/io/io_awaitable.hpp"
#include <sys/socket.h>
#include <cstring>

namespace ynet::async::io {

class RecvFrom : public IoOperation<RecvFrom> {
public:
    RecvFrom(int fd, void* buf, size_t len) noexcept
        : IoOperation<RecvFrom>(
            [](io_uring_sqe* sqe, int f, msghdr* msg, unsigned fl) {
                io_uring_prep_recvmsg(sqe, f, msg, fl);
            },
            fd, &m_msg, 0
        )
        , m_fd(fd) {
        m_iov.iov_base = buf;
        m_iov.iov_len = len;
        std::memset(&m_msg, 0, sizeof(m_msg));
        std::memset(&m_src_addr, 0, sizeof(m_src_addr));
        m_msg.msg_name = &m_src_addr;
        m_msg.msg_namelen = sizeof(m_src_addr);
        m_msg.msg_iov = &m_iov;
        m_msg.msg_iovlen = 1;
    }

    IoResult<size_t> resume() noexcept {
        if (m_callback.m_result < 0) {
            if (m_callback.m_result == -EAGAIN) {
                return std::unexpected(make_io_error(EAGAIN));
            }
            if (m_callback.m_result == -EINTR) {
                return std::unexpected(make_io_error(EINTR));
            }
            return std::unexpected(make_io_error(m_callback.m_result));
        }
        m_flags = m_msg.msg_flags;
        return static_cast<size_t>(m_callback.m_result);
    }

    void resubmit() override {
        if (m_callback.m_has_deadline && m_callback.is_expired()) {
            m_callback.m_result = -ETIMEDOUT;
            m_callback.m_completed = true;
            return;
        }
        auto* ctx = IoUringEngine::current();
        auto* new_sqe = ctx->get_sqe();
        if (new_sqe) {
            *new_sqe = *m_sqe;
            io_uring_sqe_set_data(new_sqe, &m_callback);
            m_sqe = new_sqe;
            ctx->increment_pending();
        }
    }

    const sockaddr_storage& source_addr() const noexcept { return m_src_addr; }
    socklen_t source_addr_len() const noexcept { return m_msg.msg_namelen; }
    int flags() const noexcept { return m_flags; }

private:
    int m_fd;
    iovec m_iov{};
    msghdr m_msg{};
    sockaddr_storage m_src_addr{};
    int m_flags{0};
};

} // namespace ynet::async::io

#pragma once
#include "ultranet/io/io_awaitable.hpp"
#include <sys/socket.h>

namespace ynet::async::io {

class SendTo : public IoOperation<SendTo> {
public:
    SendTo(int fd, const void* buf, size_t len,
           const sockaddr* dest_addr, socklen_t addrlen) noexcept
        : IoOperation<SendTo>(
            [](io_uring_sqe* sqe, int f, const void* b, size_t l, int fl,
               const sockaddr* a, socklen_t al) {
                io_uring_prep_sendto(sqe, f, b, l, fl, a, al);
            },
            fd, buf, len, 0, dest_addr, addrlen
        )
        , m_fd(fd) {}

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

private:
    int m_fd;
};

} // namespace ynet::async::io

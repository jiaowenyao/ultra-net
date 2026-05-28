#pragma once
#include <liburing.h>
#include "ultranet/io/io_awaitable.hpp"

namespace ynet::async::io {

class Read : public IoOperation<Read> {
public:
    Read(int fd, void* buf, size_t count) noexcept
        : IoOperation<Read>(
            [](io_uring_sqe* sqe, int f, void* b, size_t c) {
                io_uring_prep_recv(sqe, f, b, c, 0);
            },
            fd, buf, count
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

#pragma once
#include "ultranet/io/io_awaitable.hpp"

namespace ynet::async::io {

class Write : public IoOperation<Write> {
public:
    Write(int fd, const void* buf, size_t count, off_t offset = 0) noexcept
        : IoOperation<Write>(
            [](io_uring_sqe* sqe, int f, const void* b, size_t c, off_t o) {
                io_uring_prep_write(sqe, f, b, c, o);
            },
            fd, buf, count, offset
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

class Writev : public IoOperation<Writev> {
public:
    Writev(int fd, const iovec* iov, int iovcnt, off_t offset = 0) noexcept
        : IoOperation<Writev>(
            [](io_uring_sqe* sqe, int f, const iovec* i, int ic, off_t o) {
                io_uring_prep_writev(sqe, f, i, ic, o);
            },
            fd, iov, iovcnt, offset
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

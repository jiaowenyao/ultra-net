#pragma once
#include "src/io/io_awaitable.hpp"

namespace ynet::async::io {

// Socket - 异步创建socket
class Socket : public IoOperation<Socket> {
public:
    Socket(int domain, int type, int protocol) noexcept
        : IoOperation<Socket>(
            [](io_uring_sqe* sqe, int d, int t, int p) {
                io_uring_prep_socket(sqe, d, t, p, 0);
            },
            domain, type | SOCK_NONBLOCK, protocol
        ) {}

    IoResult<int> resume() noexcept {
        if (m_callback.m_result < 0) {
            return std::unexpected(make_io_error(m_callback.m_result));
        }
        return m_callback.m_result;
    }
};

} // namespace ynet::async::io

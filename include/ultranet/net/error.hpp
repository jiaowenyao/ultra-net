#pragma once

#include "ultranet/io/io_awaitable.hpp"
#include <cerrno>

namespace ynet::async::io {

inline bool is_timeout(const std::error_code& ec) noexcept {
    return ec.value() == ETIMEDOUT;
}

inline bool is_retryable(const std::error_code& ec) noexcept {
    int v = ec.value();
    return v == EAGAIN || v == EINTR;
}

inline bool is_closed(const std::error_code& ec) noexcept {
    int v = ec.value();
    return v == ECONNRESET || v == EPIPE || v == ENOTCONN || v == EBADF;
}

inline bool is_refused(const std::error_code& ec) noexcept {
    return ec.value() == ECONNREFUSED;
}

inline bool is_eof(const IoResult<size_t>& result) noexcept {
    return result.has_value() && *result == 0;
}

} // namespace ynet::async::io

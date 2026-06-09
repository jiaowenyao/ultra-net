#pragma once

#include "io_engine.hpp"
#include "io_callback.hpp"
#include <chrono>
#include <coroutine>


namespace ynet::async::io {

// Safe no-op for resubmit/cancel on timeout operations.
class NoopTimeoutOp : public IoOperationBase {
public:
    void resubmit() override {}
    void cancel() override {}
};
inline NoopTimeoutOp s_noop_timeout_op{};

class SleepAwaitable {
public:
    SleepAwaitable(struct __kernel_timespec ts)
        : m_ts(ts) {
        m_callback.m_operation = &s_noop_timeout_op;
    }

    bool await_ready() const noexcept {
        if (m_ts.tv_sec == 0 && m_ts.tv_nsec == 0) {
            return true;
        }
        return m_callback.m_completed;
    }

    bool await_suspend(std::coroutine_handle<> handle) noexcept {
        auto* ctx = IoUringEngine::current();
        if (!ctx) {
            handle.resume();
            return false;
        }

        auto* sqe = ctx->get_sqe();
        if (sqe) [[likely]] {
            io_uring_prep_timeout(sqe, &m_ts, 0, 0);
            io_uring_sqe_set_data(sqe, &m_callback);
            m_callback.m_handle = handle;
            ctx->increment_pending();
            ctx->submit_now();
            return true;
        } else {
            handle.resume();
            return false;
        }
    }

    void await_resume() const noexcept {}

private:
    struct __kernel_timespec m_ts;
    IoCallback m_callback;
};

inline SleepAwaitable sleep_for(std::chrono::nanoseconds duration) noexcept {
    using namespace std::chrono;
    auto sec = duration_cast<seconds>(duration);
    auto nsec = duration_cast<nanoseconds>(duration - sec);
    struct __kernel_timespec ts = {sec.count(), nsec.count()};
    return SleepAwaitable(ts);
}

template <typename Rep, typename Period>
inline SleepAwaitable sleep_for(std::chrono::duration<Rep, Period> duration) noexcept {
    return sleep_for(std::chrono::duration_cast<std::chrono::nanoseconds>(duration));
}

inline SleepAwaitable sleep_until(std::chrono::steady_clock::time_point deadline) noexcept {
    auto now = std::chrono::steady_clock::now();
    if (deadline <= now) {
        struct __kernel_timespec ts = {0, 0};
        return SleepAwaitable(ts);
    }
    return sleep_for(deadline - now);
}

} // namespace ynet::async::io

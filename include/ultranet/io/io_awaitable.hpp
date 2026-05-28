#pragma once

#include "io_engine.hpp"
#include "io_callback.hpp"
#include <expected>
#include <system_error>
#include <functional>
#include <chrono>
#include <iostream>
#include <atomic>


namespace ynet::async::io {

inline std::error_code make_io_error(int err) noexcept {
    return std::error_code(err < 0 ? -err : err, std::system_category());
}

template <typename T = std::size_t>
using IoResult = std::expected<T, std::error_code>;

// IO操作 CRTP 基类
template <typename Derived>
class IoOperation : public IoOperationBase {
protected:
    template <typename F, typename... Args>
        requires std::is_invocable_v<F, io_uring_sqe*, Args...>
    IoOperation(F&& f, Args... args)
        : m_setup_fn([f = std::decay_t<F>(std::forward<F>(f)),
                       ...args = std::decay_t<Args>(std::forward<Args>(args))]
                      (io_uring_sqe* sqe) mutable {
            std::invoke(f, sqe, args...);
          }) {
        m_callback.m_operation = this;
        m_is_parent = true;
    }

    IoOperation() noexcept = default;

public:
    IoOperation(const IoOperation&) = delete;
    IoOperation& operator=(const IoOperation&) = delete;

    IoOperation(IoOperation&&) = delete;
    IoOperation& operator=(IoOperation&&) = delete;

    ~IoOperation() override = default;

    Derived& with_timeout(std::chrono::nanoseconds duration) noexcept {
        m_callback.set_timeout(duration);
        return static_cast<Derived&>(*this);
    }

    bool await_ready() const noexcept {
        return m_callback.m_completed;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        m_callback.m_handle = handle;
        if (!m_is_parent) return;

        auto* ctx = IoUringEngine::current();

        if (m_callback.m_has_deadline) {
            auto* timeout_sqe = ctx->get_sqe();
            if (timeout_sqe) {
                struct __kernel_timespec ts = make_rel_timespec(m_callback.m_deadline);
                io_uring_prep_timeout(timeout_sqe, &ts, 0, 0);
                timeout_sqe->flags |= IOSQE_IO_LINK;
                io_uring_sqe_set_data(timeout_sqe, nullptr);
                ctx->increment_pending();
                m_has_timeout_sqe = true;
            }
        }

        m_sqe = ctx->get_sqe();
        if (m_sqe) [[likely]] {
            m_setup_fn(m_sqe);
            io_uring_sqe_set_data(m_sqe, &m_callback);
            ctx->increment_pending();

            if (ctx->should_submit()) {
                ctx->submit();
            }
        } else {
            m_callback.m_result = -ENOBUFS;
            m_callback.m_completed = true;
        }
    }

    auto await_resume() {
        if (m_callback.m_has_deadline && m_callback.m_result == -ECANCELED) {
            m_callback.m_result = -ETIMEDOUT;
        }
        return static_cast<Derived*>(this)->resume();
    }

    void resubmit() override {
        if (!m_is_parent) return;
        if (m_callback.m_has_deadline && m_callback.is_expired()) {
            m_callback.m_result = -ETIMEDOUT;
            m_callback.m_completed = true;
            return;
        }

        auto* ctx = IoUringEngine::current();

        if (m_callback.m_has_deadline && !m_has_timeout_sqe) {
            auto* timeout_sqe = ctx->get_sqe();
            if (timeout_sqe) {
                struct __kernel_timespec ts = make_rel_timespec(m_callback.m_deadline);
                io_uring_prep_timeout(timeout_sqe, &ts, 0, 0);
                timeout_sqe->flags |= IOSQE_IO_LINK;
                io_uring_sqe_set_data(timeout_sqe, nullptr);
                ctx->increment_pending();
                m_has_timeout_sqe = true;
            }
        }

        auto* new_sqe = ctx->get_sqe();
        if (new_sqe) {
            *new_sqe = *m_sqe;
            io_uring_sqe_set_data(new_sqe, &m_callback);
            m_sqe = new_sqe;
            ctx->increment_pending();
            if (ctx->should_submit()) {
                ctx->submit();
            }
        }
    }

    void cancel() override {
        if (!m_is_parent || !m_sqe || m_callback.m_completed) {
            return;
        }

        auto* ctx = IoUringEngine::current();
        if (auto* sqe = ctx->get_sqe()) {
            io_uring_prep_cancel(sqe, &m_callback, 0);
            io_uring_sqe_set_data(sqe, nullptr);
            ctx->increment_pending();
        }

        m_callback.m_result = -ECANCELED;
        m_callback.m_completed = true;
        if (m_callback.m_handle) {
            m_callback.m_handle.resume();
        }
    }

protected:
    io_uring_sqe* m_sqe{nullptr};
    bool m_is_parent{false};

private:
    std::function<void(io_uring_sqe*)> m_setup_fn;
    bool m_has_timeout_sqe{false};

    static struct __kernel_timespec make_rel_timespec(
        std::chrono::steady_clock::time_point deadline) noexcept {
        using namespace std::chrono;
        auto now = steady_clock::now();
        if (deadline <= now) {
            return {0, 1};
        }
        auto diff = deadline - now;
        auto sec = duration_cast<seconds>(diff);
        auto nsec = duration_cast<nanoseconds>(diff - sec);
        return {sec.count(), nsec.count()};
    }
};

} // namespace ynet::async::io

#pragma once

#include "io_engine.hpp"
#include "io_callback.hpp"
#include <expected>
#include <system_error>
#include <functional>
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
    // 父操作构造函数：创建SQE并设置数据
    template <typename F, typename... Args>
        requires std::is_invocable_v<F, io_uring_sqe*, Args...>
    IoOperation(F&& f, Args... args)
        : m_sqe(IoUringEngine::current()->get_sqe()) {
        if (m_sqe) [[likely]] {
            std::invoke(std::forward<F>(f), m_sqe, std::forward<Args>(args)...);
            io_uring_sqe_set_data(m_sqe, &m_callback);
            m_callback.m_operation = this;
            m_is_parent = true;
        }
        else {
            m_callback.m_result = -ENOBUFS;
            m_callback.m_completed = true;
        }
    }

    // 子操作构造函数：不创建SQE
    IoOperation() noexcept = default;

public:
    IoOperation(const IoOperation&) = delete;
    IoOperation& operator=(const IoOperation&) = delete;

    IoOperation(IoOperation&&) = delete;
    IoOperation& operator=(IoOperation&&) = delete;

    virtual ~IoOperation() = default;

    // awaitable 接口
    bool await_ready() const noexcept {
        return m_callback.m_completed;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        m_callback.m_handle = handle;

        if (m_is_parent && m_sqe) {
            // 批量提交：增加待提交计数
            auto* ctx = IoUringEngine::current();
            ctx->increment_pending();

            // 检查是否达到批量阈值
            if (ctx->should_submit()) {
                ctx->submit();
            }
        }
    }

    auto await_resume() {
        return static_cast<Derived*>(this)->resume();
    }

    // 重新提交（用于 -EAGAIN / -EINTR）
    void resubmit() override {
        if (m_is_parent && m_sqe) {
            auto* ctx = IoUringEngine::current();
            auto* new_sqe = ctx->get_sqe();
            if (new_sqe) {
                // 复制原 SQE 的内容
                *new_sqe = *m_sqe;
                io_uring_sqe_set_data(new_sqe, &m_callback);
                m_sqe = new_sqe;
                ctx->increment_pending();
                if (ctx->should_submit()) {
                    ctx->submit();
                }
            }
        }
    }

    // 取消操作
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
    bool m_is_parent{false};  // true: 拥有SQE, false: 等待父操作结果
};

} // namespace ynet::async::io

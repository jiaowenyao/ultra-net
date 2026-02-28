#pragma once

#include "io_context.hpp"
#include "io_callback.hpp"
#include <expected>
#include <system_error>
#include <functional>
#include <iostream>


namespace ynet::async::io {

inline std::error_code make_io_error(int err) noexcept {
    return std::error_code(err < 0 ? -err : err, std::system_category());
}

template <typename T = std::size_t>
using IoResult = std::expected<T, std::error_code>;

// IO操作基类
template <typename Derived>
class IoOperation {
protected:
    using Base = IoOperation<Derived>;
    // 父操作构造函数：真正提交SQE
    template <typename F, typename... Args>
        requires std::is_invocable_v<F, io_uring_sqe*, Args...>
    IoOperation(F&& f, Args... args)
        : m_sqe(IoUringContext::current()->get_sqe()) {
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

    // 子操作构造函数：不创建SQE，只等待
    IoOperation() noexcept = default;

public:
    IoOperation(const IoOperation&) = delete;
    IoOperation& operator=(const IoOperation&) = delete;

    // 禁止移动,父操作必须稳定在内存中
    IoOperation(IoOperation&&) = delete;
    IoOperation& operator=(IoOperation&&) = delete;

    virtual ~IoOperation() = default;

    // awaitable 接口
    bool await_ready() const noexcept {
        return m_callback.m_completed;
    }

    void await_suspend(std::coroutine_handle<> handle) noexcept {
        m_callback.m_handle = handle;
        std::cout << "await suspend" << std::endl;
        if (m_is_parent && m_sqe) {
            // 只有父操作才提交
            IoUringContext::current()->submit();
        }
    }

    auto await_resume() {
        return static_cast<Derived*>(this)->resume();
    }

    // 取消操作
    virtual bool cancel() noexcept {
        if (!m_is_parent || !m_sqe || m_callback.m_completed) {
            return false;
        }

        m_callback.m_result = -ECANCELED;
        m_callback.m_completed = true;
        if (m_callback.m_handle) {
            m_callback.m_handle.resume();
        }
        return true;
    }

protected:
    io_uring_sqe* m_sqe{nullptr};
    IoCallback m_callback{};
    bool m_is_parent{false};  // true: 拥有SQE, false: 等待父操作结果
};

} // namespace ynet::async::io

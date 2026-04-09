#pragma once

#include <coroutine>
#include <atomic>
#include <chrono>
#include <cassert>
#include <liburing.h>


namespace ynet::async::io {

// 前向声明
class IoOperationBase;

// 回调结构 - 存储协程句柄和操作结果
struct IoCallback {
    std::coroutine_handle<> m_handle{nullptr};
    int m_result{0};
    bool m_completed{false};
    void* m_operation{nullptr};

    // 超时支持
    std::chrono::steady_clock::time_point m_deadline{};
    bool m_has_deadline{false};

    // 操作开始时间（用于延迟统计）
    std::chrono::steady_clock::time_point m_start_time{};

    // 重置回调，准备重用
    void reset() noexcept {
        m_handle = nullptr;
        m_result = 0;
        m_completed = false;
        m_operation = nullptr;
        m_deadline = {};
        m_has_deadline = false;
        m_start_time = {};
    }

    // 检查是否超时
    bool is_expired() const noexcept {
        if (!m_has_deadline) return false;
        return std::chrono::steady_clock::now() >= m_deadline;
    }

    // 设置超时
    template <typename Rep, typename Period>
    void set_timeout(std::chrono::duration<Rep, Period> duration) noexcept {
        m_deadline = std::chrono::steady_clock::now() + duration;
        m_has_deadline = true;
    }

    // 获取延迟（微秒）
    int64_t latency_us() const noexcept {
        if (m_start_time == std::chrono::steady_clock::time_point{}) return 0;
        return std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - m_start_time
        ).count();
    }
};

// 操作基类 - 用于公共操作（如取消、重新提交）
class IoOperationBase {
public:
    virtual ~IoOperationBase() = default;

    // 重新提交操作（用于 -EAGAIN / -EINTR 后）
    virtual void resubmit() = 0;

    // 取消操作
    virtual void cancel() = 0;

    // 获取关联的回调
    IoCallback* callback() noexcept { return &m_callback; }

protected:
    IoCallback m_callback{};
};

} // namespace ynet::async::io
// UnifiedTask — 统一的协程/函数任务包装器。
// 使用 std::variant 统一存储 std::coroutine_handle<> 和 std::function<void()>，
// 让线程池可以用同一个队列处理协程任务和普通回调。
//
// operator() 恢复协程执行或调用函数。协程完成后（done() == true），
// 自动调用 destroy() 释放协程帧，防止内存泄漏。
#pragma once
#include <coroutine>
#include <functional>
#include <variant>
#include <type_traits>

namespace ynet::async::scheduling {

namespace detail {
    template <typename T>
    struct is_coroutine_handle : std::false_type {};
    template <typename Promise>
    struct is_coroutine_handle<std::coroutine_handle<Promise>> : std::true_type {};
}

template <typename T>
inline constexpr bool is_coroutine_handle_v =
    detail::is_coroutine_handle<std::remove_cvref_t<T>>::value;

class UnifiedTask {
    using TaskVariant = std::variant<std::coroutine_handle<>, std::function<void()>>;
    TaskVariant m_task;

public:
    UnifiedTask() = default;

    // 从协程句柄构造
    template <typename Promise>
    UnifiedTask(std::coroutine_handle<Promise> handle) noexcept
        : m_task(std::coroutine_handle<>(handle)) {}

    // 从可调用对象构造（SFINAE 排除 UnifiedTask 自身和协程句柄）
    template <typename Func>
        requires(std::is_invocable_v<std::decay_t<Func>>
                && !std::is_same_v<std::decay_t<Func>, UnifiedTask>
                && !is_coroutine_handle_v<Func>)
    UnifiedTask(Func&& func)
        : m_task(std::function<void()>(std::forward<Func>(func))) {}

    UnifiedTask(UnifiedTask&&) noexcept = default;
    UnifiedTask& operator=(UnifiedTask&&) noexcept = default;
    UnifiedTask(const UnifiedTask&) = delete;
    UnifiedTask& operator=(const UnifiedTask&) = delete;

    // 执行任务：恢复协程或调用函数。
    // 注意：协程帧的销毁由 TaskFinalAwaiter / Task<T> 生命周期管理，
    // UnifiedTask 只负责执行，不参与所有权。
    void operator()() {
        std::visit([](auto& task) {
            using T = std::decay_t<decltype(task)>;
            if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
                if (task && !task.done()) {
                    task.resume();
                }
            } else {
                if (task) {
                    task();
                }
            }
        }, m_task);
    }

    explicit operator bool() const noexcept {
        return std::visit([](auto& task) -> bool {
            using T = std::decay_t<decltype(task)>;
            if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
                return task != nullptr;
            } else {
                return static_cast<bool>(task);
            }
        }, m_task);
    }
};

} // namespace ynet::async::scheduling

#pragma once
#include <functional>
#include <coroutine>
#include <memory>

namespace ynet::async::scheduling {

namespace detail {
    template <typename T>
    struct is_coroutine_handle : std::false_type {};
    template <typename Promise>
    struct is_coroutine_handle<std::coroutine_handle<Promise>> : std::true_type {};
}

template <typename T>
inline constexpr bool is_coroutine_handle_v =
    detail::is_coroutine_handle<std::remove_cvref<T>>::value;

class UnifiedTask {
    struct TaskConcept {
        virtual ~TaskConcept() = default;
        virtual void execute() = 0;
    };
    template <typename Func>
    struct FunctionTask : TaskConcept {
        Func func;
        template <typename F>
        explicit FunctionTask(F&& f) : func(std::forward<F>(f)) {}
        void execute() override { func(); }
    };
    template <typename Promise>
    struct CoroutineTask : TaskConcept {
        std::coroutine_handle<Promise> handle;
        explicit CoroutineTask(std::coroutine_handle<Promise> h) : handle(h) {}
        void execute() override {
            if (handle && !handle.done()) {
                handle.resume();
            }
        }
    };
    std::unique_ptr<TaskConcept> m_impl;
public:
    UnifiedTask() = default;
    template <typename Func>
        requires(std::is_invocable_v<std::decay_t<Func>>
                && !std::is_same_v<std::decay_t<Func>, UnifiedTask>
                && !is_coroutine_handle_v<Func>)
    UnifiedTask(Func&& func)
        : m_impl(std::make_unique<FunctionTask<std::decay_t<Func>>>(std::forward<Func>(func))) {}
    template <typename Promise>
    UnifiedTask(std::coroutine_handle<Promise> handle)
        : m_impl(std::make_unique<CoroutineTask<Promise>>(handle)) {}
    UnifiedTask(UnifiedTask&&) noexcept = default;
    UnifiedTask& operator=(UnifiedTask&&) noexcept = default;
    UnifiedTask(const UnifiedTask&) = delete;
    UnifiedTask& operator=(const UnifiedTask&) = delete;
    void operator()() { if (m_impl) m_impl->execute(); }
    explicit operator bool() const noexcept { return m_impl != nullptr; }
};

}

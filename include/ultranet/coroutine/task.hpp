#include <coroutine>
#include <exception>
#include <iostream>
#include <optional>
#include <assert.h>
#include <format>
#include "ultranet/utils/noncopyable.h"
#include "thread_pool.hpp"


namespace ynet::async {

struct TaskPromiseBase {
    TaskPromiseBase() {
        m_creator_scheduler = ExecutionContext::current();
    }

    // 最终的等待器
    struct TaskFinalAwaiter {
        // 总是挂起进入清理流程
        constexpr bool await_ready() const noexcept {
            return false;
        }

        /** 
         * callee: 当前被挂起的协程
         * return: void或者下一个要恢复的协程句柄
         */
        template <typename T>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<T> callee) const noexcept {
            auto& promise = callee.promise();
            auto* scheduler = promise.m_creator_scheduler;
            if (scheduler != nullptr) {
                scheduler->decrement_tasks();
            }

            std::coroutine_handle<> parent = promise.m_caller;
            promise.m_caller = nullptr;

            if (parent && !parent.done()) {
                return parent;
            }

            if (promise.m_ex != nullptr) [[unlikely]] {
                // 处理未捕获的异常
                try {
                    std::rethrow_exception(promise.m_ex);
                }
                catch (const std::exception& e) {
                    std::cerr << std::format("catch a exception: {}", e.what());
                    std::terminate();
                }
            }
            // 这里不主动调用destroy，而是交给Task析构时调用
            // callee.destroy();
            // 返回空协程句柄
            return std::noop_coroutine();
        }

        constexpr void await_resume() const noexcept {}

    };


    // 初始化时挂起
    constexpr std::suspend_always initial_suspend() const noexcept {
        return {};
    }

    // 使用自定义的最终等待器
    constexpr TaskFinalAwaiter final_suspend() const noexcept {
        return {};
    }

    void unhandled_exception() noexcept {
        m_ex = std::move(std::current_exception());
        assert(m_ex != nullptr);
    }


    std::atomic<std::coroutine_handle<>> m_caller{nullptr};
    std::exception_ptr m_ex{nullptr};
    Scheduler* m_creator_scheduler{nullptr};
};

template <typename T>
class Task;


template <typename T>
struct TaskPromise final : public TaskPromiseBase {
    Task<T> get_return_object() noexcept;

    template <typename F>
        requires std::is_convertible_v<F&&, T> && std::is_constructible_v<T, F&&>
    void return_value(F&& value) {
        m_value = std::forward<F>(value);
    }

    T& result() & {
        if (m_ex != nullptr) [[unlikely]] {
            std::rethrow_exception(m_ex);
        }
        assert(m_value.has_value());
        return m_value.value();
    }

    T&& result() && {
        if (m_ex != nullptr) [[unlikely]] {
            std::rethrow_exception(m_ex);
        }
        assert(m_value.has_value());
        return std::move(m_value.value());
    }

private:
    std::optional<T> m_value = std::nullopt;
};

template <>
struct TaskPromise<void> final : public TaskPromiseBase {
    Task<void> get_return_object() noexcept;

    constexpr void return_void() const noexcept {}

    void result() const {
        if (m_ex != nullptr) [[unlikely]] {
            return std::rethrow_exception(m_ex);
        }
    }

};


template <typename T = void>
class [[nodiscard]] Task : ynet::utils::Noncopyable {
public:
    using promise_type = TaskPromise<T>;
private:
    struct AwaitableBase {
        // 被等待的对象
        std::coroutine_handle<promise_type> m_callee;
        AwaitableBase(std::coroutine_handle<promise_type> callee) noexcept
            : m_callee(callee) {}

        // 如果已经完成，就不需要等待了
        bool await_ready() const noexcept {
            return !m_callee || m_callee.done();
        }

        // 记录调用者，并跳转到被等待的协程
        template <typename PromiseType>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<PromiseType> caller) {
            if (!m_callee || m_callee.done()) {
                return caller;
            }

            auto& callee_promise = m_callee.promise();
            auto& caller_promise = caller.promise();

            callee_promise.m_caller.store(caller, std::memory_order_release);

            if (auto* scheduler = callee_promise.m_creator_scheduler) {
                scheduler->submit(m_callee);
                return std::noop_coroutine();
            }

            return m_callee;
        }
    };
public:
    Task() noexcept = default;

    explicit Task(std::coroutine_handle<promise_type> handle)
        : m_handle(handle) {}

    ~Task() {
        if (m_handle && m_handle.done()) {
            m_handle.destroy();
        }
    }

    Task(Task&& other) noexcept
        : m_handle(std::move(other.m_handle)) {
        other.m_handle = nullptr;
    }

    Task& operator=(Task&& other) noexcept {
        if (std::addressof(other) != this) [[likely]] {
            if (m_handle) {
                m_handle.destroy();
            }
            m_handle = std::move(other.m_handle);
            other.m_handle = nullptr;
        }
        return *this;
    }

    auto operator co_await() const & noexcept {
        struct Awaitable final : public AwaitableBase {
            decltype(auto) await_resume() {
                if (!this->m_callee) [[unlikely]] {
                    throw std::logic_error("m_handle is nullptr");
                }
                return this->m_callee.promise().result();
            }
        };
        return Awaitable(m_handle);
    }

    auto operator co_await() const && noexcept {
        struct Awaitable final : public AwaitableBase {
            decltype(auto) await_resume() {
                if (!this->m_callee) [[unlikely]] {
                    throw std::logic_error("m_handle is nullptr");
                }
                if constexpr (std::is_same_v<T, void>) {
                    return this->m_callee.promise().result();
                }
                else {
                    return std::move(this->m_callee.promise().result());
                }
            }
        };
        return Awaitable(m_handle);
    }

    std::coroutine_handle<promise_type> task() {
        if (m_handle == nullptr) [[unlikely]] {
            throw std::logic_error("m_handle is nullptr");
        }
        auto res = std::move(m_handle);
        m_handle = nullptr;
        return res;
    }

    std::coroutine_handle<promise_type> handle() {
        return m_handle;
    }

    void resume() const {
        m_handle.resume();
    }

private:
    std::coroutine_handle<promise_type> m_handle = nullptr;
};


template <typename T>
inline Task<T> TaskPromise<T>::get_return_object() noexcept {
    return Task<T>(std::coroutine_handle<TaskPromise>::from_promise(*this));
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
    return Task<void>(std::coroutine_handle<TaskPromise>::from_promise(*this));
}


} // namesapce ynet::async


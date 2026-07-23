// Task<T> — C++20 协程任务类型，ultra-net 的协程运行时核心。
//
// 设计要点：
//   1. 强制堆分配（operator new），阻止编译器 HALO 优化将协程帧放在栈上，
//      确保协程可以在线程间传递而不产生悬空指针。
//   2. initial_suspend 返回 suspend_always，协程创建后立即挂起，由调度器
//      显式提交到线程池后才开始执行。
//   3. final_suspend 返回自定义 TaskFinalAwaiter，在协程完成后通知完成回调，
//      并恢复父协程（对称转移），最后由 UnifiedTask 销毁协程帧。
//   4. 异常通过 std::exception_ptr 传播，在 result() 访问时重新抛出。
#pragma once
#include <atomic>
#include <coroutine>
#include <exception>
#include <optional>
#include <assert.h>
#include <format>
#include "ultranet/utils/noncopyable.h"
#include "ultranet/coroutine/execution_context.hpp"
#include "ultranet/log/logger.hpp"


namespace ynet::async {

// ── Promise 基类 ──────────────────────────────────────────────────────────

struct TaskPromiseBase {
    using NotifyFn = void (*)(void* ctx);

    TaskPromiseBase() = default;

    // ── 协程帧池 ──────────────────────────────────────────────────────────
    // 线程局部空闲链表：避免每次 co_await 都调用 malloc/free。
    // 最近释放的帧被缓存，下次同大小分配时直接复用。池上限 16 帧/大小。
    // 超过上限或大小不匹配时回退到 ::operator new/delete。

    static void* operator new(std::size_t size) {
        auto& pool = t_frame_pool;
        // 查找同大小的空闲帧
        for (auto& slot : pool) {
            if (slot.ptr && slot.size == size) {
                void* p = slot.ptr;
                slot.ptr = nullptr;
                return p;
            }
        }
        return ::operator new(size);
    }

    static void operator delete(void* ptr, std::size_t size) {
        auto& pool = t_frame_pool;
        // 尝试放入空闲槽位
        for (auto& slot : pool) {
            if (!slot.ptr) {
                slot.ptr = ptr;
                slot.size = size;
                return;
            }
        }
        // 池满或未找到空槽位，直接释放
        ::operator delete(ptr);
    }

    static void operator delete(void* ptr) {
        // 无 size 信息的 delete 直接释放（不常见路径）
        ::operator delete(ptr);
    }

    // 协程完成时由 final_suspend 调用，通知线程池任务计数减一
    void notify_complete() noexcept {
        if (m_notify_fn) {
            m_notify_fn(m_notify_ctx);
        }
    }

    // ── 最终等待器 ─────────────────────────────────────────────────────
    // await_ready() 返回 false：协程在最终挂起点总是挂起。
    // await_suspend() 处理三件事：
    //   1. 通知完成回调（线程池任务计数）
    //   2. 对称转移到父协程（如果存在且未完成）
    //   3. 处理未捕获异常（记录到 stderr）
    // 协程帧在 await_suspend 返回后由 UnifiedTask::operator() 销毁。

    struct TaskFinalAwaiter {
        constexpr bool await_ready() const noexcept {
            return false;  // 总是挂起，让调用方有机会销毁协程帧
        }

        template <typename T>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<T> callee) const noexcept {
            auto& promise = callee.promise();

            // 1. 通知线程池：此协程已完成
            promise.notify_complete();

            // 2. 取出父协程句柄并清空
            std::coroutine_handle<> parent = promise.m_caller;
            promise.m_caller = nullptr;

            // 3. 如果父协程存在且未完成，对称转移回父协程
            if (parent && !parent.done()) {
                return parent;
            }

            // 4. 无父协程（顶层任务 / release）→ 记录异常并自行销毁帧
            if (promise.m_ex != nullptr) [[unlikely]] {
                try {
                    std::rethrow_exception(promise.m_ex);
                }
                catch (const std::exception& e) {
                    ULTRA_LOG_ERROR("coroutine exception: {}", e.what());
                }
                catch (...) {
                    ULTRA_LOG_ERROR("coroutine exception: unknown");
                }
            }

            // 无等待者——最终挂起点自行释放协程帧。
            // 有父协程的路径在 if(parent) 分支中对称转移，由父 Task 析构负责销毁。
            callee.destroy();
            return std::noop_coroutine();
        }

        constexpr void await_resume() const noexcept {}
    };

    // 初始挂起点：协程创建后立即挂起，等待调度器显式恢复
    constexpr std::suspend_always initial_suspend() const noexcept {
        return {};
    }

    constexpr TaskFinalAwaiter final_suspend() const noexcept {
        return {};
    }

    // 协程体内未捕获异常时由编译器调用
    void unhandled_exception() noexcept {
        m_ex = std::move(std::current_exception());
        assert(m_ex != nullptr);
    }

    std::atomic<std::coroutine_handle<>> m_caller{nullptr};
    std::exception_ptr m_ex{nullptr};
    NotifyFn m_notify_fn{nullptr};
    void* m_notify_ctx{nullptr};

    // 线程局部帧池（每线程最多缓存 8 个不同大小的帧）
    struct FrameSlot { void* ptr = nullptr; std::size_t size = 0; };
    static thread_local FrameSlot t_frame_pool[8];
};

// ── 有返回值 Promise ──────────────────────────────────────────────────────

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

    // 左值引用版本：返回存储值的引用
    T& result() & {
        if (m_ex != nullptr) [[unlikely]] {
            std::rethrow_exception(m_ex);
        }
        assert(m_value.has_value());
        return m_value.value();
    }

    // 右值引用版本：移动返回存储值
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

// ── void 特化 Promise ─────────────────────────────────────────────────────

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

// ── Task 协程句柄 ─────────────────────────────────────────────────────────

template <typename T = void>
class [[nodiscard]] Task : ynet::utils::Noncopyable {
public:
    using promise_type = TaskPromise<T>;

private:
    // co_await 的 Awaitable 基类
    struct AwaitableBase {
        std::coroutine_handle<promise_type> m_callee;

        AwaitableBase(std::coroutine_handle<promise_type> callee) noexcept
            : m_callee(callee) {}

        // 如果协程句柄为空或已完成，不需要挂起
        bool await_ready() const noexcept {
            return !m_callee || m_callee.done();
        }

        // 挂起当前协程，建立父子关系，将子协程提交到线程池
        template <typename PromiseType>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<PromiseType> caller) {
            if (!m_callee || m_callee.done()) {
                // 子协程已完成或句柄无效，直接继续父协程
                return caller;
            }

            auto& callee_promise = m_callee.promise();
            auto& caller_promise = caller.promise();

            // 建立父子链：子协程完成后通过 m_caller 回到父协程
            callee_promise.m_caller.store(caller, std::memory_order_release);

            // 通过调度器提交子协程到线程池执行
            if (auto* scheduler = ExecutionContext::current()) {
                scheduler->submit(m_callee);
                return std::noop_coroutine();  // 父协程挂起
            }

            // 无调度器时直接执行子协程（内联）
            return m_callee;
        }
    };

public:
    Task() noexcept = default;

    explicit Task(std::coroutine_handle<promise_type> handle)
        : m_handle(handle) {}

    // 析构：如果协程已完成，销毁协程帧释放内存
    ~Task() {
        if (m_handle && m_handle.done()) {
            m_handle.destroy();
        }
    }

    // 移动构造：转移句柄所有权
    Task(Task&& other) noexcept
        : m_handle(std::move(other.m_handle)) {
        other.m_handle = nullptr;
    }

    Task& operator=(Task&& other) noexcept {
        if (std::addressof(other) != this) [[likely]] {
            if (m_handle && m_handle.done()) {
                m_handle.destroy();
            }
            m_handle = std::move(other.m_handle);
            other.m_handle = nullptr;
        }
        return *this;
    }

    // ── co_await 支持 ───────────────────────────────────────────────────

    // 左值 co_await：返回存储值的引用
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

    // 右值 co_await：移动返回存储值
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

    // ── 句柄管理 ───────────────────────────────────────────────────────

    // 释放句柄所有权（用于提交到线程池）
    std::coroutine_handle<promise_type> release() {
        if (m_handle == nullptr) [[unlikely]] {
            throw std::logic_error("Task handle already released");
        }
        auto res = std::move(m_handle);
        m_handle = nullptr;
        return res;
    }

    std::coroutine_handle<promise_type> handle() noexcept {
        return m_handle;
    }

    void resume() const {
        m_handle.resume();
    }

private:
    std::coroutine_handle<promise_type> m_handle = nullptr;
};

// ── Promise::get_return_object 实现 ───────────────────────────────────────

template <typename T>
inline Task<T> TaskPromise<T>::get_return_object() noexcept {
    return Task<T>(std::coroutine_handle<TaskPromise>::from_promise(*this));
}

inline Task<void> TaskPromise<void>::get_return_object() noexcept {
    return Task<void>(std::coroutine_handle<TaskPromise>::from_promise(*this));
}

// ── 线程局部帧池 ──────────────────────────────────────────────────────────
inline thread_local TaskPromiseBase::FrameSlot TaskPromiseBase::t_frame_pool[8];

} // namespace ynet::async

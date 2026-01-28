#pragma once
#include <memory>
#include <functional>
#include <coroutine>

namespace ynet::async {

// 基础调度器接口
class Scheduler {
public:
    virtual ~Scheduler() = default;
    virtual void submit(std::coroutine_handle<> handle) = 0;
    virtual bool is_current() const = 0;
    virtual const char* name() const = 0;
};

// 线程局部上下文管理器
class SchedulerContext {
public:
    static Scheduler* current() noexcept;
    // 设置当前调度器，并返回上一个调度器
    static Scheduler* set_current(Scheduler* scheduler) noexcept;
    // 恢复到上一个调度器
    static void restore() noexcept;

public:
    // 作用域守卫
    class ScopedGuard {
    public:
        // 禁止拷贝
        ScopedGuard(const ScopedGuard&) = delete;
        ScopedGuard& operator=(const ScopedGuard&) = delete;

        // 允许移动
        ScopedGuard(ScopedGuard&& other) noexcept;

        explicit ScopedGuard(Scheduler* scheduler);
        ~ScopedGuard();
    private:
        Scheduler* m_prev;
        bool m_active;
    };

private:
    // 当前线程的调度器
    static thread_local Scheduler* t_current_scheduler;
    // 上一个调度器
    static thread_local Scheduler* t_previous_scheduler;
};

inline thread_local Scheduler* SchedulerContext::t_current_scheduler = nullptr;
inline thread_local Scheduler* SchedulerContext::t_previous_scheduler = nullptr;

}


#pragma once
#include "scheduler.h"
#include <memory>
#include <functional>

namespace ynet::async {

// 线程局部上下文管理器
class ExecutionContext {
public:
    static Scheduler* current() noexcept {
        return current_scheduler();
    }

    static bool has_current() noexcept {
        return current() != nullptr;
    }

    // 工具函数，在当前调度器上执行
    template <typename Func>
    static void execute_on_current(Func&& func) {
        if (auto* scheduler = current()) {
            scheduler->submit([func = std::forward<Func>(func)]() mutable {
                func();
            });
        }
        else {
            // 没有调度器就直接执行
            func();
        }
    }

public:
    // 作用域守卫
    class Scope {
    public:
        explicit Scope(Scheduler* scheduler) noexcept
            : m_previous(current_scheduler()) {
            set_current_scheduler(scheduler);
        }
        ~Scope() {
            set_current_scheduler(m_previous);
        }

        // 禁止拷贝
        Scope(const Scope&) = delete;
        Scope& operator=(const Scope&) = delete;

        // 允许移动
        Scope(Scope&& other) noexcept;
    private:
        Scheduler* m_previous;
    };

private:
    // 当前线程的调度器
    static thread_local Scheduler* t_current_scheduler;

    static Scheduler* current_scheduler() noexcept {
        return t_current_scheduler;
    }

    static void set_current_scheduler(Scheduler* scheduler) noexcept {
        t_current_scheduler = scheduler;
    }
};

inline thread_local Scheduler* ExecutionContext::t_current_scheduler = nullptr;

}


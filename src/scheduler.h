#pragma once

#include <coroutine>
#include <cstddef>

namespace ynet::async {

// Scheduler 接口 - 运行时多态
// 具体实现（如 WorkStealingThreadPool）使用 final 修饰符帮助编译器优化
class Scheduler {
public:
    virtual ~Scheduler() = default;

    virtual void submit(std::coroutine_handle<> handle) = 0;

    // 将已挂起的协程重新加入调度队列（不增加任务计数）
    virtual void resubmit(std::coroutine_handle<> handle) = 0;

    // 检查当前线程是否属于此调度器
    virtual bool is_current_thread() const = 0;

    virtual const char* name() const noexcept = 0;

    virtual size_t worker_count() const noexcept = 0;

    virtual size_t pending_tasks() const noexcept = 0;

    // 让调度器自己实现 increment 和 decrement
    virtual void increment_tasks() noexcept = 0;

    virtual void decrement_tasks() noexcept = 0;

    // 统计接口
    virtual size_t total_submitted_ops() const noexcept = 0;
    virtual size_t total_completed_ops() const noexcept = 0;
};

} // namespace ynet::async
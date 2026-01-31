#pragma once

#include <coroutine>
#include <cstddef>

namespace ynet::async {

class Scheduler {
public:
    virtual ~Scheduler() = default;

    virtual void submit(std::coroutine_handle<> handle) = 0;

    // 检查当前线程是否属于此调度器
    virtual bool is_current_thread() const = 0;

    virtual const char* name() const noexcept = 0;

    virtual size_t worker_count() const noexcept = 0;

    virtual size_t pending_tasks() const noexcept = 0;

    // 让调度器自己实现increment和decrement
    virtual void increment_tasks() noexcept = 0;

    virtual void decrement_tasks() noexcept = 0;
};

} // namespace ynet::async




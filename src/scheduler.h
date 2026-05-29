#pragma once

#include <coroutine>
#include <cstddef>

namespace ynet::async {

// 任务调度器 — 仅负责协程的入队和重新调度
class Scheduler {
public:
    virtual ~Scheduler() = default;

    // 提交新协程（增加任务计数）
    virtual void submit(std::coroutine_handle<> handle) = 0;

    // 将已挂起的协程重新加入调度队列（不增加任务计数）
    virtual void resubmit(std::coroutine_handle<> handle) = 0;
};

} // namespace ynet::async
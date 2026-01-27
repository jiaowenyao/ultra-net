#pragma once
#include <coroutine>
#include <memory>


namespace ynet::runtime {


class UnifiedTask {
private:
    struct TaskVariant {
        virtual ~TaskVariant() = default;
        virtual void execute() = 0;
        virtual bool is_coroutine() const noexcept { return false; }
        virtual std::coroutine_handle<> get_coroutine_handle() {
            return std::coroutine_handle<>{};
        }
    };

    template <typename F>
    struct FunctionTask : TaskVariant {
        F func;
        FunctionTask(F&& f) : func(std::forward<F>(f)) {}
        void execute() override { func(); }
    };

    template <typename Promise>
    struct CoroutineTask : TaskVariant {
        std::coroutine_handle<Promise> handle;
        CoroutineTask(std::coroutine_handle<Promise> h) : handle(h) {}
        void execute() override {
            if (handle && !handle.done()) {
                handle.resume();
            }
        }
        bool is_coroutine() const noexcept override { return true; }
        std::coroutine_handle<> get_coroutine_handle() override { return handle; }
    };

    std::unique_ptr<TaskVariant> m_task = nullptr;
public:
    UnifiedTask() = default;

    template <typename F>
    UnifiedTask(F&& f)
        : m_task(std::make_unique<FunctionTask<F>>(std::forward<F>(f))) {}

    template <typename Promise>
    UnifiedTask(std::coroutine_handle<Promise> handle)
        : m_task(std::make_unique<CoroutineTask<Promise>>(handle)) {}

    void operator()() { if (m_task) { m_task->execute(); } }

    bool is_coroutine() const noexcept { return m_task ? m_task->is_coroutine() : false; }

    std::coroutine_handle<> get_coroutine_handle() const {
        return m_task ? m_task->get_coroutine_handle() : std::coroutine_handle<>{};
    }

    explicit operator bool() const { return bool(m_task); }
};


} // namespace ynet::runtime


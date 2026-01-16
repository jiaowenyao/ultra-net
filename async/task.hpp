#include <coroutine>
#include <exception>
#include <format>
#include <iostream>


namespace ynet::async {

struct TaskPromiseBase {

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
        template<typename T>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<T> callee) const noexcept {
            /**
             * 如果有调用者，就恢复调用者
             * 如果没有，说明是顶层任务
             */
            if (callee.promise().m_caller) {
                return callee.promise().m_caller;
            }
            else {
                if (callee.promise().m_ex != nullptr) [[unlikely]] {
                    // 处理未捕获的异常
                    try {
                        std::rethrow_exception(callee.promise().m_ex);
                    }
                    catch (const std::exception& e) {
                        std::cerr << std::format("catch a exception: {}", e.what());
                        std::terminate();
                    }
                    // 销毁协程
                    callee.destroy();
                    // 返回空协程句柄
                    return std::noop_coroutine();
                }
            }
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


    std::coroutine_handle<> m_caller = nullptr;
    std::exception_ptr m_ex = nullptr;
};



} // namesapce ynet::async


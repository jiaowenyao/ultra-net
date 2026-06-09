#pragma once

#include "task.hpp"
#include "ultranet/coroutine/execution_context.hpp"
#include <memory>
#include <tuple>
#include <variant>
#include <atomic>
#include <type_traits>

namespace ynet::async {

namespace detail {

// Map result slot: T -> std::monostate for void, T for non-void
template <typename T>
using result_slot_t = std::conditional_t<std::is_void_v<T>, std::monostate, T>;

// === when_all ===

template <typename... Ts>
struct WhenAllState {
    static constexpr size_t N = sizeof...(Ts);
    std::atomic<size_t> remaining{N};
    std::tuple<result_slot_t<Ts>...> results{};
    std::coroutine_handle<> caller{nullptr};
    std::exception_ptr exception{nullptr};

    bool decrement_and_check_last() {
        return remaining.fetch_sub(1, std::memory_order_acq_rel) == 1;
    }
};

template <typename T, size_t I, typename State>
Task<void> when_all_wrapper(std::shared_ptr<State> state, Task<T> task) {
    try {
        if constexpr (!std::is_void_v<T>) {
            auto value = co_await task;
            std::get<I>(state->results) = std::move(value);
        } else {
            co_await task;
        }
    } catch (...) {
        if (!state->exception) {
            state->exception = std::current_exception();
        }
    }

    if (state->decrement_and_check_last()) {
        auto* sched = ExecutionContext::current();
        if (sched) sched->resubmit(state->caller);
    }
}

template <typename... Ts>
class WhenAllAwaiter {
    using State = WhenAllState<result_slot_t<Ts>...>;
    using ResultTuple = std::tuple<result_slot_t<Ts>...>;

    std::shared_ptr<State> m_state;
    std::tuple<Task<Ts>...> m_tasks;

public:
    explicit WhenAllAwaiter(Task<Ts>... tasks)
        : m_state(std::make_shared<State>())
        , m_tasks(std::move(tasks)...) {}

    bool await_ready() const noexcept {
        return m_state->remaining.load(std::memory_order_relaxed) == 0;
    }

    void await_suspend(std::coroutine_handle<> caller) {
        m_state->caller = caller;
        launch_wrappers(std::index_sequence_for<Ts...>{});
    }

    ResultTuple await_resume() {
        if (m_state->exception) {
            std::rethrow_exception(m_state->exception);
        }
        return std::move(m_state->results);
    }

private:
    template <size_t... Is>
    void launch_wrappers(std::index_sequence<Is...>) {
        auto* sched = ExecutionContext::current();
        // Fold: launch one wrapper per task
        (launch_one<Is>(sched), ...);
    }

    template <size_t I>
    void launch_one(Scheduler* sched) {
        using TaskType = std::tuple_element_t<I, std::tuple<Task<Ts>...>>;
        using ValueType = typename TaskType::promise_type;
        // Actually, we need the T from Task<T>. Task<T> doesn't expose T directly.
        // We'll use Ts... from the class template parameter.
        using T = std::tuple_element_t<I, std::tuple<Ts...>>;

        auto wrapper = detail::when_all_wrapper<T, I>(
            m_state,
            std::move(std::get<I>(m_tasks))
        );
        sched->submit(wrapper.release());
    }
};

// === when_any ===

template <typename... Ts>
struct WhenAnyState {
    std::atomic<bool> done{false};
    size_t winner_index{static_cast<size_t>(-1)};
    std::tuple<result_slot_t<Ts>...> results{};
    std::coroutine_handle<> caller{nullptr};
    std::exception_ptr exception{nullptr};

    bool try_claim(size_t idx) {
        bool expected = false;
        if (done.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
            winner_index = idx;
            return true;
        }
        return false;
    }
};

template <typename T, size_t I, typename State>
Task<void> when_any_wrapper(std::shared_ptr<State> state, Task<T> task) {
    try {
        if constexpr (!std::is_void_v<T>) {
            auto value = co_await task;
            if (state->try_claim(I)) {
                std::get<I>(state->results) = std::move(value);
                auto* sched = ExecutionContext::current();
                if (sched) sched->resubmit(state->caller);
            }
        } else {
            co_await task;
            if (state->try_claim(I)) {
                auto* sched = ExecutionContext::current();
                if (sched) sched->resubmit(state->caller);
            }
        }
    } catch (...) {
        if (state->try_claim(I)) {
            state->exception = std::current_exception();
            auto* sched = ExecutionContext::current();
            if (sched) sched->resubmit(state->caller);
        }
    }
}

template <typename... Ts>
class WhenAnyAwaiter {
    using State = WhenAnyState<result_slot_t<Ts>...>;
    using ResultTuple = std::tuple<result_slot_t<Ts>...>;

    std::shared_ptr<State> m_state;
    std::tuple<Task<Ts>...> m_tasks;

public:
    explicit WhenAnyAwaiter(Task<Ts>... tasks)
        : m_state(std::make_shared<State>())
        , m_tasks(std::move(tasks)...) {}

    bool await_ready() const noexcept { return false; }

    void await_suspend(std::coroutine_handle<> caller) {
        m_state->caller = caller;
        launch_wrappers(std::index_sequence_for<Ts...>{});
    }

    std::pair<size_t, ResultTuple> await_resume() {
        if (m_state->exception) {
            std::rethrow_exception(m_state->exception);
        }
        return {m_state->winner_index, std::move(m_state->results)};
    }

private:
    template <size_t... Is>
    void launch_wrappers(std::index_sequence<Is...>) {
        auto* sched = ExecutionContext::current();
        (launch_one<Is>(sched), ...);
    }

    template <size_t I>
    void launch_one(Scheduler* sched) {
        using T = std::tuple_element_t<I, std::tuple<Ts...>>;
        auto wrapper = detail::when_any_wrapper<T, I>(
            m_state,
            std::move(std::get<I>(m_tasks))
        );
        sched->submit(wrapper.release());
    }
};

} // namespace detail

// === Public API ===

template <typename... Ts>
    requires (sizeof...(Ts) >= 1)
detail::WhenAllAwaiter<Ts...> when_all(Task<Ts>... tasks) {
    return detail::WhenAllAwaiter<Ts...>(std::move(tasks)...);
}

template <typename... Ts>
    requires (sizeof...(Ts) >= 1)
detail::WhenAnyAwaiter<Ts...> when_any(Task<Ts>... tasks) {
    return detail::WhenAnyAwaiter<Ts...>(std::move(tasks)...);
}

} // namespace ynet::async

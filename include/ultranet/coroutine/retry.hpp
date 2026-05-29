#pragma once

#include "ultranet/coroutine/task.hpp"
#include "ultranet/io/timer.hpp"
#include "ultranet/net/error.hpp"
#include <chrono>
#include <random>
#include <algorithm>
#include <format>

namespace ynet::async {

class ExponentialBackoff {
public:
    std::chrono::milliseconds base_delay;
    std::chrono::milliseconds max_delay;
    size_t max_retries;
    double jitter_factor;

    ExponentialBackoff(
        std::chrono::milliseconds base = std::chrono::milliseconds(100),
        std::chrono::milliseconds max = std::chrono::milliseconds(5000),
        size_t retries = 3,
        double jitter = 0.2) noexcept
        : base_delay(base), max_delay(max), max_retries(retries), jitter_factor(jitter) {}

    std::chrono::milliseconds delay_for(size_t attempt) const noexcept {
        using namespace std::chrono;
        auto scaled = base_delay * (1ull << std::min(attempt, size_t(20)));
        auto base = (scaled.count() < max_delay.count())
            ? std::chrono::milliseconds(scaled.count())
            : max_delay;
        if (jitter_factor <= 0.0 || base.count() == 0) return base;

        thread_local std::mt19937 rng(std::random_device{}());
        double factor = 1.0 + std::uniform_real_distribution<double>(
            -jitter_factor, jitter_factor)(rng);
        auto ms = static_cast<int64_t>(base.count() * factor);
        if (ms < 1) ms = 1;
        return milliseconds(static_cast<milliseconds::rep>(ms));
    }
};

namespace detail {

template <typename T>
struct TaskValueType;

template <typename T>
struct TaskValueType<Task<T>> { using type = T; };

} // namespace detail

template <typename TaskFn, typename Pred = bool(*)(const std::error_code&)>
    requires std::is_invocable_v<TaskFn>
Task<typename detail::TaskValueType<std::invoke_result_t<TaskFn>>::type>
with_retry(
    TaskFn task_fn,
    ExponentialBackoff policy = ExponentialBackoff{},
    Pred should_retry = [](const std::error_code&) { return true; })
{
    using T = typename detail::TaskValueType<std::invoke_result_t<TaskFn>>::type;
    size_t attempt = 0;

    while (true) {
        std::exception_ptr ex;
        try {
            if constexpr (std::is_void_v<T>) {
                co_await task_fn();
                co_return;
            } else {
                co_return co_await task_fn();
            }
        } catch (...) {
            ex = std::current_exception();
        }

        if (ex) {
            bool should_throw = true;
            try {
                std::rethrow_exception(ex);
            } catch (const std::system_error& e) {
                if (should_retry(e.code()) && attempt < policy.max_retries) {
                    should_throw = false;
                }
            } catch (...) {
                // Non-system_error: always rethrow
            }
            if (should_throw) {
                std::rethrow_exception(ex);
            }

            if (attempt > 0 || policy.delay_for(0).count() > 0) {
                co_await io::sleep_for(policy.delay_for(attempt));
            }
            ++attempt;
        }
    }
}

} // namespace ynet::async

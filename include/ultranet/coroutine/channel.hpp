#pragma once

#include "task.hpp"
#include "ultranet/coroutine/execution_context.hpp"
#include <array>
#include <queue>
#include <optional>
#include <atomic>
#include <coroutine>

namespace ynet::async {

namespace detail {

class SpinLock {
public:
    void lock() noexcept {
        while (m_flag.test_and_set(std::memory_order_acquire)) {
            // spin
        }
    }
    void unlock() noexcept {
        m_flag.clear(std::memory_order_release);
    }
    bool try_lock() noexcept {
        return !m_flag.test_and_set(std::memory_order_acquire);
    }
private:
    std::atomic_flag m_flag = ATOMIC_FLAG_INIT;
};

class SpinLockGuard {
public:
    explicit SpinLockGuard(SpinLock& lock) noexcept : m_lock(lock) { m_lock.lock(); }
    ~SpinLockGuard() { m_lock.unlock(); }
    SpinLockGuard(const SpinLockGuard&) = delete;
    SpinLockGuard& operator=(const SpinLockGuard&) = delete;
private:
    SpinLock& m_lock;
};

} // namespace detail

template <typename T, size_t Capacity = 256>
class Channel {
    static_assert(Capacity > 0, "Capacity must be > 0");

    using Lock = detail::SpinLock;
    using Guard = detail::SpinLockGuard;

    class WriteAwaitable {
    public:
        WriteAwaitable(Channel* ch, T value) noexcept
            : m_channel(ch), m_value(std::move(value)) {}

        bool await_ready() noexcept {
            std::coroutine_handle<> to_wake{};
            bool ready = false;
            {
                Guard lock(m_channel->m_lock);
                if (m_channel->m_closed) {
                    m_closed = true;
                    return true;
                }
                // 可用写空间需扣除已预留给被唤醒写者的槽位
                if (m_channel->m_count + m_channel->m_write_reserved < Capacity) {
                    m_channel->do_write_nolock(std::move(m_value));
                    to_wake = m_channel->wake_one_reader_nolock();
                    ready = true;
                } else {
                    m_would_block = true;
                    ready = false;
                }
            }
            // SpinLock 不可重入：必须在持锁外 schedule，避免内联 resume 再次加锁死锁
            if (to_wake) {
                Channel::schedule(to_wake);
            }
            return ready;
        }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            std::coroutine_handle<> to_wake{};
            bool resume_self = false;
            {
                Guard lock(m_channel->m_lock);
                if (m_channel->m_closed) {
                    m_closed = true;
                    m_would_block = false;
                    resume_self = true;
                } else if (m_channel->m_count + m_channel->m_write_reserved < Capacity) {
                    m_channel->do_write_nolock(std::move(m_value));
                    to_wake = m_channel->wake_one_reader_nolock();
                    // 已在锁内写完：清 m_would_block，避免 await_resume 再次加锁
                    m_would_block = false;
                    resume_self = true;
                } else {
                    m_channel->m_writers.push(h);
                }
            }
            if (to_wake) {
                Channel::schedule(to_wake);
            }
            if (resume_self) {
                h.resume();
            }
        }

        bool await_resume() const noexcept {
            if (m_closed) {
                return false;
            }
            if (m_would_block) {
                std::coroutine_handle<> to_wake{};
                bool ok = true;
                {
                    Guard lock(m_channel->m_lock);
                    // 被唤醒的写者持有一条写预留；关闭时释放预留且不写入
                    if (m_channel->m_closed) {
                        --m_channel->m_write_reserved;
                        ok = false;
                    } else {
                        m_channel->do_write_nolock(std::move(m_value));
                        --m_channel->m_write_reserved;
                        to_wake = m_channel->wake_one_reader_nolock();
                    }
                }
                if (to_wake) {
                    Channel::schedule(to_wake);
                }
                return ok;
            }
            return true;
        }

    private:
        Channel* m_channel;
        mutable T m_value;
        mutable bool m_would_block{false};
        mutable bool m_closed{false};
    };

    class ReadAwaitable {
    public:
        explicit ReadAwaitable(Channel* ch) noexcept : m_channel(ch) {}

        bool await_ready() noexcept {
            std::coroutine_handle<> to_wake{};
            bool ready = false;
            {
                Guard lock(m_channel->m_lock);
                // 仅消费未被读预留占用的元素，避免偷走已唤醒读者的数据
                if (m_channel->m_count > m_channel->m_read_reserved) {
                    m_value = m_channel->do_read_nolock();
                    to_wake = m_channel->wake_one_writer_nolock();
                    ready = true;
                } else if (m_channel->m_closed) {
                    m_closed = true;
                    ready = true;
                }
            }
            if (to_wake) {
                Channel::schedule(to_wake);
            }
            return ready;
        }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            std::coroutine_handle<> to_wake{};
            bool resume_self = false;
            {
                Guard lock(m_channel->m_lock);
                if (m_channel->m_count > m_channel->m_read_reserved) {
                    m_value = m_channel->do_read_nolock();
                    to_wake = m_channel->wake_one_writer_nolock();
                    resume_self = true;
                } else if (m_channel->m_closed) {
                    m_closed = true;
                    resume_self = true;
                } else {
                    m_channel->m_readers.push(h);
                }
            }
            if (to_wake) {
                Channel::schedule(to_wake);
            }
            if (resume_self) {
                h.resume();
            }
        }

        std::optional<T> await_resume() const noexcept {
            if (m_closed) {
                return std::nullopt;
            }
            if (!m_value.has_value()) {
                std::coroutine_handle<> to_wake{};
                {
                    Guard lock(m_channel->m_lock);
                    if (m_channel->m_count > 0) {
                        // 本读者持有一条读预留，消费后释放
                        m_value = m_channel->do_read_nolock();
                        --m_channel->m_read_reserved;
                        to_wake = m_channel->wake_one_writer_nolock();
                    } else if (m_channel->m_closed) {
                        // close 唤醒的空读：释放预留，报告 EOF
                        --m_channel->m_read_reserved;
                        m_closed = true;
                    }
                    // 非关闭且无数据：预留不变量应保证元素已在；不把存活 channel 当作 EOF
                }
                if (to_wake) {
                    Channel::schedule(to_wake);
                }
                if (m_closed) {
                    return std::nullopt;
                }
            }
            return std::move(m_value);
        }

    private:
        Channel* m_channel;
        mutable std::optional<T> m_value;
        mutable bool m_closed{false};
    };

public:
    Channel() = default;

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;
    Channel(Channel&&) = delete;
    Channel& operator=(Channel&&) = delete;

    WriteAwaitable write(T value) noexcept {
        return WriteAwaitable(this, std::move(value));
    }

    bool try_write(T value) noexcept {
        std::coroutine_handle<> to_wake{};
        {
            Guard lock(m_lock);
            if (m_closed || m_count + m_write_reserved >= Capacity) {
                return false;
            }
            do_write_nolock(std::move(value));
            to_wake = wake_one_reader_nolock();
        }
        if (to_wake) {
            schedule(to_wake);
        }
        return true;
    }

    ReadAwaitable read() noexcept {
        return ReadAwaitable(this);
    }

    std::optional<T> try_read() noexcept {
        std::coroutine_handle<> to_wake{};
        std::optional<T> val;
        {
            Guard lock(m_lock);
            if (m_count <= m_read_reserved) {
                return std::nullopt;
            }
            val = do_read_nolock();
            to_wake = wake_one_writer_nolock();
        }
        if (to_wake) {
            schedule(to_wake);
        }
        return val;
    }

    void close() noexcept {
        std::queue<std::coroutine_handle<>> readers;
        std::queue<std::coroutine_handle<>> writers;
        {
            Guard lock(m_lock);
            m_closed = true;
            readers.swap(m_readers);
            writers.swap(m_writers);
            // 与 wake_one_* 一致：被唤醒的等待者各持有一条预留，resume 时释放
            m_read_reserved += readers.size();
            m_write_reserved += writers.size();
        }
        while (!readers.empty()) {
            schedule(readers.front());
            readers.pop();
        }
        while (!writers.empty()) {
            schedule(writers.front());
            writers.pop();
        }
    }

    bool is_closed() const noexcept {
        Guard lock(m_lock);
        return m_closed;
    }

    size_t size() const noexcept {
        Guard lock(m_lock);
        return m_count;
    }

    bool empty() const noexcept {
        Guard lock(m_lock);
        return m_count == 0;
    }

    bool full() const noexcept {
        Guard lock(m_lock);
        return m_count + m_write_reserved >= Capacity;
    }

    size_t capacity() const noexcept { return Capacity; }

private:
    void do_write_nolock(T value) noexcept {
        m_buffer[m_write_pos] = std::move(value);
        if (++m_write_pos >= Capacity) {
            m_write_pos = 0;
        }
        ++m_count;
    }

    T do_read_nolock() noexcept {
        T val = std::move(m_buffer[m_read_pos]);
        if (++m_read_pos >= Capacity) {
            m_read_pos = 0;
        }
        --m_count;
        return val;
    }

    // 预留一条读槽并弹出读者句柄；调用方必须先解锁再 schedule
    std::coroutine_handle<> wake_one_reader_nolock() {
        if (!m_readers.empty()) {
            ++m_read_reserved;
            auto h = m_readers.front();
            m_readers.pop();
            return h;
        }
        return {};
    }

    // 预留一条写槽并弹出写者句柄；调用方必须先解锁再 schedule
    std::coroutine_handle<> wake_one_writer_nolock() {
        if (!m_writers.empty()) {
            ++m_write_reserved;
            auto h = m_writers.front();
            m_writers.pop();
            return h;
        }
        return {};
    }

    static void schedule(std::coroutine_handle<> h) {
        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->resubmit(h);
        } else {
            h.resume();
        }
    }

    std::array<T, Capacity> m_buffer{};
    size_t m_write_pos{0};
    size_t m_read_pos{0};
    size_t m_count{0};
    size_t m_write_reserved{0};
    size_t m_read_reserved{0};
    bool m_closed{false};

    std::queue<std::coroutine_handle<>> m_readers;
    std::queue<std::coroutine_handle<>> m_writers;
    mutable detail::SpinLock m_lock;
};

} // namespace ynet::async

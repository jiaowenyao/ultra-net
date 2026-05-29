#pragma once

#include "task.hpp"
#include "execution_context.hpp"
#include <array>
#include <queue>
#include <optional>
#include <atomic>

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
            Guard lock(m_channel->m_lock);
            if (m_channel->m_closed) {
                m_closed = true;
                return true;
            }
            if (m_channel->m_count < Capacity) {
                m_channel->do_write_nolock(std::move(m_value));
                m_channel->wake_one_reader_nolock();
                return true;
            }
            m_would_block = true;
            return false;
        }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            Guard lock(m_channel->m_lock);
            if (m_channel->m_closed) {
                m_closed = true;
                m_would_block = false;
                h.resume();
                return;
            }
            if (m_channel->m_count < Capacity) {
                m_channel->do_write_nolock(std::move(m_value));
                m_channel->wake_one_reader_nolock();
                m_would_block = false;
                h.resume();
                return;
            }
            m_channel->m_writers.push(h);
        }

        bool await_resume() const noexcept {
            if (m_closed) return false;
            if (m_would_block) {
                Guard lock(m_channel->m_lock);
                m_channel->do_write_nolock(std::move(m_value));
                m_channel->wake_one_reader_nolock();
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
            Guard lock(m_channel->m_lock);
            if (m_channel->m_count > 0) {
                m_value = m_channel->do_read_nolock();
                m_channel->wake_one_writer_nolock();
                return true;
            }
            if (m_channel->m_closed) {
                m_closed = true;
                return true;
            }
            return false;
        }

        void await_suspend(std::coroutine_handle<> h) noexcept {
            Guard lock(m_channel->m_lock);
            if (m_channel->m_count > 0) {
                m_value = m_channel->do_read_nolock();
                m_channel->wake_one_writer_nolock();
                h.resume();
                return;
            }
            if (m_channel->m_closed) {
                m_closed = true;
                h.resume();
                return;
            }
            m_channel->m_readers.push(h);
        }

        std::optional<T> await_resume() const noexcept {
            if (m_closed) return std::nullopt;
            if (!m_value.has_value()) {
                Guard lock(m_channel->m_lock);
                if (m_channel->m_count == 0 && m_channel->m_closed) {
                    m_closed = true;
                    return std::nullopt;
                }
                if (m_channel->m_count > 0) {
                    m_value = m_channel->do_read_nolock();
                    m_channel->wake_one_writer_nolock();
                }
            }
            return m_value;
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
        Guard lock(m_lock);
        if (m_closed || m_count >= Capacity) return false;
        do_write_nolock(std::move(value));
        wake_one_reader_nolock();
        return true;
    }

    ReadAwaitable read() noexcept {
        return ReadAwaitable(this);
    }

    std::optional<T> try_read() noexcept {
        Guard lock(m_lock);
        if (m_count == 0) return std::nullopt;
        auto val = do_read_nolock();
        wake_one_writer_nolock();
        return val;
    }

    void close() noexcept {
        Guard lock(m_lock);
        m_closed = true;
        while (!m_readers.empty()) {
            auto h = m_readers.front();
            m_readers.pop();
            schedule(h);
        }
        while (!m_writers.empty()) {
            auto h = m_writers.front();
            m_writers.pop();
            schedule(h);
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
        return m_count >= Capacity;
    }

    size_t capacity() const noexcept { return Capacity; }

private:
    void do_write_nolock(T value) noexcept {
        m_buffer[m_write_pos] = std::move(value);
        if (++m_write_pos >= Capacity) m_write_pos = 0;
        ++m_count;
    }

    T do_read_nolock() noexcept {
        T val = std::move(m_buffer[m_read_pos]);
        if (++m_read_pos >= Capacity) m_read_pos = 0;
        --m_count;
        return val;
    }

    void wake_one_reader_nolock() {
        if (!m_readers.empty()) {
            auto h = m_readers.front();
            m_readers.pop();
            schedule(h);
        }
    }

    void wake_one_writer_nolock() {
        if (!m_writers.empty()) {
            auto h = m_writers.front();
            m_writers.pop();
            schedule(h);
        }
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
    bool m_closed{false};

    std::queue<std::coroutine_handle<>> m_readers;
    std::queue<std::coroutine_handle<>> m_writers;
    mutable detail::SpinLock m_lock;
};

} // namespace ynet::async

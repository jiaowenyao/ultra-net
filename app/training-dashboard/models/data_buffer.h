// Model: Generic ring buffer for time-series data.
#pragma once

#include <vector>
#include <deque>
#include <algorithm>
#include <numeric>
#include <span>

namespace dashboard::model {

template <typename T>
class RingBuffer {
public:
    static constexpr size_t kDefaultCapacity = 3600;  // 1 hour at 1 Hz.

    explicit RingBuffer(size_t capacity = kDefaultCapacity)
        : m_capacity(capacity) {}

    void push(T&& value) {
        if (m_data.size() >= m_capacity) {
            m_data.pop_front();
        }
        m_data.push_back(std::move(value));
    }

    void push(const T& value) {
        if (m_data.size() >= m_capacity) {
            m_data.pop_front();
        }
        m_data.push_back(value);
    }

    const std::deque<T>& data() const { return m_data; }

    T latest() const {
        if (m_data.empty()) {
            return T{};
        }
        return m_data.back();
    }

    // Get the last N elements (or fewer if buffer is smaller).
    std::vector<T> last_n(size_t n) const {
        std::vector<T> result;
        size_t count = std::min(n, m_data.size());
        result.reserve(count);
        auto it = m_data.end() - static_cast<ssize_t>(count);
        for (; it != m_data.end(); ++it) {
            result.push_back(*it);
        }
        return result;
    }

    // Apply a reducer function over the last N elements.
    template <typename Reducer>
    auto reduce_last_n(size_t n, Reducer&& reducer,
                       decltype(reducer(T{})) initial) const
        -> decltype(initial)
    {
        if (m_data.empty()) {
            return initial;
        }
        size_t count = std::min(n, m_data.size());
        auto it = m_data.end() - static_cast<ssize_t>(count);
        decltype(initial) acc = initial;
        for (; it != m_data.end(); ++it) {
            acc = reducer(acc, *it);
        }
        return acc;
    }

    void clear() { m_data.clear(); }

    size_t size() const { return m_data.size(); }
    size_t capacity() const { return m_capacity; }
    bool empty() const { return m_data.empty(); }

    // Resize capacity (may drop oldest elements).
    void resize(size_t new_capacity) {
        m_capacity = new_capacity;
        while (m_data.size() > m_capacity) {
            m_data.pop_front();
        }
    }

private:
    std::deque<T> m_data;
    size_t m_capacity;
};

} // namespace dashboard::model

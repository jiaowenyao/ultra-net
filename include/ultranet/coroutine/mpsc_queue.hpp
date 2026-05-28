#pragma once

#include <atomic>
#include <optional>
#include <array>
#include <bit>

namespace ynet::async::scheduling {

template <typename T, size_t Capacity = 256>
class MpscQueue {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of 2");
    static_assert(Capacity >= 2, "Capacity must be at least 2");

    struct Slot {
        std::atomic<size_t> sequence;
        T data;
    };

    static constexpr size_t kMask = Capacity - 1;
    std::array<Slot, Capacity> m_buffer;
    alignas(64) std::atomic<size_t> m_enqueue_pos{0};
    alignas(64) size_t m_dequeue_pos{0};

public:
    MpscQueue() {
        for (size_t i = 0; i < Capacity; ++i) {
            m_buffer[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    MpscQueue(const MpscQueue&) = delete;
    MpscQueue& operator=(const MpscQueue&) = delete;
    MpscQueue(MpscQueue&&) = delete;
    MpscQueue& operator=(MpscQueue&&) = delete;

    bool try_push(T item) {
        size_t pos = m_enqueue_pos.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = m_buffer[pos & kMask];
            size_t seq = slot.sequence.load(std::memory_order_acquire);
            if (seq < pos) {
                return false;
            }
            if (seq == pos &&
                m_enqueue_pos.compare_exchange_weak(pos, pos + 1,
                    std::memory_order_relaxed)) {
                slot.data = std::move(item);
                slot.sequence.store(pos + 1, std::memory_order_release);
                return true;
            }
        }
    }

    std::optional<T> try_pop() {
        Slot& slot = m_buffer[m_dequeue_pos & kMask];
        size_t seq = slot.sequence.load(std::memory_order_acquire);
        if (seq != m_dequeue_pos + 1) {
            return std::nullopt;
        }
        T item = std::move(slot.data);
        slot.sequence.store(m_dequeue_pos + Capacity, std::memory_order_release);
        ++m_dequeue_pos;
        return item;
    }

    size_t approximate_size() const noexcept {
        size_t enq = m_enqueue_pos.load(std::memory_order_relaxed);
        size_t deq = m_dequeue_pos;
        return (enq >= deq) ? (enq - deq) : 0;
    }

    bool empty() const noexcept {
        return approximate_size() == 0;
    }
};

} // namespace ynet::async::scheduling

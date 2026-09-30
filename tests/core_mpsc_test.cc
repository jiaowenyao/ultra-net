#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <future>
#include <chrono>
#include <memory>
#include "ultranet/coroutine/mpsc_queue.hpp"

// MPSC Queue comprehensive tests.

using namespace ynet::async::scheduling;

static int g_passed = 0, g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

void test_push_pop() {
    T("basic push/pop");
    MpscQueue<int, 64> q;
    CHECK(q.try_push(42), "push");
    auto v = q.try_pop();
    CHECK(v.has_value() && *v == 42, "pop value");
    CHECK(!q.try_pop().has_value(), "empty returns nullopt");
    PASS();
}

void test_fill_drain() {
    T("fill to capacity and drain");
    constexpr size_t N = 32;
    MpscQueue<int, N> q;
    for (size_t i = 0; i < N; ++i) {
        CHECK(q.try_push(static_cast<int>(i)), "push");
    }
    for (size_t i = 0; i < N; ++i) {
        auto v = q.try_pop();
        CHECK(v.has_value() && *v == static_cast<int>(i), "pop preserves order");
    }
    CHECK(!q.try_pop().has_value(), "empty after drain");
    PASS();
}

void test_full_queue() {
    T("full queue rejects push");
    constexpr size_t N = 8;
    MpscQueue<int, N> q;
    for (size_t i = 0; i < N; ++i) {
        CHECK(q.try_push(static_cast<int>(i)), "fill");
    }
    CHECK(!q.try_push(99), "full queue rejects extra push");
    PASS();
}

void test_wrap_around() {
    T("wrap-around after drain");
    constexpr size_t N = 4;
    MpscQueue<int, N> q;
    // Fill, drain, fill again — tests sequence number wrap.
    for (int round = 0; round < 3; ++round) {
        for (size_t i = 0; i < N; ++i) {
            q.try_push(static_cast<int>(i + round * 100));
        }
        for (size_t i = 0; i < N; ++i) {
            q.try_pop();
        }
    }
    CHECK(q.try_push(999), "push after wrap works");
    auto v = q.try_pop();
    CHECK(v.has_value() && *v == 999, "correct value after wrap");
    PASS();
}

void test_move_only() {
    T("move-only type");
    MpscQueue<std::unique_ptr<int>, 16> q;
    CHECK(q.try_push(std::make_unique<int>(42)), "push unique_ptr");
    auto v = q.try_pop();
    CHECK(v.has_value() && **v == 42, "pop unique_ptr preserves value");
    PASS();
}

void test_single_producer_thread() {
    T("single producer thread");
    constexpr int N = 2000;
    MpscQueue<int, 4096> q;
    std::atomic<bool> producer_done{false};
    std::atomic<int> total_popped{0};

    std::thread producer([&] {
        for (int i = 0; i < N; ++i) {
            while (!q.try_push(i)) {
                std::this_thread::yield();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    // Busy-wait pop until producer is done AND queue is empty.
    while (!producer_done.load(std::memory_order_acquire)) {
        while (auto v = q.try_pop()) {
            ++total_popped;
        }
    }
    // Drain any remaining items.
    while (auto v = q.try_pop()) {
        ++total_popped;
    }

    producer.join();
    CHECK(total_popped.load() == N, "all messages received");
    PASS();
}

void test_multi_producer_threads() {
    T("multi-producer threads");
    constexpr int N = 1000;
    constexpr int K = 2;
    MpscQueue<int, 2048> q;
    std::atomic<int> total_popped{0};

    auto producer_fn = [&](int offset) {
        for (int i = 0; i < N; ++i) {
            while (!q.try_push(offset * 10000 + i)) {
                std::this_thread::yield();
            }
        }
    };

    std::thread t1(producer_fn, 1);
    std::thread t2(producer_fn, 2);
    t1.join();
    t2.join();

    while (auto v = q.try_pop()) {
        ++total_popped;
    }
    CHECK(total_popped.load() == N * K, "all messages received");
    PASS();
}

void test_multi_producer_with_consumer() {
    T("multi-producer with concurrent consumer");
    constexpr int kProducers = 4;
    constexpr int kPerProducer = 20000;
    constexpr int kTotal = kProducers * kPerProducer;
    MpscQueue<int, 256> q;
    std::atomic<int> total_popped{0};
    std::atomic<int> producers_done{0};

    auto run = [&]() {
        std::vector<std::thread> producers;
        producers.reserve(kProducers);
        for (int pid = 0; pid < kProducers; ++pid) {
            producers.emplace_back([&, pid] {
                for (int i = 0; i < kPerProducer; ++i) {
                    const int value = pid * 100000 + i;
                    while (!q.try_push(value)) {
                        std::this_thread::yield();
                    }
                }
                producers_done.fetch_add(1, std::memory_order_release);
            });
        }

        std::thread consumer([&] {
            while (total_popped.load(std::memory_order_relaxed) < kTotal) {
                if (auto v = q.try_pop()) {
                    ++total_popped;
                } else {
                    std::this_thread::yield();
                }
            }
        });

        for (auto& t : producers) {
            t.join();
        }
        consumer.join();
    };

    // 3 秒超时：livelock 时 try_push 永不返回，future 无法完成。
    auto fut = std::async(std::launch::async, run);
    if (fut.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
        FAIL("livelock: multi-producer try_push did not complete within 3s");
        return;
    }
    fut.get();
    CHECK(total_popped.load() == kTotal, "all messages received");
    CHECK(producers_done.load() == kProducers, "all producers finished");
    PASS();
}

void test_failed_push_keeps_source() {
    T("failed push keeps unique_ptr");
    MpscQueue<std::unique_ptr<int>, 2> q;
    auto a = std::make_unique<int>(1);
    auto b = std::make_unique<int>(2);
    auto c = std::make_unique<int>(3);
    CHECK(q.try_push(a), "first");
    CHECK(a == nullptr, "moved on success");
    CHECK(q.try_push(b), "second");
    CHECK(!q.try_push(c), "full");
    CHECK(c != nullptr, "kept on failure");
    CHECK(*c == 3, "value intact");
    auto p1 = q.try_pop();
    CHECK(p1.has_value() && *p1 && **p1 == 1, "pop first");
    PASS();
}

void test_empty() {
    T("empty queue operations");
    MpscQueue<int, 64> q;
    CHECK(q.empty(), "initially empty");
    q.try_push(1);
    CHECK(!q.empty(), "not empty after push");
    q.try_pop();
    CHECK(q.empty(), "empty after pop");
    PASS();
}

int main() {
    std::cout << "=== MPSC Queue Tests ===" << std::endl;
    test_push_pop();
    test_fill_drain();
    test_full_queue();
    test_wrap_around();
    test_move_only();
    test_single_producer_thread();
    test_multi_producer_threads();
    test_multi_producer_with_consumer();
    test_failed_push_keeps_source();
    test_empty();

    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

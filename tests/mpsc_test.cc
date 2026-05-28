#include "ultranet/coroutine/mpsc_queue.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cassert>

using namespace ynet::async::scheduling;

static int passed = 0;
static int failed = 0;

#define TEST(name) do { std::cout << "  " << name << "... "; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(msg) do { std::cout << "FAILED: " << msg << std::endl; ++failed; } while(0)
#define CHECK(cond, msg) do { if (!(cond)) { FAIL(msg); return; } } while(0)

void test_push_pop() {
    TEST("single push/pop");
    MpscQueue<int, 8> q;
    CHECK(q.try_pop() == std::nullopt, "empty queue should return nullopt");
    CHECK(q.try_push(42), "push should succeed");
    auto v = q.try_pop();
    CHECK(v.has_value(), "pop should return value");
    CHECK(*v == 42, "value should be 42");
    CHECK(q.try_pop() == std::nullopt, "queue should be empty again");
    PASS();
}

void test_fill_and_drain() {
    TEST("fill to capacity");
    constexpr size_t CAP = 8;
    MpscQueue<int, CAP> q;
    for (size_t i = 0; i < CAP; ++i) {
        CHECK(q.try_push(static_cast<int>(i * 10)), "push should succeed");
    }
    CHECK(!q.try_push(80), "push should fail when full");
    for (size_t i = 0; i < CAP; ++i) {
        auto v = q.try_pop();
        CHECK(v.has_value(), "pop should return value");
        CHECK(*v == static_cast<int>(i * 10), "value mismatch");
    }
    CHECK(q.try_pop() == std::nullopt, "queue should be empty");
    PASS();
}

void test_approximate_size() {
    TEST("approximate_size");
    MpscQueue<int, 8> q;
    CHECK(q.approximate_size() == 0, "initial size should be 0");
    q.try_push(1);
    CHECK(q.approximate_size() >= 1, "size should be >= 1");
    q.try_push(2);
    q.try_push(3);
    CHECK(q.approximate_size() >= 3, "size should be >= 3");
    q.try_pop();
    CHECK(q.approximate_size() >= 1, "size should be >= 1 after pop");
    PASS();
}

void test_empty() {
    TEST("empty check");
    MpscQueue<int, 8> q;
    CHECK(q.empty(), "should be empty");
    q.try_push(1);
    CHECK(!q.empty(), "should not be empty");
    q.try_pop();
    CHECK(q.empty(), "should be empty again");
    PASS();
}

void test_single_producer_thread() {
    TEST("single producer thread (20K ops)");
    MpscQueue<int, 1024> q;
    const int N = 20'000;
    std::atomic<bool> start{false};

    std::thread producer([&] {
        while (!start.load(std::memory_order_acquire)) {}
        for (int i = 0; i < N; ++i) {
            while (!q.try_push(i)) {}
        }
    });

    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire)) {}
        int sum = 0;
        int count = 0;
        while (count < N) {
            auto v = q.try_pop();
            if (v) {
                sum += *v;
                ++count;
            }
        }
        CHECK(sum == (N - 1) * N / 2, "sum mismatch");
    });

    start.store(true, std::memory_order_release);
    producer.join();
    consumer.join();
    PASS();
}

void test_multi_producer_threads() {
    TEST("2 producers, 1 consumer (10K ops)");
    MpscQueue<int, 1024> q;
    const int N_PER_PRODUCER = 5'000;
    const int NUM_PRODUCERS = 2;
    const int TOTAL = N_PER_PRODUCER * NUM_PRODUCERS;
    std::atomic<bool> start{false};
    std::atomic<int> consumed{0};
    std::atomic<int64_t> sum{0};

    std::vector<std::thread> producers;
    for (int p = 0; p < NUM_PRODUCERS; ++p) {
        producers.emplace_back([&, p] {
            while (!start.load(std::memory_order_acquire)) {}
            for (int i = 0; i < N_PER_PRODUCER; ++i) {
                int val = p * N_PER_PRODUCER + i;
                while (!q.try_push(val)) { std::this_thread::yield(); }
            }
        });
    }

    std::thread consumer([&] {
        while (!start.load(std::memory_order_acquire)) {}
        while (consumed.load(std::memory_order_acquire) < TOTAL) {
            auto v = q.try_pop();
            if (v) {
                sum.fetch_add(*v, std::memory_order_relaxed);
                consumed.fetch_add(1, std::memory_order_release);
            }
        }
    });

    start.store(true, std::memory_order_release);
    for (auto& t : producers) t.join();
    consumer.join();

    int64_t expected = 0;
    for (int p = 0; p < NUM_PRODUCERS; ++p)
        for (int i = 0; i < N_PER_PRODUCER; ++i)
            expected += p * N_PER_PRODUCER + i;

    CHECK(consumed.load() == TOTAL, "consumed count mismatch");
    CHECK(sum.load() == expected, "sum mismatch");
    PASS();
}

void test_wrap_around() {
    TEST("wrap around (2x capacity)");
    constexpr size_t CAP = 8;
    MpscQueue<int, CAP> q;
    for (int round = 0; round < 4; ++round) {
        for (size_t i = 0; i < CAP; ++i) {
            CHECK(q.try_push(round * 10 + static_cast<int>(i)), "push should succeed");
        }
        for (size_t i = 0; i < CAP; ++i) {
            auto v = q.try_pop();
            CHECK(v.has_value(), "pop should return value");
            CHECK(*v == round * 10 + static_cast<int>(i), "value mismatch");
        }
    }
    PASS();
}

void test_move_only_type() {
    TEST("move-only type (unique_ptr)");
    MpscQueue<std::unique_ptr<int>, 8> q;
    q.try_push(std::make_unique<int>(42));
    auto v = q.try_pop();
    CHECK(v.has_value(), "pop should return value");
    CHECK(**v == 42, "value should be 42");
    PASS();
}

int main() {
    std::cout << "=== MPSC Queue Tests ===" << std::endl;
    test_push_pop();
    test_fill_and_drain();
    test_approximate_size();
    test_empty();
    test_wrap_around();
    test_move_only_type();
    test_single_producer_thread();
    test_multi_producer_threads();

    std::cout << "\n=== MPSC Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

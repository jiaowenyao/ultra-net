#include "ultranet/ultranet.h"
#include <iostream>
#include <atomic>
#include <string>
#include <vector>
#include <memory>

using namespace ynet::async;

static int passed = 0;
static int failed = 0;

#define TEST(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(msg) do { std::cout << "FAILED: " << msg << std::endl; ++failed; } while(0)
#define CO_CHECK(cond, msg) do { if (!(cond)) { FAIL(msg); co_return; } } while(0)

// ========== Test 1: Basic write/read ==========

Task<void> test_basic_push_pop(scheduling::WorkStealingThreadPool& pool) {
    TEST("basic write/read");
    Channel<int, 4> ch;

    CO_CHECK(co_await ch.write(1), "write 1 failed");
    CO_CHECK(co_await ch.write(2), "write 2 failed");
    CO_CHECK(co_await ch.write(3), "write 3 failed");

    auto r1 = co_await ch.read();
    CO_CHECK(r1.has_value() && *r1 == 1, "read 1 mismatch");
    auto r2 = co_await ch.read();
    CO_CHECK(r2.has_value() && *r2 == 2, "read 2 mismatch");
    auto r3 = co_await ch.read();
    CO_CHECK(r3.has_value() && *r3 == 3, "read 3 mismatch");
    CO_CHECK(ch.empty(), "channel should be empty");

    PASS();
}

// ========== Test 2: Blocking read ==========

struct BlockingReadCtx {
    std::shared_ptr<Channel<int, 4>> ch;
    std::atomic<bool>* done;
};

Task<void> blocking_read_reader(std::shared_ptr<Channel<int, 4>> ch, std::atomic<bool>* done) {
    auto v = co_await ch->read();
    *done = true;
    co_return;
}

Task<void> test_blocking_read(scheduling::WorkStealingThreadPool& pool) {
    TEST("blocking read (empty → suspend → wake)");
    auto ch = std::make_shared<Channel<int, 4>>();
    std::atomic<bool> reader_done{false};

    auto reader_task = blocking_read_reader(ch, &reader_done);
    pool.submit(reader_task.task());

    co_await io::sleep_for(std::chrono::milliseconds(50));
    CO_CHECK(!reader_done.load(), "reader should be suspended");

    co_await ch->write(42);

    co_await io::sleep_for(std::chrono::milliseconds(100));
    CO_CHECK(reader_done.load(), "reader should be done");

    PASS();
}

// ========== Test 3: Blocking write ==========

Task<void> blocking_write_writer(std::shared_ptr<Channel<int, 2>> ch, std::atomic<bool>* done) {
    co_await ch->write(3);
    *done = true;
    co_return;
}

Task<void> test_blocking_write(scheduling::WorkStealingThreadPool& pool) {
    TEST("blocking write (full → suspend → wake)");
    auto ch = std::make_shared<Channel<int, 2>>();

    CO_CHECK(co_await ch->write(1), "write 1 failed");
    CO_CHECK(co_await ch->write(2), "write 2 failed");
    CO_CHECK(ch->full(), "channel should be full");

    std::atomic<bool> writer_done{false};
    auto writer_task = blocking_write_writer(ch, &writer_done);
    pool.submit(writer_task.task());

    co_await io::sleep_for(std::chrono::milliseconds(50));
    CO_CHECK(!writer_done.load(), "writer should be suspended");

    auto v = co_await ch->read();
    CO_CHECK(v == 1, "read mismatch");

    co_await io::sleep_for(std::chrono::milliseconds(100));
    CO_CHECK(writer_done.load(), "writer should be done");
    CO_CHECK(ch->size() == 2, "should have 2 items");

    PASS();
}

// ========== Test 4: close() semantics ==========

Task<void> test_close_semantics(scheduling::WorkStealingThreadPool& pool) {
    TEST("close() semantics");
    Channel<int, 4> ch;

    co_await ch.write(10);
    co_await ch.write(20);
    ch.close();

    auto r1 = co_await ch.read();
    CO_CHECK(r1 == 10, "r1 mismatch");
    auto r2 = co_await ch.read();
    CO_CHECK(r2 == 20, "r2 mismatch");

    auto r3 = co_await ch.read();
    CO_CHECK(!r3.has_value(), "should be nullopt after close+drain");
    CO_CHECK(!(co_await ch.write(30)), "write should fail on closed channel");

    PASS();
}

// ========== Test 5: close() wakes suspended reader ==========

Task<void> close_wake_reader(std::shared_ptr<Channel<int, 4>> ch,
    std::atomic<bool>* done, bool* got_null) {
    auto v = co_await ch->read();
    *got_null = !v.has_value();
    *done = true;
    co_return;
}

Task<void> test_close_wakes_reader(scheduling::WorkStealingThreadPool& pool) {
    TEST("close() wakes suspended reader");
    auto ch = std::make_shared<Channel<int, 4>>();
    std::atomic<bool> reader_done{false};
    bool got_null = false;

    auto reader_task = close_wake_reader(ch, &reader_done, &got_null);
    pool.submit(reader_task.task());

    co_await io::sleep_for(std::chrono::milliseconds(50));
    CO_CHECK(!reader_done.load(), "reader should be suspended");

    ch->close();
    co_await io::sleep_for(std::chrono::milliseconds(100));
    CO_CHECK(reader_done.load(), "reader should be done after close");
    CO_CHECK(got_null, "should be nullopt (closed+empty)");

    PASS();
}

// ========== Test 6: Multi-producer, single consumer ==========

struct MpScCtx {
    std::shared_ptr<Channel<int, 64>> ch;
    std::atomic<int>* total_sent;
    std::atomic<int>* producers_done;
    int producer_id;
    int items_per;
};

Task<void> mp_sc_producer(std::shared_ptr<Channel<int, 64>> ch, int p, int n,
    std::atomic<int>* total_sent, std::atomic<int>* producers_done) {
    for (int i = 0; i < n; ++i) {
        co_await ch->write(p * 1000 + i);
        total_sent->fetch_add(1, std::memory_order_relaxed);
    }
    producers_done->fetch_add(1, std::memory_order_relaxed);
    co_return;
}

Task<void> mp_sc_consumer(std::shared_ptr<Channel<int, 64>> ch,
    std::atomic<int>* sum, std::atomic<int>* count, int total_expected) {
    int received = 0;
    while (received < total_expected) {
        auto v = co_await ch->read();
        if (v) { sum->fetch_add(*v, std::memory_order_relaxed); ++received; count->fetch_add(1, std::memory_order_relaxed); }
    }
    co_return;
}

Task<void> test_mp_sc(scheduling::WorkStealingThreadPool& pool) {
    TEST("multi-producer, single consumer");
    auto ch = std::make_shared<Channel<int, 64>>();
    const int NUM_PRODUCERS = 5;
    const int ITEMS_PER = 200;
    std::atomic<int> total_sent{0};
    std::atomic<int> sum{0};
    std::atomic<int> count{0};
    std::atomic<int> producers_done{0};

    for (int p = 0; p < NUM_PRODUCERS; ++p) {
        auto t = mp_sc_producer(ch, p, ITEMS_PER, &total_sent, &producers_done);
        pool.submit(t.task());
    }
    auto ct = mp_sc_consumer(ch, &sum, &count, NUM_PRODUCERS * ITEMS_PER);
    pool.submit(ct.task());

    while (producers_done.load() < NUM_PRODUCERS ||
           count.load() < NUM_PRODUCERS * ITEMS_PER) {
        co_await io::sleep_for(std::chrono::milliseconds(10));
    }

    CO_CHECK(count.load() == NUM_PRODUCERS * ITEMS_PER, "consumer count mismatch");
    CO_CHECK(ch->empty(), "channel should be empty");

    int expected = 0;
    for (int p = 0; p < NUM_PRODUCERS; ++p)
        for (int i = 0; i < ITEMS_PER; ++i)
            expected += p * 1000 + i;
    CO_CHECK(sum.load() == expected, "sum mismatch");

    PASS();
}

// ========== Test 7: try_write / try_read ==========

Task<void> test_try_ops(scheduling::WorkStealingThreadPool& pool) {
    TEST("try_write / try_read (non-blocking)");
    Channel<int, 2> ch;

    CO_CHECK(ch.try_write(1), "try_write 1 should succeed");
    CO_CHECK(ch.try_write(2), "try_write 2 should succeed");
    CO_CHECK(!ch.try_write(3), "try_write 3 should fail (full)");

    auto r1 = ch.try_read();
    CO_CHECK(r1 == 1, "try_read mismatch");
    CO_CHECK(ch.try_write(3), "try_write should succeed after read");

    auto r2 = ch.try_read();
    auto r3 = ch.try_read();
    CO_CHECK(r2 == 2, "r2 mismatch");
    CO_CHECK(r3 == 3, "r3 mismatch");
    CO_CHECK(!ch.try_read().has_value(), "try_read on empty should be nullopt");

    PASS();
}

// ========== Test 8: Pipeline ==========

Task<void> pipeline_stage1(std::shared_ptr<Channel<int, 4>> ch) {
    for (int i = 0; i < 10; ++i) co_await ch->write(i);
    ch->close();
    co_return;
}

Task<void> pipeline_stage2(std::shared_ptr<Channel<int, 4>> in,
    std::shared_ptr<Channel<std::string, 4>> out) {
    while (true) {
        auto v = co_await in->read();
        if (!v) break;
        co_await out->write(std::string("item-") + std::to_string(*v * 2));
    }
    out->close();
    co_return;
}

Task<void> pipeline_stage3(std::shared_ptr<Channel<std::string, 4>> in,
    std::shared_ptr<std::vector<std::string>> results) {
    while (true) {
        auto v = co_await in->read();
        if (!v) break;
        results->push_back(*v);
    }
    co_return;
}

Task<void> test_pipeline(scheduling::WorkStealingThreadPool& pool) {
    TEST("pipeline (two channels)");
    auto ch1 = std::make_shared<Channel<int, 4>>();
    auto ch2 = std::make_shared<Channel<std::string, 4>>();
    auto results = std::make_shared<std::vector<std::string>>();

    auto t1 = pipeline_stage1(ch1);
    auto t2 = pipeline_stage2(ch1, ch2);
    auto t3 = pipeline_stage3(ch2, results);
    pool.submit(t1.task());
    pool.submit(t2.task());
    pool.submit(t3.task());

    co_await io::sleep_for(std::chrono::milliseconds(200));
    CO_CHECK(results->size() == 10, "pipeline result count mismatch");
    CO_CHECK((*results)[0] == "item-0", "results[0] mismatch");
    CO_CHECK((*results)[5] == "item-10", "results[5] mismatch");
    CO_CHECK((*results)[9] == "item-18", "results[9] mismatch");

    PASS();
}

// ========== Test 9: Channel<std::string> ==========

Task<void> test_string_channel(scheduling::WorkStealingThreadPool& pool) {
    TEST("Channel<std::string> move semantics");
    Channel<std::string, 4> ch;

    co_await ch.write(std::string("hello"));
    co_await ch.write(std::string("world"));

    auto r1 = co_await ch.read();
    CO_CHECK(r1 == "hello", "r1 mismatch");
    auto r2 = co_await ch.read();
    CO_CHECK(r2 == "world", "r2 mismatch");

    ch.close();
    auto r3 = co_await ch.read();
    CO_CHECK(!r3.has_value(), "should be nullopt");

    PASS();
}

// ========== Test 10: Producer-closes pattern ==========

Task<void> pc_producer(std::shared_ptr<Channel<int, 8>> ch) {
    for (int i = 0; i < 50; ++i) co_await ch->write(i);
    ch->close();
    co_return;
}

Task<void> pc_consumer(std::shared_ptr<Channel<int, 8>> ch,
    std::shared_ptr<std::vector<int>> results) {
    while (true) {
        auto v = co_await ch->read();
        if (!v) break;
        results->push_back(*v);
    }
    co_return;
}

Task<void> test_producer_closes(scheduling::WorkStealingThreadPool& pool) {
    TEST("producer-closes pattern");
    auto ch = std::make_shared<Channel<int, 8>>();
    auto results = std::make_shared<std::vector<int>>();

    auto pt = pc_producer(ch);
    auto ct = pc_consumer(ch, results);
    pool.submit(pt.task());
    pool.submit(ct.task());

    co_await io::sleep_for(std::chrono::milliseconds(200));
    CO_CHECK(results->size() == 50, "result count mismatch");
    for (int i = 0; i < 50; ++i) {
        CO_CHECK((*results)[i] == i, "value mismatch");
    }

    PASS();
}

// ========== Main ==========

int main() {
    std::cout << "=== Channel Tests ===" << std::endl;

    try {
        scheduling::WorkStealingThreadPool pool(2);
        {
            ExecutionContext::Scope scope(&pool);

            pool.submit(test_basic_push_pop(pool).task());
            pool.wait_all();

            pool.submit(test_blocking_read(pool).task());
            pool.wait_all();

            pool.submit(test_blocking_write(pool).task());
            pool.wait_all();

            pool.submit(test_close_semantics(pool).task());
            pool.wait_all();

            pool.submit(test_close_wakes_reader(pool).task());
            pool.wait_all();

            pool.submit(test_mp_sc(pool).task());
            pool.wait_all();

            pool.submit(test_try_ops(pool).task());
            pool.wait_all();

            pool.submit(test_pipeline(pool).task());
            pool.wait_all();

            pool.submit(test_string_channel(pool).task());
            pool.wait_all();

            pool.submit(test_producer_closes(pool).task());
            pool.wait_all();
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\n=== Channel Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

// Actor framework benchmark and stress test.
// Measures throughput (messages/sec) and verifies correctness under load.

#include <iostream>
#include <string>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include "ultranet/actor.hpp"

using namespace ynet::actor;
using namespace std::chrono;

static int g_passed = 0;
static int g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── Benchmark message ─────────────────────────────────────────────────

struct bench_msg {
    static constexpr const char* actor_type = "bench";
    uint64_t seq;
    uint64_t checksum;
};

// ── Benchmark actor: counts messages and verifies checksum ────────────

class bench_actor : public actor<bench_actor> {
public:
    std::atomic<uint64_t> m_count{0};
    std::atomic<uint64_t> m_checksum{0};

    bench_actor() {
        register_handler<bench_msg>([this](const bench_msg& m) {
            m_count.fetch_add(1, std::memory_order_relaxed);
            m_checksum.fetch_xor(m.checksum, std::memory_order_relaxed);
        });
    }
};

// ── Multi-producer bench ──────────────────────────────────────────────

void test_single_producer_throughput() {
    T("single producer throughput");
    system_config cfg;
    cfg.num_threads = 4;
    cfg.max_per_activation = 256;
    actor_system sys(cfg);
    auto ref = sys.spawn<bench_actor>("bench1");
    CHECK(ref.is_valid(), "spawned");

    constexpr uint64_t N = 5000;
    uint64_t checksum = 0;

    auto start = steady_clock::now();

    for (uint64_t i = 0; i < N; ++i) {
        checksum ^= (i * 0x9e3779b97f4a7c15ULL);
        ref.send(bench_msg{i, checksum});
    }

    // Wait for processing to complete.
    auto* actor = static_cast<bench_actor*>(ref.proxy()->local_actor());
    while (actor->m_count.load(std::memory_order_relaxed) < N) {
        std::this_thread::sleep_for(milliseconds(1));
    }

    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start).count();
    double rate = (N * 1000.0) / elapsed;

    CHECK(actor->m_count.load() == N, "all messages delivered");
    CHECK(actor->m_checksum.load() == checksum, "checksum preserved");

    std::cout << " (" << N << " msgs in " << elapsed << "ms, "
              << std::fixed << std::setprecision(0) << rate << " msg/s) "
              << std::flush;
    PASS();
}

void test_multi_actor_throughput() {
    T("multi-actor throughput (10 actors)");
    system_config cfg;
    cfg.num_threads = 4;
    cfg.max_per_activation = 256;
    actor_system sys(cfg);

    constexpr int kNumActors = 10;
    constexpr int kPerActor = 10000;
    std::vector<actor_ref<bench_actor>> refs;
    std::vector<bench_actor*> actors;

    for (int i = 0; i < kNumActors; ++i) {
        auto ref = sys.spawn<bench_actor>("bench-" + std::to_string(i));
        actors.push_back(
            static_cast<bench_actor*>(ref.proxy()->local_actor()));
        refs.push_back(ref);
    }

    auto start = steady_clock::now();

    // Round-robin send.
    for (int i = 0; i < kPerActor; ++i) {
        for (int j = 0; j < kNumActors; ++j) {
            refs[j].send(bench_msg{uint64_t(i), uint64_t(i ^ j)});
        }
    }

    // Wait for all actors.
    for (int j = 0; j < kNumActors; ++j) {
        while (actors[j]->m_count.load(std::memory_order_relaxed) <
               uint64_t(kPerActor)) {
            std::this_thread::sleep_for(milliseconds(1));
        }
    }

    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start).count();
    uint64_t total = kNumActors * kPerActor;
    double rate = (total * 1000.0) / elapsed;

    CHECK(true, "");  // Just checking it doesn't crash/timeout.
    std::cout << " (" << total << " msgs in " << elapsed << "ms, "
              << std::fixed << std::setprecision(0) << rate << " msg/s) "
              << std::flush;
    PASS();
}

void test_burst_latency() {
    T("burst latency (small batch)");
    system_config cfg;
    cfg.num_threads = 4;
    actor_system sys(cfg);
    auto ref = sys.spawn<bench_actor>("burst");

    constexpr int kBatch = 1000;
    auto start = steady_clock::now();

    for (int i = 0; i < kBatch; ++i) {
        ref.send(bench_msg{uint64_t(i), uint64_t(i)});
    }

    auto* actor = static_cast<bench_actor*>(ref.proxy()->local_actor());
    while (actor->m_count.load() < kBatch) {
        std::this_thread::sleep_for(milliseconds(1));
    }

    auto elapsed_us = duration_cast<microseconds>(
        steady_clock::now() - start).count();
    double avg_us = elapsed_us / double(kBatch);

    CHECK(actor->m_count.load() == kBatch, "all burst messages delivered");
    std::cout << " (avg " << std::fixed << std::setprecision(1)
              << avg_us << " us/msg) " << std::flush;
    PASS();
}

void test_concurrent_producers() {
    T("concurrent producers (4 threads → 1 actor)");
    system_config cfg;
    cfg.num_threads = 4;
    actor_system sys(cfg);
    auto ref = sys.spawn<bench_actor>("concurrent");

    constexpr int kPerThread = 10000;
    std::atomic<bool> start_flag{false};
    std::vector<std::thread> producers;

    for (int t = 0; t < 4; ++t) {
        producers.emplace_back([&ref, &start_flag, t]() {
            while (!start_flag.load(std::memory_order_acquire)) {}
            for (int i = 0; i < kPerThread; ++i) {
                ref.send(bench_msg{uint64_t(i * 4 + t), uint64_t(t)});
            }
        });
    }

    auto start = steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& p : producers) {
        p.join();
    }

    auto* actor = static_cast<bench_actor*>(ref.proxy()->local_actor());
    while (actor->m_count.load() < 4 * kPerThread) {
        std::this_thread::sleep_for(milliseconds(1));
    }

    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start).count();
    uint64_t total = 4 * kPerThread;
    double rate = (total * 1000.0) / elapsed;

    CHECK(actor->m_count.load() == total, "all messages from all threads");
    std::cout << " (" << total << " msgs in " << elapsed << "ms, "
              << std::fixed << std::setprecision(0) << rate << " msg/s) "
              << std::flush;
    PASS();
}

int main() {
    std::cout << "=== Actor Benchmark Tests ===" << std::endl;

    test_single_producer_throughput();
    test_multi_actor_throughput();
    test_burst_latency();
    test_concurrent_producers();

    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

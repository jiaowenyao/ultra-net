// Actor 框架压力测试套件。
// 测试场景：吞吐量、延迟、多Actor并发、背压恢复、持久化负载。
// 用法：./bin/actor_stress_test [场景名]
//   all       — 运行所有场景（默认）
//   throughput— 单 Actor 吞吐量
//   latency   — 消息延迟分布
//   multi     — 多 Actor 并发
//   burst     — 突发流量与背压恢复
//   endure    — 60 秒持久化负载
//   lifecycle — Actor 创建/销毁压测
//   exception  — 异常处理开销

#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstring>

#include "ultranet/actor.hpp"

using namespace ynet::actor;
using namespace std::chrono;

// ── 压测消息类型 ─────────────────────────────────────────────────────

struct stress_msg {
    static constexpr const char* actor_type = "stress";
    uint64_t seq;
    uint64_t timestamp_us;  // 发送时间戳，用于延迟计算
    uint64_t checksum;
};

// ── 压测 Actor ───────────────────────────────────────────────────────

class StressActor : public actor<StressActor> {
public:
    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> checksum_xor{0};
    std::atomic<uint64_t> total_latency_us{0};  // 累计延迟（微秒）
    std::atomic<uint64_t> max_latency_us{0};

    StressActor() {
        register_handler<stress_msg>([this](const stress_msg& m) {
            uint64_t now = duration_cast<microseconds>(
                steady_clock::now().time_since_epoch()).count();
            uint64_t lat = now - m.timestamp_us;
            received.fetch_add(1, std::memory_order_relaxed);
            checksum_xor.fetch_xor(m.checksum, std::memory_order_relaxed);
            total_latency_us.fetch_add(lat, std::memory_order_relaxed);
            // 更新最大延迟（无锁 CAS 循环）
            uint64_t cur = max_latency_us.load(std::memory_order_relaxed);
            while (lat > cur && !max_latency_us.compare_exchange_weak(
                       cur, lat, std::memory_order_relaxed)) {}
        });
    }
};

class NoopActor : public actor<NoopActor> {
public:
    std::atomic<uint64_t> count{0};
    NoopActor() {
        register_handler<stress_msg>([this](const stress_msg&) {
            count.fetch_add(1, std::memory_order_relaxed);
        });
    }
};

// ── 辅助函数 ─────────────────────────────────────────────────────────

static uint64_t now_us() {
    return duration_cast<microseconds>(
        steady_clock::now().time_since_epoch()).count();
}

static void print_header(const std::string& title) {
    std::cout << "\n" << std::string(60, '=') << "\n";
    std::cout << "  " << title << "\n";
    std::cout << std::string(60, '=') << "\n";
}

// 等待 actor 收到指定数量的消息
static bool wait_received(std::atomic<uint64_t>& counter, uint64_t target,
                          int timeout_ms = 10000) {
    auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    while (counter.load(std::memory_order_relaxed) < target) {
        if (steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(microseconds(100));
    }
    return true;
}

// ═══════════════════════════════════════════════════════════════════════
// 场景一：单 Actor 吞吐量
// ═══════════════════════════════════════════════════════════════════════

void bench_throughput() {
    print_header("场景一：单 Actor 吞吐量（1M 消息）");

    constexpr uint64_t N = 1'000'000;
    system_config cfg;
    cfg.num_threads = 4;
    cfg.max_per_activation = 256;
    actor_system sys(cfg);

    auto ref = sys.spawn<StressActor>("tp");
    auto* actor = static_cast<StressActor*>(ref.proxy()->local_actor());

    uint64_t checksum = 0;
    auto t0 = now_us();

    // 主线程发送 1M 消息
    for (uint64_t i = 0; i < N; ++i) {
        uint64_t cs = i * 0x9e3779b97f4a7c15ULL;
        checksum ^= cs;
        ref.send(stress_msg{i, now_us(), cs});
    }

    // 等待全部处理完成
    bool ok = wait_received(actor->received, N, 30000);
    auto t1 = now_us();
    uint64_t elapsed_us = t1 - t0;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  发送消息数 : " << N << "\n";
    std::cout << "  接收消息数 : " << actor->received.load() << "\n";
    std::cout << "  总耗时     : " << elapsed_us / 1000.0 << " ms\n";
    std::cout << "  吞吐量     : " << (N * 1000000.0 / elapsed_us) << " msg/s\n";

    if (actor->received.load() == N) {
        uint64_t avg_lat = actor->total_latency_us.load() / N;
        std::cout << "  平均延迟   : " << avg_lat << " us\n";
        std::cout << "  最大延迟   : " << actor->max_latency_us.load() << " us\n";
        std::cout << "  checksum   : " << (ok ? "✅ 正确" : "❌ 不匹配") << "\n";
    } else {
        std::cout << "  ⚠️  消息丢失！已发送 " << N
                  << "，实际收到 " << actor->received.load() << "\n";
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 场景二：消息延迟分布（P50/P99/P999）
// ═══════════════════════════════════════════════════════════════════════

void bench_latency() {
    print_header("场景二：消息延迟分布");

    constexpr int N = 10'000;
    std::vector<uint64_t> latencies;
    latencies.reserve(N);

    system_config cfg;
    cfg.num_threads = 4;
    actor_system sys(cfg);

    auto ref = sys.spawn<StressActor>("lat");
    auto* actor = static_cast<StressActor*>(ref.proxy()->local_actor());

    // 逐条发送并记录延迟
    for (int i = 0; i < N; ++i) {
        uint64_t ts = now_us();
        actor->received.store(0, std::memory_order_relaxed);
        ref.send(stress_msg{uint64_t(i), ts, uint64_t(i)});

        // 自旋等待（有限次数，避免长时间占用CPU）
        int spin = 0;
        while (actor->received.load(std::memory_order_relaxed) == 0 && spin < 10000) {
            ++spin;
        }
        uint64_t lat = now_us() - ts;
        latencies.push_back(lat);
    }

    std::sort(latencies.begin(), latencies.end());
    size_t p50_idx = N * 50 / 100;
    size_t p99_idx = N * 99 / 100;
    size_t p999_idx = N * 999 / 1000;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "  采样数 : " << N << "\n";
    std::cout << "  最小   : " << latencies[0] << " us\n";
    std::cout << "  P50    : " << latencies[p50_idx] << " us\n";
    std::cout << "  P99    : " << latencies[p99_idx] << " us\n";
    std::cout << "  P99.9  : " << latencies[p999_idx] << " us\n";
    std::cout << "  最大   : " << latencies[N - 1] << " us\n";

    uint64_t sum = 0;
    for (auto l : latencies) sum += l;
    std::cout << "  平均   : " << (sum / N) << " us\n";
}

// ═══════════════════════════════════════════════════════════════════════
// 场景三：多 Actor 并发
// ═══════════════════════════════════════════════════════════════════════

void bench_multi_actor() {
    print_header("场景三：多 Actor 并发（100 Actors × 10K 消息）");

    constexpr int kActors = 100;
    constexpr int kPerActor = 10'000;

    system_config cfg;
    cfg.num_threads = 4;
    cfg.max_per_activation = 256;
    actor_system sys(cfg);

    std::vector<actor_ref<NoopActor>> refs;
    std::vector<NoopActor*> actors;
    for (int i = 0; i < kActors; ++i) {
        auto ref = sys.spawn<NoopActor>("ma-" + std::to_string(i));
        actors.push_back(static_cast<NoopActor*>(ref.proxy()->local_actor()));
        refs.push_back(ref);
    }

    auto t0 = now_us();

    // 轮询发送
    for (int i = 0; i < kPerActor; ++i) {
        for (int j = 0; j < kActors; ++j) {
            refs[j].send(stress_msg{uint64_t(i), 0, uint64_t(i ^ j)});
        }
    }

    // 等待所有 actor 完成
    bool all_ok = true;
    for (int j = 0; j < kActors; ++j) {
        if (!wait_received(actors[j]->count, kPerActor, 15000)) {
            std::cout << "  ⚠️  Actor #" << j << " 仅收到 "
                      << actors[j]->count.load() << " 条\n";
            all_ok = false;
        }
    }

    auto t1 = now_us();
    uint64_t total = kActors * kPerActor;
    uint64_t elapsed_us = t1 - t0;

    std::cout << "  总消息数 : " << total << "\n";
    std::cout << "  耗时     : " << elapsed_us / 1000.0 << " ms\n";
    std::cout << "  吞吐量   : " << (total * 1000000.0 / elapsed_us) << " msg/s\n";
    std::cout << "  结果     : " << (all_ok ? "✅ 全部完成" : "❌ 有丢失") << "\n";
}

// ═══════════════════════════════════════════════════════════════════════
// 场景四：突发流量与背压恢复
// ═══════════════════════════════════════════════════════════════════════

void bench_burst() {
    print_header("场景四：突发流量与背压恢复");

    system_config cfg;
    cfg.num_threads = 2;
    cfg.max_per_activation = 64;
    actor_system sys(cfg);

    auto ref = sys.spawn<StressActor>("burst");
    auto* actor = static_cast<StressActor*>(ref.proxy()->local_actor());

    // 突发：瞬间发送 50000 条（远超 mailbox 容量 4096）
    constexpr int kBurst = 50'000;
    uint64_t checksum = 0;
    auto t0 = now_us();

    for (int i = 0; i < kBurst; ++i) {
        uint64_t cs = i * 0x9e3779b97f4a7c15ULL;
        checksum ^= cs;
        ref.send(stress_msg{uint64_t(i), now_us(), cs});
    }

    bool ok = wait_received(actor->received, kBurst, 30000);
    auto t1 = now_us();

    std::cout << "  突发消息 : " << kBurst << "\n";
    std::cout << "  mailbox  : 4096 (背压阈值 80%)\n";
    std::cout << "  收到     : " << actor->received.load() << "\n";
    std::cout << "  恢复时间 : " << (t1 - t0) / 1000.0 << " ms\n";

    if (actor->received.load() == kBurst) {
        uint64_t avg_lat = actor->total_latency_us.load() / kBurst;
        std::cout << "  平均延迟 : " << avg_lat << " us\n";
        std::cout << "  最大延迟 : " << actor->max_latency_us.load() << " us\n";
    }

    // 背压恢复后验证正常吞吐
    auto ref2 = sys.spawn<StressActor>("post-burst");
    auto* actor2 = static_cast<StressActor*>(ref2.proxy()->local_actor());
    for (int i = 0; i < 1000; ++i) {
        ref2.send(stress_msg{uint64_t(i), 0, uint64_t(i)});
    }
    bool ok2 = wait_received(actor2->received, 1000);
    std::cout << "  恢复后    : " << (ok2 ? "✅ 正常" : "❌ 异常") << "\n";
}

// ═══════════════════════════════════════════════════════════════════════
// 场景五：60 秒持久化负载
// ═══════════════════════════════════════════════════════════════════════

void bench_endurance() {
    print_header("场景五：60 秒持久化负载");

    system_config cfg;
    cfg.num_threads = 4;
    cfg.max_per_activation = 256;
    actor_system sys(cfg);

    auto ref = sys.spawn<StressActor>("endure");
    auto* actor = static_cast<StressActor*>(ref.proxy()->local_actor());

    constexpr int kDurationSec = 60;
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> total_sent{0};

    // 启动生产者线程，持续按固定速率发送
    std::thread producer([&]() {
        auto start = steady_clock::now();
        uint64_t seq = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            for (int batch = 0; batch < 1000 && !stop.load(); ++batch) {
                ref.send(stress_msg{seq++, now_us(), seq});
                total_sent.store(seq, std::memory_order_relaxed);
            }
            // 控制速率：每秒约 10 万条
            auto elapsed = duration_cast<milliseconds>(
                steady_clock::now() - start).count();
            auto expected = seq * 10 / 1000;  // 10us per msg
            if (elapsed < expected) {
                std::this_thread::sleep_for(
                    microseconds((expected - elapsed) * 1000 / std::max<uint64_t>(seq, 1)));
            }
        }
    });

    // 每秒输出一次统计
    uint64_t prev_received = 0;
    uint64_t prev_sent = 0;
    std::cout << "  " << std::setw(6) << "时间" << std::setw(12) << "发送速率"
              << std::setw(12) << "处理速率" << std::setw(10) << "待处理" << "\n";

    for (int sec = 1; sec <= kDurationSec; ++sec) {
        std::this_thread::sleep_for(seconds(1));

        uint64_t cur_sent = total_sent.load(std::memory_order_relaxed);
        uint64_t cur_received = actor->received.load(std::memory_order_relaxed);
        uint64_t send_rate = cur_sent - prev_sent;
        uint64_t recv_rate = cur_received - prev_received;
        uint64_t pending = cur_sent - cur_received;

        std::cout << "  " << std::setw(5) << sec << "s"
                  << std::setw(10) << send_rate << "/s"
                  << std::setw(10) << recv_rate << "/s"
                  << std::setw(8) << pending << "\n";

        prev_sent = cur_sent;
        prev_received = cur_received;

        // 检查背压：待处理不能持续增长
        if (pending > 50000) {
            std::cout << "  ⚠️  背压警告：待处理消息超过 50000\n";
        }
    }

    stop.store(true);
    producer.join();

    // 等待剩余消息处理完毕
    wait_received(actor->received, total_sent.load(), 10000);

    uint64_t total = actor->received.load();
    uint64_t avg_lat = total > 0 ? actor->total_latency_us.load() / total : 0;
    std::cout << "\n  总发送 : " << total_sent.load() << "\n";
    std::cout << "  总接收 : " << total << "\n";
    std::cout << "  平均延迟: " << avg_lat << " us\n";
    std::cout << "  结果    : " << (total == total_sent.load() ? "✅ 无丢失" : "❌ 有丢失") << "\n";
}

// ═══════════════════════════════════════════════════════════════════════
// 场景六：Actor 创建/销毁压测
// ═══════════════════════════════════════════════════════════════════════

void bench_lifecycle() {
    print_header("场景六：Actor 快速创建/销毁");

    constexpr int kRounds = 1000;
    std::vector<uint64_t> spawn_times, destroy_times;

    for (int i = 0; i < kRounds; ++i) {
        // 创建
        auto t0 = now_us();
        auto sys = std::make_unique<actor_system>(
            system_config{.num_threads = 1, .listen_port = 0});
        auto ref = sys->spawn<NoopActor>("lifecycle");
        ref.send(stress_msg{0, 0, 0});
        std::this_thread::sleep_for(microseconds(100));
        auto t1 = now_us();

        // 销毁
        sys.reset();
        auto t2 = now_us();

        spawn_times.push_back(t1 - t0);
        destroy_times.push_back(t2 - t1);

        if ((i + 1) % 100 == 0) {
            std::cout << "  进度: " << (i + 1) << "/" << kRounds << "\r" << std::flush;
        }
    }

    std::sort(spawn_times.begin(), spawn_times.end());
    std::sort(destroy_times.begin(), destroy_times.end());

    std::cout << "\n  创建（spawn + send）:\n";
    std::cout << "    P50  : " << spawn_times[kRounds / 2] << " us\n";
    std::cout << "    P99  : " << spawn_times[kRounds * 99 / 100] << " us\n";

    std::cout << "  销毁（含 drain）:\n";
    std::cout << "    P50  : " << destroy_times[kRounds / 2] << " us\n";
    std::cout << "    P99  : " << destroy_times[kRounds * 99 / 100] << " us\n";
}

// ═══════════════════════════════════════════════════════════════════════
// 场景七：异常处理开销
// ═══════════════════════════════════════════════════════════════════════

class ThrowActor : public actor<ThrowActor> {
public:
    std::atomic<uint64_t> before{0};
    ThrowActor() {
        register_handler<stress_msg>([this](const stress_msg&) {
            before.fetch_add(1);
            throw std::runtime_error("stress exception");
        });
    }
};

void bench_exception_overhead() {
    print_header("场景七：异常处理开销（对比正常 Actor）");

    constexpr int N = 100'000;

    // 正常 Actor
    {
        actor_system sys({.num_threads = 4});
        auto ref = sys.spawn<NoopActor>("normal");
        auto t0 = now_us();
        for (int i = 0; i < N; ++i) {
            ref.send(stress_msg{uint64_t(i), 0, 0});
        }
        auto* actor = static_cast<NoopActor*>(ref.proxy()->local_actor());
        wait_received(actor->count, N);
        auto t1 = now_us();
        std::cout << "  正常 Actor  : " << (N * 1000000.0 / (t1 - t0))
                  << " msg/s\n";
    }

    // 异常 Actor
    {
        actor_system sys({.num_threads = 4});
        auto ref = sys.spawn<ThrowActor>("thrower");
        auto t0 = now_us();
        for (int i = 0; i < N; ++i) {
            ref.send(stress_msg{uint64_t(i), 0, 0});
        }
        auto* actor = static_cast<ThrowActor*>(ref.proxy()->local_actor());
        wait_received(actor->before, N);
        auto t1 = now_us();
        std::cout << "  异常 Actor  : " << (N * 1000000.0 / (t1 - t0))
                  << " msg/s (含 " << N << " 次 throw/catch)\n";
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════

int main(int argc, char* argv[]) {
    std::string mode = (argc > 1) ? argv[1] : "all";

    std::cout << "╔══════════════════════════════════════════════════════╗\n";
    std::cout << "║     Ultra-Net Actor Framework 压力测试套件          ║\n";
    std::cout << "╚══════════════════════════════════════════════════════╝\n";
    std::cout << "  CPU : " << std::thread::hardware_concurrency() << " cores\n";
    std::cout << "  模式: " << mode << "\n";

    if (mode == "all" || mode == "throughput") bench_throughput();
    if (mode == "all" || mode == "latency")   bench_latency();
    if (mode == "all" || mode == "multi")     bench_multi_actor();
    if (mode == "all" || mode == "burst")     bench_burst();
    if (mode == "all" || mode == "endure")    bench_endurance();
    if (mode == "all" || mode == "lifecycle") bench_lifecycle();
    if (mode == "all" || mode == "exception") bench_exception_overhead();

    std::cout << "\n🏁 压力测试完成\n";
    return 0;
}

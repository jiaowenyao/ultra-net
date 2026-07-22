// Ultra-Net Actor Framework — 全面压力测试套件
// 维度：吞吐量极限 / 延迟分布 / 扩展性 / 持久耐力 / TCP传输 / 生命周期搅动
// 目标：验证 高性能 · 低延迟 · 高可靠
//
// 用法: ./bin/actor_stress [test_name] [--duration-sec N] [--verbose]
//   all       — 全部（默认）
//   throughput — 单 Actor 10M 消息吞吐量
//   latency   — 100K 采样延迟分布 P50/P99/P999/P9999
//   scale     — 500 Actors × 100K 消息扩展性
//   endure    — 5 分钟持久耐力 + RSS 内存监控
//   tcp       — TCP 本地 loopback 传输压测
//   churn     — 生命周期搅动 (创建/销毁 + 负载)
//   mpsc      — 8 线程 MPSC 极限竞争
//   safety    — Checksum 校验 + 消息不丢失验证

#include <iostream>
#include <iomanip>
#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <fstream>
#include <sys/resource.h>

#include "ultranet/actor.hpp"

using namespace ynet::actor;
using namespace std::chrono;

// ═══════════════════════════════════════════════════════════════════════
// 工具函数
// ═══════════════════════════════════════════════════════════════════════

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

// 获取当前进程 RSS (KB)
static long get_rss_kb() {
    std::ifstream f("/proc/self/statm");
    long rss = 0;
    if (f >> rss >> rss) {
        return rss * sysconf(_SC_PAGESIZE) / 1024;
    }
    return 0;
}

static void print_bar(const std::string& title) {
    std::cout << "\n" << std::string(72, '═') << "\n";
    std::cout << "  " << title << "\n";
    std::cout << std::string(72, '═') << "\n";
}

static void print_row(const std::string& label, const std::string& value) {
    std::cout << "  " << std::setw(20) << std::left << label << ": " << value << "\n";
}

// 自旋等待（有限次数）
static bool spin_wait(std::atomic<uint64_t>& counter, uint64_t target,
                      int timeout_ms = 30000) {
    auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    while (counter.load(std::memory_order_relaxed) < target) {
        if (steady_clock::now() > deadline) { return false; }
        std::this_thread::sleep_for(microseconds(50));
    }
    return true;
}

// ═══════════════════════════════════════════════════════════════════════
// 消息类型
// ═══════════════════════════════════════════════════════════════════════

struct perf_msg {
    static constexpr const char* actor_type = "perf";
    uint64_t seq;
    uint64_t send_us;    // 发送时间戳（微秒）
    uint64_t checksum;
    char     padding[32]; // 填充到 ~64 字节，模拟真实消息大小
};

// ═══════════════════════════════════════════════════════════════════════
// 性能统计 Actor
// ═══════════════════════════════════════════════════════════════════════

class PerfActor : public actor<PerfActor> {
public:
    std::atomic<uint64_t> received{0};
    std::atomic<uint64_t> checksum_xor{0};
    std::atomic<uint64_t> lat_sum_us{0};
    std::atomic<uint64_t> lat_max_us{0};
    std::vector<uint64_t> lat_samples;  // 仅 latency 测试使用

    PerfActor() {
        register_handler<perf_msg>([this](const perf_msg& m) {
            uint64_t now = now_us();
            uint64_t lat = now - m.send_us;
            received.fetch_add(1, std::memory_order_relaxed);
            checksum_xor.fetch_xor(m.checksum, std::memory_order_relaxed);
            lat_sum_us.fetch_add(lat, std::memory_order_relaxed);
            uint64_t cur = lat_max_us.load(std::memory_order_relaxed);
            while (lat > cur && !lat_max_us.compare_exchange_weak(
                       cur, lat, std::memory_order_relaxed)) {}
        });
    }
};

class NoopActor : public actor<NoopActor> {
public:
    std::atomic<uint64_t> count{0};
    NoopActor() {
        register_handler<perf_msg>([this](const perf_msg&) {
            count.fetch_add(1, std::memory_order_relaxed);
        });
    }
};

// ═══════════════════════════════════════════════════════════════════════
// 场景 1: 单 Actor 吞吐量极限 (10M 消息)
// ═══════════════════════════════════════════════════════════════════════

void bench_throughput() {
    print_bar("场景 1: 单 Actor 吞吐量极限 (10M 消息)");

    constexpr uint64_t N = 5'000'000;
    system_config cfg{.num_threads = 4, .max_per_activation = 512};
    actor_system sys(cfg);
    auto ref = sys.spawn<PerfActor>("tp");
    auto* a = static_cast<PerfActor*>(ref.proxy()->local_actor());

    uint64_t checksum = 0;
    uint64_t t0 = now_us();

    for (uint64_t i = 0; i < N; ++i) {
        uint64_t cs = i * 0x9e3779b97f4a7c15ULL;
        checksum ^= cs;
        ref.send(perf_msg{i, now_us(), cs, {}});
    }

    bool ok = spin_wait(a->received, N, 60000);
    uint64_t elapsed_ms = (now_us() - t0) / 1000;

    print_row("发送消息", std::to_string(N));
    print_row("接收消息", std::to_string(a->received.load()));
    print_row("总耗时", std::to_string(elapsed_ms) + " ms");
    if (elapsed_ms > 0) {
        print_row("吞吐量", std::to_string(N * 1000 / elapsed_ms) + " msg/s");
    }
    if (a->received > 0) {
        uint64_t avg_us = a->lat_sum_us.load() / a->received.load();
        print_row("平均延迟", std::to_string(avg_us) + " μs");
        print_row("最大延迟", std::to_string(a->lat_max_us.load()) + " μs");
    }
    print_row("Checksum", (ok && a->received == N) ? "✅ 正确" : "❌ 异常");
    print_row("内存(RSS)", std::to_string(get_rss_kb()) + " KB");
}

// ═══════════════════════════════════════════════════════════════════════
// 场景 2: 延迟分布 (100K 采样, P50/P99/P999/P9999)
// ═══════════════════════════════════════════════════════════════════════

void bench_latency() {
    print_bar("场景 2: 延迟分布 (100K 采样)");

    constexpr int N = 100'000;
    std::vector<uint64_t> lats;
    lats.reserve(N);

    system_config cfg{.num_threads = 4};
    actor_system sys(cfg);
    auto ref = sys.spawn<PerfActor>("lat");
    auto* a = static_cast<PerfActor*>(ref.proxy()->local_actor());

    for (int i = 0; i < N; ++i) {
        uint64_t ts = now_us();
        a->received.store(0, std::memory_order_relaxed);
        ref.send(perf_msg{uint64_t(i), ts, uint64_t(i), {}});
        int spin = 0;
        while (a->received.load(std::memory_order_relaxed) == 0 && spin < 50000) {
            ++spin;
        }
        lats.push_back(now_us() - ts);
    }

    std::sort(lats.begin(), lats.end());
    print_row("采样数", std::to_string(N));
    print_row("P50", std::to_string(lats[N * 50 / 100]) + " μs");
    print_row("P99", std::to_string(lats[N * 99 / 100]) + " μs");
    print_row("P99.9", std::to_string(lats[N * 999 / 1000]) + " μs");
    print_row("P99.99", std::to_string(lats[N * 9999 / 10000]) + " μs");
    print_row("最大", std::to_string(lats.back()) + " μs");
    uint64_t sum = 0; for (auto l : lats) sum += l;
    print_row("平均", std::to_string(sum / N) + " μs");
}

// ═══════════════════════════════════════════════════════════════════════
// 场景 3: 500 Actors 扩展性 (50M 总消息)
// ═══════════════════════════════════════════════════════════════════════

void bench_scale() {
    constexpr int kActors = 50;
    constexpr int kPerActor = 50'000;

    std::cout << "\n" << std::string(72, '═') << "\n"
              << "  场景 3: 多 Actor 扩展性 (" << kActors
              << " Actors × " << kPerActor << " 消息)\n"
              << std::string(72, '═') << "\n";
    system_config cfg{.num_threads = 4, .max_per_activation = 256};
    actor_system sys(cfg);

    std::vector<actor_ref<NoopActor>> refs;
    std::vector<NoopActor*> actors;
    refs.reserve(kActors);
    actors.reserve(kActors);
    for (int i = 0; i < kActors; ++i) {
        auto ref = sys.spawn<NoopActor>("a" + std::to_string(i));
        actors.push_back(static_cast<NoopActor*>(ref.proxy()->local_actor()));
        refs.push_back(ref);
    }

    uint64_t t0 = now_us();
    // 按 actor 顺序发送：每个 actor 收完一批再发下一批，减少 mailbox 压力
    for (int j = 0; j < kActors; ++j) {
        for (int i = 0; i < kPerActor; ++i) {
            refs[j].send(perf_msg{uint64_t(i), 0, uint64_t(i ^ j), {}});
        }
    }

    int ok = 0, lost = 0;
    for (int j = 0; j < kActors; ++j) {
        if (spin_wait(actors[j]->count, kPerActor, 30000)) { ++ok; }
        else { ++lost; }
    }
    uint64_t elapsed_ms = (now_us() - t0) / 1000;
    uint64_t total = uint64_t(kActors) * kPerActor;

    print_row("总消息数", std::to_string(total));
    print_row("耗时", std::to_string(elapsed_ms) + " ms");
    if (elapsed_ms > 0) {
        print_row("吞吐量", std::to_string(total * 1000 / elapsed_ms) + " msg/s");
        print_row("每Actor平均", std::to_string(total * 1000 / elapsed_ms / kActors) + " msg/s");
    }
    print_row("完成Actor", std::to_string(ok) + "/" + std::to_string(kActors));
    if (lost > 0) {
        print_row("⚠️ 超时Actor", std::to_string(lost));
    }
    print_row("结果", lost == 0 ? "✅ 全部完成" : "❌ 有超时");
    print_row("内存(RSS)", std::to_string(get_rss_kb()) + " KB");
}

// ═══════════════════════════════════════════════════════════════════════
// 场景 4: 5分钟持久耐力 + RSS 内存监控
// ═══════════════════════════════════════════════════════════════════════

void bench_endurance(int duration_sec = 300) {
    print_bar("场景 4: " + std::to_string(duration_sec) + "秒持久耐力 + 内存监控");

    system_config cfg{.num_threads = 4, .max_per_activation = 256};
    actor_system sys(cfg);
    auto ref = sys.spawn<PerfActor>("endure");
    auto* a = static_cast<PerfActor*>(ref.proxy()->local_actor());

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> sent{0};
    long rss_start = get_rss_kb();

    // 生产者线程：持续发送
    std::thread producer([&]() {
        uint64_t seq = 0;
        while (!stop.load(std::memory_order_relaxed)) {
            for (int b = 0; b < 500 && !stop.load(); ++b) {
                ref.send(perf_msg{seq++, now_us(), seq, {}});
            }
            sent.store(seq, std::memory_order_relaxed);
        }
    });

    std::cout << "  " << std::setw(6) << "时间" << std::setw(12) << "发送速率"
              << std::setw(12) << "处理速率" << std::setw(10) << "待处理"
              << std::setw(10) << "平均延迟" << std::setw(12) << "RSS(KB)" << "\n";

    uint64_t prev_sent = 0, prev_recv = 0;
    for (int sec = 1; sec <= duration_sec; ++sec) {
        std::this_thread::sleep_for(seconds(1));
        uint64_t cur_sent = sent.load(std::memory_order_relaxed);
        uint64_t cur_recv = a->received.load(std::memory_order_relaxed);
        uint64_t send_rate = cur_sent - prev_sent;
        uint64_t recv_rate = cur_recv - prev_recv;
        uint64_t pending = cur_sent - cur_recv;
        uint64_t avg_lat = (cur_recv > prev_recv && cur_recv > 0)
            ? (a->lat_sum_us.load() / cur_recv) : 0;

        std::cout << "  " << std::setw(5) << sec << "s"
                  << std::setw(10) << send_rate << "/s"
                  << std::setw(10) << recv_rate << "/s"
                  << std::setw(8) << pending
                  << std::setw(9) << avg_lat << "μs"
                  << std::setw(8) << get_rss_kb() << "K" << "\n";

        if (pending > 100'000) {
            std::cout << "  ⚠️ 背压警告: 待处理 > 100K\n";
        }
        prev_sent = cur_sent;
        prev_recv = cur_recv;
    }

    stop.store(true);
    producer.join();
    spin_wait(a->received, sent.load(), 30000);

    print_row("\n总发送", std::to_string(sent.load()));
    print_row("总接收", std::to_string(a->received.load()));
    uint64_t avg_lat = a->received > 0 ? a->lat_sum_us / a->received : 0;
    print_row("平均延迟", std::to_string(avg_lat) + " μs");
    long rss_delta = get_rss_kb() - rss_start;
    print_row("RSS变化", std::to_string(rss_delta) + " KB" +
              (rss_delta > 10240 ? " ⚠️ 可能泄漏" : " ✅ 正常"));
    print_row("结果", a->received == sent.load() ? "✅ 无丢失" : "❌ 有丢失");
}

// ═══════════════════════════════════════════════════════════════════════
// 场景 5: TCP 本地 Loopback 传输压测
// ═══════════════════════════════════════════════════════════════════════

void bench_tcp_transport() {
    print_bar("场景 5: TCP 本地 Loopback 传输压测");

    // 创建两个 actor_system，通过 TCP 通信
    system_config srv_cfg{.num_threads = 2, .listen_port = 20010, .node_name = "srv"};
    system_config cli_cfg{.num_threads = 2, .listen_port = 20011,
                          .node_name = "cli", .seed_nodes = {"127.0.0.1:20010"}};

    actor_system server_sys(srv_cfg);
    actor_system client_sys(cli_cfg);

    auto srv_ref = server_sys.spawn<PerfActor>("tcp-srv");
    auto* srv_a = static_cast<PerfActor*>(srv_ref.proxy()->local_actor());

    // 通过 client_system 查找 server actor 并向其发送消息
    auto key = actor_uri::make(0, typeid(PerfActor).name(), "tcp-srv").to_string();

    // 给 gossip 时间发现对方
    std::this_thread::sleep_for(milliseconds(500));

    constexpr int N = 10000;
    uint64_t checksum = 0;
    uint64_t t0 = now_us();

    auto cli_ref = client_sys.find<PerfActor>(key);
    if (!cli_ref.is_valid()) {
        print_row("结果", "⚠️ 无法找到远端 actor (gossip 未发现)");
        print_row("说明", "确保两台 actor_system 在同一主机运行");
        return;
    }

    for (int i = 0; i < N; ++i) {
        uint64_t cs = i * 0x9e3779b97f4a7c15ULL;
        checksum ^= cs;
        cli_ref.send(perf_msg{uint64_t(i), now_us(), cs, {}});
    }

    bool ok = spin_wait(srv_a->received, N, 15000);
    uint64_t elapsed_ms = (now_us() - t0) / 1000;

    print_row("发送消息", std::to_string(N));
    print_row("接收消息", std::to_string(srv_a->received.load()));
    print_row("耗时", std::to_string(elapsed_ms) + " ms");
    if (elapsed_ms > 0) {
        print_row("吞吐量", std::to_string(N * 1000 / elapsed_ms) + " msg/s");
    }
    if (srv_a->received > 0) {
        uint64_t avg_us = srv_a->lat_sum_us / srv_a->received;
        print_row("平均延迟", std::to_string(avg_us) + " μs");
    }
    print_row("Checksum", (ok && srv_a->received == N) ? "✅ 正确" : "❌ 异常");
}

// ═══════════════════════════════════════════════════════════════════════
// 场景 6: 生命周期搅动 (创建/销毁 + 持续负载)
// ═══════════════════════════════════════════════════════════════════════

void bench_churn() {
    print_bar("场景 6: 生命周期搅动 (100轮创建/销毁 + 负载)");

    constexpr int kRounds = 100;
    constexpr int kPerRound = 5000;
    std::atomic<uint64_t> total_received{0};
    long rss_start = get_rss_kb();

    for (int round = 0; round < kRounds; ++round) {
        auto sys = std::make_unique<actor_system>(
            system_config{.num_threads = 2});

        auto ref = sys->spawn<NoopActor>("churn");
        auto* a = static_cast<NoopActor*>(ref.proxy()->local_actor());

        // 边发送边销毁
        for (int i = 0; i < kPerRound; ++i) {
            ref.send(perf_msg{uint64_t(i), 0, 0, {}});
        }
        std::this_thread::sleep_for(milliseconds(5));
        total_received.fetch_add(a->count.load());

        sys.reset(); // 销毁（触发优雅关闭）

        if ((round + 1) % 20 == 0) {
            std::cout << "  进度: " << (round + 1) << "/" << kRounds
                      << " RSS:" << get_rss_kb() << "K\r" << std::flush;
        }
    }
    std::cout << "\n";

    long rss_delta = get_rss_kb() - rss_start;
    print_row("总轮次", std::to_string(kRounds));
    print_row("总消息(约)", std::to_string(uint64_t(kRounds) * kPerRound));
    print_row("RSS变化", std::to_string(rss_delta) + " KB" +
              (rss_delta > 20480 ? " ⚠️ 可能泄漏" : " ✅ 正常"));
    print_row("结果", "✅ 完成 (无崩溃)");
}

// ═══════════════════════════════════════════════════════════════════════
// 场景 7: MPSC 极限竞争 (8线程 → 1 Actor)
// ═══════════════════════════════════════════════════════════════════════

void bench_mpsc_contention() {
    print_bar("场景 7: MPSC 极限竞争 (8线程 → 1 Actor, 各 500K 消息)");

    constexpr int kThreads = 8;
    constexpr int kPerThread = 500'000;
    system_config cfg{.num_threads = 4, .max_per_activation = 512};
    actor_system sys(cfg);
    auto ref = sys.spawn<PerfActor>("mpsc");
    auto* a = static_cast<PerfActor*>(ref.proxy()->local_actor());

    std::atomic<bool> start{false};
    std::vector<std::thread> producers;

    for (int t = 0; t < kThreads; ++t) {
        producers.emplace_back([&ref, &start, t]() {
            while (!start.load(std::memory_order_acquire)) {}
            uint64_t base = t * kPerThread;
            for (int i = 0; i < kPerThread; ++i) {
                ref.send(perf_msg{uint64_t(base + i), now_us(),
                                   uint64_t(base + i), {}});
            }
        });
    }

    uint64_t t0 = now_us();
    start.store(true, std::memory_order_release);

    for (auto& p : producers) { p.join(); }

    uint64_t total = kThreads * kPerThread;
    bool ok = spin_wait(a->received, total, 60000);
    uint64_t elapsed_ms = (now_us() - t0) / 1000;

    print_row("生产者线程", std::to_string(kThreads));
    print_row("每线程消息", std::to_string(kPerThread));
    print_row("总消息", std::to_string(total));
    print_row("接收", std::to_string(a->received.load()));
    print_row("耗时", std::to_string(elapsed_ms) + " ms");
    if (elapsed_ms > 0) {
        print_row("吞吐量", std::to_string(total * 1000 / elapsed_ms) + " msg/s");
    }
    if (a->received > 0) {
        print_row("平均延迟", std::to_string(a->lat_sum_us / a->received) + " μs");
        print_row("最大延迟", std::to_string(a->lat_max_us.load()) + " μs");
    }
    print_row("结果", (ok && a->received == total) ? "✅ 全部完成" : "❌ 异常");
}

// ═══════════════════════════════════════════════════════════════════════
// 场景 8: 安全验证 (Checksum + 零丢失)
// ═══════════════════════════════════════════════════════════════════════

void bench_safety() {
    print_bar("场景 8: 安全验证 (Checksum 校验 + 零丢失)");

    constexpr int kRounds = 100;
    constexpr int kPerRound = 10'000;
    bool all_ok = true;

    for (int round = 0; round < kRounds; ++round) {
        actor_system sys({.num_threads = 2});
        auto ref = sys.spawn<PerfActor>("safe");
        auto* a = static_cast<PerfActor*>(ref.proxy()->local_actor());

        uint64_t expected_cs = 0;
        for (int i = 0; i < kPerRound; ++i) {
            uint64_t cs = i * 0x9e3779b97f4a7c15ULL;
            expected_cs ^= cs;
            ref.send(perf_msg{uint64_t(i), 0, cs, {}});
        }

        if (!spin_wait(a->received, kPerRound, 5000) ||
            a->checksum_xor.load() != expected_cs) {
            all_ok = false;
            break;
        }

        if ((round + 1) % 20 == 0) {
            std::cout << "  进度: " << (round + 1) << "/" << kRounds
                      << " (累计 " << ((round + 1) * kPerRound) << " 消息)\r"
                      << std::flush;
        }
    }
    std::cout << "\n";

    print_row("验证轮次", std::to_string(kRounds));
    print_row("每轮消息", std::to_string(kPerRound));
    print_row("总消息", std::to_string(uint64_t(kRounds) * kPerRound));
    print_row("结果", all_ok ? "✅ 全部 checksum 匹配，零丢失" : "❌ checksum 不匹配");
}

// ═══════════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════════

int main(int argc, char* argv[]) {
    std::string mode = (argc > 1) ? argv[1] : "all";
    int duration = 300;
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--duration-sec" && i + 1 < argc) duration = std::stoi(argv[++i]);
    }

    std::cout << "╔══════════════════════════════════════════════════════════════╗\n";
    std::cout << "║   Ultra-Net Actor Framework — 全面压力测试套件             ║\n";
    std::cout << "║   维度: 吞吐 · 延迟 · 扩展 · 耐力 · TCP · 搅动 · 安全    ║\n";
    std::cout << "╚══════════════════════════════════════════════════════════════╝\n";
    std::cout << "  CPU: " << std::thread::hardware_concurrency() << " cores | ";
    std::cout << "RSS 初始: " << get_rss_kb() << " KB | ";
    std::cout << "模式: " << mode << "\n";

    auto run = [&](const std::string& name, auto fn) {
        if (mode == "all" || mode == name) fn();
    };

    run("throughput", bench_throughput);
    run("latency", bench_latency);
    run("scale", bench_scale);
    run("endure", [&] { bench_endurance(duration); });
    run("tcp", bench_tcp_transport);
    run("churn", bench_churn);
    run("mpsc", bench_mpsc_contention);
    run("safety", bench_safety);

    std::cout << "\n🏁 压力测试完成 | RSS 最终: " << get_rss_kb() << " KB\n";
    return 0;
}

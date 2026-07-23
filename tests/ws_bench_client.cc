// WebSocket 压测客户端 — 测量吞吐量和延迟
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <thread>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
using namespace ynet::async;
using namespace ynet::async;
using namespace ynet::async::net;
using namespace ynet::async::io;
using namespace std::chrono;

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

int main(int argc, char* argv[]) {
    uint16_t port = (argc > 1) ? static_cast<uint16_t>(std::atoi(argv[1])) : 9001;
    int N = (argc > 2) ? std::atoi(argv[2]) : 10000;
    int conns = (argc > 3) ? std::atoi(argv[3]) : 1;

    std::cout << "╔══════════════════════════════════════════╗\n";
    std::cout << "║   Ultra-Net WebSocket 压测               ║\n";
    std::cout << "╚══════════════════════════════════════════╝\n";
    std::cout << "目标: 127.0.0.1:" << port << "\n";
    std::cout << "消息: " << N << " 连接: " << conns << "\n\n";

    std::atomic<uint64_t> total_sent{0}, total_recv{0};
    std::atomic<uint64_t> lat_sum_us{0}, lat_max_us{0};
    std::atomic<bool> start{false};

    auto bench_conn = [&](int id) -> Task<void> {
        // 连接到服务器
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        // 等待启动信号
        while (!start.load()) {}

        std::string payload = "bench_msg_" + std::to_string(id) + "_";
        // 补齐到 64 字节
        payload.resize(64, 'x');

        for (int i = 0; i < N; ++i) {
            uint64_t ts = now_us();
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            uint64_t lat = now_us() - ts;

            total_sent.fetch_add(1);
            total_recv.fetch_add(1);
            lat_sum_us.fetch_add(lat);

            uint64_t cur = lat_max_us.load();
            while (lat > cur && !lat_max_us.compare_exchange_weak(cur, lat)) {}
        }
        co_await ws.close();
    };

    return Launcher().threads(4).run(
        [&]() -> Task<void> {
            // 建立所有连接
            std::vector<Task<void>> tasks;
            for (int i = 0; i < conns; ++i) {
                tasks.push_back(bench_conn(i));
            }

            auto t0 = now_us();
            start.store(true);

            // 等待所有连接完成
            for (auto& t : tasks) { co_await t; }

            auto elapsed_ms = (now_us() - t0) / 1000;
            uint64_t total = total_recv.load();

            std::cout << std::fixed << std::setprecision(0);
            std::cout << "  总消息  : " << total << "\n";
            std::cout << "  耗时    : " << elapsed_ms << " ms\n";
            if (elapsed_ms > 0) {
                std::cout << "  吞吐量  : " << (total * 1000 / elapsed_ms) << " msg/s\n";
            }
            if (total > 0) {
                std::cout << "  平均延迟: " << (lat_sum_us / total) << " μs\n";
                std::cout << "  最大延迟: " << lat_max_us.load() << " μs\n";
            }
            std::cout << "  结果    : " << (total_sent == total_recv ? "✅ 无丢失" : "❌ 有丢失") << "\n";
        });
}

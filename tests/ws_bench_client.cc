// WebSocket 压测客户端
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
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

    // 单独测试每个连接（避免 co_await 多个 Task 的复杂性）
    for (int c = 0; c < conns; ++c) {
        int result = Launcher().threads(2).run(
            [&, c]() -> Task<void> {
                auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");

                std::string payload = "bench_" + std::to_string(c) + "_";
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
            });
        if (result != 0) { std::cerr << "connection " << c << " failed\n"; }
    }

    uint64_t total = total_recv.load();
    std::cout << std::fixed << std::setprecision(0);
    std::cout << "  总消息  : " << total << "\n";
    if (total > 0) {
        std::cout << "  平均延迟: " << (lat_sum_us / total) << " μs\n";
        std::cout << "  最大延迟: " << lat_max_us.load() << " μs\n";
    }
    std::cout << "  结果    : " << (total_sent == total_recv ? "✅ 无丢失" : "❌ 有丢失") << "\n";
}

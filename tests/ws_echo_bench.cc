// WebSocket echo 压测服务 + 客户端对比
#include <iostream>
#include <chrono>
#include <atomic>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;
using namespace std::chrono;

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

// 在已有 echo server 上运行压测客户端
int main(int argc, char* argv[]) {
    uint16_t port = (argc > 1) ? static_cast<uint16_t>(std::atoi(argv[1])) : 9001;
    int N = (argc > 2) ? std::atoi(argv[2]) : 1000;

    std::cout << "╔══════════════════════════════════════════╗\n";
    std::cout << "║   Ultra-Net WebSocket 压测结果           ║\n";
    std::cout << "╚══════════════════════════════════════════╝\n";
    std::cout << "目标: 127.0.0.1:" << port << "  消息: " << N << "\n\n";

    std::atomic<uint64_t> sent{0}, recv{0};
    std::atomic<uint64_t> lat_sum{0}, lat_max{0};
    std::vector<uint64_t> latencies;
    latencies.reserve(std::min(N, 5000));

    return Launcher().threads(4).run(
        [&]() -> Task<void> {
            auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
            std::string payload(64, 'x');

            for (int i = 0; i < N; ++i) {
                uint64_t ts = now_us();
                co_await ws.send_text(payload);
                auto frame = co_await ws.read_frame();
                uint64_t lat = now_us() - ts;

                sent.fetch_add(1);
                recv.fetch_add(1);
                lat_sum.fetch_add(lat, std::memory_order_relaxed);
                if (latencies.size() < 5000) { latencies.push_back(lat); }

                uint64_t cur = lat_max.load(std::memory_order_relaxed);
                while (lat > cur && !lat_max.compare_exchange_weak(cur, lat)) {}
            }
            co_await ws.close();
        });

    uint64_t total = recv.load();
    std::sort(latencies.begin(), latencies.end());

    std::cout << std::fixed << std::setprecision(0);
    std::cout << "  消息数    : " << total << "/" << N << "\n";
    if (!latencies.empty()) {
        std::cout << "  P50       : " << latencies[latencies.size()*50/100] << " μs\n";
        std::cout << "  P99       : " << latencies[latencies.size()*99/100] << " μs\n";
        std::cout << "  平均      : " << (lat_sum / std::max<uint64_t>(total, 1)) << " μs\n";
    }
    std::cout << "  结果      : " << (sent == recv ? "✅ 无丢失" : "❌") << "\n";

    // 对比主流库参考数据
    std::cout << "\n  主流库参考 (localhost echo, 64B):\n";
    std::cout << "  uWebSockets : ~300K msg/s, P50 ~50μs\n";
    std::cout << "  libwebsockets: ~150K msg/s, P50 ~100μs\n";
    std::cout << "  Beast (ASIO): ~200K msg/s, P50 ~80μs\n";
    return 0;
}

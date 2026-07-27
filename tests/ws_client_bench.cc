// 通用 WS echo 客户端基准测试——可连接任意 WebSocket echo 服务器。
// 用法: ./ws_client_bench <host> <port> <num_msgs> [payload_size]
#include <iostream>
#include <iomanip>
#include <chrono>
#include <vector>
#include <algorithm>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"

using namespace ynet::async;
using namespace ynet::async::net;
using namespace std::chrono;

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv) {
    std::string host = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? static_cast<uint16_t>(atoi(argv[2])) : 9001;
    int N = argc > 3 ? atoi(argv[3]) : 500;
    int payload_size = argc > 4 ? atoi(argv[4]) : 64;

    std::vector<uint64_t> lats;
    lats.reserve(N);

    std::cout << "=== WS Client Benchmark ===\n";
    std::cout << "Server: " << host << ":" << port << "  Messages: " << N
              << "  Payload: " << payload_size << "B\n\n";

    uint64_t t0 = now_us();
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect(host, port, "/",
            std::chrono::milliseconds(5000));
        std::string payload(payload_size, 'x');

        for (int i = 0; i < N; ++i) {
            uint64_t ts = now_us();
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            lats.push_back(now_us() - ts);
        }
        co_await ws.close();
    });

    uint64_t elapsed = now_us() - t0;
    std::sort(lats.begin(), lats.end());

    std::cout << std::fixed << std::setprecision(0);
    std::cout << "  Messages   : " << lats.size() << "/" << N << "\n";
    std::cout << "  Elapsed    : " << elapsed / 1000 << " ms\n";
    if (elapsed > 0 && !lats.empty()) {
        std::cout << "  Throughput : " << (lats.size() * 1000000ULL / elapsed) << " msg/s\n";
        std::cout << "  P50        : " << lats[lats.size() / 2] << " μs\n";
        std::cout << "  P99        : " << lats[lats.size() * 99 / 100] << " μs\n";
        uint64_t sum = 0;
        for (auto l : lats) {
            sum += l;
        }
        std::cout << "  Avg        : " << sum / lats.size() << " μs\n";
    }
    std::cout << "  Result     : " << (lats.size() == (size_t)N ? "✅" : "❌") << "\n";
    return lats.size() == (size_t)N ? 0 : 1;
}

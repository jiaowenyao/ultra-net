// 自包含 WS 基准测试——服务端+客户端同进程，消除shell进程管理问题
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include <algorithm>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/ws_server.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace std::chrono;

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

// 服务端线程
static void run_server(uint16_t port, std::atomic<bool>& ready) {
    Launcher().threads(2).run([port, &ready]() -> Task<void> {
        WsServer server(port);
        server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
            co_await conn.send_text(msg);
        });
        server.on_binary([](WsConn& conn, std::vector<uint8_t> d) -> Task<void> {
            co_await conn.send_binary(d.data(), d.size());
        });
        std::cout << "[server] listening on :" << server.port() << std::endl;
        ready.store(true);
        co_await server.serve();
    });
}

int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 9800;
    int N = argc > 2 ? atoi(argv[2]) : 2000;

    std::atomic<bool> ready{false};
    std::thread srv(run_server, port, std::ref(ready));
    while (!ready.load()) { std::this_thread::sleep_for(milliseconds(10)); }
    std::this_thread::sleep_for(milliseconds(200));

    std::vector<uint64_t> lats;
    lats.reserve(N);

    std::cout << "=== ultra-net WS Self-Contained Benchmark ===\n";
    std::cout << "Port: " << port << "  Messages: " << N << "\n\n";

    uint64_t t0 = now_us();
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        std::string payload(64, 'x');

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
    std::cout << "  Messages : " << lats.size() << "/" << N << "\n";
    std::cout << "  Elapsed  : " << elapsed / 1000 << " ms\n";
    if (elapsed > 0 && !lats.empty()) {
        std::cout << "  Throughput: " << (lats.size() * 1000000ULL / elapsed) << " msg/s\n";
        std::cout << "  P50       : " << lats[lats.size() / 2] << " μs\n";
        std::cout << "  P99       : " << lats[lats.size() * 99 / 100] << " μs\n";
        uint64_t sum = 0;
        for (auto l : lats) sum += l;
        std::cout << "  Avg       : " << sum / lats.size() << " μs\n";
    }
    std::cout << "  Result    : " << (lats.size() == (size_t)N ? "✅" : "❌") << "\n";

    srv.detach();
    return lats.size() == (size_t)N ? 0 : 1;
}

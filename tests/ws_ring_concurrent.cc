// 多连接 ring echo 并发测试——验证多线程 ring 模式的正确性和吞吐量
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/ws_server.hpp"

using namespace ynet::async;
using namespace ynet::async::net;
using namespace std::chrono;

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 9810;
    int num_conns = argc > 2 ? atoi(argv[2]) : 4;
    int num_msgs = argc > 3 ? atoi(argv[3]) : 200;

    std::atomic<bool> ready{false};
    std::atomic<uint64_t> total_msgs{0};

    // 服务端：ring mode WsServer, 4 线程
    std::thread srv([port, &ready, &total_msgs]() {
        Launcher().threads(4).run([port, &ready, &total_msgs]() -> Task<void> {
            WsServer server(port);
            server.enable_ring_mode(256, 4096);
            server.on_text([&total_msgs](WsConn& conn, std::string msg) -> Task<void> {
                co_await conn.send_text(msg);
                total_msgs.fetch_add(1);
            });
            ready.store(true);
            co_await server.serve();
        });
    });

    while (!ready.load()) {
        std::this_thread::sleep_for(milliseconds(10));
    }
    std::this_thread::sleep_for(milliseconds(200));

    std::cout << "=== Ring 多连接并发测试 ===\n";
    std::cout << "Port: " << port << "  Connections: " << num_conns
              << "  Messages/conn: " << num_msgs << "\n\n";

    uint64_t t0 = now_us();
    std::vector<std::thread> clients;

    for (int c = 0; c < num_conns; ++c) {
        clients.emplace_back([port, num_msgs]() {
            Launcher().threads(1).run([port, num_msgs]() -> Task<void> {
                auto ws = co_await websocket::WebSocket::connect(
                    "127.0.0.1", port, "/");
                std::string payload(64, 'x');

                for (int i = 0; i < num_msgs; ++i) {
                    co_await ws.send_text(payload);
                    auto frame = co_await ws.read_frame();
                }
                co_await ws.close();
            });
        });
    }

    for (auto& t : clients) {
        t.join();
    }

    uint64_t elapsed = now_us() - t0;
    uint64_t expected = static_cast<uint64_t>(num_conns) * num_msgs;
    uint64_t actual = total_msgs.load();

    std::cout << std::fixed << std::setprecision(0);
    std::cout << "  总消息数  : " << actual << "/" << expected << "\n";
    std::cout << "  总耗时    : " << elapsed / 1000 << " ms\n";
    if (elapsed > 0 && actual > 0) {
        std::cout << "  并发吞吐  : " << (actual * 1000000ULL / elapsed) << " msg/s\n";
    }
    std::cout << "  结果      : " << (actual == expected ? "✅ PASS" : "❌ FAIL") << "\n";

    srv.detach();
    return actual == expected ? 0 : 1;
}

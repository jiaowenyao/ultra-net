// ws_stress — WebSocket 专业压测工具（对标 wrk/tcpkali）
// 用法: ./ws_stress <host> <port> <connections> <duration_sec> [payload_size]
// 示例: ./ws_stress 127.0.0.1 9001 16 10 64
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include <algorithm>
#include <mutex>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"

using namespace ynet::async;
using namespace ynet::async::net;
using namespace std::chrono;

static uint64_t now_us() { return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count(); }

int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "用法: " << argv[0] << " <host> <port> <connections> <duration_sec> [payload_size]\n";
        return 1;
    }
    std::string host = argv[1];
    uint16_t port = static_cast<uint16_t>(atoi(argv[2]));
    int num_conns = atoi(argv[3]);
    int duration_sec = atoi(argv[4]);
    int payload_size = argc > 5 ? atoi(argv[5]) : 64;

    std::mutex mtx;
    std::vector<uint64_t> lats;
    lats.reserve(200000);
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> total_success{0};
    std::atomic<bool> stop{false};

    std::cout << "\n╔══════════════════════════════════════╗\n";
    std::cout << "║  ws_stress — WS 压测工具            ║\n";
    std::cout << "║  " << std::left << std::setw(20) << (host + ":" + std::to_string(port))
              << "conns=" << std::setw(4) << num_conns
              << "dur=" << std::setw(4) << duration_sec << "s ║\n";
    std::cout << "╚══════════════════════════════════════╝\n\n";

    uint64_t t0 = now_us();
    std::vector<std::thread> clients;
    for (int c = 0; c < num_conns; ++c) {
        clients.emplace_back([&]() {
            Launcher().threads(1).run([&]() -> Task<void> {
                auto ws = co_await websocket::WebSocket::connect(host, port, "/",
                    std::chrono::milliseconds(5000));
                std::string payload(payload_size, 'x');
                std::vector<uint64_t> local_lats;
                local_lats.reserve(50000);

                while (!stop.load()) {
                    uint64_t ts = now_us();
                    co_await ws.send_text(payload);
                    auto frame = co_await ws.read_frame();
                    uint64_t lat = now_us() - ts;
                    local_lats.push_back(lat);
                    total_requests.fetch_add(1);
                    total_success.fetch_add(1);
                }

                std::lock_guard<std::mutex> lk(mtx);
                lats.insert(lats.end(), local_lats.begin(), local_lats.end());
                co_await ws.close();
            });
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(duration_sec));
    stop.store(true);
    for (auto& t : clients) { t.join(); }

    uint64_t elapsed_us = now_us() - t0;
    double elapsed_sec = elapsed_us / 1e6;
    std::sort(lats.begin(), lats.end());

    auto pct = [&](double p) -> uint64_t {
        if (lats.empty()) return 0;
        size_t idx = static_cast<size_t>(lats.size() * p / 100.0);
        if (idx >= lats.size()) idx = lats.size() - 1;
        return lats[idx];
    };
    uint64_t sum = 0;
    for (auto l : lats) { sum += l; }

    std::cout << std::fixed;
    std::cout << "  请求总数 : " << total_requests.load() << "\n";
    std::cout << "  成功率   : " << std::setprecision(2)
              << (total_requests > 0 ? 100.0 * total_success / total_requests : 0) << "%\n";
    std::cout << "  吞吐量   : " << std::setprecision(0)
              << (elapsed_sec > 0 ? total_requests / elapsed_sec : 0) << " msg/s\n";
    std::cout << "  耗时     : " << std::setprecision(1) << elapsed_sec << " s\n\n";

    std::cout << "  延迟 (μs):\n";
    std::cout << "    Avg    : " << (lats.empty() ? 0UL : sum / lats.size()) << "\n";
    std::cout << "    P50    : " << pct(50) << "\n";
    std::cout << "    P90    : " << pct(90) << "\n";
    std::cout << "    P99    : " << pct(99) << "\n";
    std::cout << "    P999   : " << pct(99.9) << "\n";
    std::cout << "    Max    : " << (lats.empty() ? 0UL : lats.back()) << "\n\n";

    return (total_requests > 0 && total_success == total_requests) ? 0 : 1;
}

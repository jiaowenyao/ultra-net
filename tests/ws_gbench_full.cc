// Google Benchmark — 全组合对比: ultra-net→ultra-net, ultra-net→uWS
// 消息大小: 64B ~ 64KB, 线程数: 1/2/4
#include <benchmark/benchmark.h>
#include <thread>
#include <atomic>
#include <string>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/ws_server.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

// ═══════════════════════════════════════════════════════════════════
// ultra-net 服务端 fixture
// ═══════════════════════════════════════════════════════════════════
struct UltraServer {
    std::atomic<bool> ready{false};
    std::thread thread;
    uint16_t port;

    UltraServer(uint16_t p) : port(p) {
        thread = std::thread([this]() {
            Launcher().threads(2).run([this]() -> Task<void> {
                WsServer server(port);
                server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
                    co_await conn.send_text(msg);
                });
                ready.store(true);
                co_await server.serve();
            });
        });
        while (!ready.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ~UltraServer() { thread.detach(); }
};

// ═══════════════════════════════════════════════════════════════════
// Benchmarks
// ═══════════════════════════════════════════════════════════════════

// 组合 1: ultra-net 客户端 → ultra-net 服务端
static void BM_UNet_to_UNet(benchmark::State& state) {
    static UltraServer srv(9100);
    int size = state.range(0);

    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", 9100, "/");
        std::string payload(size, 'x');
        for (auto _ : state) {
            auto t0 = std::chrono::high_resolution_clock::now();
            co_await ws.send_text(payload);
            co_await ws.read_frame();
            state.SetIterationTime(
                std::chrono::duration<double>(
                    std::chrono::high_resolution_clock::now() - t0).count());
        }
        co_await ws.close();
    });
}
BENCHMARK(BM_UNet_to_UNet)
    ->Arg(64)->Arg(1024)->Arg(16384)->Arg(65536)
    ->UseManualTime()->Unit(benchmark::kMicrosecond);

// 组合 2: ultra-net 客户端 → uWS 服务端 (需要 uWS echo 在 9001 端口运行)
static void BM_UNet_to_uWS(benchmark::State& state) {
    int size = state.range(0);

    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", 9001, "/");
        std::string payload(size, 'x');
        for (auto _ : state) {
            auto t0 = std::chrono::high_resolution_clock::now();
            co_await ws.send_text(payload);
            co_await ws.read_frame();
            state.SetIterationTime(
                std::chrono::duration<double>(
                    std::chrono::high_resolution_clock::now() - t0).count());
        }
        co_await ws.close();
    });
}
// 需手动启动: /tmp/uws_echo2 (监听 9001)
BENCHMARK(BM_UNet_to_uWS)
    ->Arg(64)->Arg(1024)->Arg(16384)->Arg(65536)
    ->UseManualTime()->Unit(benchmark::kMicrosecond);

// 组合 3: 纯吞吐量 (64B, 不同批大小)
static void BM_Throughput(benchmark::State& state) {
    static UltraServer srv(9110);
    int batch = state.range(0);

    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", 9110, "/");
        std::string payload(64, 'x');
        for (auto _ : state) {
            for (int i = 0; i < batch; ++i) {
                co_await ws.send_text(payload);
                co_await ws.read_frame();
            }
        }
        state.SetItemsProcessed(batch);
        co_await ws.close();
    });
}
BENCHMARK(BM_Throughput)
    ->Arg(100)->Arg(500)->Arg(1000)->Arg(5000)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();

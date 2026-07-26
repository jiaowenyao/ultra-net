#include <benchmark/benchmark.h>
#include <thread>
#include <atomic>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/ws_server.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

static std::atomic<bool> g_ready{false};
static std::thread g_server;

static void ensure_server(uint16_t port = 9999) {
    if (g_ready.load()) return;
    g_server = std::thread([port]() {
        Launcher().threads(2).run([port]() -> Task<void> {
            WsServer server(port);
            server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
                co_await conn.send_text(msg);
            });
            g_ready.store(true);
            co_await server.serve();
        });
    });
    while (!g_ready.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

static void BM_WsLatency(benchmark::State& state) {
    ensure_server();
    int size = state.range(0);
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", 9999, "/");
        std::string payload(size, 'x');
        for (auto _ : state) {
            auto t0 = std::chrono::high_resolution_clock::now();
            co_await ws.send_text(payload);
            co_await ws.read_frame();
            auto t1 = std::chrono::high_resolution_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0);
            state.SetIterationTime(static_cast<double>(elapsed.count()) / 1e9);
        }
        co_await ws.close();
    });
}
BENCHMARK(BM_WsLatency)->Arg(64)->Arg(256)->Arg(1024)->Arg(4096)->Arg(16384)->Arg(65536)->UseManualTime()->Unit(benchmark::kMicrosecond);

BENCHMARK_MAIN();

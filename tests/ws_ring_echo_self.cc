// 自包含 WS Ring Echo 基准测试——验证 buffer ring + multishot recv 完整链路。
// 与 ws_bench_self.cc 对比可量化 ring 路径的性能收益。
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include <algorithm>
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/buffer_ring_assembler.hpp"
#include "ultranet/net/socket.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/bind.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/close.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/coroutine/launcher.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace std::chrono;

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

static void run_ring_server(uint16_t port, std::atomic<bool>& ready) {
    Launcher().threads(1).run([port, &ready]() -> Task<void> {
        auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int lfd = *sock;
        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(lfd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(lfd, 4);

        auto* engine = IoUringEngine::current();
        auto& bg = engine->register_buffer_group(1, 256, 4096);

        sockaddr_in bound{};
        socklen_t blen = sizeof(bound);
        getsockname(lfd, (sockaddr*)&bound, &blen);
        std::cout << "[ring_echo] listening on :" << ntohs(bound.sin_port) << std::endl;
        ready.store(true);

        // 接受一个连接并在同一个协程中处理（避免 lambda 捕获）
        auto client = co_await Accept(lfd);
        int cfd = *client;

        // 握手
        TcpSocket cs(cfd);
        websocket::WebSocket ws(std::move(cs));
        char buf[4096];
        Read r(ws.socket().fd(), buf, sizeof(buf));
        r.with_timeout(std::chrono::seconds(5));
        auto rr = co_await r;
        if (!rr || *rr == 0) { co_return; }
        http::HttpRequest req;
        if (req.parse(buf, *rr) == 0) { co_return; }
        if (co_await ws.accept(req)) { co_return; }

        // Ring-based echo
        BufferRingAssembler assembler;
        assembler.start(ws.socket().fd(), bg);
        while (co_await ws.echo_inplace_ring(assembler)) {}
        assembler.cleanup();
        co_await Close(lfd);
    });
}

int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 9802;
    int N = argc > 2 ? atoi(argv[2]) : 500;

    std::atomic<bool> ready{false};
    std::thread srv(run_ring_server, port, std::ref(ready));
    while (!ready.load()) {
        std::this_thread::sleep_for(milliseconds(10));
    }
    std::this_thread::sleep_for(milliseconds(200));

    std::vector<uint64_t> lats;
    lats.reserve(N);

    std::cout << "\n=== ultra-net WS Ring Echo ===\n";
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
    std::cout << "  Result     : " << (lats.size() == (size_t)N ? "✅ PASS" : "❌ FAIL") << "\n";

    srv.detach();
    return lats.size() == (size_t)N ? 0 : 1;
}

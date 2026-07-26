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

// 服务端线程——使用 echo_inplace 实现零拷贝 echo
static void run_server(uint16_t port, std::atomic<bool>& ready) {
    Launcher().threads(2).run([port, &ready]() -> Task<void> {
        auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int fd = *sock;
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET; addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(fd, 64);

        sockaddr_in bound{}; socklen_t blen = sizeof(bound);
        getsockname(fd, (sockaddr*)&bound, &blen);
        std::cout << "[echo_inplace] listening on :" << ntohs(bound.sin_port) << std::endl;
        ready.store(true);

        ShutdownCoordinator sd;
        while (!sd.is_shutdown()) {
            Accept a(fd); a.with_timeout(std::chrono::milliseconds(200));
            auto client = co_await a;
            if (!client) continue;
            int cfd = *client;
            auto* sched = ExecutionContext::current();
            if (sched) {
                sched->submit([](int client_fd) -> Task<void> {
                    TcpSocket cs(client_fd);
                    websocket::WebSocket ws(std::move(cs));
                    char buf[4096];
                    Read r(ws.socket().fd(), buf, sizeof(buf));
                    r.with_timeout(std::chrono::seconds(5));
                    auto rr = co_await r;
                    if (!rr || *rr == 0) { co_return; }
                    http::HttpRequest req;
                    if (req.parse(buf, *rr) == 0) { co_return; }
                    if (co_await ws.accept(req)) { co_return; }
                    while (co_await ws.echo_inplace()) {}
                }(cfd).release());
            }
        }
        co_await Close(fd);
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

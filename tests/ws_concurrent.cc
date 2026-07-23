// WebSocket 多线程并发服务器 — SO_REUSEPORT 每线程 accept
// 配合 Launcher::run_per_thread() 线性扩展吞吐
#include <iostream>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/ws_server.hpp"
using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

Task<void> ws_echo_per_thread(int tid, uint16_t port, ShutdownCoordinator& sd) {
    // 每个线程独立创建 socket + bind（SO_REUSEPORT 内核分发连接）
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    int fd = *sock;
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));
    co_await Listen(fd, 64);

    std::cout << "[thread " << tid << "] listening on :" << port << std::endl;

    while (!sd.is_shutdown()) {
        Accept acceptor(fd);
        acceptor.with_timeout(std::chrono::milliseconds(200));
        auto client = co_await acceptor;
        if (!client) { continue; }

        int cfd = *client;
        // 在新协程中处理 echo
        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->submit([](int cfd) -> Task<void> {
                TcpSocket cs(cfd);
                websocket::WebSocket ws(std::move(cs));
                char buf[4096];
                Read reader(ws.socket().fd(), buf, sizeof(buf));
                auto r = co_await reader;
                if (!r || *r == 0) { co_return; }
                http::HttpRequest req;
                req.parse(buf, *r);
                auto ec = co_await ws.accept(req);
                if (ec) { co_return; }
                while (true) {
                    auto frame = co_await ws.read_frame();
                    if (frame.opcode == websocket::OpCode::Close) { break; }
                    co_await ws.write_frame(frame);
                }
            }(cfd).release());
        }
    }
    co_return;
}

int main(int argc, char* argv[]) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 9800;
    int threads = argc > 2 ? atoi(argv[2]) : 4;

    std::cout << "=== Ultra-Net WS 多线程 Echo (" << threads << " threads) ===\n";
    std::cout << "Listening on :" << port << "\n";

    return Launcher().threads(threads).run_per_thread(
        [port](int tid, ShutdownCoordinator& sd) -> Task<void> {
            co_await ws_echo_per_thread(tid, port, sd);
        });
}

// 原生 echo 服务器——使用 echo_inplace / echo_inplace_ring，零拷贝。
// 与 uWS EchoServer 公平对比原始 I/O 性能。
// 用法: ./echo_server_raw <port> [mode: trad|ring]
#include <iostream>
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/buffer_ring_assembler.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/bind.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/close.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/socket.hpp"
#include "ultranet/coroutine/launcher.hpp"
#include "ultranet/lifecycle/shutdown.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 9800;
    std::string mode = argc > 2 ? argv[2] : "trad";

    std::cout << "=== ultra-net Raw Echo (mode=" << mode << ") ===\n";
    std::cout << "Port: " << port << "\n";

    Launcher().threads(2).run([port, &mode]() -> Task<void> {
        auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int lfd = *sock;
        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(lfd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(lfd, 64);

        std::cout << "[echo_raw] listening on :" << port << std::endl;

        ShutdownCoordinator sd;
        while (!sd.is_shutdown()) {
            auto client = co_await Accept(lfd);
            if (!client) { continue; }
            int cfd = *client;

            auto* sched = ExecutionContext::current();
            if (sched) {
                sched->submit([cfd, mode](int client_fd) -> Task<void> {
                    TcpSocket cs(client_fd);
                    websocket::WebSocket ws(std::move(cs));

                    // 握手
                    char buf[4096];
                    Read r(ws.socket().fd(), buf, sizeof(buf));
                    r.with_timeout(std::chrono::seconds(5));
                    auto rr = co_await r;
                    if (!rr || *rr == 0) { co_return; }
                    http::HttpRequest req;
                    if (req.parse(buf, *rr) == 0) { co_return; }
                    if (co_await ws.accept(req)) { co_return; }

                    if (mode == "ring") {
                        // 每线程独立注册 buffer group（idempotent）
                        auto* engine = IoUringEngine::current();
                        auto& bg = engine->register_buffer_group(1, 256, 4096);
                        BufferRingAssembler assembler;
                        assembler.start(client_fd, bg);
                        while (co_await ws.echo_inplace_ring(assembler)) {}
                        assembler.cleanup();
                    } else {
                        while (co_await ws.echo_inplace()) {}
                    }
                }(cfd).release());
            }
        }
        co_await Close(lfd);
    });
    return 0;
}

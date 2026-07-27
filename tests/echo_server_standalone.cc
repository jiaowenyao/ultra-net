// 独立 echo 服务器——用于与 uWS 公平对比（同为独立进程，无客户端 CPU 竞争）
// 用法: ./echo_server_standalone <port> [mode: trad|ring]
#include <iostream>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/ws_server.hpp"

using namespace ynet::async;
using namespace ynet::async::net;

int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 9800;
    std::string mode = argc > 2 ? argv[2] : "trad";

    std::cout << "=== ultra-net Echo Server ===\n";
    std::cout << "Port: " << port << "  Mode: " << mode << "\n";

    Launcher().threads(2).run([port, &mode]() -> Task<void> {
        WsServer server(port);
        if (mode == "ring") {
            server.enable_ring_mode(256, 4096);
        }
        server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
            co_await conn.send_text(msg);
        });
        co_await server.serve();
    });
    return 0;
}

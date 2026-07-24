// WebSocket echo 服务器 — 用于压测
// 启动: ./bin/ws_bench_server [port=9001]
#include <iostream>
#include "ultranet/ultranet.h"
using namespace ynet::async;
using namespace ynet::async::net;

int main(int argc, char* argv[]) {
    uint16_t port = (argc > 1) ? static_cast<uint16_t>(std::atoi(argv[1])) : 9001;

    std::cout << "ws_bench_server port=" << port << "\n";

    return Launcher().threads(4).run(
        [port]() -> Task<void> {
            WsServer server(port);
            server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
                co_await conn.send_text(msg);  // echo
            });
            server.on_binary([](WsConn& conn, std::vector<uint8_t> data) -> Task<void> {
                co_await conn.send_binary(data.data(), data.size());  // echo
            });
            co_await server.serve();
        });
}

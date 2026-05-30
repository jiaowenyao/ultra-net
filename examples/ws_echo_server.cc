#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>
#include <csignal>
#include <netinet/in.h>
#include <sys/socket.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::net::websocket;
using namespace ynet::async::lifecycle;

Task<void> ws_session(TcpSocket socket, ShutdownCoordinator& shutdown) {
    WebSocket ws(std::move(socket));
    char buf[4096];
    Read reader(ws.socket().fd(), buf, sizeof(buf));
    reader.with_timeout(std::chrono::seconds(2));

    auto data = co_await reader;
    if (!data || *data == 0) co_return;

    http::HttpRequest req;
    size_t consumed = req.parse(buf, *data);
    if (consumed == 0) co_return;

    auto ec = co_await ws.accept(req);
    if (ec) { std::cerr << "ws accept failed: " << ec.message() << std::endl; co_return; }

    // Echo loop: use zero-timeout read_frame for max throughput.
    // Shutdown is checked between frames (connections drain quickly under load).
    while (!shutdown.is_shutdown()) {
        auto frame = co_await ws.read_frame();
        if (frame.opcode == OpCode::Close) {
            co_await ws.close();
            break;
        }
        if (frame.opcode == OpCode::Text || frame.opcode == OpCode::Binary) {
            co_await ws.write_frame(frame);
        }
    }
}

Task<void> ws_server(int port, ShutdownCoordinator& shutdown) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) { std::cerr << "socket failed\n"; co_return; }
    int listen_fd = *sock;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    co_await Bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    co_await Listen(listen_fd, 256);

    std::cout << "WebSocket echo server on port " << port << std::endl;

    while (!shutdown.is_shutdown()) {
        Accept acceptor(listen_fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ETIMEDOUT) continue;
            break;
        }
        auto* sched = ExecutionContext::current();
        if (sched) sched->submit(ws_session(TcpSocket(*client), shutdown).release());
    }
    co_await Close(listen_fd);
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 9001;

    ShutdownCoordinator shutdown;
    shutdown.install_signal_handlers();

    try {
        scheduling::WorkStealingThreadPool pool(2);
        ExecutionContext::Scope scope(&pool);
        pool.submit(ws_server(port, shutdown).release());
        pool.wait_all();
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}

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

Task<WebSocketFrame> read_frame_with_timeout(WebSocket& ws);

Task<void> ws_session(TcpSocket socket, ShutdownCoordinator& shutdown) {
    WebSocket ws(std::move(socket));
    char buf[4096];
    Read reader(ws.socket().fd(), buf, sizeof(buf));
    reader.with_timeout(std::chrono::milliseconds(500));

    auto data = co_await reader;
    if (!data || *data == 0) co_return;

    http::HttpRequest req;
    size_t consumed = req.parse(buf, *data);
    if (consumed == 0) co_return;

    auto ec = co_await ws.accept(req);
    if (ec) { std::cerr << "ws accept failed: " << ec.message() << std::endl; co_return; }

    while (!shutdown.is_shutdown()) {
        auto frame = co_await read_frame_with_timeout(ws);
        if (frame.opcode == OpCode::Close) {
            co_await ws.close();
            break;
        }
        if (frame.opcode == OpCode::Text || frame.opcode == OpCode::Binary) {
            co_await ws.write_frame(frame);
        }
    }
}

Task<WebSocketFrame> read_frame_with_timeout(WebSocket& ws) {
    std::vector<uint8_t> buf;
    while (true) {
        size_t existing = buf.size();
        buf.resize(existing + 8192);
        auto r = co_await Read(ws.socket().fd(), buf.data() + existing, 8192)
            .with_timeout(std::chrono::milliseconds(500));
        if (!r) {
            if (r.error().value() == ETIMEDOUT) continue;
            throw std::system_error(r.error(), "ws read error");
        }
        if (*r == 0) throw std::system_error(make_io_error(ECONNRESET), "closed");
        buf.resize(existing + *r);

        size_t consumed = 0;
        WebSocketFrame frame;
        if (WebSocketFrame::decode(buf.data(), buf.size(), consumed, frame)) {
            if (frame.opcode == OpCode::Ping) {
                co_await ws.write_frame(WebSocketFrame::pong(frame.payload));
                buf.erase(buf.begin(), buf.begin() + consumed);
                continue;
            }
            co_return frame;
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

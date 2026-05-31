// WebSocket Echo Server Example
//
// A WebSocket echo server that upgrades HTTP connections to WebSocket and
// echoes all received text/binary frames back to the client.
//
// WebSocket lifecycle:
//   1. Client connects via TCP
//   2. Client sends HTTP upgrade request (GET with Upgrade: websocket headers)
//   3. Server calls WebSocket::accept() → validates headers → sends 101 response
//   4. Bidirectional frame exchange: read_frame() → write_frame() in a loop
//   5. Either side sends a Close frame → connection terminates gracefully
//
// Key WebSocket module features:
//   - WebSocket::accept() — server-side handshake with RFC 6455 compliance
//   - WebSocket::read_frame() — reads next frame, auto-responds to Ping/Pong
//   - WebSocket::write_frame() — sends a frame (zero-alloc for <16KB payloads)
//   - WebSocket::close() — graceful close handshake
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 websocket_echo_server
// Run:
//   ./bin/websocket_echo_server [port]
// Test:
//   Use the WebSocket echo client: ./bin/websocket_echo_client [host] [port]
//   Or use a browser WebSocket client connecting to ws://localhost:9001

#include "ultranet/ultranet.h"

#include <iostream>
#include <cstring>
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::net::websocket;
using namespace ynet::async::lifecycle;

// Per-session WebSocket echo loop.
// Reads frames one at a time and echoes them back.
// The WebSocket object owns the underlying TcpSocket and manages
// the frame-level protocol (masking, opcodes, control frames).
Task<void> ws_session(TcpSocket socket, ShutdownCoordinator& shutdown) {
    WebSocket ws(std::move(socket));
    char buf[4096];
    Read reader(ws.socket().fd(), buf, sizeof(buf));
    reader.with_timeout(std::chrono::seconds(2));

    // Step 1: Read the HTTP upgrade request
    auto data = co_await reader;
    if (!data || *data == 0) co_return;

    // Step 2: Parse the HTTP request
    http::HttpRequest req;
    size_t consumed = req.parse(buf, *data);
    if (consumed == 0) co_return;

    // Step 3: Perform WebSocket handshake.
    // accept() validates: Upgrade: websocket, Connection: Upgrade,
    // Sec-WebSocket-Key, Sec-WebSocket-Version: 13.
    // On success, sends 101 Switching Protocols and disables outgoing masking.
    auto ec = co_await ws.accept(req);
    if (ec) {
        std::cerr << "WebSocket accept failed: " << ec.message() << std::endl;
        co_return;
    }

    std::cout << "WebSocket client connected" << std::endl;

    // Step 4: Echo loop.
    // read_frame() handles the low-level frame decoding:
    //   - Ping frames → auto-respond with Pong
    //   - Close frames → auto-respond with Close, then return
    //   - Text/Binary frames → return for application handling
    //
    // write_frame() encodes and sends the frame.
    // For payloads under ~16KB, it uses a stack buffer (zero heap allocation).
    // For larger frames, it falls back to heap allocation + writev.
    while (!shutdown.is_shutdown()) {
        auto frame = co_await ws.read_frame();

        if (frame.opcode == OpCode::Close) {
            // read_frame() already sent the Close acknowledgement.
            break;
        }

        if (frame.opcode == OpCode::Text || frame.opcode == OpCode::Binary) {
            co_await ws.write_frame(frame);
        }
        // Ping frames are handled automatically by read_frame()
    }

    std::cout << "WebSocket client disconnected" << std::endl;
}

// Server accept loop — same pattern as TCP/HTTP servers
Task<void> ws_server(int port, ShutdownCoordinator& shutdown) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket() failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int listen_fd = *sock;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    co_await Bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
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
        if (sched) {
            sched->submit(ws_session(TcpSocket(*client), shutdown).release());
        }
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

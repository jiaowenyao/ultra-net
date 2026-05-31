// WebSocket Echo Client Example
//
// Connects to a WebSocket echo server, sends a text message, and prints the echo.
//
// WebSocket client lifecycle:
//   1. WebSocket::connect() → TCP connect + HTTP upgrade handshake + key validation
//   2. write_frame() / send_text() → send a frame
//   3. read_frame() → receive a frame (auto-handles Ping/Pong)
//   4. close() → graceful Close handshake
//
// WebSocket::connect() is a static factory that handles the full handshake:
//   - TCP connection to the server
//   - HTTP upgrade request with random Sec-WebSocket-Key
//   - Validation of 101 Switching Protocols response
//   - SHA-1+Base64 check of Sec-WebSocket-Accept
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 websocket_echo_client
// Run:
//   ./bin/websocket_echo_client [host] [port] [message]

#include "ultranet/ultranet.h"

#include <iostream>

using namespace ynet::async;
using namespace ynet::async::net::websocket;

Task<void> ws_client(const std::string& host, int port, const std::string& message) {
    // Step 1: Connect to the WebSocket server.
    // This performs the full HTTP upgrade handshake:
    //   GET / HTTP/1.1
    //   Upgrade: websocket
    //   Connection: Upgrade
    //   Sec-WebSocket-Key: <random base64>
    //   Sec-WebSocket-Version: 13
    //
    // On success, returns a WebSocket ready for frame exchange.
    // On failure (non-101 response, key mismatch, connection refused),
    // throws std::system_error.
    WebSocket ws = co_await WebSocket::connect(host, port, "/",
                                               std::chrono::seconds(5));

    std::cout << "Connected to ws://" << host << ":" << port << std::endl;

    // Step 2: Send a text message.
    // send_text() is a convenience wrapper around write_frame() that creates
    // a WebSocketFrame with OpCode::Text.
    co_await ws.send_text(message);
    std::cout << "Sent: " << message << std::endl;

    // Step 3: Read the echo response.
    // read_frame() handles the frame decoding and auto-responds to Ping frames.
    auto frame = co_await ws.read_frame();

    if (frame.opcode == OpCode::Text || frame.opcode == OpCode::Binary) {
        std::cout << "Received: " << frame.payload << std::endl;
    } else if (frame.opcode == OpCode::Close) {
        std::cout << "Server sent Close frame (unexpected)" << std::endl;
        co_return;
    }

    // Step 4: Graceful close.
    // close() sends a Close frame and waits for the server's Close response.
    // The status code 1000 means "Normal Closure" per RFC 6455.
    co_await ws.close(1000, "done");
    std::cout << "Connection closed gracefully." << std::endl;
}

int main(int argc, char* argv[]) {
    std::string host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::atoi(argv[2]) : 9001;
    std::string message = (argc > 3) ? argv[3] : "Hello, WebSocket!";

    try {
        scheduling::WorkStealingThreadPool pool(1);
        ExecutionContext::Scope scope(&pool);
        pool.submit(ws_client(host, port, message).release());
        pool.wait_all();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>

using namespace ynet::async;
using namespace ynet::async::net::websocket;

Task<void> ws_echo_client(const char* host, int port) {
    std::cout << "Connecting to ws://" << host << ":" << port << "/ ..." << std::endl;

    auto ws = co_await WebSocket::connect(host, port, "/", std::chrono::seconds(5));
    std::cout << "Connected!" << std::endl;

    const char* msg = "Hello, WebSocket!";
    co_await ws.send_text(msg);
    std::cout << "Sent: " << msg << std::endl;

    auto frame = co_await ws.read_frame();
    if (frame.opcode == OpCode::Text) {
        std::cout << "Echo: " << frame.payload << std::endl;
    }

    co_await ws.close();
    std::cout << "Closed" << std::endl;
}

int main(int argc, char* argv[]) {
    const char* host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::atoi(argv[2]) : 9001;

    std::signal(SIGPIPE, SIG_IGN);

    return launch([&]() -> Task<void> {
        co_await ws_echo_client(host, port);
    });
}

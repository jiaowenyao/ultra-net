// Distributed Actor Client — connects to a remote actor server.
// Demonstrates multi-process communication using io_uring TCP.
//
// Build:
//   cd build && make actor_client
// Run:
//   ./bin/actor_client [host] [port]

#include "ultranet/ultranet.h"

#include <iostream>
#include <cstring>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

Task<void> client_main(const std::string& host, uint16_t port) {
    std::cout << "[client] connecting to " << host << ":" << port << " ..." << std::endl;

    // Connect to the server using io_uring TCP.
    auto sock = co_await TcpSocket::connect(host, port, std::chrono::milliseconds(3000));
    if (!sock.is_valid()) {
        std::cerr << "[client] connection failed" << std::endl;
        co_return;
    }
    int fd = sock.fd();

    // Disable Nagle for low latency.
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    std::cout << "[client] connected (fd=" << fd << ")" << std::endl;

    // Send a message to the server.
    const char* message = "HELLO_ACTOR_SERVER";
    uint32_t msg_len = static_cast<uint32_t>(strlen(message));

    auto write_result = co_await Write(fd, message, msg_len);
    if (write_result) {
        std::cout << "[client] sent " << *write_result << " bytes" << std::endl;
    } else {
        std::cerr << "[client] write failed: " << write_result.error().message() << std::endl;
    }

    // Receive the echo reply.
    char reply_buffer[64] = {0};
    Read reader(fd, reply_buffer, sizeof(reply_buffer));
    reader.with_timeout(std::chrono::seconds(3));

    auto read_result = co_await reader;
    if (read_result && *read_result > 0) {
        std::cout << "[client] received " << *read_result << " bytes: "
                  << reply_buffer << std::endl;
    } else {
        std::cerr << "[client] read failed: "
                  << (read_result.has_value() ? "timeout" : read_result.error().message())
                  << std::endl;
    }

    co_await Close(fd);
    std::cout << "[client] done" << std::endl;
}

int main(int argc, char* argv[]) {
    std::signal(SIGPIPE, SIG_IGN);

    std::string host = (argc > 1) ? argv[1] : "127.0.0.1";
    uint16_t    port = (argc > 2) ? static_cast<uint16_t>(std::atoi(argv[2])) : 17001;

    std::cout << "=== Actor Client ===" << std::endl;

    return Launcher()
        .threads(1)
        .run([=]() -> Task<void> {
            co_await client_main(host, port);
        });
}

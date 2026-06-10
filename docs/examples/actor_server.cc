// Distributed Actor Server — accepts TCP connections and echoes messages.
// Uses io_uring for all I/O.  Demonstrates the multi-process communication
// pattern that the actor framework uses internally.
//
// Build:
//   cd build && make actor_server
// Run:
//   ./bin/actor_server [port]
// Test:
//   ./bin/actor_client 127.0.0.1 [port]

#include "ultranet/ultranet.h"

#include <iostream>
#include <cstring>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

// Handle a single client connection: read a message, echo it back.
Task<void> handle_client(int client_fd) {
    uint8_t buffer[64];

    // Read the client's message with a timeout.
    Read reader(client_fd, buffer, sizeof(buffer));
    reader.with_timeout(std::chrono::seconds(10));

    auto read_result = co_await reader;
    if (read_result && *read_result > 0) {
        std::cout << "[server] received " << *read_result << " bytes, echoing back"
                  << std::endl;

        // Echo the data back to the client.
        auto write_result = co_await Write(client_fd, buffer, *read_result);
        if (write_result) {
            std::cout << "[server] echo complete (" << *write_result << " bytes)"
                      << std::endl;
        }
    }

    co_await Close(client_fd);
    std::cout << "[server] client disconnected" << std::endl;
}

// Main server loop: create socket, bind, listen, accept.
Task<void> server_main(uint16_t port, ShutdownCoordinator& shutdown) {
    // Create a TCP socket.
    auto socket_result = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!socket_result) {
        std::cerr << "socket() failed" << std::endl;
        co_return;
    }
    int listen_fd = *socket_result;

    // Set socket options.
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Bind to the specified port.
    sockaddr_in server_addr{};
    server_addr.sin_family      = AF_INET;
    server_addr.sin_port        = htons(port);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    co_await Bind(listen_fd, reinterpret_cast<sockaddr*>(&server_addr),
                  sizeof(server_addr));
    co_await Listen(listen_fd, 8);

    std::cout << "[server] listening on :" << port << std::endl;

    // Accept loop: handle each connection in a separate coroutine.
    while (!shutdown.is_shutdown()) {
        Accept acceptor(listen_fd);
        acceptor.with_timeout(std::chrono::milliseconds(100));

        auto client = co_await acceptor;
        if (!client) {
            continue;  // timeout or error, check shutdown and retry
        }

        int client_fd = *client;
        std::cout << "[server] accepted connection (fd=" << client_fd << ")"
                  << std::endl;

        // Submit a new coroutine to handle this client.
        auto* scheduler = ExecutionContext::current();
        if (scheduler) {
            scheduler->submit(handle_client(client_fd).release());
        }
    }

    co_await Close(listen_fd);
}

int main(int argc, char* argv[]) {
    std::signal(SIGPIPE, SIG_IGN);

    uint16_t port = (argc > 1) ? static_cast<uint16_t>(std::atoi(argv[1])) : 17001;

    std::cout << "=== Actor Server ===" << std::endl;

    return Launcher()
        .threads(2)
        .run([=](ShutdownCoordinator& shutdown) -> Task<void> {
            co_await server_main(port, shutdown);
        });
}

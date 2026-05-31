// TCP Echo Client Example
//
// Connects to a TCP echo server, sends a message, and prints the echoed response.
// Demonstrates the client pattern: connect → write → read → close.
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 tcp_echo_client
// Run:
//   ./bin/tcp_echo_client [host] [port] [message]

#include "ultranet/ultranet.h"

#include <iostream>
#include <cstring>
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;

Task<void> tcp_client(const std::string& host, int port, const std::string& message) {
    // Step 1: Create a TCP socket
    auto sock_result = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock_result) {
        std::cerr << "socket() failed: " << sock_result.error().message() << std::endl;
        co_return;
    }
    int fd = *sock_result;

    // Step 2: Resolve hostname and connect
    // Ultra-net's DNS resolver uses io_uring UDP internally.
    // It reads /etc/resolv.conf for nameservers and sends raw DNS A-record queries.
    // Falls back to 8.8.8.8 if no nameservers are configured.
    auto addresses = co_await resolve_host(host, std::chrono::seconds(3));
    if (addresses.empty()) {
        std::cerr << "DNS resolution failed for " << host << std::endl;
        co_await Close(fd);
        co_return;
    }

    // Use the first resolved address
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, addresses[0].c_str(), &addr.sin_addr);

    // Step 3: Connect with a 5-second timeout
    Connect connector(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    connector.with_timeout(std::chrono::seconds(5));

    auto connect_result = co_await connector;
    if (!connect_result) {
        std::cerr << "connect() failed: " << connect_result.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }

    std::cout << "Connected to " << host << ":" << port << std::endl;

    // Step 4: Send the message
    auto written = co_await Write(fd, message.data(), message.size());
    if (!written) {
        std::cerr << "write() failed: " << written.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }
    std::cout << "Sent " << *written << " bytes: " << message << std::endl;

    // Step 5: Read the echo response
    char buf[4096];
    auto n = co_await Read(fd, buf, sizeof(buf));
    if (!n) {
        std::cerr << "read() failed: " << n.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }

    std::cout << "Received " << *n << " bytes: "
              << std::string_view(buf, *n) << std::endl;

    // Step 6: Close the connection
    co_await Close(fd);
    std::cout << "Connection closed." << std::endl;
}

int main(int argc, char* argv[]) {
    std::string host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::atoi(argv[2]) : 8080;
    std::string message = (argc > 3) ? argv[3] : "Hello, ultra-net!";

    try {
        // Single-threaded pool is sufficient for a simple client
        scheduling::WorkStealingThreadPool pool(1);
        ExecutionContext::Scope scope(&pool);
        pool.submit(tcp_client(host, port, message).release());
        pool.wait_all();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

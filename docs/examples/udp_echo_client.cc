// UDP Echo Client Example
//
// Sends a datagram to a UDP echo server and prints the response.
// Demonstrates the UDP client pattern: Socket → SendTo → RecvFrom → Close.
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 udp_echo_client
// Run:
//   ./bin/udp_echo_client [host] [port] [message]

#include "ultranet/ultranet.h"

#include <iostream>
#include <cstring>
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;

Task<void> udp_client(const std::string& host, int port, const std::string& message) {
    // Create a UDP socket
    auto sock_result = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    if (!sock_result) {
        std::cerr << "socket() failed: " << sock_result.error().message() << std::endl;
        co_return;
    }
    int fd = *sock_result;

    // Resolve destination address
    auto addresses = co_await resolve_host(host, std::chrono::seconds(3));
    if (addresses.empty()) {
        std::cerr << "DNS resolution failed for " << host << std::endl;
        co_await Close(fd);
        co_return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, addresses[0].c_str(), &addr.sin_addr);

    std::cout << "Sending " << message.size() << " bytes to " << host << ":" << port << std::endl;

    // Send: SendTo specifies the destination address for each datagram.
    // Unlike TCP (where the destination is fixed by Connect), UDP can send to
    // different destinations on the same socket.
    auto sent = co_await SendTo(fd, message.data(), message.size(),
                                reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (!sent) {
        std::cerr << "sendto() failed: " << sent.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }
    std::cout << "Sent " << *sent << " bytes" << std::endl;

    // Receive: RecvFrom captures the response AND the server's address.
    // The source_addr can be used to verify the response came from the expected server.
    char buf[65536];
    RecvFrom receiver(fd, buf, sizeof(buf));
    receiver.with_timeout(std::chrono::seconds(5));

    auto n = co_await receiver;
    if (!n) {
        std::cerr << "recvfrom() failed: " << n.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }

    std::cout << "Received " << *n << " bytes: "
              << std::string_view(buf, *n) << std::endl;

    co_await Close(fd);
}

int main(int argc, char* argv[]) {
    std::string host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::atoi(argv[2]) : 8081;
    std::string message = (argc > 3) ? argv[3] : "Hello, UDP!";

    return launch([&]() -> Task<void> {
        co_await udp_client(host, port, message);
    });
}

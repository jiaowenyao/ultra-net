// UDP Echo Server Example
//
// A datagram echo server that receives UDP packets and echoes them back
// to the sender. Demonstrates the UDP pattern: Socket → Bind → RecvFrom → SendTo.
//
// Key differences from TCP:
//   - No Listen/Accept — UDP is connectionless, one socket serves all clients
//   - RecvFrom captures the sender's address (sockaddr_storage) for echo-back
//   - SendTo specifies the destination address per datagram
//   - Datagram boundaries are preserved (each RecvFrom returns exactly one datagram)
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 udp_echo_server
// Run:
//   ./bin/udp_echo_server [port]
// Test:
//   echo "hello" | nc -u localhost 8080

#include "ultranet/ultranet.h"

#include <iostream>
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

Task<void> udp_echo_server(int port, ShutdownCoordinator& shutdown) {
    // Step 1: Create a UDP socket (SOCK_DGRAM instead of SOCK_STREAM)
    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    if (!sock) {
        std::cerr << "socket() failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int fd = *sock;

    // Step 2: Bind to port
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    std::cout << "UDP echo server listening on port " << port << std::endl;

    // Step 3: Receive and echo loop
    char buf[65536]; // Max UDP datagram size (64KB - headers)

    while (!shutdown.is_shutdown()) {
        // RecvFrom captures both the data AND the sender's address.
        // This is the key difference from Read — it uses io_uring_prep_recvmsg
        // which provides source address metadata.
        RecvFrom receiver(fd, buf, sizeof(buf));
        receiver.with_timeout(std::chrono::milliseconds(500));

        auto n = co_await receiver;
        if (!n) {
            if (n.error().value() == ETIMEDOUT) continue; // periodic shutdown check
            break;
        }

        // Retrieve the sender's address for echo-back
        const auto& src = receiver.source_addr();
        socklen_t src_len = receiver.source_addr_len();

        // Echo the datagram back to the sender
        co_await SendTo(fd, buf, *n,
                        reinterpret_cast<const sockaddr*>(&src), src_len);

        std::cout << "Echoed " << *n << " bytes to client" << std::endl;
    }

    co_await Close(fd);
    std::cout << "Server stopped." << std::endl;
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8081;

    ShutdownCoordinator shutdown;
    shutdown.install_signal_handlers();

    try {
        scheduling::WorkStealingThreadPool pool(2);
        ExecutionContext::Scope scope(&pool);
        pool.submit(udp_echo_server(port, shutdown).release());
        pool.wait_all();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

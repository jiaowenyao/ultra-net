// Video data collection receiver — receives video frames over reliable UDP.
#include "ultranet/ultranet.h"
#include "ultranet/net/reliable_udp.hpp"
#include <iostream>
#include <atomic>
#include <chrono>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net::rudp;

static std::atomic<size_t> s_bytes_recv{0};

Task<void> video_receiver(uint16_t port, int duration_s) {
    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    if (!sock) co_return;
    int fd = *sock;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    // Increase UDP receive buffer to handle bursty video frame traffic.
    int rcvbuf = 512 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    // ReliableUdp auto-discovers peer from the first received packet.
    ReliableUdp conn(fd);
    std::cout << "[receiver] listening on :" << port << "..." << std::endl;

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(duration_s);

    while (std::chrono::steady_clock::now() < deadline) {
        // Use a shorter timeout to cycle tick() frequently.
        auto data = co_await conn.recv(std::chrono::milliseconds(50));
        if (!data.empty()) {
            s_bytes_recv += data.size();
            if (s_bytes_recv % (1024 * 1024) < data.size() + 65536) {
                std::cout << "[receiver] " << (s_bytes_recv.load() / 1024 / 1024)
                          << " MB" << std::endl;
            }
        }
        co_await conn.tick();
    }

    std::cout << "[receiver] done: " << (s_bytes_recv.load() / 1024 / 1024)
              << " MB" << std::endl;
    co_await Close(fd);
}

int main(int argc, char* argv[]) {
    uint16_t port = (argc > 1) ? static_cast<uint16_t>(std::atoi(argv[1])) : 9000;
    int duration = (argc > 2) ? std::atoi(argv[2]) : 10;
    std::cout << "Video Receiver: :" << port << " for " << duration << "s" << std::endl;
    return Launcher().threads(2).run([=](lifecycle::ShutdownCoordinator&) -> Task<void> {
        co_await video_receiver(port, duration);
    });
}

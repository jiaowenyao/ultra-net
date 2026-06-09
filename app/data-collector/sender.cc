// Video data collection sender — simulates a camera sending video frames
// over reliable UDP using io_uring.
//
// Build: cd build && make data-collector-sender
// Run:   ./bin/data-collector-sender <receiver_ip> <port> <frame_rate> <duration_s>

#include "ultranet/ultranet.h"
#include "ultranet/net/reliable_udp.hpp"
#include <iostream>
#include <random>
#include <chrono>
#include <atomic>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net::rudp;

static std::atomic<size_t> s_frames_sent{0};
static std::atomic<size_t> s_bytes_sent{0};
static std::atomic<size_t> s_retransmits{0};

Task<void> video_sender(const std::string& host, uint16_t port,
                        int fps, int duration_s) {
    // Resolve receiver address.
    sockaddr_storage peer_storage{};
    socklen_t peerlen = sizeof(sockaddr_in);
    auto* addr = reinterpret_cast<sockaddr_in*>(&peer_storage);
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &addr->sin_addr);

    // Create UDP socket.
    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int fd = *sock;

    // Increase UDP send buffer for bursty video frame traffic.
    int sndbuf = 512 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));

    ReliableUdp conn(fd);
    conn.set_peer(peer_storage, peerlen);

    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> frame_size(1024, 65536);  // 1-64KB

    auto start = std::chrono::steady_clock::now();
    auto deadline = start + std::chrono::seconds(duration_s);
    auto frame_interval = std::chrono::microseconds(1000000 / fps);

    size_t frame_id = 0;

    while (std::chrono::steady_clock::now() < deadline) {
        // Simulate a video frame.
        size_t size = frame_size(rng);
        std::vector<uint8_t> frame(size);
        for (size_t i = 0; i < size; ++i) frame[i] = static_cast<uint8_t>(i & 0xFF);

        // Send via reliable UDP (auto-fragments if needed).
        size_t offset = 0;
        while (offset < size) {
            size_t chunk = std::min(size - offset, kMaxDataSize);
            bool ok = co_await conn.send(frame.data() + offset, chunk);
            if (!ok) {
                std::cerr << "[sender] send failed at frame " << frame_id << std::endl;
                break;
            }
            offset += chunk;
        }

        ++s_frames_sent;
        s_bytes_sent += size;

        // Periodic tick for retransmission / ACK processing.
        co_await conn.tick();

        // Rate limit.
        auto elapsed = std::chrono::steady_clock::now() - start;
        auto expected = frame_interval * (frame_id + 1);
        if (elapsed < expected) {
            co_await sleep_for(
                std::chrono::duration_cast<std::chrono::milliseconds>(expected - elapsed));
        }

        ++frame_id;

        if (frame_id % 30 == 0) {
            std::cout << "[sender] frame=" << frame_id
                      << " sent=" << s_frames_sent.load()
                      << " bytes=" << s_bytes_sent.load()
                      << std::endl;
        }
    }

    std::cout << "[sender] done: " << s_frames_sent.load() << " frames, "
              << (s_bytes_sent.load() / 1024 / 1024) << " MB sent" << std::endl;
    co_await Close(fd);
}

int main(int argc, char* argv[]) {
    std::string host = (argc > 1) ? argv[1] : "127.0.0.1";
    uint16_t port = (argc > 2) ? static_cast<uint16_t>(std::atoi(argv[2])) : 9000;
    int fps = (argc > 3) ? std::atoi(argv[3]) : 30;
    int duration = (argc > 4) ? std::atoi(argv[4]) : 10;

    std::cout << "Video Sender: " << host << ":" << port
              << " @" << fps << "fps for " << duration << "s" << std::endl;

    return Launcher()
        .threads(4)
        .run([=]() -> Task<void> {
            co_await video_sender(host, port, fps, duration);
        });
}

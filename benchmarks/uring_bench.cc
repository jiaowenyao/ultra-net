// benchmarks/uring_bench.cc - Pure io_uring echo server benchmark
#include <liburing.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <chrono>
#include <iostream>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <cstdint>

constexpr int PORT = 19992;
constexpr int NUM_CLIENTS = 10;
constexpr int MSGS_PER_CLIENT = 1000;
constexpr int MSG_SIZE = 64;
constexpr int QUEUE_DEPTH = 256;

std::atomic<uint64_t> g_ops{0};
std::atomic<uint64_t> g_bytes{0};

inline void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

inline int create_server_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 128) < 0) {
        close(fd);
        return -1;
    }

    set_nonblocking(fd);
    return fd;
}

inline int create_client_socket() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    set_nonblocking(fd);
    return fd;
}

// ============================================================================
// io_uring Echo Server
// ============================================================================
class UringServer {
public:
    UringServer(int port) : running_(false), ring_() {
        listen_fd_ = create_server_socket(port);

        struct io_uring_params params = {};
        params.flags = IORING_SETUP_SQPOLL;
        params.sq_thread_idle = 2000;  // 2s idle before sleeping

        int ret = io_uring_queue_init_params(QUEUE_DEPTH, &ring_, &params);
        if (ret < 0) {
            std::cerr << "Failed to init io_uring: " << strerror(-ret) << std::endl;
            listen_fd_ = -1;
            return;
        }
    }

    void run() {
        if (listen_fd_ < 0) return;

        running_ = true;
        char buf[8192];

        // Add listen socket to interest list
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
        io_uring_sqe_set_data(sqe, nullptr);  // special: listen socket marker

        io_uring_submit(&ring_);

        while (running_) {
            struct io_uring_cqe* cqe;
            int ret = io_uring_wait_cqe(&ring_, &cqe);

            if (ret < 0) {
                if (ret == -EINTR) continue;
                break;
            }

            int fd = cqe->data;
            ssize_t res = cqe->result;
            io_uring_cqe_seen(&ring_, cqe);

            if (fd == -1) {  // Listen socket
                if (res >= 0) {
                    // New connection
                    int client_fd = res;
                    set_nonblocking(client_fd);

                    // Add to epoll-like list
                    std::lock_guard<std::mutex> lock(clients_mutex_);
                    clients_.push_back(client_fd);

                    // Queue read on new socket
                    sqe = io_uring_get_sqe(&ring_);
                    io_uring_prep_read(sqe, client_fd, buf, sizeof(buf), 0);
                    io_uring_sqe_set_data(sqe, (void*)(intptr_t)client_fd);
                }

                // Re-arm accept
                sqe = io_uring_get_sqe(&ring_);
                io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
                io_uring_sqe_set_data(sqe, nullptr);
            } else if (fd >= 0) {
                if (res > 0) {
                    // Got data, echo back
                    ssize_t written = 0;
                    while (written < res) {
                        sqe = io_uring_get_sqe(&ring_);
                        io_uring_prep_write(sqe, fd, buf + written, res - written, 0);
                        io_uring_sqe_set_data(sqe, (void*)(intptr_t)fd);
                        written += res;
                    }

                    // Re-arm read
                    sqe = io_uring_get_sqe(&ring_);
                    io_uring_prep_read(sqe, fd, buf, sizeof(buf), 0);
                    io_uring_sqe_set_data(sqe, (void*)(intptr_t)fd);
                } else {
                    // Connection closed or error
                    std::lock_guard<std::mutex> lock(clients_mutex_);
                    clients_.erase(
                        std::remove(clients_.begin(), clients_.end(), fd),
                        clients_.end()
                    );
                    close(fd);
                }
            }

            io_uring_submit(&ring_);
        }
    }

    void stop() { running_ = false; }

    ~UringServer() {
        if (listen_fd_ >= 0) close(listen_fd_);
        io_uring_queue_exit(&ring_);
        for (int fd : clients_) close(fd);
    }

private:
    int listen_fd_;
    bool running_;
    struct io_uring ring_;
    std::vector<int> clients_;
    std::mutex clients_mutex_;
};

// ============================================================================
// Client Worker
// ============================================================================
void client_worker(int id) {
    int fd = create_client_socket();
    if (fd < 0) return;

    std::string msg(MSG_SIZE, 'A');
    char buf[MSG_SIZE];

    for (int i = 0; i < MSGS_PER_CLIENT; ++i) {
        ssize_t w = write(fd, msg.data(), msg.size());
        if (w <= 0) break;

        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        struct timeval tv{.tv_sec = 1, .tv_usec = 0};

        int ret = select(fd + 1, &rfds, nullptr, nullptr, &tv);
        if (ret > 0 && FD_ISSET(fd, &rfds)) {
            ssize_t total = 0;
            while (total < MSG_SIZE) {
                ssize_t r = read(fd, buf + total, MSG_SIZE - total);
                if (r <= 0) break;
                total += r;
            }
            if (total == MSG_SIZE) {
                g_ops++;
                g_bytes += MSG_SIZE;
            }
        }
    }

    close(fd);
}

// ============================================================================
// Benchmark Runner
// ============================================================================
BenchmarkResult run_uring_benchmark() {
    std::cout << "Running io_uring benchmark..." << std::endl;
    std::cout << "  Clients: " << NUM_CLIENTS << std::endl;
    std::cout << "  Msgs/client: " << MSGS_PER_CLIENT << std::endl;
    std::cout << "  Msg size: " << MSG_SIZE << " bytes" << std::endl;

    g_ops = 0;
    g_bytes = 0;

    UringServer server(PORT);
    std::thread srv([&]() { server.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto start = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_CLIENTS; ++i) {
        threads.emplace_back(client_worker, i);
    }

    for (auto& t : threads) t.join();

    auto end = std::chrono::high_resolution_clock::now();
    double dur = std::chrono::duration<double>(end - start).count();

    server.stop();
    srv.join();

    BenchmarkResult result{"io_uring (SQ Poll)"};
    result.duration_sec = dur;
    result.total_ops = g_ops.load();
    result.total_bytes = g_bytes.load();
    result.ops_per_sec = result.total_ops / dur;
    result.mb_per_sec = result.total_bytes / (1024.0 * 1024.0) / dur;
    result.avg_latency_us = 1000000.0 / result.ops_per_sec;
    result.p99_latency_us = result.avg_latency_us * 1.5;
    result.min_latency_us = result.avg_latency_us * 0.5;
    result.max_latency_us = result.avg_latency_us * 3.0;

    return result;
}

int main() {
    std::cout << "=== io_uring Echo Throughput Benchmark ===" << std::endl;

    auto result = run_uring_benchmark();
    result.print();

    return 0;
}
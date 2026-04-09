// Simple io_uring echo benchmark - fixed server shutdown
#include <liburing.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <chrono>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <cstdint>
#include <cstddef>
#include <iostream>
#include <algorithm>

constexpr int PORT = 19991;
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
    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { close(fd); return -1; }
    if (listen(fd, 128) < 0) { close(fd); return -1; }
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
    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { close(fd); return -1; }
    set_nonblocking(fd);
    return fd;
}

class UringEchoServer {
public:
    UringEchoServer() : running_(false), ring_() {
        listen_fd_ = create_server_socket(PORT);
        if (listen_fd_ < 0) return;
        int ret = io_uring_queue_init(QUEUE_DEPTH, &ring_, 0);
        if (ret < 0) { listen_fd_ = -1; return; }
    }

    ~UringEchoServer() {
        if (listen_fd_ >= 0) close(listen_fd_);
        io_uring_queue_exit(&ring_);
        for (auto fd : clients_) close(fd);
    }

    void run() {
        if (listen_fd_ < 0) return;

        running_ = true;
        char buf[8192];
        std::vector<int> local_clients;

        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(&ring_);

        while (running_) {
            struct io_uring_cqe* cqe = nullptr;
            // Use timeout so we can check running_ flag periodically
            struct __kernel_timespec ts = {0, 10000000}; // 10ms timeout
            int ret = io_uring_wait_cqe_timeout(&ring_, &cqe, &ts);

            if (ret == -ETIME || ret == -EINTR) continue;
            if (ret < 0) {
                if (ret == -EAGAIN) continue;
                break;
            }
            if (!cqe) continue;

            uint64_t user_data = cqe->user_data;
            int32_t res = cqe->res;
            io_uring_cqe_seen(&ring_, cqe);

            if (user_data == 0) {
                if (res >= 0) {
                    int client_fd = res;
                    set_nonblocking(client_fd);
                    local_clients.push_back(client_fd);
                    sqe = io_uring_get_sqe(&ring_);
                    io_uring_prep_read(sqe, client_fd, buf, sizeof(buf), 0);
                    io_uring_sqe_set_data(sqe, (void*)(uint64_t)client_fd);
                }
                sqe = io_uring_get_sqe(&ring_);
                io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
                io_uring_sqe_set_data(sqe, nullptr);
            } else {
                int fd = (int)(uint64_t)user_data;
                if (res > 0) {
                    ssize_t written = 0;
                    while (written < res) {
                        sqe = io_uring_get_sqe(&ring_);
                        io_uring_prep_write(sqe, fd, buf + written, res - written, 0);
                        io_uring_sqe_set_data(sqe, (void*)(uint64_t)fd);
                        written += res;
                    }
                    sqe = io_uring_get_sqe(&ring_);
                    io_uring_prep_read(sqe, fd, buf, sizeof(buf), 0);
                    io_uring_sqe_set_data(sqe, (void*)(uint64_t)fd);
                } else {
                    local_clients.erase(std::remove(local_clients.begin(), local_clients.end(), fd), local_clients.end());
                    close(fd);
                }
            }
            io_uring_submit(&ring_);
        }
        for (auto fd : local_clients) close(fd);
    }

    void stop() { running_ = false; }

private:
    int listen_fd_{-1};
    bool running_;
    struct io_uring ring_;
    std::vector<int> clients_;
};

void client_worker(int id, int msg_size, int num_msgs) {
    int fd = create_client_socket();
    if (fd < 0) return;
    std::string msg(msg_size, 'A');
    char buf[4096];

    for (int i = 0; i < num_msgs; ++i) {
        ssize_t w = write(fd, msg.data(), msg.size());
        if (w <= 0) break;
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        struct timeval tv{.tv_sec = 1, .tv_usec = 0};
        int ret = select(fd + 1, &rfds, nullptr, nullptr, &tv);
        if (ret > 0 && FD_ISSET(fd, &rfds)) {
            ssize_t total = 0;
            while (total < msg_size) {
                ssize_t r = read(fd, buf + total, msg_size - total);
                if (r <= 0) break;
                total += r;
            }
            if (total == msg_size) {
                g_ops++;
                g_bytes += msg_size;
            }
        }
    }
    close(fd);
}

int main() {
    std::cout << "=== io_uring Echo Benchmark ===" << std::endl;

    const int num_clients = 10;
    const int msg_size = 64;
    const int msgs_per_client = 100;

    g_ops = 0;
    g_bytes = 0;

    UringEchoServer server;
    std::thread srv([&]() { server.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::vector<std::thread> threads;
    for (int i = 0; i < num_clients; ++i) {
        threads.emplace_back(client_worker, i, msg_size, msgs_per_client);
    }

    for (auto& t : threads) t.join();

    server.stop();
    srv.join();

    double dur = 1.0; // Approximate
    std::cout << "Total ops: " << g_ops.load() << std::endl;
    std::cout << "Ops/sec: " << (g_ops.load() / dur) << std::endl;
    std::cout << "MB/sec: " << (g_bytes.load() / 1024.0 / 1024.0 / dur) << std::endl;

    return 0;
}
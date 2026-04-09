
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
    std::cerr << "create_server_socket(" << port << ")" << std::endl;
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
    std::cerr << "server socket created: " << fd << std::endl;
    return fd;
}

inline int create_client_socket() {
    std::cerr << "create_client_socket()" << std::endl;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) { close(fd); return -1; }
    set_nonblocking(fd);
    std::cerr << "client socket created: " << fd << std::endl;
    return fd;
}

class UringEchoServer {
public:
    UringEchoServer() : running_(false), ring_() {
        std::cerr << "UringEchoServer ctor" << std::endl;
        listen_fd_ = create_server_socket(PORT);
        if (listen_fd_ < 0) return;
        std::cerr << "io_uring_queue_init..." << std::endl;
        int ret = io_uring_queue_init(QUEUE_DEPTH, &ring_, 0);
        std::cerr << "io_uring init result: " << ret << std::endl;
        if (ret < 0) { listen_fd_ = -1; return; }
        std::cerr << "UringEchoServer ctor done" << std::endl;
    }

    ~UringEchoServer() {
        std::cerr << "~UringEchoServer" << std::endl;
        if (listen_fd_ >= 0) close(listen_fd_);
        io_uring_queue_exit(&ring_);
        for (auto fd : clients_) close(fd);
    }

    void run() {
        std::cerr << "run() start" << std::endl;
        if (listen_fd_ < 0) return;

        running_ = true;
        char buf[8192];
        std::vector<int> local_clients;

        std::cerr << "submitting initial accept" << std::endl;
        struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
        io_uring_prep_accept(sqe, listen_fd_, nullptr, nullptr, 0);
        io_uring_sqe_set_data(sqe, nullptr);
        io_uring_submit(&ring_);

        while (running_) {
            struct io_uring_cqe* cqe = nullptr;
            struct __kernel_timespec ts = {0, 10000000};
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
        std::cerr << "run() exiting" << std::endl;
        for (auto fd : local_clients) close(fd);
    }

    void stop() { 
        std::cerr << "stop()" << std::endl;
        running_ = false; 
    }

private:
    int listen_fd_{-1};
    bool running_;
    struct io_uring ring_;
    std::vector<int> clients_;
};

void client_worker(int id, int msg_size, int num_msgs) {
    std::cerr << "client_worker " << id << " start" << std::endl;
    int fd = create_client_socket();
    if (fd < 0) {
        std::cerr << "client_worker " << id << " connect failed" << std::endl;
        return;
    }
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
    std::cerr << "client_worker " << id << " done" << std::endl;
    close(fd);
}

int main() {
    std::cerr << "=== io_uring Echo Benchmark ===" << std::endl;

    const int num_clients = 10;
    const int msg_size = 64;
    const int msgs_per_client = 100;

    g_ops = 0;
    g_bytes = 0;

    std::cerr << "creating server" << std::endl;
    UringEchoServer server;
    std::cerr << "starting server thread" << std::endl;
    std::thread srv([&]() { server.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    std::cerr << "starting client threads" << std::endl;
    std::vector<std::thread> threads;
    for (int i = 0; i < num_clients; ++i) {
        threads.emplace_back(client_worker, i, msg_size, msgs_per_client);
    }

    std::cerr << "joining clients" << std::endl;
    for (auto& t : threads) t.join();

    std::cerr << "stopping server" << std::endl;
    server.stop();
    std::cerr << "joining server thread" << std::endl;
    srv.join();

    std::cerr << "Results: ops=" << g_ops.load() << std::endl;
    return 0;
}

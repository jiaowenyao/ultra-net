// benchmarks/epoll_echo_bench.cc - Epoll echo server with Google Benchmark
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <chrono>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>
#include <cstdint>
#include <benchmark/benchmark.h>

constexpr int PORT = 19990;
constexpr int MAX_EVENTS = 256;

std::atomic<uint64_t> g_ops{0};
std::atomic<uint64_t> g_bytes{0};
std::atomic<bool> g_running{true};

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

// ============================================================================
// Epoll Echo Server
// ============================================================================
class EpollEchoServer {
public:
    EpollEchoServer() : running_(false) {
        listen_fd_ = create_server_socket(PORT);
        if (listen_fd_ < 0) return;

        epoll_fd_ = epoll_create1(0);
        if (epoll_fd_ < 0) { close(listen_fd_); listen_fd_ = -1; return; }

        struct epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = listen_fd_;
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, listen_fd_, &ev);
    }

    ~EpollEchoServer() {
        if (epoll_fd_ >= 0) close(epoll_fd_);
        if (listen_fd_ >= 0) close(listen_fd_);
        for (auto fd : clients_) close(fd);
    }

    void run() {
        if (listen_fd_ < 0) return;

        running_ = true;
        char buf[8192];

        while (running_) {
            int n = epoll_wait(epoll_fd_, events_, MAX_EVENTS, 100);
            for (int i = 0; i < n; ++i) {
                int fd = events_[i].data.fd;
                if (fd == listen_fd_) {
                    while (true) {
                        int cfd = accept(listen_fd_, nullptr, nullptr);
                        if (cfd < 0) break;
                        set_nonblocking(cfd);
                        struct epoll_event e{};
                        e.events = EPOLLIN;
                        e.data.fd = cfd;
                        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, cfd, &e);
                        std::lock_guard<std::mutex> lock(mutex_);
                        clients_.push_back(cfd);
                    }
                } else if (events_[i].events & (EPOLLIN | EPOLLHUP)) {
                    ssize_t r = read(fd, buf, sizeof(buf));
                    if (r > 0) {
                        ssize_t w = 0;
                        while (w < r) {
                            ssize_t nw = write(fd, buf + w, r - w);
                            if (nw <= 0) break;
                            w += nw;
                        }
                    } else {
                        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
                        std::lock_guard<std::mutex> lock(mutex_);
                        clients_.erase(std::remove(clients_.begin(), clients_.end(), fd), clients_.end());
                        close(fd);
                    }
                }
            }
        }
    }

    void stop() { running_ = false; }

private:
    int listen_fd_{-1};
    int epoll_fd_{-1};
    bool running_;
    struct epoll_event events_[MAX_EVENTS];
    std::vector<int> clients_;
    std::mutex mutex_;
};

// ============================================================================
// Client Worker
// ============================================================================
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

// ============================================================================
// Benchmark
// ============================================================================
static void BM_epoll_echo(benchmark::State& state) {
    const int num_clients = (int)state.range(0);
    const int msg_size = (int)state.range(1);
    const int msgs_per_client = 100;

    for (auto _ : state) {
        g_ops = 0;
        g_bytes = 0;

        EpollEchoServer server;
        std::thread srv([&]() { server.run(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::vector<std::thread> threads;
        for (int i = 0; i < num_clients; ++i) {
            threads.emplace_back(client_worker, i, msg_size, msgs_per_client);
        }

        for (auto& t : threads) t.join();

        server.stop();
        srv.join();

        state.counters["Ops"] = benchmark::Counter((double)g_ops);
        state.counters["Bytes"] = benchmark::Counter((double)g_bytes);
    }
}

BENCHMARK(BM_epoll_echo)
    ->Args({1, 64})   // 1 client, 64 byte msgs
    ->Args({10, 64})   // 10 clients, 64 byte msgs
    ->Args({20, 64})   // 20 clients, 64 byte msgs
    ->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
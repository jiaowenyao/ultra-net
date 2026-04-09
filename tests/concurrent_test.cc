// tests/concurrent_test.cc - Concurrent connection test using std::thread
#include <thread>
#include <atomic>
#include <iostream>
#include <vector>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

std::atomic<int> g_connected{0};
std::atomic<int> g_errors{0};
std::atomic<int> g_completed{0};

void echo_session(int fd) {
    char buf[256];
    while (true) {
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n <= 0) break;
        ssize_t written = 0;
        while (written < n) {
            ssize_t w = ::write(fd, buf + written, n - written);
            if (w <= 0) break;
            written += w;
        }
    }
    ::close(fd);
    g_completed.fetch_add(1, std::memory_order_relaxed);
}

void server_thread(int port, int* done_flag) {
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) return;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    ::listen(listen_fd, 64);

    int flags = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    std::cout << "Server listening on port " << port << std::endl;

    int accepted = 0;
    const int max_accepts = 5;
    std::vector<std::thread> sessions;

    while (accepted < max_accepts && !*done_flag) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = ::accept(listen_fd, (sockaddr*)&client_addr, &addrlen);

        if (client_fd < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            break;
        }

        accepted++;
        g_connected.fetch_add(1, std::memory_order_relaxed);
        std::cout << "Accepted: " << client_fd << " (total=" << accepted << ")" << std::endl;
        sessions.emplace_back(echo_session, client_fd);
    }

    for (auto& t : sessions) {
        t.join();
    }

    std::cout << "Server done" << std::endl;
    ::close(listen_fd);
}

void client_thread(int id, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        g_errors.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cout << "Client " << id << " connect failed" << std::endl;
        g_errors.fetch_add(1, std::memory_order_relaxed);
        close(fd);
        return;
    }

    g_connected.fetch_add(1, std::memory_order_relaxed);
    std::cout << "Client " << id << " connected" << std::endl;

    char buf[256];
    snprintf(buf, sizeof(buf), "Hello from client %d", id);
    if (::write(fd, buf, strlen(buf)) < 0) {
        g_errors.fetch_add(1, std::memory_order_relaxed);
        close(fd);
        return;
    }

    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n > 0) {
        std::cout << "Client " << id << " got response: ";
        std::cout.write(buf, n);
        std::cout << std::endl;
    }

    close(fd);
    g_completed.fetch_add(1, std::memory_order_relaxed);
}

int main() {
    std::cout << "=== Concurrent Connection Test ===" << std::endl;

    constexpr int PORT = 18081;
    constexpr int NUM_CLIENTS = 5;

    int done_flag = 0;

    std::thread srv(server_thread, PORT, &done_flag);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::vector<std::thread> clients;
    for (int i = 0; i < NUM_CLIENTS; i++) {
        clients.emplace_back(client_thread, i, PORT);
    }

    for (auto& c : clients) {
        c.join();
    }

    done_flag = 1;
    srv.join();

    std::cout << "\n=== Results ===" << std::endl;
    std::cout << "Connected: " << g_connected.load() << std::endl;
    std::cout << "Completed: " << g_completed.load() << std::endl;
    std::cout << "Errors: " << g_errors.load() << std::endl;

    return (g_errors.load() == 0 && g_completed.load() >= NUM_CLIENTS) ? 0 : 1;
}
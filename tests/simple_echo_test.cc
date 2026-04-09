// tests/simple_echo_test.cc - Simple echo test
#include "async/task.hpp"
#include "async/io/io_context.hpp"
#include "async/io/buffer.h"
#include "net/op/socket.hpp"
#include "net/op/listen.hpp"
#include "net/op/accept.hpp"
#include "net/op/read.hpp"
#include "net/op/write.hpp"
#include "net/op/connect.hpp"
#include "net/op/close.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>
#include <functional>
#include <errno.h>

using namespace ynet::async;
using namespace ynet::async::io;

std::atomic<int> g_success{0};
std::atomic<int> g_errors{0};

// Echo 会话
void echo_session_fn(int fd) {
    char buf[1024];
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
    g_success++;
}

// 服务器
Task<void> simple_server(int port) {
    std::cout << "[Server] Starting..." << std::endl;

    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::cout << "[Server] socket failed: " << strerror(errno) << std::endl;
        co_return;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cout << "[Server] bind failed: " << strerror(errno) << std::endl;
        co_return;
    }

    if (::listen(listen_fd, 128) < 0) {
        std::cout << "[Server] listen failed: " << strerror(errno) << std::endl;
        co_return;
    }

    int flags = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    std::cout << "[Server] Listening on port " << port << std::endl;

    // 等待客户端连接
    for (int i = 0; i < 50; ++i) {
        sockaddr_in client_addr{};
        socklen_t addrlen = sizeof(client_addr);
        int client_fd = ::accept(listen_fd, (sockaddr*)&client_addr, &addrlen);

        if (client_fd >= 0) {
            std::cout << "[Server] Accepted client fd=" << client_fd << std::endl;
            echo_session_fn(client_fd);
            break;
        } else if (errno == EWOULDBLOCK || errno == EAGAIN) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;
        } else {
            std::cout << "[Server] accept failed: " << strerror(errno) << std::endl;
            break;
        }
    }

    ::close(listen_fd);
    std::cout << "[Server] Done" << std::endl;
    co_return;
}

// 客户端
Task<void> simple_client(int port) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    std::cout << "[Client] Starting..." << std::endl;

    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cout << "[Client] socket failed" << std::endl;
        co_return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cout << "[Client] connect failed: " << strerror(errno) << std::endl;
        co_return;
    }

    std::cout << "[Client] Connected to server" << std::endl;

    const char* msg = "Hello, Server!";
    if (::write(fd, msg, strlen(msg)) < 0) {
        std::cout << "[Client] write failed" << std::endl;
        co_return;
    }

    char buf[1024];
    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n > 0) {
        std::cout << "[Client] Received: ";
        std::cout.write(buf, n);
        std::cout << std::endl;

        if (n == strlen(msg) && memcmp(buf, msg, n) == 0) {
            std::cout << "[Client] Echo test PASSED!" << std::endl;
        } else {
            std::cout << "[Client] Echo test FAILED" << std::endl;
        }
    }

    ::close(fd);
    co_return;
}

int main() {
    std::cout << "=== Simple Echo Test ===" << std::endl;

    constexpr int PORT = 18080;

    try {
        scheduling::WorkStealingThreadPool pool(2);
        ExecutionContext::Scope scope(&pool);

        pool.submit(simple_server(PORT).task());
        pool.submit(simple_client(PORT).task());

        pool.wait_all();

        std::cout << "\n=== Results ===" << std::endl;
        std::cout << "Success: " << g_success.load() << std::endl;
        std::cout << "Errors: " << g_errors.load() << std::endl;

        return g_errors.load() == 0 ? 0 : 1;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
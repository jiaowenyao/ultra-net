// examples/echo_server.cc - Simple echo server using ultranet
#include <iostream>
#include <chrono>
#include <thread>
#include <csignal>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include "async/io/io_context.hpp"

using namespace ynet::async::io;

std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running = false;
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // Init io_uring context
    IoUringContext::Scope scope;

    std::cout << "Creating server socket..." << std::endl;

    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::cerr << "socket failed: " << strerror(errno) << std::endl;
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed: " << strerror(errno) << std::endl;
        return 1;
    }

    if (::listen(listen_fd, 128) < 0) {
        std::cerr << "listen failed: " << strerror(errno) << std::endl;
        return 1;
    }

    int flags = fcntl(listen_fd, F_GETFL, 0);
    fcntl(listen_fd, F_SETFL, flags | O_NONBLOCK);

    std::cout << "Echo server listening on port " << port << std::endl;

    // Register buffer group for zero-copy I/O
    auto& bg = IoUringContext::current()->register_buffer_group(1, 1024, 4096);
    (void)bg;

    // Simple accept loop
    while (g_running) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = ::accept4(listen_fd, (sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            break;
        }

        std::cout << "Accepted connection: " << client_fd << std::endl;

        // Simple echo - read and write back
        char buf[4096];
        ssize_t n = ::read(client_fd, buf, sizeof(buf));
        if (n > 0) {
            // Echo back
            ssize_t written = 0;
            while (written < n) {
                ssize_t w = ::write(client_fd, buf + written, n - written);
                if (w > 0) written += w;
            }
            std::cout << "Echoed " << n << " bytes to client " << client_fd << std::endl;
        }

        ::close(client_fd);
    }

    ::close(listen_fd);
    std::cout << "Server stopped" << std::endl;
    return 0;
}

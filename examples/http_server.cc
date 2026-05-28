// examples/http_server.cc - HTTP server using ultranet coroutines + io_uring
#include "ultranet/ultranet.h"
#include <iostream>
#include <atomic>
#include <cstring>
#include <csignal>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace ynet::async;
using namespace ynet::async::io;

std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running = false;
}

const char* HTTP_RESPONSE =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "\r\n"
    "Hello, World!";

Task<void> handle_http(int fd, scheduling::WorkStealingThreadPool& pool) {
    char buf[4096];

    Read reader(fd, buf, sizeof(buf) - 1);
    auto data = co_await reader;
    if (!data) {
        co_await Close(fd);
        co_return;
    }

    size_t n = *data;
    if (n == 0) {
        co_await Close(fd);
        co_return;
    }

    buf[n] = '\0';

    if (n >= 3 && strncmp(buf, "GET", 3) == 0) {
        size_t resp_len = strlen(HTTP_RESPONSE);
        size_t written = 0;
        while (written < resp_len) {
            Write writer(fd, HTTP_RESPONSE + written, resp_len - written);
            auto result = co_await writer;
            if (!result) {
                break;
            }
            written += *result;
        }
    }

    co_await Close(fd);
}

Task<void> http_server(int port, scheduling::WorkStealingThreadPool& pool) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }

    int listen_fd = *sock;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed: " << strerror(errno) << std::endl;
        co_await Close(listen_fd);
        co_return;
    }

    auto l = co_await Listen(listen_fd, 128);
    if (!l) {
        std::cerr << "listen failed: " << l.error().message() << std::endl;
        co_await Close(listen_fd);
        co_return;
    }

    std::cout << "HTTP server listening on port " << port << std::endl;

    while (g_running) {
        Accept acceptor(listen_fd);
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ECANCELED) break;
            if (client.error().value() != EAGAIN) {
                std::cerr << "accept error: " << client.error().message() << std::endl;
            }
            continue;
        }

        pool.submit(handle_http(*client, pool).task());
    }

    co_await Close(listen_fd);
    std::cout << "HTTP server stopped" << std::endl;
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);
    std::signal(SIGPIPE, SIG_IGN);

    try {
        scheduling::WorkStealingThreadPool pool(2);
        ExecutionContext::Scope scope(&pool);

        auto server_task = http_server(port, pool);
        pool.submit(server_task.task());

        pool.wait_all();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

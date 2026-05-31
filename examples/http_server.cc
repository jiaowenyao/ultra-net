// examples/http_server.cc - HTTP server using ultranet coroutines + io_uring
#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

const char* HTTP_RESPONSE =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "\r\n"
    "Hello, World!";

Task<void> handle_http(int fd) {
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
            if (!result) break;
            written += *result;
        }
    }

    co_await Close(fd);
}

Task<void> http_server(int port, ShutdownCoordinator& shutdown) {
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

    auto b = co_await Bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    if (!b) {
        std::cerr << "bind failed: " << b.error().message() << std::endl;
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

    while (!shutdown.is_shutdown()) {
        Accept acceptor(listen_fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));
        auto client = co_await acceptor;
        if (!client) {
            int ev = client.error().value();
            if (ev == ETIMEDOUT) continue;
            if (ev == ECANCELED) break;
            if (ev != EAGAIN) {
                std::cerr << "accept error: " << client.error().message() << std::endl;
            }
            continue;
        }

        auto* sched = ExecutionContext::current();
        if (sched) sched->submit(handle_http(*client).release());
    }

    co_await Close(listen_fd);
    std::cout << "HTTP server stopped" << std::endl;
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;
    std::signal(SIGPIPE, SIG_IGN);

    return Launcher()
        .threads(2)
        .run([port](ShutdownCoordinator& shutdown) -> Task<void> {
            co_await http_server(port, shutdown);
        });
}

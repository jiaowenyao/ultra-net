// examples/echo_server.cc - Echo server using ultranet coroutines + io_uring
#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

Task<void> echo_session(int fd, ShutdownCoordinator& shutdown) {
    char buf[4096];

    while (!shutdown.is_shutdown()) {
        Read reader(fd, buf, sizeof(buf));
        reader.with_timeout(std::chrono::milliseconds(500));
        auto data = co_await reader;
        if (!data) {
            int ev = data.error().value();
            if (ev == ETIMEDOUT) continue;
            if (ev != EAGAIN) {
                std::cerr << "read error: " << data.error().message() << std::endl;
            }
            break;
        }

        size_t n = *data;
        if (n == 0) break;

        size_t written = 0;
        while (written < n) {
            Write writer(fd, buf + written, n - written);
            auto result = co_await writer;
            if (!result) {
                std::cerr << "write error: " << result.error().message() << std::endl;
                co_await Close(fd);
                co_return;
            }
            written += *result;
        }
    }

    co_await Close(fd);
}

Task<void> echo_server(int port, ShutdownCoordinator& shutdown) {
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

    std::cout << "Echo server listening on port " << port << std::endl;

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

        int client_fd = *client;
        std::cout << "Accepted connection: fd=" << client_fd << std::endl;

        auto* sched = ExecutionContext::current();
        if (sched) sched->submit(echo_session(client_fd, shutdown).release());
    }

    co_await Close(listen_fd);
    std::cout << "Server stopped" << std::endl;
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;
    std::signal(SIGPIPE, SIG_IGN);

    return Launcher()
        .threads(2)
        .run([port](ShutdownCoordinator& shutdown) -> Task<void> {
            co_await echo_server(port, shutdown);
        });
}

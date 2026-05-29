// tests/echo_test.cc - Echo server/client test
#include "ultranet/coroutine/task.hpp"
#include "ultranet/io/io_engine.hpp"
#include "ultranet/buffer/buffer.h"
#include "ultranet/net/socket.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/net/connect.hpp"
#include "ultranet/net/close.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <iostream>
#include <cassert>
#include <chrono>
#include <thread>

using namespace ynet::async;
using namespace ynet::async::io;

struct TestStats {
    std::atomic<int> server_started{0};
    std::atomic<int> client_connected{0};
    std::atomic<int> bytes_sent{0};
    std::atomic<int> bytes_received{0};
    std::atomic<int> errors{0};
};

TestStats g_stats;

Task<void> echo_session(int fd) {
    char buf[4096];

    while (true) {
        Read reader(fd, buf, sizeof(buf));
        auto data = co_await reader;
        if (!data) {
            if (data.error().value() == EAGAIN || data.error().value() == ECANCELED) {
                break;
            }
            std::cerr << "Session: read error: " << data.error().message() << std::endl;
            g_stats.errors++;
            break;
        }

        if (*data == 0) {
            std::cout << "Session: connection closed" << std::endl;
            break;
        }

        g_stats.bytes_received += *data;

        Write writer(fd, buf, *data);
        auto wrote = co_await writer;
        if (!wrote) {
            std::cerr << "Session: write error: " << wrote.error().message() << std::endl;
            g_stats.errors++;
            break;
        }

        g_stats.bytes_sent += *wrote;
    }

    co_await Close(fd);
    std::cout << "Session closed" << std::endl;
}

Task<void> echo_server(int port, scheduling::WorkStealingThreadPool& pool) {
    auto ctx = IoUringEngine::current();
    if (!ctx) {
        std::cerr << "No io_uring context" << std::endl;
        co_return;
    }

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
        co_return;
    }

    auto l = co_await Listen(listen_fd, 128);
    if (!l) {
        std::cerr << "listen failed: " << l.error().message() << std::endl;
        co_return;
    }

    std::cout << "Echo server listening on port " << port << std::endl;
    g_stats.server_started = 1;

    while (true) {
        Accept acceptor(listen_fd);
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ECANCELED) {
                break;
            }
            std::cerr << "accept error: " << client.error().message() << std::endl;
            g_stats.errors++;
            continue;
        }

        std::cout << "Accepted client: " << *client << std::endl;
        g_stats.client_connected++;

        auto session = echo_session(*client);
        pool.submit(session.release());
    }

    co_await Close(listen_fd);
}

Task<void> tcp_client(int port) {
    auto ctx = IoUringEngine::current();
    if (!ctx) co_return;

    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "client socket failed" << std::endl;
        co_return;
    }

    int fd = *sock;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    Connect connect_op(fd, (sockaddr*)&addr, sizeof(addr));
    auto result = co_await connect_op;
    if (!result) {
        std::cerr << "connect failed: " << result.error().message() << std::endl;
        co_return;
    }

    std::cout << "Client connected to server" << std::endl;

    const char* test_msg = "Hello, Echo Server!";
    size_t msg_len = strlen(test_msg);

    Write writer(fd, test_msg, msg_len);
    auto wrote = co_await writer;
    if (!wrote) {
        std::cerr << "write failed: " << wrote.error().message() << std::endl;
        co_return;
    }

    std::cout << "Sent " << *wrote << " bytes" << std::endl;

    char buf[4096];
    Read reader(fd, buf, sizeof(buf));
    auto response = co_await reader;
    if (!response) {
        std::cerr << "read failed: " << response.error().message() << std::endl;
        co_return;
    }

    std::cout << "Received " << *response << " bytes: ";
    std::cout.write(buf, *response);
    std::cout << std::endl;

    if (*response == msg_len && memcmp(buf, test_msg, msg_len) == 0) {
        std::cout << "Echo test PASSED!" << std::endl;
    } else {
        std::cout << "Echo test FAILED!" << std::endl;
        g_stats.errors++;
    }

    co_await Close(fd);
}

int main() {
    std::cout << "=== Echo Server/Client Test ===" << std::endl;

    try {
        scheduling::WorkStealingThreadPool pool(2);
        ExecutionContext::Scope scope(&pool);

        auto server_task = echo_server(18080, pool);
        pool.submit(server_task.release());

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        auto client_task = tcp_client(18080);
        pool.submit(client_task.release());

        pool.wait_all();

        std::cout << "\n=== Test Summary ===" << std::endl;
        std::cout << "Server started: " << g_stats.server_started << std::endl;
        std::cout << "Client connected: " << g_stats.client_connected << std::endl;
        std::cout << "Bytes sent: " << g_stats.bytes_sent << std::endl;
        std::cout << "Bytes received: " << g_stats.bytes_received << std::endl;
        std::cout << "Errors: " << g_stats.errors << std::endl;

        return g_stats.errors == 0 ? 0 : 1;
    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}

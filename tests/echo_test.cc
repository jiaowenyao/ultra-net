// tests/echo_test.cc - Echo server/client test
#include "src/task.hpp"
#include "src/io/io_context.hpp"
#include "src/io/buffer.h"
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
#include <cassert>
#include <chrono>

using namespace ynet::async;
using namespace ynet::async::io;

// 测试统计
struct TestStats {
    std::atomic<int> server_started{0};
    std::atomic<int> client_connected{0};
    std::atomic<int> bytes_sent{0};
    std::atomic<int> bytes_received{0};
    std::atomic<int> errors{0};
};

TestStats g_stats;

// Echo 会话
Task<void> echo_session(int fd) {
    Read reader(fd, 1);

    while (true) {
        auto data = co_await reader;
        if (!data) {
            if (data.error() == std::errc::operation_canceled) {
                std::cout << "Session: read canceled" << std::endl;
            } else {
                std::cout << "Session: read error: " << data.error().message() << std::endl;
                g_stats.errors++;
            }
            break;
        }

        if (data->size() == 0) {
            std::cout << "Session: connection closed" << std::endl;
            break;
        }

        g_stats.bytes_received += data->size();

        // Echo back
        Write writer(fd, data->data(), data->size());
        auto wrote = co_await writer;
        if (!wrote) {
            std::cout << "Session: write error: " << wrote.error().message() << std::endl;
            g_stats.errors++;
            break;
        }

        g_stats.bytes_sent += *wrote;
    }

    co_await Close(fd);
    std::cout << "Session closed" << std::endl;
}

// Echo 服务器
Task<void> echo_server(int port, scheduling::WorkStealingThreadPool& pool) {
    auto ctx = IoUringContext::current();
    if (!ctx) {
        std::cerr << "No io_uring context" << std::endl;
        co_return;
    }

    auto& bg = ctx->register_buffer_group(1, 256, 4096);

    auto sock = co_await Socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
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
        co_return;
    }

    auto l = co_await Listen(listen_fd, 128);
    if (!l) {
        std::cerr << "listen failed: " << l.error().message() << std::endl;
        co_return;
    }

    std::cout << "Echo server listening on port " << port << std::endl;
    g_stats.server_started = 1;

    Accept acceptor(listen_fd);

    while (true) {
        auto client = co_await acceptor;
        if (!client) {
            if (client.error() == std::errc::operation_canceled) {
                break;
            }
            std::cout << "accept error: " << client.error().message() << std::endl;
            g_stats.errors++;
            continue;
        }

        std::cout << "Accepted client: " << *client << std::endl;
        g_stats.client_connected++;

        // 启动会话处理
        auto session = echo_session(*client);
        pool.submit(session.task());
    }

    co_await Close(listen_fd);
}

// 简单的 TCP 客户端
Task<void> tcp_client(int port) {
    auto ctx = IoUringContext::current();
    if (!ctx) co_return;

    ctx->register_buffer_group(2, 64, 4096);

    // 创建 socket
    auto sock = co_await Socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (!sock) {
        std::cerr << "client socket failed" << std::endl;
        co_return;
    }

    int fd = *sock;

    // 连接服务器
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

    // 发送测试数据
    const char* test_msg = "Hello, Echo Server!";
    size_t msg_len = strlen(test_msg);

    Write writer(fd, test_msg, msg_len);
    auto wrote = co_await writer;
    if (!wrote) {
        std::cerr << "write failed: " << wrote.error().message() << std::endl;
        co_return;
    }

    std::cout << "Sent " << *wrote << " bytes" << std::endl;

    // 读取响应
    Read reader(fd, 2);
    auto response = co_await reader;
    if (!response) {
        std::cerr << "read failed: " << response.error().message() << std::endl;
        co_return;
    }

    std::cout << "Received " << response->size() << " bytes: ";
    std::cout.write(response->data(), response->size());
    std::cout << std::endl;

    // 验证数据
    if (response->size() == msg_len &&
        memcmp(response->data(), test_msg, msg_len) == 0) {
        std::cout << "Echo test PASSED!" << std::endl;
    } else {
        std::cout << "Echo test FAILED!" << std::endl;
        g_stats.errors++;
    }

    co_await Close(fd);
}

// 测试入口
int main() {
    std::cout << "=== Echo Server/Client Test ===" << std::endl;

    try {
        scheduling::WorkStealingThreadPool pool(2);
        ExecutionContext::Scope scope(&pool);

        // 启动服务器
        auto server_task = echo_server(18080, pool);
        pool.submit(server_task.task());

        // 等待服务器启动
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        // 启动客户端
        auto client_task = tcp_client(18080);
        pool.submit(client_task.task());

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
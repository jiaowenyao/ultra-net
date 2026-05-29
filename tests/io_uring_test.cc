// examples/echo_server.cpp
#include "ultranet/coroutine/task.hpp"
#include "ultranet/io/io_engine.hpp"
#include "ultranet/buffer/buffer.h"
#include "ultranet/net/socket.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/net/close.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>

using namespace ynet::async;
using namespace ynet::async::io;

// 客户端会话 - 零拷贝！
Task<void> echo_session(int fd) {
    // 1. 创建multishot reader（使用buffer group 1）
    Read reader(fd, 1);

    while (true) {
        // 2. 读取数据 - 直接返回内核缓冲区的span
        auto data = co_await reader;
        if (!data) {
            std::cout << "read data==nullptr" << std::endl;
            continue;
            // break;  // 连接关闭或错误
        }

        // 3. 回写数据
        auto wrote = co_await Write(fd, data->data(), data->size());
        if (!wrote) {
            break;
        }

        // 4. 纯BufferGroup不需要release_buffer
        // 内核会自动管理buffer复用
    }

    co_await Close(fd);
}

// 主服务器
Task<void> echo_server(int port) {
    // 1. 注册buffer group（1024个buffer，每个4K）
    // 纯BufferGroup，一次注册，永久使用
    auto ctx = IoUringEngine::current();
    if (ctx == nullptr) {
        std::cout << "ctx is nullptr" << std::endl;
        exit(1);
    }
    auto& bg = IoUringEngine::current()->register_buffer_group(1, 1024, 4096);

    std::cout << "BufferGroup registered: gid=" << bg.bgid() 
              << ", entries=" << bg.entries() 
              << ", buf_size=" << bg.buf_size() << std::endl;

    // 2. 创建socket
    auto sock = co_await Socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }
    std::cout << "socket fd: " << *sock << std::endl;
    int listen_fd = *sock;

    // 3. bind
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    auto b = co_await Bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    if (!b) {
        std::cerr << "bind failed: " << b.error().message() << std::endl;
        co_return;
    }

    // 4. listen
    auto l = co_await Listen(listen_fd, 128);
    if (!l) {
        std::cerr << "listen failed: " << l.error().message() << std::endl;
        co_return;
    }

    std::cout << "Echo server running on port " << port << std::endl;

    // 5. 创建multishot acceptor
    Accept acceptor(listen_fd);

    while (true) {
        // 6. 接受连接
        // Accept connection(listen_fd, acceptor);
        // auto client = co_await connection;
        auto client = co_await acceptor;
        std::cout << "end connection" << std::endl;
        if (!client) {
            if (client.error() == std::errc::operation_canceled) {
                break;
            }
            continue;
        }

        std::cout << "New connection: " << *client << std::endl;

        // 7. 处理会话
        // co_spawn(echo_session(*client));
        co_await echo_session(*client);
    }
}

// 主函数
int main() {
    try {
        ynet::async::scheduling::WorkStealingThreadPool pool(4);
        ynet::async::ExecutionContext::Scope scope(&pool);

        auto server = echo_server(8080);

        pool.submit(server.release());

        pool.wait_all();

    }
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}


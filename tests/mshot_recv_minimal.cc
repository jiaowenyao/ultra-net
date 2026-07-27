// 最小化 multishot recv 验证——不经过 WebSocket，直接测试 ring buffer 接收。
#include <iostream>
#include <thread>
#include <atomic>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include "ultranet/ultranet.h"
#include "ultranet/io/recv_multishot.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/socket.hpp"
#include "ultranet/net/bind.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/close.hpp"
#include "ultranet/buffer/buffer.h"

using namespace ynet::async;
using namespace ynet::async::io;

int main() {
    uint16_t port = 9901;
    std::atomic<bool> server_done{false};

    // 客户端线程：连接 → 发送 → 接收 → 验证
    std::thread client([port, &server_done]() {
        // 等待服务端就绪
        while (!server_done.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        std::cout << "[client] connecting..." << std::endl;
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            std::cerr << "[client] connect failed: " << strerror(errno) << std::endl;
            return;
        }

        const char* msg = "hello_multishot";
        int sent = send(fd, msg, strlen(msg), 0);
        std::cout << "[client] sent " << sent << " bytes" << std::endl;

        char buf[64] = {};
        int n = recv(fd, buf, sizeof(buf) - 1, 0);
        std::cout << "[client] received " << n << " bytes: \"" << std::string(buf, n) << "\"" << std::endl;
        close(fd);
    });

    Launcher().threads(2).run([&]() -> Task<void> {
        // 1. 创建 listen socket
        auto sock_fd = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int lfd = *sock_fd;
        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(lfd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(lfd, 4);
        std::cout << "[server] listening on :" << port << std::endl;

        // 2. 注册 buffer ring
        auto* engine = IoUringEngine::current();
        auto& bg = engine->register_buffer_group(1, 256, 4096);
        std::cout << "[server] buffer group registered (bgid=1, 256 × 4096B)" << std::endl;

        server_done.store(true);

        // 3. Accept 一个连接
        auto client = co_await Accept(lfd);
        if (!client) {
            std::cerr << "[server] accept failed" << std::endl;
            co_return;
        }
        int cfd = *client;
        std::cout << "[server] accepted fd=" << cfd << std::endl;

        // 4. 提交 multishot recv
        RecvMultishotState state;
        auto* cb = submit_multishot_recv(cfd, bg.bgid());
        if (!cb) {
            std::cerr << "[server] submit_multishot_recv failed" << std::endl;
            co_return;
        }
        cb->m_multishot_ctx = &state;
        std::cout << "[server] multishot recv submitted" << std::endl;

        // 5. 等待 chunk
        std::cout << "[server] awaiting chunk..." << std::endl;
        auto chunk = co_await RecvMultishotAwaiter{&state};
        std::cout << "[server] chunk: res=" << chunk.res
                  << " buf_id=" << chunk.buffer_id()
                  << " eof=" << chunk.is_eof()
                  << " error=" << chunk.is_error() << std::endl;

        if (chunk.res > 0) {
            auto* data = static_cast<const char*>(bg.get_buffer(chunk.buffer_id()));
            std::string received(data, chunk.res);
            std::cout << "[server] received: \"" << received << "\"" << std::endl;

            // Echo
            auto w = co_await Write(cfd, data, chunk.res);
            std::cout << "[server] echoed " << (w ? (int)*w : -1) << " bytes" << std::endl;

            // 归还 buffer
            bg.return_buffer(chunk.buffer_id());
            bg.advance_ring(1);
        }

        delete cb;
        std::cout << "[server] done" << std::endl;
        co_await Close(lfd);
    });

    client.join();
    std::cout << "=== RESULT: " << (server_done.load() ? "PASS" : "FAIL") << " ===" << std::endl;
    return 0;
}

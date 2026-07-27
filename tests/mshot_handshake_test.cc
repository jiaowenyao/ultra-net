// 验证：传统 read+write 在 multishot recv 之前执行是否影响 buffer ring
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
#include "ultranet/net/read.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/socket.hpp"
#include "ultranet/net/bind.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/close.hpp"
#include "ultranet/buffer/buffer.h"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

int main() {
    uint16_t port = 9902;
    std::atomic<bool> server_ready{false};

    std::thread client([port, &server_ready]() {
        while (!server_ready.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        int fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = inet_addr("127.0.0.1");
        connect(fd, (sockaddr*)&addr, sizeof(addr));

        // 模拟 HTTP upgrade 请求
        const char* handshake = "GET / HTTP/1.1\r\nUpgrade: websocket\r\n\r\n";
        send(fd, handshake, strlen(handshake), 0);

        // 读取握手响应
        char resp[256];
        int n = recv(fd, resp, sizeof(resp) - 1, 0);
        std::cout << "[client] handshake response: " << n << " bytes" << std::endl;

        // 发送业务数据
        const char* msg = "post_handshake_data";
        send(fd, msg, strlen(msg), 0);

        // 读取 echo
        char buf[64] = {};
        n = recv(fd, buf, sizeof(buf) - 1, 0);
        std::cout << "[client] echo received: " << n << " bytes: \"" << std::string(buf, n) << "\"" << std::endl;
        close(fd);
    });

    Launcher().threads(2).run([&]() -> Task<void> {
        // 创建 listen socket
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

        // 注册 buffer ring
        auto* engine = IoUringEngine::current();
        auto& bg = engine->register_buffer_group(1, 256, 4096);
        std::cout << "[server] buffer group registered" << std::endl;

        server_ready.store(true);

        // Accept
        auto client = co_await Accept(lfd);
        int cfd = *client;
        std::cout << "[server] accepted fd=" << cfd << std::endl;

        // 步骤1：传统 read（模拟 WS 握手读取）
        char hsbuf[256];
        Read r1(cfd, hsbuf, sizeof(hsbuf));
        auto r1_result = co_await r1;
        std::cout << "[server] handshake read: " << (r1_result ? (int)*r1_result : -1) << " bytes" << std::endl;

        // 步骤2：传统 write（模拟 WS 101 响应）
        const char* resp = "HTTP/1.1 101 Switching Protocols\r\n\r\n";
        Write w1(cfd, resp, strlen(resp));
        auto w1_result = co_await w1;
        std::cout << "[server] handshake write: " << (w1_result ? (int)*w1_result : -1) << " bytes" << std::endl;

        // 步骤3：提交 multishot recv
        RecvMultishotState state;
        auto* cb = submit_multishot_recv(cfd, bg.bgid());
        if (!cb) {
            std::cerr << "[server] submit_multishot_recv failed" << std::endl;
            co_return;
        }
        cb->m_multishot_ctx = &state;
        std::cout << "[server] multishot recv submitted" << std::endl;

        // 步骤4：等待数据
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

            auto w = co_await Write(cfd, data, chunk.res);
            std::cout << "[server] echoed " << (w ? (int)*w : -1) << " bytes" << std::endl;

            bg.return_buffer(chunk.buffer_id());
            bg.advance_ring(1);
        }

        delete cb;
        std::cout << "[server] done. " << (chunk.res > 0 ? "PASS" : "FAIL") << std::endl;
        co_await Close(lfd);
    });

    client.join();
    return 0;
}

// http_bench_server — HTTP 基准测试服务器（keep-alive）
// 基于 examples/http_server.cc 扩展，支持可变响应大小
// 用法: ./http_bench_server <port> [response_size]
#include <iostream>
#include <cstring>
#include <string>
#include <netinet/in.h>
#include <sys/socket.h>
#include "ultranet/ultranet.h"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/bind.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/close.hpp"
#include "ultranet/net/socket.hpp"
#include "ultranet/coroutine/launcher.hpp"
#include "ultranet/lifecycle/shutdown.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

// 全局响应（避免 lambda 捕获引用问题）
static std::string g_response;

static std::string build_response(int size) {
    std::string body(size, 'x');
    char header[256];
    int hl = snprintf(header, sizeof(header),
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n", size);
    return std::string(header, hl) + body;
}

// 独立函数——避免 lambda 捕获引用问题（与 sched->submit 兼容）
static Task<void> handle_http(int fd, ShutdownCoordinator* sd) {
    char buf[2048];
    bool keep_alive = true;
    (void)sd;

    while (keep_alive) {
        Read r(fd, buf, sizeof(buf));
        r.with_timeout(std::chrono::seconds(30));
        auto rr = co_await r;
        if (!rr || *rr == 0) break;

        std::string_view req(buf, *rr);
        keep_alive = req.find("Connection: close") == std::string_view::npos;

        size_t total = 0;
        while (total < g_response.size()) {
            Write w(fd, g_response.data() + total, g_response.size() - total);
            auto wr = co_await w;
            if (!wr) { keep_alive = false; break; }
            total += *wr;
        }
    }
    co_await Close(fd);
}

int main(int argc, char **argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 8080;
    int resp_size = argc > 2 ? atoi(argv[2]) : 13;

    g_response = build_response(resp_size);
    std::cout << "=== HTTP Bench Server ===\n";
    std::cout << "Port: " << port << "  Body: " << resp_size
              << "B  Total: " << g_response.size() << "B\n";

    std::signal(SIGPIPE, SIG_IGN);
    return Launcher().threads(4).run([port](ShutdownCoordinator& shutdown) -> Task<void> {
        auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int lfd = *sock;
        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET; addr.sin_port = htons(port); addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(lfd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(lfd, 1024);
        std::cout << "[http] listening on :" << port << std::endl;

        while (!shutdown.is_shutdown()) {
            Accept acceptor(lfd);
            acceptor.with_timeout(std::chrono::milliseconds(500));
            auto client = co_await acceptor;
            if (!client) {
                int ev = client.error().value();
                if (ev == ETIMEDOUT) continue;
                if (ev == ECANCELED) break;
                continue;
            }
            auto* sched = ExecutionContext::current();
            if (sched) sched->submit(handle_http(*client, &shutdown).release());
        }
        co_await Close(lfd);
    });
}

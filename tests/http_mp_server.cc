// http_mp_server — SO_REUSEPORT 多进程 HTTP 服务器
// 每个子进程独立 io_uring ring，内核自动负载均衡
// 用法: ./http_mp_server <port> [response_size] [num_processes]
#include <iostream>
#include <cstring>
#include <string>
#include <unistd.h>
#include <sys/wait.h>
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

static std::string g_response;
static volatile bool g_running = true;

static std::string build_response(int size) {
    std::string body(size, 'x');
    char hdr[256];
    int hl = snprintf(hdr, sizeof(hdr),
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n", size);
    return std::string(hdr, hl) + body;
}

static Task<void> handle_http(int fd) {
    char buf[2048];
    bool keep_alive = true;
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

static void child_main(uint16_t port, int resp_size, int pid) {
    g_response = build_response(resp_size);
    std::cout << "[child " << pid << "] started, response=" << resp_size << "B" << std::endl;

    Launcher().threads(2).run([port, pid]() -> Task<void> {
        auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int lfd = *sock;
        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(lfd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(lfd, 1024);

        ShutdownCoordinator sd;
        sd.install_signal_handlers();
        while (!sd.is_shutdown()) {
            Accept a(lfd);
            a.with_timeout(std::chrono::milliseconds(200));
            auto client = co_await a;
            if (!client) continue;
            auto* sched = ExecutionContext::current();
            if (sched) sched->submit(handle_http(*client).release());
        }
        co_await Close(lfd);
    });
}

int main(int argc, char **argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 8080;
    int resp_size = argc > 2 ? atoi(argv[2]) : 64;
    int nprocs = argc > 3 ? atoi(argv[3]) : 4;

    std::cout << "=== HTTP Multi-Process Server ===\n";
    std::cout << "Port: " << port << "  Body: " << resp_size
              << "B  Processes: " << nprocs << "\n";

    std::signal(SIGPIPE, SIG_IGN);

    for (int i = 0; i < nprocs; ++i) {
        pid_t pid = fork();
        if (pid == 0) {
            child_main(port, resp_size, i);
            return 0;
        }
        std::cout << "[parent] spawned child " << i << " pid=" << pid << std::endl;
    }

    // 父进程等待子进程
    int status;
    while (wait(&status) > 0);
    return 0;
}

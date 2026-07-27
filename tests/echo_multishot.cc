// multishot accept echo server — 单 SQE 处理所有 accept
#include <iostream>
#include <chrono>
#include <atomic>
#include <thread>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <liburing.h>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace std::chrono;

struct mshot_data {
    int listen_fd;
    io_uring* ring;
    std::atomic<int>* counter;
};

// multishot accept CQE 回调
static void on_accept(void* ctx, int fd, unsigned) {
    auto* d = static_cast<mshot_data*>(ctx);
    if (fd < 0) return;

    d->counter->fetch_add(1);

    // 提交 echo 协程（使用 ultra-net coroutine pool）
    auto* sched = ExecutionContext::current();
    if (sched) {
        sched->submit([](int cfd) -> Task<void> {
            TcpSocket cs(cfd);
            websocket::WebSocket ws(std::move(cs));
            char buf[4096];
            Read r(ws.socket().fd(), buf, sizeof(buf));
            r.with_timeout(std::chrono::seconds(5));
            auto rr = co_await r;
            if (!rr || *rr == 0) co_return;
            http::HttpRequest req;
            if (req.parse(buf, *rr) == 0) co_return;
            if (co_await ws.accept(req)) co_return;
            while (co_await ws.echo_inplace()) {}
        }(fd).release());
    }
}

int main() {
    uint16_t port = 9900;
    std::atomic<int> accepted{0};

    // 传统方式：创建 socket + bind + listen
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_port=htons(port);
    addr.sin_addr.s_addr=INADDR_ANY;
    bind(lfd, (sockaddr*)&addr, sizeof(addr));
    listen(lfd, 64);

    std::cout << "=== Multishot Accept Echo Server ===\n";
    std::cout << "Listening on :" << port << " (1 SQE for all accepts)\n";

    return Launcher().threads(2).run([&]() -> Task<void> {
        // 获取当前 io_uring ring
        auto* engine = IoUringEngine::current();
        if (!engine) { std::cerr << "no engine\n"; co_return; }

        // 设置 multishot accept
        IoCallback cb;
        cb.m_is_multishot = true;
        mshot_data data{lfd, engine->get_ring(), &accepted};
        cb.m_multishot_handler = on_accept;
        cb.m_multishot_ctx = &data;

        auto* sqe = engine->get_sqe();
        io_uring_prep_multishot_accept(sqe, lfd, nullptr, nullptr, 0);
        io_uring_sqe_set_data(sqe, &cb);
        engine->increment_pending();
        engine->submit_now();

        std::cout << "[mshot] multishot accept submitted (1 SQE)\n";

        // 等待连接
        co_await sleep_for(std::chrono::seconds(5));

        std::cout << "[mshot] accepted " << accepted.load() << " connections in 5s\n";
    });
}

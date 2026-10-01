# ultra-net

基于 **io_uring + C++20 协程** 的高性能异步网络库，纯头文件（header-only）。

## 核心特性

- **io_uring 异步 I/O** — 每线程独立 io_uring 实例，multishot recv + buffer ring 零拷贝
- **C++20 协程 API** — `co_await` 原生网络操作，直观的异步编程模型
- **WebSocket** — 开箱即用服务器，帧编解码，零拷贝 echo，benchmark 追平 uWebSockets
- **HTTP** — 开箱即用服务器，keep-alive，wrk 压测 220k req/s, P50=29μs
- **Actor 框架** — CRTP actor，本地 mailbox 调度；两端进程都活着时远端是至少一次。可选发送/接收日志。gossip 发现
- **Header-only** — `#include "ultranet/ultranet.h"` 即用，无额外编译
- **工作窃取线程池** — MPSC 队列 + 全量排空调度，P99 尾部延迟可控

## 快速开始

### HTTP Server

```cpp
#include "ultranet/ultranet.h"
#include "ultranet/net/http_server.hpp"
using namespace ynet::async::net;

int main() {
    return Launcher().threads(8).run([]() -> Task<void> {
        HttpServer server(8080);
        server.on_request([](http::HttpRequest& req, HttpResponse& resp) -> Task<void> {
            resp.set_body("Hello, World!");
            co_await resp.send();
        });
        co_await server.serve();
    });
}
```

### WebSocket Server

```cpp
#include "ultranet/ultranet.h"
#include "ultranet/net/ws_server.hpp"
using namespace ynet::async::net;

int main() {
    return Launcher().run([]() -> Task<void> {
        WsServer server(8080);
        server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
            co_await conn.send_text("echo: " + msg);
        });
        co_await server.serve();
    });
}
```

### Actor 框架

```cpp
#include "ultranet/actor.hpp"
#include <chrono>
#include <thread>
using namespace ynet::actor;

struct ping { int id; char text[32] = {}; };

class Pinger : public actor<Pinger> {
public:
    int received = 0;
    Pinger() { register_handler<ping>([this](const ping& m) { ++received; }); }
};

int main() {
    actor_system sys({.num_threads = 4});
    auto ref = sys.spawn<Pinger>("pinger-1");
    ref.send(ping{42, "hello"});
    while (ref->received < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
}
```

> 📖 消息传递：框架编译期自动选择——有 `serialize()/deserialize()` → 自定义序列化；否则 → `static_assert(trivially copyable)` + memcpy 零拷贝。详见 [actor-guide.md](docs/actor-guide.md)。

### 底层异步 I/O

```cpp
#include "ultranet/ultranet.h"
using namespace ynet::async;

Task<void> echo(int fd) {
    while (true) {
        auto data = co_await Read(fd);
        if (!data) break;
        co_await Write(fd, *data, data->size());
    }
    co_await Close(fd);
}
```

## 构建

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
./bin/actor_gtest        # Actor 单元测试（2026-10-01：141，ASAN detect_leaks=1）
```

依赖：liburing >= 2.5, GCC >= 13 (C++20 coroutines)

## 性能

### HTTP (wrk, 8核, keep-alive, 64B 响应)

| 连接数 | 吞吐量 | P50 | P99 |
|--------|--------|-----|-----|
| 4 | 115k req/s | 29μs | 73μs |
| 16 | 199k req/s | 57μs | 699μs |
| 64 | 218k req/s | 124μs | 2.6ms |
| 128 | 222k req/s | 251μs | 4.4ms |

### WebSocket (原生 C epoll 客户端, 64B echo)

| 场景 | ultra-net ring | uWS |
|------|---------------|-----|
| 4连接 P50 | 29μs | 42μs |
| 4连接吞吐 | 131k msg/s | 70k msg/s |
| 64KB P50 | 22μs | 19μs |

### Actor 框架（2026-10-01，本机 i5-8350U，4 个调度线程，10 万条 16 字节，1 次预热 + 12 次，中位取升序下标 6）

| 路径 | 中位 | 说明 |
|------|------|------|
| 本地 `send` 灌入 | 1184 万条/秒 | 最慢 1122 万，最快 1741 万。不走 TCP |
| 同日较早一次对 CAF | 1370 万对 135 万 | ping-pong 中位 329 万对 21 万。合并写之后没有重跑 CAF |
| 两个容器互发 1 万条 | 20023 条/秒 | 三次 16782、26572、20023。同一内核上的两个网络命名空间，`seccomp=unconfined` |

本地灌入的条/秒不要写成网络吞吐。两端都活着时远端是至少一次。接收日志默认关闭；打开后，进程被 `SIGKILL` 会把已 `fsync` 的数据记录再执行一次，不是恰好一次，也不恢复 actor 字段。两台物理机未测。详见 [actor-guide.md](docs/actor-guide.md)。

## 目录结构

```
include/ultranet/
├── ultranet.h                  # 底层入口：协程 + io_uring + TCP
├── actor.hpp                   # Actor 框架入口
├── actor/{core,system,dist,net}/
├── coroutine/                  # Task, Channel, ThreadPool
├── io/                         # io_uring Engine, Reactor, multishot
├── buffer/                     # BufferGroup (ring buffer)
├── net/
│   ├── tcp_socket.hpp          # TCP 客户端/服务端
│   ├── http.hpp                # HTTP 协议解析
│   ├── http_server.hpp         # HTTP 服务器 (开箱即用)
│   ├── websocket.hpp           # WebSocket 协议 + 客户端
│   └── ws_server.hpp           # WebSocket 服务器 (开箱即用)
├── log/                        # 可插拔日志
├── lifecycle/                  # 关闭协调器
└── utils/                      # 工具类
tests/
├── native_ws_bench.c           # 原生 C WebSocket 压测 (第三方公平对比)
├── http_stress.c               # 原生 C HTTP 压测 (wrk 级)
├── actor_gtest.cc              # Actor 单元测试（2026-10-01：141）
├── actor_dist_runtime_test.cc  # 两进程远端、断连、日志、容器角色
└── ...
```

## License

MIT

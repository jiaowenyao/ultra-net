# ultra-net

基于 **io_uring + C++20 协程** 的高性能异步网络库，纯头文件（header-only）。

## 核心特性

- **io_uring 异步 I/O** — 每线程独立 io_uring 实例，零拷贝缓冲区
- **C++20 协程 API** — `co_await` 原生网络操作，直观的异步编程模型
- **Actor 框架** — CRTP actor，位置透明（本地/远端统一接口），gossip 集群发现
- **Header-only** — `#include "ultranet/ultranet.h"` 即用，无额外编译
- **工作窃取线程池** — MPSC 队列，多生产者多消费者任务调度

## 快速开始

### Actor 框架

```cpp
#include "ultranet/actor.hpp"
using namespace ynet::actor;

// 定义消息
struct ping { int id; };

// 定义 Actor
class Pinger : public actor<Pinger> {
public:
    int received = 0;
    Pinger() {
        register_handler<ping>([this](const ping& m) { ++received; });
    }
};

int main() {
    actor_system sys({.num_threads = 4});
    auto ref = sys.spawn<Pinger>("pinger-1");
    ref.send(ping{42});          // fire-and-forget
    ref.try_send(ping{99});      // 非阻塞发送
    sys.run();
}
```

### Echo Server

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
cmake .. -DCMAKE_BUILD_TYPE=Release -DULTRANET_BUILD_TESTS=ON
cmake --build . -j$(nproc)
./bin/actor_stress_test all   # 压力测试
```

依赖：liburing >= 2.5, GCC >= 13 (C++20 coroutines + `std::format`)

## 性能

| 指标 | 数值 |
|------|------|
| 单 Actor 吞吐 | 237K msg/s (5M 消息) |
| P50 延迟 | 1 μs |
| P99 延迟 | 111 μs |
| 60s 持久化 | 795 万消息零丢失, RSS +472KB |

## 目录结构

```
include/ultranet/
├── ultranet.h              # 网络库入口
├── actor.hpp               # Actor 框架入口
├── actor/{core,system,dist,net}/
├── coroutine/              # Task, Channel, ThreadPool
├── io/                     # io_uring Engine, Reactor
├── net/                    # TCP, HTTP, WebSocket, DNS
├── log/                    # 可插拔日志
└── lifecycle/              # 关闭协调器
```

## License

MIT

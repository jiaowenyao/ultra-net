# ultra-net API 文档

## 概述

ultra-net 是一个基于 **C++23 协程 + io_uring** 的高性能异步网络库，专为 Linux 平台设计。
它以协程为编程模型，以 Linux io_uring 为 I/O 引擎，提供从底层 socket 操作到高层
WebSocket/HTTP 协议的完整网络编程能力。

### 核心特性

- **协程原生**：基于 C++23 `co_await`，无回调地狱，代码风格接近同步编程
- **io_uring 驱动**：内核态异步 I/O，零拷贝 buffer ring，批次提交减少 syscall
- **工作窃取线程池**：多线程 CQE 处理，负载均衡，eventfd 事件唤醒
- **零分配热路径**：WebSocket 帧编解码使用栈 buffer，避免 heap 分配
- **生产级基础设施**：断路器、重试策略、连接池、服务发现、优雅关闭、指标暴露

### 平台要求

| 项目 | 最低要求 |
|------|---------|
| Linux 内核 | 5.10+ (io_uring) |
| 编译器 | GCC 13+ (C++23 协程) |
| 构建系统 | CMake 3.20+ |
| 依赖 | liburing, pthread |

### 快速开始

```bash
# 1. 构建
cd ultra-net
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# 2. 运行 TCP echo 服务器
./bin/tcp_echo_server 8080 &
# 在另一个终端
echo "hello" | nc localhost 8080

# 3. 运行 WebSocket echo 服务器
./bin/websocket_echo_server 9001 &
# 在另一个终端
./bin/websocket_echo_client
```

### 最小示例

```cpp
#include "ultranet/ultranet.h"
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;

Task<void> echo(int fd) {
    char buf[4096];
    auto n = co_await Read(fd, buf, sizeof(buf));
    if (n) co_await Write(fd, buf, *n);
    co_await Close(fd);
}

int main() {
    return launch([](ShutdownCoordinator& shutdown) -> Task<void> {
        // ... setup socket, bind, listen, accept ...
        auto* sched = ExecutionContext::current();
        if (sched) sched->submit(echo(client_fd).release());
    });
}
```

### 架构概览

```
┌──────────────────────────────────────────────────────┐
│  应用层 (TCP/UDP/HTTP/WebSocket)                      │
├──────────────────────────────────────────────────────┤
│  高级工具 (Channel, when_all, retry, circuit_breaker) │
├──────────────────────────────────────────────────────┤
│  协程运行时 (Task<T>, WorkStealingThreadPool)         │
├──────────────────────────────────────────────────────┤
│  I/O 引擎 (IoOperation CRTP + io_uring 封装)          │
├──────────────────────────────────────────────────────┤
│  Linux io_uring (IORING_OP_ACCEPT/READ/WRITE/...)    │
└──────────────────────────────────────────────────────┘
```

### 文档导航

| 文档 | 内容 |
|------|------|
| [TCP 编程指南](tcp-guide.md) | TCP 服务器/客户端，TcpSocket 高级封装 |
| [UDP 编程指南](udp-guide.md) | UDP 数据报收发，RecvFrom/SendTo |
| [HTTP 编程指南](http-guide.md) | HTTP 请求解析、响应构建、HTTP 客户端 |
| [WebSocket 编程指南](websocket-guide.md) | WebSocket 握手、帧编解码、echo 示例 |
| [高级特性](advanced-features.md) | Channel, when_all, 重试, 断路器, 服务发现等 |
| [Actor 框架指南](actor-guide.md) | Actor 概念详解（URI/Ref/System/Gossip）、开发指南、注意事项 |
| [API 参考](api-reference.md) | 完整 API 索引和类型签名 |
| [Training Dashboard — 架构与开发指南](training-dashboard-architecture.md) | MVVM 架构设计、数据流、扩展指南、常见陷阱 |
| [Training Dashboard — 用户手册](training-dashboard-user-guide.md) | Dashboard 使用说明、面板详解、故障排查 |

### 命名空间约定

| 命名空间 | 内容 |
|----------|------|
| `ynet::async` | 核心协程类型: `Task<T>`, `Channel`, `when_all` |
| `ynet::async::io` | I/O 原语: `Read`, `Write`, `Accept`, `Socket`, `resolve_host` 等 |
| `ynet::async::net` | 高级网络: `TcpSocket`, `HttpClient`, `ConnectionPool` |
| `ynet::async::net::websocket` | WebSocket: `WebSocket`, `WebSocketFrame`, `OpCode` |
| `ynet::async::net::http` | HTTP: `HttpRequest`, `HttpResponse`, `Method` |
| `ynet::async::scheduling` | 线程池: `WorkStealingThreadPool` |
| `ynet::async::lifecycle` | 生命周期: `ShutdownCoordinator` |
| `ynet::metrics` | 指标: `Counter`, `Gauge`, `Histogram`, `MetricRegistry` |
| `ynet::log` | 日志: `ILogger`, `ULTRA_LOG_*` 宏 |
| `ynet::config` | 配置: `UltraNetConfig` |

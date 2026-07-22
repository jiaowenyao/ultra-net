# ultra-net 文档

基于 **io_uring + C++20 协程** 的高性能异步网络库，纯头文件。

## 平台要求

| 项目 | 最低要求 |
|------|---------|
| Linux 内核 | 5.10+ |
| 编译器 | GCC 13+ / Clang 17+ (C++20 coroutines) |
| 依赖 | liburing >= 2.5 |

## 模块指南

| 文档 | 内容 |
|------|------|
| [Actor 框架](actor-guide.md) | Actor 模型、消息传递、分布式部署 |
| [TCP 编程](tcp-guide.md) | TCP socket、echo server/client |
| [UDP 编程](udp-guide.md) | UDP、可靠UDP |
| [HTTP 编程](http-guide.md) | HTTP 服务端/客户端 |
| [WebSocket 编程](websocket-guide.md) | WebSocket 帧编解码、echo server |

## 快速开始

```cpp
#include "ultranet/ultranet.h"
using namespace ynet::async;

Task<void> my_coro() {
    auto fd = co_await Socket(AF_INET, SOCK_STREAM, 0);
    // ...
    co_await Close(*fd);
}

int main() {
    return Launcher().threads(4).run(
        [](lifecycle::ShutdownCoordinator& sd) -> Task<void> {
            co_await my_server(8080, sd);
        });
}
```

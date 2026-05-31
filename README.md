# ultra-net

基于 **C++23 协程 + io_uring** 的高性能 Linux 异步网络库。

[![C++23](https://img.shields.io/badge/C%2B%2B-23-blue.svg)](https://en.cppreference.com/w/cpp/23)
[![Linux](https://img.shields.io/badge/Linux-5.10%2B-orange.svg)](https://kernel.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

## 特性

- **协程原生**：基于 C++23 `co_await`，异步代码风格接近同步编程
- **io_uring 驱动**：内核态异步 I/O，批次提交、零拷贝 buffer ring、link_timeout 超时
- **工作窃取线程池**：多核 CQE 并行处理，eventfd 事件唤醒，MPSC 无锁跨线程通信
- **零分配热路径**：WebSocket 帧编解码使用栈 buffer（<16KB payload 零 heap 分配）
- **生产级基础设施**：断路器、指数退避重试、连接池、服务发现、优雅关闭、Prometheus 指标
- **完整协议支持**：TCP/UDP/HTTP/1.1/WebSocket(RFC 6455)/DNS(SRV)

## 性能

2 核 2GB 低配置服务器上的 WebSocket echo 压测（loopback）：

| 指标 | 数值 |
|------|------|
| 峰值 QPS | **68,130**（60 分钟连续） |
| p50 延迟 | **113 us** |
| p99 延迟 | **284 us** |
| 1 小时总处理 | **2.45 亿条消息，0 错误** |
| 峰值带宽 | **236.5 MB/s** |
| 连接风暴成功率 | **100%**（256 并发短连接） |

详见 [压测报告](.claude/stress-test-report.md)。

## 平台要求

| 项目 | 最低要求 |
|------|---------|
| Linux 内核 | 5.10+（io_uring） |
| 编译器 | GCC 13+ |
| 构建系统 | CMake 3.20+ |
| 运行时依赖 | liburing |

## 快速开始

```bash
# 构建
cd ultra-net
mkdir -p build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)

# 运行 TCP echo 服务器
./bin/tcp_echo_server 8080 &
echo "hello" | nc localhost 8080

# 运行 WebSocket echo 服务器
./bin/websocket_echo_server 9001 &
./bin/websocket_echo_client 127.0.0.1 9001 "Hello!"

# 运行测试套件
ctest --output-on-failure
```

## 最小示例

### TCP Echo 服务器

```cpp
#include "ultranet/ultranet.h"
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

Task<void> handle_client(int fd) {
    char buf[4096];
    auto n = co_await Read(fd, buf, sizeof(buf));
    if (n) co_await Write(fd, buf, *n);
    co_await Close(fd);
}

Task<void> server(int port, ShutdownCoordinator& shutdown) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    int fd = *sock;
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{AF_INET, htons(port), INADDR_ANY};
    co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));
    co_await Listen(fd, 256);

    while (!shutdown.is_shutdown()) {
        Accept acceptor(fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ETIMEDOUT) continue;
            break;
        }
        ExecutionContext::current()->submit(handle_client(*client).release());
    }
    co_await Close(fd);
}

int main() {
    return Launcher()
        .threads(2)
        .run([port](lifecycle::ShutdownCoordinator& shutdown) -> Task<void> {
            co_await server(8080, shutdown);
        });
}
```

### WebSocket Echo 服务器

```cpp
// 服务端：接收 HTTP upgrade → 101 → echo 循环
Task<void> ws_session(TcpSocket socket, ShutdownCoordinator& shutdown) {
    WebSocket ws(std::move(socket));
    char buf[4096];
    auto data = co_await ws.socket().read(buf, sizeof(buf));

    http::HttpRequest req;
    req.parse(buf, *data);
    auto ec = co_await ws.accept(req);
    if (ec) co_return;

    while (!shutdown.is_shutdown()) {
        auto frame = co_await ws.read_frame();
        if (frame.opcode == OpCode::Close) break;
        if (frame.opcode == OpCode::Text || frame.opcode == OpCode::Binary)
            co_await ws.write_frame(frame);
    }
}

// 客户端：connect → send → recv → close
auto ws = co_await WebSocket::connect("localhost", 9001, "/");
co_await ws.send_text("Hello!");
auto echo = co_await ws.read_frame();
co_await ws.close();
```

## 项目结构

```
ultra-net/
├── include/ultranet/            # 公开头文件
│   ├── ultranet.h               # 总入口头文件
│   ├── coroutine/               # 协程运行时 (Task, 线程池, Channel, when_all)
│   ├── io/                      # I/O 引擎 (io_uring 封装, 背压, 定时器)
│   ├── net/                     # 网络协议 (TCP, UDP, HTTP, WebSocket, DNS)
│   ├── lifecycle/               # 生命周期管理 (优雅关闭)
│   ├── metrics/                 # Prometheus 指标 (Counter, Gauge, Histogram)
│   ├── log/                     # 日志接口
│   ├── config/                  # 配置系统
│   ├── trace/                   # 分布式追踪
│   └── utils/                   # 工具类
├── src/                         # 内部实现
├── examples/                    # 示例程序
├── tests/                       # 单元测试 (22 套件)
├── docs/                        # 文档
│   ├── examples/                # 文档配套示例（带详细注释）
│   ├── index.md                 # 文档导航
│   ├── tcp-guide.md             # TCP 编程指南
│   ├── udp-guide.md             # UDP 编程指南
│   ├── http-guide.md            # HTTP 编程指南
│   ├── websocket-guide.md       # WebSocket 编程指南
│   ├── advanced-features.md     # 高级特性 (Channel, when_all, retry 等)
│   ├── api-reference.md         # 完整 API 参考
│   └── code-interpretation.md   # 代码深度解读（协程机制、设计模式、模板技巧）
└── .claude/                     # 开发记录（压测报告、代码审查）
```

## 示例程序

| 程序 | 说明 | 运行 |
|------|------|------|
| `tcp_echo_server` | TCP echo 服务器 | `./bin/tcp_echo_server 8080` |
| `tcp_echo_client` | TCP echo 客户端 | `./bin/tcp_echo_client 127.0.0.1 8080 "hello"` |
| `udp_echo_server` | UDP echo 服务器 | `./bin/udp_echo_server 8081` |
| `udp_echo_client` | UDP echo 客户端 | `./bin/udp_echo_client 127.0.0.1 8081 "hello"` |
| `http_server_example` | HTTP 服务器（路由、echo） | `./bin/http_server_example 8080` |
| `websocket_echo_server` | WebSocket echo 服务器 | `./bin/websocket_echo_server 9001` |
| `websocket_echo_client` | WebSocket echo 客户端 | `./bin/websocket_echo_client 127.0.0.1 9001 "hello"` |

## 测试

```bash
# 运行全部 22 个测试套件
cd build && ctest --output-on-failure

# 测试覆盖:
# channel, circuit_breaker, config, connection_pool, dns, dns_srv,
# echo, error, http, init, io_uring, log, metrics, mpsc, pool,
# retry, schedule, service_discovery, shutdown, tcp_socket, trace,
# udp, websocket, when_all, when_any
```

## 文档

| 文档 | 内容 |
|------|------|
| [快速开始](docs/index.md) | 概述、架构、命名空间约定 |
| [TCP 编程指南](docs/tcp-guide.md) | 服务器/客户端、TcpSocket、错误处理、优雅关闭 |
| [UDP 编程指南](docs/udp-guide.md) | 数据报收发、RecvFrom/SendTo、connected 模式 |
| [HTTP 编程指南](docs/http-guide.md) | 请求解析、响应构建、HttpClient、路由示例 |
| [WebSocket 编程指南](docs/websocket-guide.md) | 握手、帧编解码、零分配优化 |
| [高级特性](docs/advanced-features.md) | Channel、when_all/when_any、重试、断路器、连接池、服务发现 |
| [API 参考](docs/api-reference.md) | 完整类型签名索引 |
| [代码解读](docs/code-interpretation.md) | 协程机制、设计模式、模板元编程、无锁数据结构、排错指南 |

## 命名空间

| 命名空间 | 内容 |
|----------|------|
| `ynet::async` | 协程核心：`Task<T>`, `Channel`, `when_all`, `when_any`, `with_retry` |
| `ynet::async::io` | I/O 原语：`Read`, `Write`, `Accept`, `Socket`, `resolve_host` |
| `ynet::async::net` | 高级网络：`TcpSocket`, `HttpClient`, `ConnectionPool` |
| `ynet::async::net::websocket` | WebSocket：`WebSocket`, `WebSocketFrame`, `OpCode` |
| `ynet::async::net::http` | HTTP：`HttpRequest`, `HttpResponse`, `Method` |
| `ynet::async::scheduling` | 线程池：`WorkStealingThreadPool` |
| `ynet::async::lifecycle` | 生命周期：`ShutdownCoordinator` |
| `ynet::metrics` | 指标：`Counter`, `Gauge`, `Histogram`, `MetricRegistry` |

## 配置

通过环境变量配置运行参数：

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `ULTRANET_POOL_THREADS` | CPU 核数 | 线程池大小 |
| `ULTRANET_IO_ENTRIES` | 1024 | io_uring SQ/CQ 条目数 |
| `ULTRANET_MAX_PENDING_OPS` | 256 | 背压水位线 |
| `ULTRANET_CB_THRESHOLD` | 5 | 断路器失败阈值 |
| `ULTRANET_RETRY_MAX` | 3 | 最大重试次数 |
| `ULTRANET_LOG_LEVEL` | info | 日志级别 |

或通过代码配置：

```cpp
auto cfg = UltraNetConfig::from_env();
cfg.pool.num_threads = 4;
cfg.io_uring.entries = 2048;
```

## License

MIT License — 详见 [LICENSE](LICENSE) 文件。

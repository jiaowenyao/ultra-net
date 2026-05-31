# TCP 编程指南

## 概述

ultra-net 提供两套 TCP API：
1. **底层 I/O 原语**（`io::Socket`, `io::Accept`, `io::Read`, `io::Write` 等）— 对标 BSD socket API，精细控制
2. **高层 `TcpSocket`**（`net::TcpSocket`）— RAII socket 管理 + 内建 DNS 解析 + 便捷 connect 工厂

## TCP 服务器

### 完整示例

源码：[`docs/examples/tcp_echo_server.cc`](examples/tcp_echo_server.cc)

```bash
# 构建
cd build && make -j2 tcp_echo_server
# 运行
./bin/tcp_echo_server 8080
```

### 服务器骨架

每个 TCP 服务器遵循相同的模式：

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
    // 1. 创建 socket
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    int fd = *sock;

    // 2. 设置 SO_REUSEADDR
    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // 3. 绑定端口
    sockaddr_in addr{AF_INET, htons(port), INADDR_ANY};
    co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));

    // 4. 开始监听
    co_await Listen(fd, 256);

    // 5. Accept 循环（带超时以支持优雅关闭）
    while (!shutdown.is_shutdown()) {
        Accept acceptor(fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ETIMEDOUT) continue;
            break;
        }
        // 提交客户端 handler 到线程池
        ExecutionContext::current()->submit(handle_client(*client).release());
    }
    co_await Close(fd);
}

int main() {
    ShutdownCoordinator shutdown;
    shutdown.install_signal_handlers();

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(server(8080, shutdown).release());
    pool.wait_all();
}
```

### 关键 API

#### Socket — 创建 socket

```cpp
auto result = co_await Socket(AF_INET, SOCK_STREAM, 0);
// result 是 IoResult<int> = std::expected<int, std::error_code>
if (!result) {
    std::cerr << result.error().message() << std::endl;
    co_return;
}
int fd = *result;  // socket fd（已设置 SOCK_NONBLOCK）
```

#### Bind — 绑定地址（同步操作）

```cpp
// bind 是同步系统调用，无 io_uring opcode
sockaddr_in addr{AF_INET, htons(port), INADDR_ANY};
co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));
```

#### Listen — 开始监听（同步操作）

```cpp
co_await Listen(fd, 256);  // backlog = 256
```

#### Accept — 接受连接（异步，io_uring）

```cpp
Accept acceptor(listen_fd);
acceptor.with_timeout(std::chrono::milliseconds(500)); // 可选超时
auto client = co_await acceptor;
// 返回 IoResult<int> — 新客户端的 fd
```

**为什么 Accept 需要超时？** 没有超时的 Accept 会无限期挂起，阻止服务器响应 Ctrl+C。
500ms 超时意味着服务器每 500ms 检查一次关闭标志，且不会过度消耗 CPU。

#### Read — 读取数据（异步，io_uring）

```cpp
char buf[4096];
Read reader(fd, buf, sizeof(buf));
reader.with_timeout(std::chrono::seconds(5)); // 可选
auto n = co_await reader;
// 返回 IoResult<size_t> — 读取的字节数
// n == 0 表示对端关闭 (FIN)
```

#### Write — 写入数据（异步，io_uring）

```cpp
auto written = co_await Write(fd, data, len);
// 返回 IoResult<size_t> — 实际写入的字节数
```

#### Close — 关闭连接（异步，io_uring）

```cpp
co_await Close(fd);
```

### 错误处理

所有 I/O 操作返回 `IoResult<T>`（即 `std::expected<T, std::error_code>`）：

```cpp
auto n = co_await Read(fd, buf, sizeof(buf));
if (!n) {
    // n.error() 是 std::error_code
    if (is_timeout(n.error())) {
        // 超时处理
    } else if (is_closed(n.error())) {
        // 对端关闭 (ECONNRESET, EPIPE, EBADF)
    } else if (is_retryable(n.error())) {
        // EAGAIN/EINTR — 可重试
    }
    co_await Close(fd);
    co_return;
}
if (*n == 0) {
    // peer sent FIN — 正常关闭
    co_await Close(fd);
    co_return;
}
// 正常处理数据...
```

错误辅助函数（定义于 `<ultranet/net/error.hpp>`）：

| 函数 | 匹配的错误码 |
|------|-------------|
| `is_timeout(ec)` | ETIMEDOUT |
| `is_retryable(ec)` | EAGAIN, EINTR |
| `is_closed(ec)` | ECONNRESET, EPIPE, ENOTCONN, EBADF |
| `is_refused(ec)` | ECONNREFUSED |
| `is_eof(result)` | result == 0 bytes read |

### 优雅关闭

使用 `ShutdownCoordinator` 实现信号驱动的优雅关闭：

```cpp
ShutdownCoordinator shutdown;
shutdown.install_signal_handlers(); // 注册 SIGINT/SIGTERM

// 在 accept 循环中检查
while (!shutdown.is_shutdown()) {
    // accept with timeout → 最多 500ms 后检查标志
}

// 关闭流程:
// 1. SIGINT → shutdown.is_shutdown() == true
// 2. Accept 超时 → 退出循环 → Close(listen_fd)
// 3. 活跃的客户端 handler 自然完成
// 4. pool.wait_all() 在 active_tasks == 0 时返回
```

## TCP 客户端

### 完整示例

源码：[`docs/examples/tcp_echo_client.cc`](examples/tcp_echo_client.cc)

```bash
cd build && make -j2 tcp_echo_client
./bin/tcp_echo_client 127.0.0.1 8080 "Hello!"
```

### 客户端流程

```cpp
Task<void> client(const std::string& host, int port, const std::string& msg) {
    // 1. 创建 socket
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    int fd = *sock;

    // 2. DNS 解析
    auto ips = co_await resolve_host(host, 3s);
    sockaddr_in addr{AF_INET, htons(port)};
    inet_pton(AF_INET, ips[0].c_str(), &addr.sin_addr);

    // 3. 连接（带超时）
    Connect conn(fd, (sockaddr*)&addr, sizeof(addr));
    conn.with_timeout(5s);
    auto r = co_await conn;
    if (!r) { /* 错误处理 */ }

    // 4. 发送数据
    co_await Write(fd, msg.data(), msg.size());

    // 5. 接收响应
    char buf[4096];
    auto n = co_await Read(fd, buf, sizeof(buf));

    // 6. 关闭
    co_await Close(fd);
}
```

### TcpSocket 高级封装

对于简单场景，`TcpSocket` 提供了更便捷的 API：

```cpp
using namespace ynet::async::net;

// connect() 自动处理 DNS 解析和连接重试（多 IP）
auto socket = co_await TcpSocket::connect("example.com", 80, 5s);
// 返回 Task<TcpSocket>，失败时抛出 std::system_error

// TcpSocket 提供便捷的 Read/Write 方法
auto r = co_await socket.read(buf, sizeof(buf));
auto w = co_await socket.write(data, len);
co_await socket.close();

// RAII：析构时自动关闭 fd
// 移动语义：可以安全地 move 到其他协程
```

## 性能提示

1. **线程池大小**：一般设为 CPU 核数（`hardware_concurrency()`），I/O 密集型可适当增加
2. **超时设置**：Accept 超时 500ms 对吞吐影响可忽略（只在空闲时触发）；避免为频繁的 Read/Write 设置过短超时
3. **避免 short read**：ultra-net 的 Read 可能返回少于请求的数据（与 BSD read 一致），如需读取固定长度，使用循环
4. **批量提交**：io_uring 的 batch_threshold 控制批量提交，默认 64 个 SQE 才 submit，可提高吞吐但增加延迟

## 相关文档

- [UDP 编程指南](udp-guide.md) — 数据报收发
- [高级特性](advanced-features.md) — 重试、断路器、连接池
- [API 参考](api-reference.md) — 完整类型签名

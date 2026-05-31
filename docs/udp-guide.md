# UDP 编程指南

## 概述

UDP 是无连接协议，ultra-net 提供 `SendTo` 和 `RecvFrom` 两个 I/O 原语来处理数据报通信。
与 TCP 的主要区别：
- 无需 `Listen`/`Accept` — 一个 socket 可服务所有客户端
- 每包独立发送，附带目标地址（`SendTo` 指定目标，`RecvFrom` 获取来源）
- 保留数据报边界 — 每个 `RecvFrom` 返回恰好一个数据报

## UDP 服务器

### 完整示例

源码：[`docs/examples/udp_echo_server.cc`](examples/udp_echo_server.cc)

```bash
cd build && make -j2 udp_echo_server
./bin/udp_echo_server 8081
# 测试: echo "hello" | nc -u localhost 8081
```

### 服务器模式

```cpp
Task<void> udp_server(int port, ShutdownCoordinator& shutdown) {
    // 1. 创建 UDP socket（SOCK_DGRAM）
    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int fd = *sock;

    // 2. 绑定端口
    sockaddr_in addr{AF_INET, htons(port), INADDR_ANY};
    co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));

    char buf[65536]; // UDP 最大数据报 64KB

    while (!shutdown.is_shutdown()) {
        // 3. 接收数据报（同时获取发送者地址）
        RecvFrom receiver(fd, buf, sizeof(buf));
        receiver.with_timeout(500ms);

        auto n = co_await receiver;
        if (!n) continue; // 超时 → 继续检查 shutdown

        // 4. 获取发送者地址
        const auto& src = receiver.source_addr();  // sockaddr_storage
        socklen_t src_len = receiver.source_addr_len();

        // 5. 回显给发送者
        co_await SendTo(fd, buf, *n, (const sockaddr*)&src, src_len);
    }
    co_await Close(fd);
}
```

### RecvFrom — 接收数据报

```cpp
char buf[65536];
RecvFrom receiver(fd, buf, sizeof(buf));
receiver.with_timeout(std::chrono::seconds(5));  // 可选超时

auto n = co_await receiver;
// 返回 IoResult<size_t> — 接收的字节数

// 访问发送者地址：
const sockaddr_storage& addr = receiver.source_addr();
socklen_t addr_len = receiver.source_addr_len();

// MSG_TRUNC 标志 — 缓冲区小于数据报时设置
int flags = receiver.flags();
if (flags & MSG_TRUNC) {
    // 数据报被截断
}
```

### SendTo — 发送数据报

```cpp
sockaddr_in dest{AF_INET, htons(port)};
inet_pton(AF_INET, "10.0.0.1", &dest.sin_addr);

auto sent = co_await SendTo(fd, data, len,
    (const sockaddr*)&dest, sizeof(dest));
// 返回 IoResult<size_t>
```

## UDP 客户端

### 完整示例

源码：[`docs/examples/udp_echo_client.cc`](examples/udp_echo_client.cc)

```bash
cd build && make -j2 udp_echo_client
./bin/udp_echo_client 127.0.0.1 8081 "Hello, UDP!"
```

### 客户端模式

```cpp
Task<void> udp_client(const std::string& host, int port) {
    // 1. 创建 UDP socket
    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int fd = *sock;

    // 2. 解析目标地址
    auto ips = co_await resolve_host(host, 3s);
    sockaddr_in addr{AF_INET, htons(port)};
    inet_pton(AF_INET, ips[0].c_str(), &addr.sin_addr);

    // 3. 发送数据报
    co_await SendTo(fd, "ping", 4, (sockaddr*)&addr, sizeof(addr));

    // 4. 接收响应
    char buf[65536];
    RecvFrom receiver(fd, buf, sizeof(buf));
    receiver.with_timeout(5s);
    auto n = co_await receiver;

    // 可选：验证响应来源
    // receiver.source_addr() 可用于检查响应是否来自期望的服务器

    co_await Close(fd);
}
```

## Connected UDP 模式

UDP socket 也可以调用 `Connect` 设置默认目标地址，之后可使用 `Write`/`Read`（与 TCP 一致的 API）：

```cpp
// 设置默认目标
sockaddr_in addr{AF_INET, htons(port)};
inet_pton(AF_INET, "10.0.0.1", &addr.sin_addr);

co_await Connect(fd, (sockaddr*)&addr, sizeof(addr));
// 当 UDP socket 被 connect 后，可以像 TCP 一样使用 Read/Write

// 发送（自动去往默认目标）
co_await Write(fd, data, len);

// 接收（只接收来自默认目标的包）
auto n = co_await Read(fd, buf, sizeof(buf));

// 内核自动过滤其他来源的数据报
```

Connected UDP 的优点：
- 使用更简单的 `Write`/`Read` API
- 内核层面过滤无关数据报
- 可获取异步 I/O 错误（如 ICMP Port Unreachable）

## UDP vs TCP 对比

| 特性 | TCP | UDP |
|------|-----|-----|
| Socket 类型 | `SOCK_STREAM` | `SOCK_DGRAM` |
| 连接模型 | Listen/Accept/Connect | 无连接（或 Connect 设默认目标） |
| 发送 API | `Write` (不需地址) | `SendTo` (每次指定目标) 或 `Write` (connect 后) |
| 接收 API | `Read` (不需地址) | `RecvFrom` (获取来源) 或 `Read` (connect 后) |
| 数据边界 | 流式，无边界保证 | 保留数据报边界 |
| 可靠性 | 可靠有序 | 不可靠（丢包/乱序） |

## 性能提示

1. **缓冲区大小**：UDP 数据报最大 65535 字节，建议使用 64KB 缓冲区以避免 MSG_TRUNC
2. **超时**：UDP 无连接状态，RecvFrom 超时不会产生副作用 — 可安全设置较短超时
3. **端口不可达**：向无监听端口发送 UDP 包在本地成功（无连接），但 ICMP Port Unreachable 可通过 MSG_ERRQUEUE 获取
4. **避免 fragmentation**：以太网 MTU 1500 字节，减去 IP/UDP 头后约 1472 字节。超过此值的数据报会被分片

## 相关文档

- [TCP 编程指南](tcp-guide.md) — stream socket 模式
- [API 参考](api-reference.md) — 完整类型签名

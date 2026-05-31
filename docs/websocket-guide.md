# WebSocket 编程指南

## 概述

ultra-net 实现了完整的 RFC 6455 WebSocket 协议，支持：
- 客户端和服务端握手（HTTP Upgrade）
- 帧编解码（text/binary/ping/pong/close，masking）
- 零分配热路径（栈 buffer 编码，<16KB payload 无 heap 分配）
- 自动响应 Ping/Pong
- 优雅的 Close 握手

**命名空间**: `ynet::async::net::websocket`

**头文件**: `#include "ultranet/net/websocket.hpp"` （或使用 `ultranet/ultranet.h`）

## 架构

```
┌─────────────────────────────────┐
│  WebSocket (高层接口)            │
│  connect / accept / read_frame  │
│  write_frame / close            │
├─────────────────────────────────┤
│  WebSocketFrame (帧层)          │
│  encode / decode / encode_into  │
├─────────────────────────────────┤
│  TcpSocket (传输层)             │
│  read / write / close           │
└─────────────────────────────────┘
```

## WebSocket 服务器

### 完整示例

源码：[`docs/examples/websocket_echo_server.cc`](examples/websocket_echo_server.cc)

```bash
cd build && make -j2 websocket_echo_server
./bin/websocket_echo_server 9001
```

### 服务端升级（WebSocket::accept）

服务端接收 HTTP 升级请求并完成握手：

```cpp
Task<void> ws_session(TcpSocket socket) {
    WebSocket ws(std::move(socket));

    // 1. 读取 HTTP 升级请求
    char buf[4096];
    auto n = co_await ws.socket().read(buf, sizeof(buf));

    // 2. 解析 HTTP 请求
    http::HttpRequest req;
    req.parse(buf, *n);

    // 3. WebSocket 握手
    // accept() 验证:
    //   - Upgrade: websocket
    //   - Connection: Upgrade
    //   - Sec-WebSocket-Key 非空
    //   - Sec-WebSocket-Version: 13
    // 成功后发送 "101 Switching Protocols" 响应
    auto ec = co_await ws.accept(req);
    if (ec) {
        std::cerr << "handshake failed: " << ec.message() << std::endl;
        co_return;
    }

    // 4. 帧交换循环
    while (true) {
        auto frame = co_await ws.read_frame();

        if (frame.opcode == OpCode::Close) break;
        if (frame.opcode == OpCode::Text || frame.opcode == OpCode::Binary) {
            co_await ws.write_frame(frame);
        }
    }
}
```

### 帧处理

```cpp
auto frame = co_await ws.read_frame();

// 帧类型检查
if (frame.opcode == OpCode::Text) {
    std::cout << "Text: " << frame.payload << std::endl;
} else if (frame.opcode == OpCode::Binary) {
    // frame.payload 包含二进制数据
} else if (frame.opcode == OpCode::Close) {
    // 对端请求关闭，read_frame() 已自动回复 Close 帧
    break;
}
// Ping 帧由 read_frame() 自动回复，应用层不需要处理
```

## WebSocket 客户端

### 完整示例

源码：[`docs/examples/websocket_echo_client.cc`](examples/websocket_echo_client.cc)

```bash
cd build && make -j2 websocket_echo_client
./bin/websocket_echo_client 127.0.0.1 9001 "Hello!"
```

### 客户端连接（WebSocket::connect）

```cpp
// connect() 是静态工厂方法，完成整个升级流程：
//   1. TCP 连接
//   2. 发送 HTTP upgrade 请求
//   3. 接收 101 Switching Protocols 响应
//   4. 验证 Sec-WebSocket-Accept key
// 失败时抛出 std::system_error
auto ws = co_await WebSocket::connect("echo.example.com", 9001, "/",
                                       std::chrono::seconds(5));
```

### 发送消息

```cpp
// 便捷方法
co_await ws.send_text("Hello, world!");
co_await ws.send_binary(binary_data);

// 完整控制
WebSocketFrame frame = WebSocketFrame::text("Hello");
co_await ws.write_frame(frame);

// Ping (自动收到 Pong)
co_await ws.send_ping("keepalive");
```

### 接收消息

```cpp
auto frame = co_await ws.read_frame();

switch (frame.opcode) {
    case OpCode::Text:
        std::cout << "Received: " << frame.payload << std::endl;
        break;
    case OpCode::Binary:
        // 处理二进制数据
        break;
    case OpCode::Close:
        std::cout << "Server closed connection" << std::endl;
        break;
    // Ping/Pong 由 read_frame() 自动处理，应用层不可见
}
```

### 关闭连接

```cpp
// 正常关闭 (code=1000)
co_await ws.close();

// 指定状态码和原因
co_await ws.close(1001, "going away");

// 检查连接状态
if (ws.is_open()) {
    // 连接仍然活跃
}
```

## WebSocketFrame API

### 帧类型 (OpCode)

```cpp
enum class OpCode : uint8_t {
    Continuation = 0x0,  // 分片帧的延续
    Text         = 0x1,  // UTF-8 文本
    Binary       = 0x2,  // 二进制数据
    Close        = 0x8,  // 关闭握手
    Ping         = 0x9,  // 心跳请求
    Pong         = 0xA   // 心跳响应
};
```

### 创建帧

```cpp
// 工厂方法 — 创建各类帧
auto text_frame  = WebSocketFrame::text("Hello, world!");
auto bin_frame   = WebSocketFrame::binary(data);
auto close_frame = WebSocketFrame::close(1000, "normal");
auto ping_frame  = WebSocketFrame::ping("keepalive");
auto pong_frame  = WebSocketFrame::pong("keepalive");

// 手动构造
WebSocketFrame f;
f.fin = true;
f.opcode = OpCode::Text;
f.mask = true;       // 客户端必须 mask (RFC 6455)
f.payload = "Hello";
```

### 编解码

```cpp
// 编码为 wire format
std::vector<uint8_t> wire = frame.encode(true); // apply_mask = true

// 零分配编码（推荐）
uint8_t buf[16384];
size_t n = frame.encode_into(buf, sizeof(buf), true);
// n > 0: 成功，buf[0..n-1] 包含编码后的帧
// n == 0: buffer 太小，需用 heap fallback

// 计算编码后大小
size_t wire_size = WebSocketFrame::encoded_size(payload.size(), apply_mask);

// 解码
const uint8_t* raw_data = ...;
size_t raw_len = ...;
size_t consumed = 0;
WebSocketFrame decoded;
bool ok = WebSocketFrame::decode(raw_data, raw_len, consumed, decoded);
// ok && consumed > 0: 成功解码一个帧
// !ok && consumed == 0: 数据不完整，需要更多数据
```

### masking 规则

RFC 6455 要求：
- **客户端 → 服务器**：帧**必须** mask（`WS::connect` 返回的 `WebSocket` 自动设置 `m_masked = true`）
- **服务器 → 客户端**：帧**不得** mask（`ws.accept()` 后自动设置 `m_masked = false`）

```cpp
// 客户端：mask 由 write_frame() 自动处理
// 服务端：accept() 后 m_masked = false

// manual control
frame.mask = true;            // 标记需要 mask
frame.masking_key = 0x12345678; // 手动设置 key（0 则随机生成）
```

## 零分配优化

ultra-net 的 WebSocket 实现针对热路径进行了零分配优化：

```cpp
// write_frame() 内部实现逻辑：
//   1. 尝试用栈 buffer (16KB) 编码
//   2. 如果 payload <= ~16KB → encode_into 成功 → 单次 write()
//   3. 如果 payload > 16KB → heap allocate + writev(header, payload)

// read_frame() 内部实现逻辑：
//   1. 尝试用栈 buffer (8KB) 读取
//   2. 如果帧在单次 read 内完成 → 零分配
//   3. 如果帧跨 TCP segment → 切换到 heap buffer（仅必要时）
```

## WebSocket 握手机制

### 客户端握手（WebSocket::connect 内部）

```
Client                                          Server
  |  GET / HTTP/1.1                              |
  |  Upgrade: websocket                          |
  |  Connection: Upgrade                         |
  |  Sec-WebSocket-Key: <random-base64>          |
  |  Sec-WebSocket-Version: 13                   |
  | -------------------------------------------> |
  |                                               |
  |  101 Switching Protocols                      |
  |  Upgrade: websocket                           |
  |  Connection: Upgrade                          |
  |  Sec-WebSocket-Accept: <SHA1(key+GUID)>       |
  | <------------------------------------------- |
  |                                               |
  |  [Frame exchange]                             |
```

`Sec-WebSocket-Accept` 验证使用 SHA-1 + Base64：
```
accept = base64(sha1(client_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
```

### 服务端握手

```cpp
// accept() 自动完成:
//   1. 查找 Upgrade/Connection/Sec-WebSocket-Key/Sec-WebSocket-Version headers
//   2. 大小写不敏感验证 "websocket" 和 "upgrade"
//   3. 计算 Sec-WebSocket-Accept
//   4. 发送 "101 Switching Protocols" 响应
// 返回 std::error_code{} 表示成功

auto ec = co_await ws.accept(request);
if (!ec) {
    // 握手成功，可以开始帧交换
}
```

## 相关文档

- [TCP 编程指南](tcp-guide.md) — 底层传输
- [HTTP 编程指南](http-guide.md) — HTTP 升级请求格式
- [API 参考](api-reference.md) — 完整类型签名

# HTTP 编程指南

## 概述

ultra-net 提供完整的 HTTP/1.1 消息模型（`HttpRequest`/`HttpResponse`）和 HTTP 客户端（`HttpClient`）。
HTTP 模块基于底层的 TCP 读写操作，可嵌入到任何 TCP 连接处理流程中。

**命名空间**: `ynet::async::net::http`

**头文件**: `#include "ultranet/ultranet.h"` （包含所有 API） 或 `#include "ultranet/net/http.hpp"`

## HTTP 服务器

### 完整示例

源码：[`docs/examples/http_server.cc`](examples/http_server.cc)

```bash
cd build && make -j2 http_server_example
./bin/http_server_example 8080
# 测试:
curl http://localhost:8080/
curl -X POST -d "hello" http://localhost:8080/echo
```

### 请求解析

```cpp
using namespace ynet::async::net::http;

char buf[8192];
auto n = co_await Read(fd, buf, sizeof(buf));

HttpRequest req;
size_t consumed = req.parse(buf, *n);
// consumed == 0: 数据不完整（需要继续读取）
// consumed > 0: 成功解析，返回消耗的字节数

// 访问请求字段
req.method;       // Method::GET, Method::POST, 等
req.path;         // "/index.html" 或 "/api/data"
req.body;         // 请求体（POST/PUT）
req.http_version; // "HTTP/1.1"

// 大小写不敏感的 header 查找
std::string_view host = req.header("host");       // 查找 "Host"
std::string_view ct = req.header("content-type"); // 查找 "Content-Type"
```

### 响应构建

```cpp
HttpResponse resp;
resp.status_code = 200;
resp.status_message = "OK";
resp.http_version = "HTTP/1.1";

// 添加 headers
resp.headers.push_back({"Content-Type", "text/html"});
resp.headers.push_back({"Server", "ultra-net/0.1"});
resp.headers.push_back({"Connection", "close"});

// 设置 body
resp.body = "<html><body><h1>Hello!</h1></body></html>";

// 序列化为字符串
std::string data = resp.serialize();
// "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n..."

// 发送
co_await Write(fd, data.data(), data.size());

// 辅助方法
resp.is_keepalive(); // 检查 Connection: keep-alive
```

### 路由示例

```cpp
HttpRequest req;
req.parse(buf, n);

HttpResponse resp;

if (req.path == "/" && req.method == Method::GET) {
    resp.status_code = 200;
    resp.body = "<h1>Welcome</h1>";
} else if (req.path == "/api/data" && req.method == Method::POST) {
    resp.status_code = 200;
    resp.body = "Received: " + req.body;
    resp.headers.push_back({"Content-Type", "text/plain"});
} else {
    resp.status_code = 404;
    resp.status_message = "Not Found";
    resp.body = "404 Not Found";
}
```

### 支持的 HTTP Method

```cpp
enum class Method : uint8_t {
    GET,      // "GET"
    POST,     // "POST"
    PUT,      // "PUT"
    DELETE_,  // "DELETE" (注意尾随下划线，避免 C++ 关键字冲突)
    HEAD,     // "HEAD"
    PATCH,    // "PATCH"
    OPTIONS,  // "OPTIONS"
    CONNECT,  // "CONNECT"
    TRACE     // "TRACE"
};

// 转换为字符串
std::string_view s = method_string(req.method);
```

## HTTP 客户端

```cpp
using namespace ynet::async::net;
using namespace ynet::async::net::http;

// 建立 TCP 连接
auto tcp_sock = co_await TcpSocket::connect("example.com", 80, 5s);

// 创建 HTTP 客户端
HttpClient client(std::move(tcp_sock), "example.com:80");

// 便捷方法
auto resp1 = co_await client.get("/");
auto resp2 = co_await client.post("/api", "data", "text/plain");

// 完整控制
HttpRequest req;
req.method = Method::GET;
req.path = "/index.html";
req.headers.push_back({"Accept", "text/html"});

auto resp3 = co_await client.send(req);

// 访问响应
resp3.status_code;    // 200
resp3.body;           // 响应体
resp3.header("content-type"); // "text/html"
```

`HttpClient::send()` 内部：
1. 序列化请求并循环发送
2. 循环读取响应数据到累积缓冲区
3. 调用 `HttpResponse::parse()` 直到解析完成
4. 返回解析后的 `HttpResponse`

## 增量解析

HTTP 模块支持增量解析，适配 TCP 流式特性：

```cpp
// 每次收到数据时调用 parse
char buf[4096];
HttpRequest req;
std::string accumulator;

while (true) {
    auto n = co_await Read(fd, buf, sizeof(buf));
    if (!n || *n == 0) break;

    // 追加到累积缓冲区
    accumulator.append(buf, *n);

    size_t consumed = req.parse(accumulator.data(), accumulator.size());
    if (consumed > 0) {
        // 解析成功
        break;
    }
    // consumed == 0: 数据不完整，继续读取
}
```

注意：当前的 HTTP 实现是为单请求-响应设计的（HTTP/1.0 语义）。
实现 keep-alive 需要在解析完成后从缓冲区移除已消费部分并重置解析器状态。

## 完整服务器示例

以下展示了一个完整的 HTTP 服务器，包含路由和错误处理：

```cpp
Task<void> handle_http(int client_fd) {
    char buf[8192];
    auto n = co_await Read(client_fd, buf, sizeof(buf));
    if (!n || *n == 0) { co_await Close(client_fd); co_return; }

    http::HttpRequest req;
    size_t consumed = req.parse(buf, *n);
    if (consumed == 0) { co_await Close(client_fd); co_return; }

    http::HttpResponse resp;
    resp.http_version = "HTTP/1.1";

    if (req.path == "/") {
        resp.status_code = 200;
        resp.body = "<h1>ultra-net HTTP Server</h1>";
        resp.headers.push_back({"Content-Type", "text/html"});
    } else if (req.path == "/echo" && req.method == http::Method::POST) {
        resp.status_code = 200;
        resp.body = req.body;
        resp.headers.push_back({"Content-Type", "text/plain"});
    } else {
        resp.status_code = 404;
        resp.body = "Not Found";
    }

    resp.headers.push_back({"Connection", "close"});
    auto data = resp.serialize();
    co_await Write(client_fd, data.data(), data.size());
    co_await Close(client_fd);
}
```

## 相关文档

- [TCP 编程指南](tcp-guide.md) — 底层 TCP 操作
- [WebSocket 编程指南](websocket-guide.md) — WebSocket 升级基于 HTTP
- [API 参考](api-reference.md)

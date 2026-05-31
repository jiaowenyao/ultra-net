# API 参考

本文档列出 ultra-net 所有公开 API 的类型签名和简要说明。
详细的教程和示例请参阅对应模块的指南文档。

## 协程核心

### Task<T>

`#include "ultranet/coroutine/task.hpp"` · `ynet::async`

```cpp
template <typename T = void>
class [[nodiscard]] Task : Noncopyable {
    Task() noexcept;
    explicit Task(coroutine_handle<promise_type> handle);
    Task(Task&& other) noexcept;
    Task& operator=(Task&& other) noexcept;
    ~Task();

    auto operator co_await() const & noexcept;   // 返回 T&
    auto operator co_await() const && noexcept;  // 返回 T&&

    coroutine_handle<promise_type> release();  // 释放句柄所有权
    void resume() const;
};
```

### WorkStealingThreadPool

`#include "ultranet/coroutine/thread_pool.hpp"` · `ynet::async::scheduling`

```cpp
class WorkStealingThreadPool final : public Scheduler {
    explicit WorkStealingThreadPool(size_t num_threads = hardware_concurrency());
    ~WorkStealingThreadPool();

    void submit(coroutine_handle<> handle) override;
    void resubmit(coroutine_handle<> handle) override;

    template <typename T> void submit_task(Task<T>& task);
    template <typename F, typename... Args>
    auto submit_with_result(F&& f, Args&&... args) -> std::future<invoke_result_t>;

    void wait_all();                   // 阻塞至 active_tasks == 0
    bool wait_all_for(const duration&); // 带超时等待

    bool is_current_thread() const;
    size_t active_tasks() const noexcept;
    size_t pending_tasks() const noexcept;
    size_t num_threads() const noexcept;
};
```

### ExecutionContext

`#include "ultranet/ultranet.h"` · `ynet::async`

```cpp
class ExecutionContext {
    static Scheduler* current() noexcept;  // 当前线程绑定的调度器
    static bool has_current() noexcept;

    class Scope {
        explicit Scope(Scheduler* scheduler) noexcept;
        ~Scope();
    };
};
```

## I/O 类型

### IoResult<T>

`#include "ultranet/io/io_awaitable.hpp"` · `ynet::async::io`

```cpp
template <typename T = size_t>
using IoResult = std::expected<T, std::error_code>;

inline std::error_code make_io_error(int err) noexcept;
```

### Socket

`#include "ultranet/net/socket.hpp"` · `ynet::async::io`

```cpp
class Socket : public IoOperation<Socket> {
    Socket(int domain, int type, int protocol) noexcept;
    // co_await → IoResult<int> (fd)
};
```

### Bind

`#include "ultranet/net/bind.hpp"` · `ynet::async::io`

```cpp
class Bind {
    Bind(int fd, const sockaddr* addr, socklen_t addrlen) noexcept;
    // co_await → IoResult<int>
};
```

### Listen

`#include "ultranet/net/listen.hpp"` · `ynet::async::io`

```cpp
class Listen {
    Listen(int fd, int backlog) noexcept;
    // co_await → IoResult<int>
};
```

### Accept

`#include "ultranet/net/accept.hpp"` · `ynet::async::io`

```cpp
class Accept : public IoOperation<Accept> {
    explicit Accept(int fd) noexcept;
    // co_await → IoResult<int> (client fd)
};
```

### Connect

`#include "ultranet/net/connect.hpp"` · `ynet::async::io`

```cpp
class Connect : public IoOperation<Connect> {
    Connect(int fd, const sockaddr* addr, socklen_t addrlen) noexcept;
    // co_await → IoResult<int>
};
```

### Read

`#include "ultranet/net/read.hpp"` · `ynet::async::io`

```cpp
class Read : public IoOperation<Read> {
    Read(int fd, void* buf, size_t count) noexcept;
    // co_await → IoResult<size_t>
};
```

### Write

`#include "ultranet/net/write.hpp"` · `ynet::async::io`

```cpp
class Write : public IoOperation<Write> {
    Write(int fd, const void* buf, size_t count, off_t offset = 0) noexcept;
    // co_await → IoResult<size_t>
};
```

### Writev

`#include "ultranet/net/write.hpp"` · `ynet::async::io`

```cpp
class Writev : public IoOperation<Writev> {
    Writev(int fd, const iovec* iov, int iovcnt, off_t offset = 0) noexcept;
    // co_await → IoResult<size_t>
};
```

### SendTo

`#include "ultranet/net/sendto.hpp"` · `ynet::async::io`

```cpp
class SendTo : public IoOperation<SendTo> {
    SendTo(int fd, const void* buf, size_t len,
           const sockaddr* dest_addr, socklen_t addrlen) noexcept;
    // co_await → IoResult<size_t>
};
```

### RecvFrom

`#include "ultranet/net/recvfrom.hpp"` · `ynet::async::io`

```cpp
class RecvFrom : public IoOperation<RecvFrom> {
    RecvFrom(int fd, void* buf, size_t len) noexcept;
    // co_await → IoResult<size_t>
    const sockaddr_storage& source_addr() const noexcept;
    socklen_t source_addr_len() const noexcept;
    int flags() const noexcept;
};
```

### Close

`#include "ultranet/net/close.hpp"` · `ynet::async::io`

```cpp
class Close : public IoOperation<Close> {
    explicit Close(int fd) noexcept;
    // co_await → IoResult<int>
};
```

### Shutdown

`#include "ultranet/net/shutdown.hpp"` · `ynet::async::io`

```cpp
class Shutdown : public IoOperation<Shutdown> {
    Shutdown(int fd, int how) noexcept; // SHUT_RD, SHUT_WR, SHUT_RDWR
    // co_await → IoResult<int>
};
```

### with_timeout

所有 `IoOperation<Derived>` 子类支持：

```cpp
Derived& with_timeout(std::chrono::nanoseconds duration) noexcept;
```

## 错误处理

`#include "ultranet/net/error.hpp"` · `ynet::async::io`

```cpp
inline bool is_timeout(const std::error_code& ec) noexcept;   // ETIMEDOUT
inline bool is_retryable(const std::error_code& ec) noexcept;  // EAGAIN, EINTR
inline bool is_closed(const std::error_code& ec) noexcept;     // ECONNRESET, EPIPE, ENOTCONN, EBADF
inline bool is_refused(const std::error_code& ec) noexcept;    // ECONNREFUSED
inline bool is_eof(const IoResult<size_t>& result) noexcept;   // result == 0
```

## 高级网络类型

### TcpSocket

`#include "ultranet/net/tcp_socket.hpp"` · `ynet::async::net`

```cpp
class TcpSocket : Noncopyable {
    TcpSocket() noexcept;
    explicit TcpSocket(int fd) noexcept;
    ~TcpSocket(); // 关闭 fd
    TcpSocket(TcpSocket&& other) noexcept;
    TcpSocket& operator=(TcpSocket&& other) noexcept;

    int fd() const noexcept;
    bool is_valid() const noexcept;
    explicit operator bool() const noexcept;

    io::Read read(void* buf, size_t count);
    io::Write write(const void* buf, size_t count);
    io::Close close();

    int release() noexcept;
    static Task<TcpSocket> connect(const std::string& host, uint16_t port,
        std::chrono::milliseconds timeout = 5000ms);
};
```

### HTTP 类型

`#include "ultranet/net/http.hpp"` · `ynet::async::net::http`

```cpp
enum class Method : uint8_t { GET, POST, PUT, DELETE_, HEAD, PATCH, OPTIONS, CONNECT, TRACE };
inline std::string_view method_string(Method m);

struct Header { std::string name; std::string value; };

struct HttpRequest {
    Method method = Method::GET;
    std::string path = "/";
    std::string http_version = "HTTP/1.1";
    std::vector<Header> headers;
    std::string body;

    std::string_view header(std::string_view name) const;
    std::string serialize() const;
    size_t parse(const char* data, size_t len, std::string* error = nullptr);
};

struct HttpResponse {
    int status_code = 200;
    std::string status_message = "OK";
    std::string http_version = "HTTP/1.1";
    std::vector<Header> headers;
    std::string body;

    std::string serialize() const;
    size_t parse(const char* data, size_t len, std::string* error = nullptr);
    std::string_view header(std::string_view name) const;
    bool is_keepalive() const;
};

class HttpClient : Noncopyable {
    explicit HttpClient(TcpSocket socket, std::string host_header = "");
    Task<HttpResponse> send(HttpRequest request);
    Task<HttpResponse> get(std::string_view path);
    Task<HttpResponse> post(std::string_view path, std::string body = {},
        std::string_view content_type = "text/plain");
    TcpSocket& socket() noexcept;
};
```

### WebSocket 类型

`#include "ultranet/net/websocket.hpp"` · `ynet::async::net::websocket`

```cpp
enum class OpCode : uint8_t {
    Continuation = 0x0, Text = 0x1, Binary = 0x2,
    Close = 0x8, Ping = 0x9, Pong = 0xA
};

struct WebSocketFrame {
    bool fin = true;
    OpCode opcode = OpCode::Text;
    bool mask = false;
    uint32_t masking_key = 0;
    std::string payload;

    static WebSocketFrame text(std::string data, bool fin = true);
    static WebSocketFrame binary(std::string data, bool fin = true);
    static WebSocketFrame close(uint16_t code = 1000, const std::string& reason = "");
    static WebSocketFrame ping(const std::string& data = "");
    static WebSocketFrame pong(const std::string& data = "");

    static constexpr size_t encoded_size(size_t payload_len, bool apply_mask);
    std::vector<uint8_t> encode(bool apply_mask = false) const;
    size_t encode_into(uint8_t* buf, size_t cap, bool apply_mask) const;
    static bool decode(const uint8_t* data, size_t len,
        size_t& consumed, WebSocketFrame& frame);
};

class WebSocket : Noncopyable {
    explicit WebSocket(TcpSocket socket) noexcept;
    WebSocket(WebSocket&&) noexcept;

    static Task<WebSocket> connect(const std::string& host, uint16_t port,
        const std::string& path = "/", milliseconds timeout = 5000ms);
    Task<std::error_code> accept(const http::HttpRequest& req);
    Task<WebSocketFrame> read_frame();
    Task<void> write_frame(const WebSocketFrame& frame);

    Task<void> send_text(std::string data);
    Task<void> send_binary(std::string data);
    Task<void> send_ping(const std::string& data = "");
    Task<void> send_pong(const std::string& data = "");
    Task<void> close(uint16_t code = 1000, const std::string& reason = "");

    TcpSocket& socket() noexcept;
    bool is_open() const noexcept;
};
```

### DNS

`#include "ultranet/net/dns.hpp"` · `ynet::async::io`

```cpp
Task<std::vector<std::string>> resolve_host(const std::string& hostname,
    std::chrono::milliseconds timeout = 5000ms);

struct SrvRecord { uint16_t priority; uint16_t weight; uint16_t port; std::string target; };

Task<std::vector<SrvRecord>> resolve_srv(const std::string& service,
    const std::string& protocol, const std::string& domain,
    std::chrono::milliseconds timeout = 5000ms);
```

## 高级协程工具

### Channel

`#include "ultranet/coroutine/channel.hpp"` · `ynet::async`

```cpp
template <typename T, size_t Capacity = 256>
class Channel {
    WriteAwaitable write(T value) noexcept;
    bool try_write(T value) noexcept;
    ReadAwaitable read() noexcept;
    std::optional<T> try_read() noexcept;
    void close() noexcept;
    bool is_closed() const noexcept;
    size_t size() const noexcept;
    bool empty() const noexcept;
    bool full() const noexcept;
    size_t capacity() const noexcept;
};
```

### when_all / when_any

`#include "ultranet/coroutine/when_all.hpp"` · `ynet::async`

```cpp
template <typename... Ts>
WhenAllAwaiter<Ts...> when_all(Task<Ts>... tasks);

template <typename... Ts>
WhenAnyAwaiter<Ts...> when_any(Task<Ts>... tasks);
```

### with_retry

`#include "ultranet/coroutine/retry.hpp"` · `ynet::async`

```cpp
class ExponentialBackoff {
    std::chrono::milliseconds base_delay = 100ms;
    std::chrono::milliseconds max_delay = 5s;
    size_t max_retries = 3;
    double jitter_factor = 0.2;
};

template <typename TaskFn, typename Pred = ...>
Task<value_type> with_retry(TaskFn task_fn,
    ExponentialBackoff policy = {},
    Pred should_retry = [](const error_code&) { return true; });
```

### CircuitBreaker

`#include "ultranet/coroutine/circuit_breaker.hpp"` · `ynet::async`

```cpp
enum class CircuitState : uint8_t { Closed, Open, HalfOpen };

class CircuitBreaker : Noncopyable {
    explicit CircuitBreaker(config::CircuitBreakerConfig cfg = {}) noexcept;

    bool try_acquire() noexcept;
    void on_success() noexcept;
    void on_failure() noexcept;
    CircuitState state() const noexcept;
    void reset() noexcept;

    struct Stats { size_t total_successes, total_failures,
        consecutive_failures, fast_fails, half_open_attempts; };
    Stats snapshot() const noexcept;
};
```

## 生命周期

### ShutdownCoordinator

`#include "ultranet/lifecycle/shutdown.hpp"` · `ynet::async::lifecycle`

```cpp
enum class ShutdownPhase : uint8_t { Running, Draining, Complete };

class ShutdownCoordinator : Noncopyable {
    ShutdownCoordinator() = default;
    void install_signal_handlers();
    void shutdown() noexcept;
    bool is_shutdown() const noexcept;
    ShutdownPhase phase() const noexcept;
    void advance_phase(ShutdownPhase p) noexcept;
    auto wait() noexcept; // 返回 channel read awaitable
};
```

## 连接管理

### ConnectionPool

`#include "ultranet/net/connection_pool.hpp"` · `ynet::async::net`

```cpp
class ConnectionPool : Noncopyable {
    ConnectionPool(config::ConnectionPoolConfig cfg,
        std::string host, uint16_t port);
    ~ConnectionPool();

    Task<TcpSocket> acquire();
    void release(TcpSocket socket);
    void invalidate(TcpSocket socket);

    struct Stats { size_t acquired, released, evicted, failed_creates; };
    Stats snapshot() const noexcept;
};
```

### ServiceDiscovery

`#include "ultranet/net/service_discovery.hpp"` · `ynet::async::discovery`

```cpp
struct Endpoint {
    std::string host; uint16_t port;
    uint16_t priority; uint16_t weight;
    bool healthy = true;
    std::string id() const;
};

enum class EndpointChange : uint8_t { Added, Removed, StateChanged, Refresh };

class ServiceDiscovery : Noncopyable {
    ServiceDiscovery(std::string service, std::string protocol,
        std::string domain, ServiceDiscoveryConfig cfg = {});
    ServiceDiscovery(std::vector<Endpoint> endpoints,
        ServiceDiscoveryConfig cfg = {});

    Task<void> run();
    void shutdown();
    std::vector<Endpoint> endpoints() const;
    Channel<EndpointEvent, 64>& events();
};
```

## 指标

`#include "ultranet/metrics/registry.hpp"` · `ynet::metrics`

```cpp
class MetricRegistry {
    static MetricRegistry& instance();
    Counter* counter(const std::string& name, const std::string& help = "");
    Gauge* gauge(const std::string& name, const std::string& help = "");
    Histogram* histogram(const std::string& name, const std::string& help = "",
        std::vector<double> buckets = Histogram::default_buckets());
    std::string to_prometheus_text() const;
    void reset();
};
```

## 配置

`#include "ultranet/config/config.hpp"` · `ynet::config`

```cpp
struct UltraNetConfig {
    io::IoUringEngineConfig io_uring;
    PoolConfig pool;
    ConnectionPoolConfig connection_pool;
    LogConfig log;
    CircuitBreakerConfig circuit_breaker;
    RetryConfig retry;
    ServiceDiscoveryConfig service_discovery;

    static UltraNetConfig from_env();
};
```

## 日志

`#include "ultranet/log/logger.hpp"` · `ynet::log`

```cpp
enum class Level { Trace, Debug, Info, Warn, Error, Critical };

class ILogger {
    virtual void log(Level level, const std::string& msg) = 0;
    virtual void flush() = 0;
};

// 宏
ULTRA_LOG_TRACE(fmt, ...)
ULTRA_LOG_DEBUG(fmt, ...)
ULTRA_LOG_INFO(fmt, ...)
ULTRA_LOG_WARN(fmt, ...)
ULTRA_LOG_ERROR(fmt, ...)
ULTRA_LOG_CRITICAL(fmt, ...)
```

## 计时器

`#include "ultranet/io/timer.hpp"` · `ynet::async::io`

```cpp
inline SleepAwaitable sleep_for(std::chrono::nanoseconds duration) noexcept;
inline SleepAwaitable sleep_until(steady_clock::time_point deadline) noexcept;
```

用法：

```cpp
co_await io::sleep_for(std::chrono::milliseconds(100));
```

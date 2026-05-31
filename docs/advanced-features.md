# 高级特性

## Channel — 协程间通信

`Channel<T, Capacity>` 是协程友好的生产者-消费者通道，基于环形缓冲区 + spinlock。
与传统的 `std::mutex` + `std::condition_variable` 不同，Channel 在满/空时**挂起协程而非阻塞线程**。

**头文件**: `#include "ultranet/coroutine/channel.hpp"`
**命名空间**: `ynet::async`

### 基本用法

```cpp
Channel<std::string, 256> ch;

// 生产者协程
Task<void> producer() {
    co_await ch.write("message 1");
    co_await ch.write("message 2");
    ch.close(); // 通知消费者结束
}

// 消费者协程
Task<void> consumer() {
    while (auto msg = co_await ch.read()) {
        // *msg 是 std::string
        process(*msg);
    }
    // read() 返回 nullopt → Channel 已关闭且为空
}
```

### API

```cpp
template <typename T, size_t Capacity = 256>
class Channel {
    // 阻塞写入（满时挂起协程）
    WriteAwaitable write(T value) noexcept;

    // 非阻塞写入（满时返回 false，关闭时返回 false）
    bool try_write(T value) noexcept;

    // 阻塞读取（空时挂起协程）
    ReadAwaitable read() noexcept;

    // 非阻塞读取
    std::optional<T> try_read() noexcept;

    void close() noexcept;     // 唤醒所有等待者
    bool is_closed() const noexcept;
    size_t size() const noexcept;
    bool empty() const noexcept;
    bool full() const noexcept;
    size_t capacity() const noexcept;
};
```

### 多生产者示例

```cpp
Channel<int, 64> results;

// 5 个生产者并发写
for (int i = 0; i < 5; i++) {
    pool.submit([]() -> Task<void> {
        for (int j = 0; j < 100; j++) {
            co_await results.write(j);
        }
    }().release());
}

// 1 个消费者汇总
Task<void> collector() {
    int sum = 0;
    while (auto v = co_await results.read()) {
        sum += *v;
    }
    std::cout << "Total: " << sum << std::endl;
}
```

## when_all / when_any — 结构化并发

并发执行多个 Task 并等待它们全部完成（`when_all`）或任一完成（`when_any`）。

**头文件**: `#include "ultranet/coroutine/when_all.hpp"`
**命名空间**: `ynet::async`

### when_all — 等待全部完成

```cpp
Task<int> fetch_a() { co_return 42; }
Task<std::string> fetch_b() { co_return "hello"; }

Task<void> example() {
    // 并发执行两个 task，等待全部完成
    auto [a, b] = co_await when_all(fetch_a(), fetch_b());
    // a = 42 (int), b = "hello" (std::string)
}
```

### when_any — 等待首个完成

```cpp
Task<int> slow() { co_await sleep_for(5s); co_return 1; }
Task<int> fast() { co_return 2; }

Task<void> example() {
    auto [index, results] = co_await when_any(slow(), fast());
    // index = 1 (fast 先完成，0-based)
    // std::get<1>(results) 包含 "fast" 的结果
}
```

### 注意事项

- 所有 task 通过调度器并发提交
- `when_all`：任一 task 抛异常时，首个异常被传播
- `when_any`：只有获胜 task 的结果有效
- Task 类型必须可移动

## with_retry — 自动重试

**头文件**: `#include "ultranet/coroutine/retry.hpp"`
**命名空间**: `ynet::async`

### 指数退避策略

```cpp
ExponentialBackoff backoff(
    std::chrono::milliseconds(100),  // base_delay
    std::chrono::seconds(5),         // max_delay
    3,                               // max_retries
    0.2                              // jitter_factor (±20%)
);
// delay_for(0) ≈ 100ms ± 20ms
// delay_for(1) ≈ 200ms ± 40ms
// delay_for(2) ≈ 400ms ± 80ms
```

### 使用示例

```cpp
// 工厂函数返回 Task<T>
auto result = co_await with_retry(
    []() -> Task<std::string> {
        auto sock = co_await TcpSocket::connect("db.internal", 5432, 2s);
        co_return "connected";
    },
    ExponentialBackoff{},
    // 只重试可恢复的错误
    [](const std::error_code& ec) {
        return is_refused(ec) || is_timeout(ec);
    }
);
```

### 重试条件

默认的 `should_retry` 对所有 `std::system_error` 返回 true。
建议根据业务场景定制：

```cpp
// 只重试临时性错误，不重试永久性错误
auto should_retry = [](const std::error_code& ec) {
    return is_refused(ec) ||      // ECONNREFUSED (服务未就绪)
           is_timeout(ec) ||      // ETIMEDOUT
           is_retryable(ec);      // EAGAIN, EINTR
};
```

## CircuitBreaker — 断路器

防止级联故障：当下游服务连续失败时自动 "断开"，快速失败而非反复尝试。

**头文件**: `#include "ultranet/coroutine/circuit_breaker.hpp"`
**命名空间**: `ynet::async`

### 状态机

```
        连续失败 ≥ threshold
  Closed ─────────────────────> Open
    ↑                            │
    │      超时到期               │
    │    ┌───────────────────────┘
    │    ↓
  HalfOpen ── 单次探测 ──→ Closed (成功)
    │
    └── 探测失败 ──→ Open
```

### 基本用法

```cpp
CircuitBreaker cb({
    .failure_threshold = 5,       // 连续 5 次失败 → Open
    .open_timeout = 30s,          // 30 秒后进入 HalfOpen
});

// 在调用下游服务前
if (!cb.try_acquire()) {
    // 断路器开路，快速失败
    co_return error_response;
}

try {
    auto result = co_await call_downstream();
    cb.on_success(); // 成功，HalfOpen → Closed
    co_return result;
} catch (...) {
    cb.on_failure(); // 失败，可能触发 Closed → Open
    throw;
}
```

### 配置

```cpp
config::CircuitBreakerConfig cfg;
cfg.failure_threshold = 5;       // 连续失败多少次后开路
cfg.open_timeout = 30s;          // 开路后多久尝试半开

CircuitBreaker cb(cfg);
```

### 状态查询

```cpp
cb.state();                   // CircuitState::Closed/Open/HalfOpen
cb.snapshot();                // Stats{successes, failures, fast_fails, ...}
cb.reset();                   // 重置到 Closed 状态
```

## ShutdownCoordinator — 优雅关闭

**头文件**: `#include "ultranet/lifecycle/shutdown.hpp"`
**命名空间**: `ynet::async::lifecycle`

### 基本用法

```cpp
ShutdownCoordinator shutdown;
shutdown.install_signal_handlers(); // 注册 SIGINT/SIGTERM

// 在工作循环中
while (!shutdown.is_shutdown()) {
    // 接受连接或处理请求
}

// 三阶段关闭
shutdown.shutdown();                     // Running → Draining
shutdown.advance_phase(ShutdownPhase::Complete);

// 协程可等待关闭信号
auto result = co_await shutdown.wait();  // nullopt = 已关闭
```

### 与 Channel 的区别

`ShutdownCoordinator` 内部使用 `Channel<bool, 1>` 作为信号广播机制。
`shutdown()` 调用 `close()`，唤醒所有等待者。

## DNS 解析

**头文件**: `#include "ultranet/net/dns.hpp"`
**命名空间**: `ynet::async::io`

### 主机名解析

```cpp
// 解析 A 记录
auto ips = co_await resolve_host("api.example.com", 5s);
// 返回 std::vector<std::string>
for (const auto& ip : ips) {
    std::cout << ip << std::endl; // "10.0.1.5", "10.0.1.6", ...
}

// DNS 查询使用 io_uring UDP，读取 /etc/resolv.conf 获取 nameserver
// fallback: 8.8.8.8
```

### SRV 记录解析

```cpp
// 解析 SRV 记录（服务发现）
auto records = co_await resolve_srv("http", "tcp", "example.com", 5s);
// 返回 std::vector<SrvRecord>
for (const auto& r : records) {
    std::cout << r.target << ":" << r.port
              << " (priority=" << r.priority
              << ", weight=" << r.weight << ")" << std::endl;
}
```

## ConnectionPool — 连接池

**头文件**: `#include "ultranet/net/connection_pool.hpp"`
**命名空间**: `ynet::async::net`

### 基本用法

```cpp
ConnectionPool pool(
    {.max_connections = 32}, "db.example.com", 5432);

// 获取连接（阻塞式，Channel 内等待）
TcpSocket conn = co_await pool.acquire();

// 使用连接...
co_await conn.write(query, len);
auto n = co_await conn.read(buf, sizeof(buf));

// 归还连接
pool.release(std::move(conn));

// 标记无效连接（不归还）
pool.invalidate(std::move(broken_conn));
```

### 配置

```cpp
config::ConnectionPoolConfig cfg;
cfg.max_connections = 64;
cfg.idle_timeout = 60s;
cfg.connect_timeout = 5s;

ConnectionPool pool(cfg, "host", port);
```

## ServiceDiscovery — 服务发现

**头文件**: `#include "ultranet/net/service_discovery.hpp"`
**命名空间**: `ynet::async::discovery`

### 静态端点列表

```cpp
std::vector<Endpoint> endpoints = {
    {"host1.internal", 8080, 1, 10},  // host, port, priority, weight
    {"host2.internal", 8080, 1, 10},
};

ServiceDiscovery sd(std::move(endpoints));

// 启动后台刷新和健康检查
pool.submit(sd.run().release());

// 获取健康端点（排除开路断路器）
auto healthy = sd.endpoints();

// 订阅变更事件
auto event = co_await sd.events().read();
```

### DNS SRV 后端

```cpp
ServiceDiscovery sd("http", "tcp", "service.internal", cfg);
pool.submit(sd.run().release());
```

## 指标 (Metrics)

**头文件**: `#include "ultranet/metrics/registry.hpp"`
**命名空间**: `ynet::metrics`

### Counter / Gauge / Histogram

```cpp
auto& reg = MetricRegistry::instance();

// Counter — 只增
auto* req_total = reg.counter("http_requests_total", "Total HTTP requests");
req_total->inc();
req_total->inc(5); // 增加任意值

// Gauge — 可增可减
auto* active = reg.gauge("active_connections", "Current connections");
active->inc();  // 连接打开
active->dec();  // 连接关闭

// Histogram — 分布统计
auto* latency = reg.histogram("request_latency_seconds",
    "Request latency", {0.001, 0.01, 0.1, 1.0, 10.0});
latency->observe(0.05); // 50ms

// 查询
latency->p50();   // 中位数
latency->p99();   // P99
latency->count(); // 样本数

// 导出 Prometheus 格式
std::string metrics = reg.to_prometheus_text();
```

## 日志

**头文件**: `#include "ultranet/log/logger.hpp"`

```cpp
// 使用默认 logger（stderr）
set_logger(std::make_shared<StdoutLogger>());

// 宏（自动包含文件名、行号、函数名）
ULTRA_LOG_INFO("Server started on port {}", port);
ULTRA_LOG_WARN("Slow request: {}ms", latency);
ULTRA_LOG_ERROR("Connection failed: {}", ec.message());

// 设置日志级别
get_logger()->set_level(Level::Debug);
```

## 配置

**头文件**: `#include "ultranet/config/config.hpp"`

```cpp
// 从环境变量加载
auto cfg = UltraNetConfig::from_env();

// 手动配置
UltraNetConfig cfg;
cfg.pool.num_threads = 4;
cfg.io_uring.entries = 2048;
cfg.io_uring.max_pending_ops = 512;
cfg.circuit_breaker.failure_threshold = 10;
cfg.retry.max_retries = 5;
```

### 环境变量

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `ULTRANET_POOL_THREADS` | hardware_concurrency() | 线程数 |
| `ULTRANET_IO_ENTRIES` | 1024 | io_uring SQ/CQ 条目数 |
| `ULTRANET_MAX_PENDING_OPS` | 256 | 背压水位线 |
| `ULTRANET_CB_THRESHOLD` | 5 | 断路器失败阈值 |
| `ULTRANET_RETRY_MAX` | 3 | 最大重试次数 |
| `ULTRANET_LOG_LEVEL` | info | 日志级别 |

## 相关文档

- [TCP 编程指南](tcp-guide.md)
- [WebSocket 编程指南](websocket-guide.md)
- [API 参考](api-reference.md)

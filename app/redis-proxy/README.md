# Redis Protocol Proxy

基于 ultra-net 的 Redis 协议代理，支持 RESP 和 INLINE 两种命令格式，
透明转发到后端 Redis 实例。

## 构建

```bash
cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 redis-proxy
```

## 使用

```bash
# 默认：代理监听 :6380，后端 127.0.0.1:6379
./bin/redis-proxy

# 自定义端口和后端
./bin/redis-proxy 6380 192.168.1.100 6379
```

## 功能测试

```bash
# 启动后端 Redis
redis-server --port 6379 --daemonize yes

# 启动代理
./bin/redis-proxy 6380 127.0.0.1 6379 &

# 通过代理操作 Redis
redis-cli -p 6380 PING      # → +PONG
redis-cli -p 6380 SET k v   # → +OK
redis-cli -p 6380 GET k     # → "v"
redis-cli -p 6380 INCR cnt  # → :1
```

## 性能数据

测试环境：2 核 1.6GB，本地 loopback，redis-benchmark -c 50 -n 100000

| 命令 | 直连 Redis | 通过代理 | 比率 |
|------|-----------|---------|------|
| PING | 62,578 rps | 34,795 rps | 55.6% |
| SET | 61,425 rps | 35,701 rps | 58.1% |
| GET | 61,728 rps | 35,945 rps | 58.2% |
| INCR | 62,539 rps | 35,436 rps | 56.7% |
| LPUSH | — | 35,112 rps | — |
| MSET(10) | — | 34,060 rps | — |

### 延迟分布（GET，c=50）

| 指标 | 直连 | 代理 |
|------|------|------|
| p50 | 0.38 ms | 0.93 ms |
| p95 | 0.99 ms | 2.09 ms |
| p99 | 1.37 ms | 2.68 ms |

### 延迟随并发度变化（GET）

| 并发 | p50 | p99 | QPS |
|------|-----|-----|-----|
| c=1 | 0.08 ms | 0.20 ms | 11,151 |
| c=10 | 0.21 ms | 0.70 ms | 31,847 |
| c=50 | 0.93 ms | 3.11 ms | 35,689 |
| c=100 | 1.75 ms | 6.38 ms | 36,523 |

### 资源占用

- 进程 RSS：**7.4 MB**（50 并发连接下）
- 每连接内存：约 148 KB（含协程帧 + 读写 buffer + 后端 TCP 连接）

## 架构

```
redis-cli ──TCP──> Proxy (:6380) ──TCP──> Redis (:6379)
                       │
              ┌────────┴────────┐
              │  RESP / INLINE   │
              │  parser          │
              ├─────────────────┤
              │  per-session     │
              │  coroutine       │
              │  (read→parse→    │
              │   fwd→recv→fwd)  │
              ├─────────────────┤
              │  1 persistent    │
              │  backend conn    │
              │  per client      │
              └─────────────────┘
```

- 每个客户端连接由一个独立协程处理
- 每个客户端会话维护一条到后端 Redis 的持久 TCP 连接
- 支持命令流水线（pipelining）：客户端可在一个 TCP segment 中发送多个命令
- 同时支持 RESP 格式（`*2\r\n$3\r\nGET\r\n...`）和 INLINE 格式（`GET key\r\n`）

## 文件

| 文件 | 说明 |
|------|------|
| `main.cc` | 入口点，参数解析 |
| `proxy.hpp` | 代理核心：accept 循环 + 会话协程 |
| `resp.hpp` | RESP/INLINE 解析器 + 响应边界检测 + 编码器 |

# Ultra-Net Actor Framework — 使用手册

> 配套示例: `actor_hello` (基础), `actor_multi` (多Actor), `actor_async` (co_await),
> `actor_ps` (参数服务器), `actor_train` (分布式训练)
> 构建: `cd build && make actor_hello actor_multi actor_async actor_ps actor_train`

## 概述

Ultra-Net Actor 框架将任意 C++ 类转化为有状态的异步 Actor，所有方法调用自动排队串行执行，无需加锁。

**核心特性**:
- 一行代码将类变为 Actor
- 基于 ultra-net `WorkStealingThreadPool` 的高性能调度
- 多邮箱支持（按优先级/类型分类）
- 零拷贝消息传递
- 为分布式 AI 训练设计（Parameter Server 模式）

## 快速开始

```cpp
#include "ultranet/actor/api.h"
using namespace ynet::actor;

// 1. 定义 Actor 类（普通 C++ 类即可）
struct counter {
    int count = 0;
    void add(int x) { count += x; }
    int get() const { return count; }
};

int main() {
    // 2. 创建 Actor 系统（线程池）
    actor_system system(4);  // 4 个工作线程

    // 3. 生成 Actor
    auto ref = spawn<counter>(system);

    // 4. 发送消息（异步，线程安全）
    ref.send<&counter::add>(10);
    ref.send<&counter::add>(20);

    // 5. 等待消息处理
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    return 0;
}
```

## 核心概念

### Actor 系统 (`actor_system`)

`actor_system` 管理线程池和 Actor 生命周期：

```cpp
actor_system system(8);           // 8 个工作线程
system.schedule([] { ... });      // 提交任务到线程池
system.shutdown();                // 等待所有任务完成
```

### Actor 配置 (`actor_config`)

```cpp
actor_config cfg;
cfg.name = "my_actor";                          // 调试用名称
cfg.max_messages_per_activation = 128;           // 每次激活最多处理消息数
cfg.mailbox_configs = {
    mailbox_config{.capacity = 4096},            // 默认邮箱
    mailbox_config{.capacity = 256},             // 高优先级邮箱
};

auto ref = spawn<my_actor>(system, std::move(cfg), constructor_args...);
```

### Actor 引用 (`actor_ref<T>`)

```cpp
actor_ref<counter> ref = spawn<counter>(system);

ref.send<&counter::add>(42);      // 发送消息（fire-and-forget）
bool ok = ref.is_valid();         // 检查引用有效性
uint64_t id = ref.id();           // 获取 Actor ID
```

### 邮箱系统 (`mailbox_set`)

每个 Actor 可配置多个邮箱，按索引区分。消息推送到指定邮箱索引，
Actor 按顺序轮询处理。

```cpp
// 配置两个邮箱
cfg.mailbox_configs = {mailbox_config{}, mailbox_config{}};
auto ref = spawn<my_actor>(system, cfg);

// 发送到默认邮箱（索引 0）
ref.send<&my_actor::handle_normal>(data);

// 发送到高优先级邮箱（索引 1）— 需要扩展 API
```

## 协程支持 (co_await)

Actor 方法通过 `call()` 返回 `Task<T>`，可以自然地用 `co_await` 等待结果：

```cpp
Task<void> orchestrate(actor_ref<calculator> calc) {
    // co_await 等待 Actor 方法返回结果 — 读起来像同步代码
    int sum = co_await calc.call<&calculator::add>(10, 20);
    int product = co_await calc.call<&calculator::multiply>(sum, 3);
    std::string greeting = co_await calc.call<&calculator::greet>("World");
}
```

`call()` 与 `send()` 的区别：
- `send()`: 异步发送，不等待结果（fire-and-forget）
- `call()`: 返回 `Task<T>`，通过 `co_await` 等待结果

> 示例: `docs/examples/actor_async.cc`

## 分布式支持

### 架构

```cpp
// 进程 A (server)
auto sys = make_distributed(local_system, 17001, {.seed_nodes = {"127.0.0.1:17002"}});
sys.start_serving(shutdown);
auto ref = spawn<my_actor>(sys.local());
// ref 现在可通过网络被其他进程访问

// 进程 B (client)
auto sys = make_distributed(local_system, 17002, {.seed_nodes = {"127.0.0.1:17001"}});
sys.start_serving(shutdown);
// 通过 gossip 协议自动发现进程 A 的节点和 actor
```

分布式组件：

| 组件 | 文件 | 功能 |
|------|------|------|
| `cluster` | `dist/cluster.h` | Gossip 协议节点发现 |
| `tcp_transport` | `dist/transport.h` | io_uring TCP 消息传输 |
| `remote_actor_system` | `dist/remote.h` | 远程 Actor 代理路由 |
| `serialization` | `dist/serialization.h` | 二进制消息序列化 |

### 集群配置

```cpp
dist::cluster_config cfg;
cfg.listen_addr = "0.0.0.0";
cfg.listen_port = 17001;
cfg.seed_nodes = {"192.168.1.100:17001"};  // 初始联络节点
cfg.gossip_interval_ms = 500;               // gossip 间隔
cfg.heartbeat_timeout_ms = 3000;             // 心跳超时
```

## 高级用法

### 多个 Actor 协同工作

```cpp
struct worker {
    int id;
    void process(int job_id) {
        std::cout << "worker " << id << " processing job " << job_id << std::endl;
    }
};

actor_system system(8);
auto w1 = spawn<worker>(system, actor_config{}, 1);
auto w2 = spawn<worker>(system, actor_config{}, 2);

// 并发发送（线程安全）
std::thread t1([&] { for (int i=0; i<1000; ++i) w1.send<&worker::process>(i); });
std::thread t2([&] { for (int i=0; i<1000; ++i) w2.send<&worker::process>(i); });
t1.join(); t2.join();
```

### 分布式训练 — Parameter Server

```cpp
struct parameter_server {
    std::vector<float> weights;

    void update_gradients(int layer, std::vector<float> grads) {
        for (size_t i = 0; i < grads.size(); ++i)
            weights[layer * 256 + i] -= 0.01f * grads[i];
    }

    std::vector<float> get_weights(int layer) {
        return std::vector<float>(weights.begin() + layer * 256,
                                   weights.begin() + (layer + 1) * 256);
    }
};

struct training_worker {
    actor_ref<parameter_server> ps;
    std::vector<float> local_weights;

    void train_step(int layer, std::span<const float> batch) {
        auto grads = compute_gradients(local_weights, batch);
        ps.send<&parameter_server::update_gradients>(layer, std::move(grads));
    }
};
```

## API 参考

### `actor_system`

| 方法 | 说明 |
|------|------|
| `actor_system(size_t n)` | 创建 n 个工作线程 |
| `schedule(fn)` | 提交任意函数到线程池 |
| `shutdown()` | 等待所有任务完成 |
| `is_stopping()` | 是否正在关闭 |

### `actor<T>`

| 方法 | 说明 |
|------|------|
| `push_message(msg*, idx)` | 推送消息到指定邮箱 |
| `pull_and_run()` | 处理邮箱中所有消息（内部调用） |
| `get()` | 获取底层用户类指针 |

### `actor_ref<T>`

| 方法 | 说明 |
|------|------|
| `send<&Method>(args...)` | 异步发送消息到 Actor |
| `is_valid()` | 引用是否有效 |
| `id()` | 获取 Actor 全局唯一 ID |

### 自由函数

| 函数 | 说明 |
|------|------|
| `spawn<T>(system, cfg, args...)` | 创建新 Actor |
| `make_message<T, Method>(instance, args...)` | 构造消息对象 |

## 设计原理

### 消息处理流程

```
sender thread                actor_system              actor mailbox
    │                             │                        │
    ├─ ref.send<&T::m>(args) ──→  │                        │
    │                             │                        │
    │                    make_message<T, &T::m>(args)      │
    │                             │                        │
    │                    actor->push_message(msg) ──────→  │
    │                             │                   m_pending++
    │                             │                   try_activate()
    │                             │                        │
    │                    schedule(pull_and_run) ←──────────┤
    │                             │                        │
    │                    [worker thread]                    │
    │                    pull_and_run():                    │
    │                      while msg = try_pop():          │
    │                        msg->run()                    │
    │                      m_pending -= executed           │
    │                      if pending > 0:                 │
    │                        try_activate()  ← 自驱动      │
```

### CAS 激活保证

```
try_activate():
    if CAS(m_activated, false → true):
        schedule(pull_and_run)   ← 只有一个线程会成功

pull_and_run():
    ... process messages ...
    m_activated = false
    if m_pending > 0:           ← 处理期间新消息到达
        try_activate()           ← 重新激活
```

### 线程安全

- **生产者**: 任意线程调用 `ref.send()` → `push_message()` → MPSC 队列
- **消费者**: 单个 worker 线程执行 `pull_and_run()`，串行处理
- **激活**: CAS 原子操作保证只有一个执行实例

## 性能基准

| 场景 | 消息数 | 线程数 | 耗时 | 吞吐量 |
|------|--------|--------|------|--------|
| 单 Actor 1K | 1,000 | 4 | <100ms | >10K msg/s |
| 单 Actor 10K | 10,000 | 4 | <500ms | >20K msg/s |
| 多 Actor 并发 | 4×1000 | 4 | <100ms | >40K msg/s |

> 测试环境: WSL2, 2核, ultra-net WorkStealingThreadPool

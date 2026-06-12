# Ultra-Net Actor 框架：概念详解与开发指南

> 状态: v2 Phase 4 (集群+传输集成完成)
> 测试覆盖率: 66 项测试，100% 通过（6 个测试套件）
> 构建: `cd build && make actor_v2_test actor_message_test actor_cluster_unit_test actor_mailbox_test actor_serialization_test actor_system_integration_test`

## 概述

Ultra-Net Actor 框架将任意 C++ 类转化为有状态的异步 Actor，所有底层通信细节由框架处理。

**核心特性**:
- 侵入式 (`class T : public actor<T>`) 和非侵入式 (普通类自动包装) 两种模式
- 类型安全的消息处理器注册 (`register_handler<Msg>()`)
- 位置透明的 `actor_ref<T>`（本地零拷贝投递，远程自动序列化+网络发送）
- **异步 mailbox** 调度：CAS 激活 + 自驱动，基于 WorkStealingThreadPool
- **分布式能力**: `remote_proxy` 跨节点消息代理 + binary 序列化
- **自动集成**: `actor_system` 构造即启动 transport serve + gossip 周期
- 基于 io_uring + C++20 协程的高性能网络传输
- Gossip 协议的去中心化集群发现

**技术栈**: C++20 coroutine + io_uring + WorkStealingThreadPool

**⚠️ 当前限制**: 消息类型必须是 trivially copyable（不含 `std::string`、`std::vector` 等堆分配成员）。框架使用 `memcpy` 传递消息负载，非平凡类型会触发 use-after-free。

---

## 目录

1. [概念篇](#概念篇初学者视角)
   - [什么是 Actor](#11-什么是-actor)
   - [Actor URI](#12-什么是-actor-uri)
   - [ActorRef（Actor 引用）](#13-什么是-actorrefactor-引用)
   - [Actor System](#14-什么是-actor-system)
   - [Gossip 集群发现](#15-什么是-gossip-集群发现)
   - [TCP Transport](#16-什么是-tcp-transport)
   - [消息处理全链路](#17-消息处理流程全链路)
   - [侵入式 vs 非侵入式](#18-侵入式-vs-非侵入式-actor)
   - [Type Hash 消息识别](#19-type-hash--消息类型识别)
2. [开发指南](#第二部分开发指南应用开发者视角)
3. [架构演进 Phase 2–4](#第三部分架构演进-phase-24)
4. [注意事项与陷阱](#第四部分注意事项与陷阱)
   - [最小示例](#21-最小可用示例)
   - [创建 Actor System](#22-创建-actor-system)
   - [创建和查找 Actor](#23-创建和查找-actor)
   - [发送消息](#24-发送消息)
   - [注册消息处理器](#25-注册消息处理器)
   - [URI 操作](#26-uri-操作)
   - [集群和 Gossip](#27-集群和-gossip)
   - [TCP Transport](#28-tcp-transport-使用)
   - [Metrics Exporter](#29-metrics-exporter指标导出)
   - [dist-bench 实际应用](#210-实际应用dist-bench-分布式训练基准测试)
3. [架构演进 Phase 2–4](#第三部分架构演进-phase-24)
   - [Phase 2: Mailbox + 异步调度](#phase-2-mailbox--异步调度)
   - [Phase 3: 序列化 + 远程代理](#phase-3-序列化--远程代理)
   - [Phase 4: Cluster + Transport 集成](#phase-4-cluster--transport-集成)
4. [注意事项与陷阱](#第四部分注意事项与陷阱)
   - [当前状态](#31-当前状态v2-phase-4集群传输集成完成66-项测试-100-通过)
   - [已知陷阱](#32-已知陷阱)
   - [编码规范](#33-编码规范来自-baseskill)
   - [测试](#34-测试)

---

## 概念篇（初学者视角）

### 1.1 什么是 Actor？

**核心概念**：Actor = 一个拥有自己状态的单线程执行单元。

```
┌─────────────────────────┐
│     Actor "pinger-1"     │
│                          │
│   private state:         │
│     int m_count = 0;     │
│     string m_last;       │
│                          │
│   handlers:              │
│     on_ping(msg) { ... } │  ← 消息处理函数
│                          │
│   mailbox:               │
│     [msg1] [msg2] [msg3] │  ← 消息排队等待处理
└─────────────────────────┘
```

**关键特性**：
- **串行执行**：同一 Actor 的消息不会并发处理，你无需加锁
- **消息驱动**：Actor 之间只通过消息通信，不共享内存
- **位置透明**：发送者不需要知道接收者在哪个机器上

**代码对应**：`include/ultranet/actor/core/base_actor.h`

```cpp
// actor_base — 所有 actor 的基类
class actor_base {
    actor_uri m_uri;                    // 全局唯一标识
    actor_system* m_system = nullptr;   // 所属系统
    std::unordered_map<uint64_t, message_handler_t> m_handlers;  // 消息处理器表

    // 注册消息处理器：告诉框架 "我能处理这种消息"
    template <typename Msg>
    void register_handler(std::function<void(const Msg&)> handler);

    // 投递消息：框架调用，用户不直接使用
    virtual void deliver(uint64_t msg_type, const void* data, size_t len);
};

// CRTP 模板类
template <typename Derived>
class actor : public actor_base {
    // 你的 actor 继承这个，获得框架能力
};
```

**两种定义 Actor 的方式**：

方式一 — 侵入式（继承 `actor<T>`）：
```cpp
class echo_actor : public actor<echo_actor> {  // CRTP 模式
public:
    int m_count = 0;
    void on_ping(ping_msg& m) { m_count++; }  // 你的业务逻辑
};
```

方式二 — 非侵入式（普通类自动包装）：
```cpp
struct plain_counter {     // 不继承任何东西
    int count = 0;
    void add(int x) { count += x; }
};
// 框架自动用 actor_adapter<T> 包装（参见 actor_system.h:41-48）
```

---

### 1.2 什么是 Actor URI？

每个 Actor 在整个集群中有一个全局唯一的地址：

```
ultra://<node_id>/<type_name>/<actor_name>
```

例如：`"ultra://node-1/echo_actor/pinger-1"` 表示在 `node-1` 上运行的类型为 `echo_actor`、名称为 `pinger-1` 的 Actor。

当 node 未知（本地系统中的 Actor），使用 `*` 通配符：`"ultra://*/echo_actor/pinger-1"`

**代码对应**：`include/ultranet/actor/core/actor_uri.h`

```cpp
struct actor_uri {
    std::string node;    // "*" = any/local, 否则为 node_id
    std::string type;    // 类型名（使用 typeid 生成）
    std::string name;    // 用户指定的名称

    static actor_uri make_local(const std::string& type_name, const std::string& actor_name);
    static actor_uri make(uint64_t node_id, const std::string& type_name, const std::string& actor_name);
    std::string to_string() const;  // "ultra://node/type/name"
};
```

---

### 1.3 什么是 ActorRef（Actor 引用）？

`actor_ref<T>` 是你持有 Actor 的"句柄"。它：

- 是一个**轻量级的值类型**（可拷贝、移动）
- **隐藏了本地/远程的区别**（位置透明）
- 提供 `send(msg)` 方法发送消息
- 可以是无效的（`is_valid() == false`），表示未找到

```
┌──────────────────┐        ┌─────────────────┐
│   actor_ref<T>   │───────▶│  actor_proxy    │  (内部代理)
│                  │        │                  │
│  - m_proxy       │        │  本地: local_actor_proxy  → 直接投递到 handler
│  - m_uri         │        │  远程: remote_actor_proxy → 序列化 → 网络发送
└──────────────────┘        └─────────────────┘
```

**代码对应**：`include/ultranet/actor/core/actor_ref.h`

```cpp
template <typename T>
class actor_ref {
public:
    bool is_valid() const;
    const actor_uri& uri() const;
    std::string name() const;

    // 发送消息（fire-and-forget，不等待回复）
    template <typename Msg>
    void send(const Msg& msg) {
        uint64_t hash = actor_type_hash<Msg>();  // 编译期计算类型 hash
        m_proxy->deliver(hash, &msg, sizeof(msg));  // 通过代理投递
    }
private:
    std::shared_ptr<actor_proxy> m_proxy;
    actor_uri m_uri;
};
```

`actor_proxy` 抽象接口：

```cpp
class actor_proxy {
public:
    virtual void deliver(uint64_t msg_type, const void* data, size_t len) = 0;
    virtual const actor_uri& uri() const = 0;
};
```

当前实现中，`local_actor_proxy`（本地代理）直接将消息投递到 actor 的 `deliver()` 方法，**零序列化开销**。

---

### 1.4 什么是 Actor System？

`actor_system` 是整个 Actor 框架的**唯一入口**。一个进程中通常只有一个实例。

```
┌─────────────── actor_system ──────────────────────────┐
│                                                        │
│  ┌────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Thread Pool │  │   Registry   │  │   Transport   │  │
│  │ (io_uring   │  │ actor_uri →  │  │ (TCP accept/  │  │
│  │  work       │  │ actor_proxy  │  │  connect)     │  │
│  │  stealing)  │  │   映射表     │  │               │  │
│  └────────────┘  └──────────────┘  └───────────────┘  │
│                                                        │
│  Public API:                                           │
│    spawn<T>(name, args...) → actor_ref<T>              │
│    find<T>(uri_str)        → actor_ref<T>              │
│    schedule(fn)            → void                      │
│    run() / shutdown()      → void                      │
└────────────────────────────────────────────────────────┘
```

**代码对应**：`include/ultranet/actor/system/actor_system.h`

```cpp
class actor_system {
public:
    explicit actor_system(const system_config& cfg = {});

    template <typename T, typename... Args>
    actor_ref<T> spawn(const std::string& name, Args&&... args);

    template <typename T>
    actor_ref<T> find(const std::string& uri_str);

    void schedule(std::function<void()> fn);
    void run();
    void shutdown();

private:
    system_config m_cfg;
    std::unique_ptr<WorkStealingThreadPool> m_pool;
    std::unordered_map<std::string, std::shared_ptr<actor_proxy>> m_registry;
    std::vector<std::unique_ptr<actor_base>> m_owned_actors;
    std::mutex m_mutex;
};
```

**配置**：

```cpp
struct system_config {
    size_t num_threads = 4;              // io_uring 线程数
    uint16_t listen_port = 0;            // TCP 监听端口（0=自动分配）
    std::string node_name = "default";   // 本节点名称
    std::vector<std::string> seed_nodes;  // 种子节点列表
};
```

---

### 1.5 什么是 Gossip 集群发现？

在分布式系统中，所有节点需要知道彼此的存在。Gossip 协议是一种去中心化的方式：

```
时间线:
  Node A                      Node B                      Node C
  ──────                      ──────                      ──────
  [自己: A]                   [自己: B]                   [自己: C]

  ──gossip(A)──▶             B 记录 A
                              ──gossip(B, A)──▶          C 记录 A, B
                              ◀──gossip(C, A, B)──       B 记录 C

  最终所有节点都知道彼此
```

**核心机制**：
- 每个节点周期性地随机选择几个对等节点，发送自己已知的节点列表
- 收到 gossip 消息的节点合并信息，并继续传播
- 心跳超时检测：如果 N 秒内未收到某节点的 gossip，标记为疑似宕机

**代码对应**：`include/ultranet/actor/net/cluster.h`

```cpp
class cluster {
public:
    std::vector<uint8_t> build_gossip();  // 构建 gossip 消息（二进制编码）
    void apply_gossip(const uint8_t* data, size_t length);  // 应用收到的 gossip
    std::vector<node_info> live_nodes(uint64_t timeout_ms = 3000);  // 存活节点
    void mark_seen(node_id_t id, const std::string& addr);  // 标记存活
    void add_seed(const std::string& addr);  // 种子节点
};
```

Gossip 消息的二进制格式：

```
┌─────────────┬──────────────┬─────────────────────────────────┐
│ sender_id   │ node_count   │  node_entry × N                 │
│ (uint64_t)  │ (uint32_t)   │  id(u64) + addr_len(u32)        │
│             │              │  + addr(str) + last_seen(u64)    │
└─────────────┴──────────────┴─────────────────────────────────┘
```

---

### 1.6 什么是 TCP Transport？

`tcp_transport` 是 Actor 框架的网络层，完全对用户**不可见**。

**功能**：
- 监听 TCP 端口，接受连接
- 自动对每个连接应用 TCP 性能优化
- 使用 **length-prefixed 帧协议** 处理 TCP 粘包/拆包
- 将收到的消息分发给消息处理器

```
发送端:  [4字节长度] [消息载荷]
接收端:  读4字节→知道长度→读完整载荷→dispatch
```

**代码对应**：`include/ultranet/actor/net/transport.h`

关键优化：

```cpp
// apply_tcp_tuning — 每个连接自动调用
inline void apply_tcp_tuning(int fd) {
    int buf_size = 256 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf_size, sizeof(buf_size));  // 256KB 发送缓冲
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf_size, sizeof(buf_size));  // 256KB 接收缓冲
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));         // 禁用 Nagle
}

// recv_buffer — 预分配缓冲区，避免热路径堆分配
struct recv_buffer {
    static constexpr size_t k_default_size = 256 * 1024;
    std::unique_ptr<uint8_t[]> data;  // 启动时一次性分配 256KB
};
```

---

### 1.7 消息处理流程（全链路）

```
  sender 代码                 framework                    receiver actor
  ──────────                  ─────────                    ──────────────
  ref.send(msg)
      │
      ▼
  actor_ref<T>::send()
      │  (1) 计算 type_hash<Msg>()
      │  (2) 调用 m_proxy->deliver(hash, &msg, sizeof(msg))
      ▼
  local_actor_proxy::deliver()
      │  (3) 调用 m_actor->deliver(hash, data, len)
      ▼
  actor_base::deliver()
      │  (4) 在 m_handlers 中查找 hash
      │  (5) 提取 handler 并调用 handler(msg)
      ▼
  你的 on_ping(ping_msg& m)
      │  (6) 你的业务逻辑执行
      ▼
   完成 ✓
```

> **注意**：当前 v2 Phase 1 实现中，消息是**立即同步投递**的（`send()` 直接调用 `deliver()`，未经过 mailbox 队列）。完整的 mailbox + 异步调度将在后续阶段实现。

---

### 1.8 侵入式 vs 非侵入式 Actor

**侵入式（Intrusive）** — 你的类继承 `actor<T>`：

```cpp
class my_actor : public actor<my_actor> {
    // 能访问 actor_base 的所有方法（uri(), system(), register_handler() 等）
};
```

**非侵入式（Non-intrusive）** — 你的类是普通 C++ 类：

```cpp
struct plain_counter {
    int count = 0;
    void add(int x) { count += x; }
};
```

框架使用 `if constexpr` 在编译期分派（`actor_system.h:62-91`）：

```cpp
if constexpr (std::is_base_of_v<actor<T>, T>) {
    // 模式 1：T 已经继承 actor<T>，直接构造
    auto* a = new T(std::forward<Args>(args)...);
} else {
    // 模式 2：T 是普通类，用 actor_adapter<T> 包装
    using Adapted = actor_adapter<T>;   // actor_adapter<T> : public actor<actor_adapter<T>>
    auto* adapted = new Adapted(std::forward<Args>(args)...);  // 内部持有 T 实例
}
```

---

### 1.9 Type Hash — 消息类型识别

框架通过**编译期类型哈希**（FNV-1a 算法）识别消息类型，无需手动注册：

```cpp
template <typename T>
static uint64_t type_hash() {
    const char* name = typeid(T).name();   // 获取 C++ 类型名
    uint64_t h = 14695981039346656037ULL;  // FNV-1a offset basis
    while (*name) {
        h ^= (uint8_t)*name++;
        h *= 1099511628211ULL;              // FNV-1a prime
    }
    return h;
}
```

> **局限**：使用 `typeid(T).name()` 在跨编译器环境可能不一致，这是已知的技术债。

---

## 第二部分：开发指南（应用开发者视角）

### 2.1 最小可用示例

```cpp
#include "ultranet/actor.hpp"
using namespace ynet::actor;

// 1. 定义消息类型
struct ping_msg { int id; std::string text; };

// 2. 定义 Actor
class ping_actor : public actor<ping_actor> {
public:
    int m_count = 0;

    ping_actor() {
        // 注册消息处理器
        register_handler<ping_msg>([this](const ping_msg& m) {
            m_count++;
            std::cout << "Received ping #" << m.id << ": " << m.text << std::endl;
        });
    }
};

// 3. 使用
int main() {
    actor_system system;                                    // 创建系统
    auto ref = system.spawn<ping_actor>("pinger-1");        // 创建 Actor
    ref.send(ping_msg{42, "hello world"});                  // 发送消息
    system.run();                                           // 等待结束
}
```

---

### 2.2 创建 Actor System

```cpp
// 默认配置：4 线程，端口自动分配
actor_system system;

// 自定义配置
system_config cfg;
cfg.num_threads = 8;
cfg.node_name = "ps-node-1";
cfg.listen_port = 9000;
cfg.seed_nodes = {"192.168.1.1:9000", "192.168.1.2:9000"};
actor_system system(cfg);
```

---

### 2.3 创建和查找 Actor

```cpp
// ── 侵入式 Actor ──
class my_actor : public actor<my_actor> {
    // ... 你的代码
};
auto ref = system.spawn<my_actor>("my-name", constructor_args...);

// ── 非侵入式 Actor ──
struct plain_class {
    void do_work(int x) { /* ... */ }
};
auto ref = system.spawn<plain_class>("worker-1");

// ── 查找 Actor（通过完整 URI） ──
auto key = actor_uri::make_local(typeid(my_actor).name(), "my-name").to_string();
auto found = system.find<my_actor>(key);
if (found.is_valid()) {
    found.send(some_message{});
}

// ── 查找不存在的 Actor ──
auto missing = system.find<my_actor>("ultra://*/my_actor/no-such");
// missing.is_valid() == false — 安全检查后不会崩溃
```

**注意事项**：

1. `find()` 需要完整的 URI 字符串（`"ultra://*/type_name/actor_name"`），其中 type_name 是 `typeid(T).name()` 的结果（与编译器相关）
2. `find()` 当前仅支持**本地查找**——远程查找标记为 `// would search cluster` 但尚未实现
3. 非侵入式 Actor 的返回类型是 `actor_ref<plain_class>`，但内部实际存储的是 `actor_adapter<plain_class>`

---

### 2.4 发送消息

```cpp
// Fire-and-forget 发送
ref.send(ping_msg{1, "hello"});

// 发送基本类型
struct gradient_msg {
    int layer_id;
    std::vector<float> data;
};
ref.send(gradient_msg{0, {0.1f, 0.2f, 0.3f}});

// 发送到无效 ref 是安全的（不会崩溃）
actor_ref<handler_actor> empty_ref;
empty_ref.send(int_msg{42});  // 内部检查 m_proxy 非空，静默忽略
```

**消息发送的当前限制**：

1. **同步传递**：`send()` 当前直接调用 `deliver()`，在调用者线程上同步执行消息处理器
2. **无返回值**：当前 `send()` 是 fire-and-forget 模式，`call()` / request-reply 模式尚未实现
3. **消息大小**：消息按值传递（`sizeof(msg)`），大型数据应使用 `std::vector` 或指针

---

### 2.5 注册消息处理器

这是最重要的部分——告诉框架你的 Actor 能处理哪些消息：

```cpp
class handler_actor : public actor<handler_actor> {
public:
    int m_int_count = 0;
    int m_str_count = 0;

    handler_actor() {
        // 注册 int 消息处理器
        register_handler<int_msg>([this](const int_msg& m) {
            m_int_count++;
            std::cout << "Got int: " << m.value << std::endl;
        });

        // 注册 string 消息处理器
        register_handler<str_msg>([this](const str_msg& m) {
            m_str_count++;
            std::cout << "Got string: " << m.text << std::endl;
        });
    }
};
```

**注意事项**：

1. **必须在构造函数中注册**：`register_handler` 必须在 Actor 对象完全初始化之前完成
2. **Lambda 捕获 `this`**：确保 Actor 的生命周期覆盖所有消息处理
3. **const 引用**：处理器接收 `const Msg&`，避免拷贝
4. **类型安全**：`register_handler<Msg>` 和 `ref.send<Msg>()` 通过相同的 `type_hash<Msg>()` 关联，如果类型签名不匹配，消息会被静默丢弃

---

### 2.6 URI 操作

```cpp
// 创建本地 URI
auto local_uri = actor_uri::make_local("my_type", "my_name");
// → ultra://*/my_type/my_name

// 创建带 node_id 的 URI
auto remote_uri = actor_uri::make(12345, "my_type", "my_name");
// → ultra://12345/my_type/my_name

// 序列化为字符串
std::string s = local_uri.to_string();

// 比较
actor_uri a = actor_uri::make_local("T", "N");
actor_uri b = actor_uri::make_local("T", "N");
assert(a == b);   // true

// 用作 unordered_map 的 key
std::unordered_map<actor_uri, int> map;
map[local_uri] = 42;  // 使用 std::hash<actor_uri>
```

---

### 2.7 集群和 Gossip

```cpp
#include "ultranet/actor/net/cluster.h"
using namespace ynet::actor::net;

// 创建集群实例
cluster c(/*self_id=*/1001, /*self_addr=*/"127.0.0.1:8001");

// 添加种子节点
c.add_seed("192.168.1.1:9000");

// 构建 gossip 消息
auto data = c.build_gossip();
send_to_peer(data);  // 通过网络发送

// 收到 gossip 消息时应用
c.apply_gossip(received_data.data(), received_data.size());

// 查询存活节点（3 秒超时）
auto nodes = c.live_nodes(3000);
for (auto& n : nodes) {
    std::cout << "Node " << n.id << " at " << n.addr << std::endl;
}

// 标记节点为存活
c.mark_seen(peer_id, "peer_address");
```

> **注意**：`cluster` 是独立模块，尚未集成到 `actor_system` 中。gossip 周期需要由调用者在外部控制。

---

### 2.8 TCP Transport 使用

```cpp
#include "ultranet/actor/net/transport.h"
using namespace ynet::actor::net;

// 消息处理器
auto handler = [](std::vector<uint8_t> data) -> Task<void> {
    // 解析消息、路由到 actor 等
    co_return;
};

// 创建 transport
tcp_transport transport(/*port=*/9000, std::move(handler));

// 启动 accept loop（在协程上下文中）
co_await transport.serve(shutdown_coordinator);

// 连接到远程节点
auto conn = co_await transport.connect("192.168.1.1", 9000);
if (conn && conn->is_valid()) {
    co_await conn->send(payload);
}
```

> **注意**：Transport 模块尚未与 `actor_system` 完全集成。当前 `actor_system` 使用 `local_actor_proxy` 进行本地消息投递，不经过网络。

---

### 2.9 Metrics Exporter（指标导出）

```cpp
#include "ultranet/actor/system/metrics_exporter.h"
using namespace ynet::actor;

auto exporter = std::make_shared<metrics_exporter>();

// 注册指标提供者（返回 JSON 的函数）
exporter->set_training_provider([&]() -> std::string {
    training_snapshot_data snap;
    snap.total_steps = 1000;
    snap.throughput_mbps = 250.5;
    return build_training_json(snap);
});

exporter->set_actors_provider([&]() -> std::string {
    return build_actors_json({
        {"ps", 0, 0.0, 1000, "running"},
        {"worker-0", 3, 50.0, 250, "running"},
    });
});

// 启动 HTTP server（在独立线程上运行，避免 io_uring 竞争）
exporter->set_port(18080);
exporter->start();

// 查询实际绑定的端口
int actual_port = exporter->port();

// 优雅关闭
exporter->stop();
```

**端点**：

| 端点 | 内容 |
|------|------|
| `/health` | 健康检查 |
| `/api/v1/nodes` | 节点拓扑和状态 |
| `/api/v1/training` | 训练进度、吞吐量、延迟 |
| `/api/v1/actors` | Actor 队列深度和消息速率 |
| `/api/v1/network` | 连接带宽统计 |

**Snapshot 模式**（当数据源生命周期短于 HTTP server 时使用）：

```cpp
// 1. 数据源可能被销毁前，保存快照
exporter->store_training_snapshot(some_json);
exporter->store_actors_snapshot(some_json);

// 2. 切换到快照模式（lambda 不再引用已销毁的变量）
exporter->use_snapshots();

// 3. HTTP server 继续服务，不再访问原始数据源
```

---

### 2.10 实际应用：dist-bench 分布式训练基准测试

`app/dist-bench/main.cc` 是这个框架最完整的应用示例。

**Local 模式**（共享内存，无网络）：
```bash
./bin/dist-bench local 4 50000 200
# workers=4, params=50000, steps=200
```

**TCP 模式**（单进程多协程）：
```bash
./bin/dist-bench tcp 4 50000 200 18001 18080
# workers=4, params=50000, steps=200, ps_port=18001, metrics_port=18080
```

**多进程模式**（真实分布式）：
```bash
# 终端 1: 启动 PS
./bin/dist-bench ps 18001 4 50000 200 18080

# 终端 2-5: 启动 Workers
./bin/dist-bench worker 127.0.0.1:18001 0 200
./bin/dist-bench worker 127.0.0.1:18001 1 200
./bin/dist-bench worker 127.0.0.1:18001 2 200
./bin/dist-bench worker 127.0.0.1:18001 3 200
```

**Dashboard 模式**（启动 benchmark + 指标服务）：
```bash
./bin/dist-bench serve 18080 2 500 20
# 运行 benchmark，启动 metrics HTTP server，等待 Dashboard 连接
```

---

## 第三部分：架构演进 (Phase 2–4)

### Phase 2: Mailbox + 异步调度

**变更思路**：Phase 1 的 `send()` 在 caller 线程上同步调用 `deliver()`，这违背了 Actor 串行执行的核心保证。Phase 2 引入真正的异步调度：

1. **`mailbox`** (`mailbox.h`)：封装 `MpscQueue<message_envelope, 4096>`，提供 `try_push()`/`try_pop()`/`drain()`/`is_backpressure()`。编译期固定容量 4096，80% 触发 backpressure。
2. **`actor_base` 扩展**：添加 `m_mailbox`、`m_activated` (CAS)、`m_pending` (原子计数)。新增 `push_envelope()`、`pull_and_run()`、`try_activate()` 三个核心方法。
3. **调度流程**：`ref.send(msg)` → `proxy->deliver()` → `actor->push_envelope()` → `mailbox.try_push()` + `pending++` + `try_activate()` → `CAS(m_activated)` → `schedule(pull_and_run)` → 在 thread pool 上执行 → `drain(limit)` → `deliver()` → handler → 结束后检查 `pending > 0` → 自驱动。
4. **`set_schedule_fn()`**：避免 `actor_base` 直接依赖 `actor_system` 造成的循环 include。`actor_system` 在 spawn 时注入一个 `std::function<void(std::function<void()>)>` 回调。
5. **`type_hash.h`**：从 `base_actor.h` 和 `actor_ref.h` 中提取重复的 FNV-1a hash 函数，消除代码重复。

### Phase 3: 序列化 + 远程代理

**变更思路**：Phase 1-2 仅支持同一进程内的 Actor 通信。Phase 3 添加跨网络能力：

1. **`serializer`** (`dist/serialization.h`)：网络字节序 (big-endian) binary 序列化器，支持 u8/u16/u32/u64/float/double/string/bytes。提供 `pack_actor_message()`/`unpack_actor_message()` 用于构建/解析传输 envelope。
2. **Wire 格式**: `[type:1][uri_len:4][uri:var][msg_hash:8][payload_len:4][payload:var]`。第一字节区分 gossip/actor_message/actor_location。
3. **`remote_proxy`** (`dist/remote_proxy.h`)：实现 `actor_proxy` 接口，`deliver()` 打包 envelope → 提交命名 struct 协程 (避免 GCC 13.2 coroutine lambda bug) → 通过 `outbound_conn::send()` 异步发送。继承 `enable_shared_from_this` 保证发送期间 proxy 存活。
4. **消息类型限制**：当前使用 `memcpy` 传递消息负载，这意味着消息类型必须是 trivially copyable。`std::string`、`std::vector` 等含堆指针的类型不支持。

### Phase 4: Cluster + Transport 集成

**变更思路**：Phase 1-3 中 `cluster` 和 `transport` 是独立模块，需要调用者手动管理。Phase 4 将其完全集成到 `actor_system` 生命周期：

1. **同步 bind + 异步 serve**：构造函数中同步调用 `bind_socket()`（C socket API），获取实际端口后创建 `tcp_transport`，再将 `serve()` 提交到 thread pool 异步执行。
2. **`gossip_loop()`**：`actor_system` 构造时自动启动 gossip 协程。周期性从 `cluster::live_nodes()` 选随机节点，发送 gossip + actor_location 消息。
3. **`handle_inbound_message()`**：解析 type byte 分派到 gossip/actor_message/actor_location 三种处理路径。actor_message 通过 unpack → URI resolve → local proxy → `push_envelope()` 投递给本地 actor。
4. **远程 `find()`**：先查本地 registry，miss 后查 `m_remote_actors` 表（由 gossip 填充），通过 `get_or_create_remote_proxy()` 创建/缓存 `remote_proxy`。
5. **`publish_actor_location()`**：`spawn()` 成功后自动将 actor 的 URI 注册到 `m_remote_actors`，随下次 gossip 传播到其他节点。

**关键技术决策总结**：

| 决策 | 理由 |
|------|------|
| `set_schedule_fn()` 回调避免循环 include | `base_actor.h` 不直接依赖 `actor_system.h` |
| 固定容量 MpscQueue (4096) | 编译期容量，零动态分配；提供背压边界 |
| CAS 激活 (`m_activated`) | 保证单次调度，防止重复执行 |
| 命名 struct 替代 lambda 用于协程 | 避免 GCC 13.2 coroutine lambda capture 损坏 |
| 同步 bind + 异步 serve | 构造函数中确定端口，serve 在 pool 上异步运行 |
| Wire type discriminator (0x01/0x02/0x03) | 单 TCP 连接复用 gossip + actor_msg + location |
| `remote_proxy` 持有 `WorkStealingThreadPool*` | 避免 `actor_system` 循环依赖 |
| Trivially copyable 限制 | memcpy 传递消息零开销，非平凡类型待 Phase 5+ 解决 |

---

## 第四部分：注意事项与陷阱

### 3.1 当前状态：v2 Phase 4（集群+传输集成完成，66 项测试 100% 通过）

**已实现**：

| 功能 | 状态 | 文件 |
|------|------|------|
| `actor_system` 创建/销毁 | ✅ | `actor_system.h` |
| `spawn<T>()` 侵入式/非侵入式 | ✅ | `actor_system.h` |
| `actor_ref<T>` + `actor_uri` | ✅ | `actor_ref.h`, `actor_uri.h` |
| `register_handler<Msg>()` | ✅ | `base_actor.h` |
| `send(msg)` 异步发送（mailbox） | ✅ | `actor_ref.h` → `base_actor.h` |
| `find()` 本地+远程查找 | ✅ | `actor_system.h` |
| **Mailbox 异步调度** | ✅ | `mailbox.h` + `base_actor.h` |
| **CAS 激活 + 自驱动** | ✅ | `base_actor.h::try_activate()` |
| **Backpressure 检测** | ✅ | `mailbox.h::is_backpressure()` |
| Gossip 集群协议 | ✅ | `cluster.h` |
| TCP Transport | ✅ | `transport.h` |
| Metrics Exporter | ✅ | `metrics_exporter.h` |
| **Binary 序列化 (network byte order)** | ✅ | `dist/serialization.h` |
| **远程代理 `remote_proxy`** | ✅ | `dist/remote_proxy.h` |
| **Cluster 集成到 actor_system** | ✅ | 构造时自动启动 |
| **Transport 集成到 actor_system** | ✅ | 同步 bind + 异步 serve |
| **Gossip 周期自动化** | ✅ | `actor_system.h::gossip_loop()` |
| **Inbound 消息路由** | ✅ | `handle_inbound_message()` |

**尚未实现**（未来阶段）：

| 功能 | 状态 |
|------|------|
| `call(msg)` 请求-回复 | ⏳ Phase 5+ |
| `ULTRA_MESSAGE` 宏（自动序列化反射） | ⏳ Phase 5+ |
| Supervision 监督树（parent/child 重启策略） | ⏳ Phase 5+ |
| 非平凡类型消息支持（std::string/vector） | ⚠️ 当前仅支持 trivially copyable |
| 多优先级 mailbox（high/medium/low） | ⏳ Phase 5+ |

---

### 3.2 已知陷阱

**1. send() 是同步的**

当前 `send()` 在调用者线程上直接执行消息处理器，而非通过 mailbox 异步调度。这意味着消息处理器中的阻塞操作会阻塞调用者。

**2. find() 使用 typeid 名称**

```cpp
auto key = actor_uri::make_local(typeid(echo_actor).name(), "finder").to_string();
```

`typeid(T).name()` 在不同编译器上返回不同的名称（GCC 返回 mangled name，MSVC 返回 decorated name）。URI 不可跨编译器传递。

**3. actor_system 析构可能阻塞**

```cpp
inline actor_system::~actor_system() {
    m_pool.reset();  // 触发 ~WorkStealingThreadPool（join 所有线程）
}
```

如果有协程仍在运行且永远不会完成，析构函数会永久阻塞。

**4. 无协程取消机制**

框架的协程不支持取消。一旦提交了一个 task，无法从外部中断它。

**5. GCC 13.2 coroutine lambda 捕获损坏**

WSL2 环境使用的 conda GCC 13.2 有已知 bug：协程 lambda 的捕获变量可能损坏。**解决方案**：避免在协程中使用 lambda，改用普通函数 + 值传递，或使用 `shared_from_this()` 模式。

**6. WSL2 特定限制**

- 多进程 TCP 测试需要 `sleep 3-5s` 等待端口就绪
- `SO_REUSEPORT` 超过 4 线程会触发 ENOBUFS
- `perf` 工具不可用
- 单进程 TCP loopback 有内核优化捷径，不代表真实网络性能

---

### 3.3 编码规范（来自 `base.skill`）

在向 Actor 框架贡献代码时，必须遵守：

1. **全局兼容性**：新增/修改的代码必须与已有功能在同一个运行时上下文中兼容
2. **禁止局部短视实现**：不允许为"当前调用能跑通"而写死逻辑、硬编码、绕过已有模块
3. **技术债声明**：修改前显式列出可能产生的技术债
4. **命名**：类/函数 `snake_case`，成员 `m_` 前缀，常量 `k` 前缀
5. **格式**：严禁压行，4 空格缩进，每个 `if/for/while` 必须用 `{}`
6. **头文件**：`#pragma once`，包含顺序：标准库 → 第三方 → 项目内部

---

### 3.4 测试

```bash
# 构建
cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_COMPILER=/home/jwy/anaconda3/envs/ultranet/bin/x86_64-conda-linux-gnu-g++ \
    -Dliburing_INCLUDE_DIR=/home/jwy/anaconda3/envs/ultranet/include \
    -Dliburing_LIBRARY=/home/jwy/anaconda3/envs/ultranet/lib/liburing.so
make actor_v2_test actor_message_test actor_cluster_unit_test -j4

# 运行测试
./bin/actor_v2_test       # v2 核心测试 (spawn/find/send/ref/uri)
./bin/actor_message_test  # 消息处理器测试
./bin/actor_cluster_unit_test  # Gossip 集群测试

# 覆盖率
cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="--coverage -O0 -g"
make actor_v2_test -j4
./bin/actor_v2_test
lcov --directory . --capture --gcov-tool $GCOV --output-file coverage.info
lcov --remove coverage.info '/usr/*' '*/gcc/*' --output-file clean.info
lcov --summary clean.info
```

---

## 性能基准

| 场景 | 配置 | 吞吐量 | P50 | 环境 |
|------|------|--------|-----|------|
| Actor Local | 4w×50K params | ~∞ (CPU bound) | 1.19ms | 共享内存 |
| Actor TCP | 1w×50K params | 0.5 MB/s | 1.20ms | TCP loopback |
| MPSC Queue | 2000 items | — | sub-μs | 单线程 |
| HTTP Proxy | c=200, 16T | 115K QPS | 1.5ms | WSL2 2核 |

## 相关文档

- [设计文档](../.claude/actor-framework-design.md) — v1 原始设计
- [v2 设计](../.claude/actor-v2-design.md) — v2 生产级设计方案
- [审查报告](../.claude/actor-framework-review.md) — 全面代码审查报告
- [行业研究](../.claude/actor-industry-research.md) — ex-actor/Akka/Orleans 等框架研究

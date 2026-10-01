# Ultra-Net Actor 框架：概念详解与开发指南

> 状态: 2026-10-01。本地 mailbox 调度可用。两端进程都活着时，远端是至少一次。不是恰好一次，也还没有在两台物理机上测过。
> 测试: `actor_gtest` 141 项通过；`actor_dist_runtime_test` 退出码 0。两边都在 `ASAN_OPTIONS=detect_leaks=1:halt_on_error=1` 下跑，stderr 没有 `LeakSanitizer`。
> 设计记录: `docs/superpowers/specs/2026-09-30-actor-distributed-runtime-design.md`、`2026-10-01-actor-remaining-gaps-design.md`、`2026-10-01-actor-contract-perf-design.md`。前两份的结果表是当时的测量，不要改写成后来的数字。

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

**消息传递**: 编译期自动选择路径——trivially copyable → memcpy（默认）；提供 `serialize()/deserialize()` → 自定义序列化。`static_assert` 在编译期拦截不符合要求的类型。`std::string`、`std::vector` 不能直接当消息字段。

**送达**:

- 本地 `send` 进 mailbox 后返回。`correlation_id` 为 0，不查去重表，默认也不写磁盘。
- 远端 `0x04` 在两端进程都活着时是至少一次。确认在处理函数返回 `ok` 或 `dead_letter` 之后发出。处理函数抛异常则释放去重项，不确认，发送方会再送。
- `remote_log_path` 非空时，发送方把数据记录 `fsync` 成功之后，`send` 才返回 true。发送进程被 `SIGKILL` 后，重放进程把还没确认的原始帧再送出去。
- `receiver_log_path` 默认空。空路径下，接收方已经确认并且发送方已经忘掉这帧，然后接收进程被杀死，这帧不会再来。路径非空时，数据记录按 32 条或 2ms 做一次 `fsync`，已处理记录 `fsync` 成功之后才确认。重启会把每一条已 `fsync` 的数据记录再执行一次，已处理记录不会让这次跳过。
- 这不是恰好一次。外部副作用在接收进程每次启动时都可能再跑一遍。`save_snapshot` 仍由调用方自己写 actor 字段，运行时不会自动序列化成员。
- 去重键是 `(sender_node_id, msg_id)`，上限 `dedup_cap`（默认 1048576）。日志关闭且表满时，淘汰 `msg_id` 最小的已完成项；没有已完成项则不执行、不确认。

**2026-10-01 测量**（本机，4 个调度线程，Release `-O3`）: 本地灌入中位 1184 万条/秒。两个容器各 2 线程、1 万条，三次 16782、26572、20023 条/秒，中位 20023。容器要 `seccomp=unconfined`，`advertise_host` 填容器里能被对端访问的地址。本地数字不是网络吞吐。两台物理机未测。

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
   
   - [两进程远端](#210-两进程远端)
3. [架构演进 Phase 2–4](#第三部分架构演进-phase-24)
   - [Phase 2: Mailbox + 异步调度](#phase-2-mailbox--异步调度)
   - [Phase 3: 序列化 + 远程代理](#phase-3-序列化--远程代理)
   - [Phase 4: Cluster + Transport 集成](#phase-4-cluster--transport-集成)
4. [注意事项与陷阱](#第四部分注意事项与陷阱)
   - [当前状态](#31-当前状态2026-10-01)
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

    // 投递消息：框架调用，用户不直接使用。返回 ok / threw / dead_letter。
    virtual deliver_result deliver(uint64_t msg_type, const void* data, size_t len);
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
│  - m_proxy       │        │  本地: local_actor_proxy  → mailbox
│  - m_uri         │        │  远程: remote_proxy → 0x04 → 对端
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

    // 收下后返回 true。本地进入 mailbox。远端在超时内重试。
    template <typename Msg>
    bool send(const Msg& msg);

    template <typename Reply, typename Msg>
    std::optional<Reply> ask(const Msg& msg, std::chrono::milliseconds timeout);
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
      │  (3) 做成 message_envelope，push_envelope()
      ▼
  mailbox（MPSC，4096）
      │  (4) try_activate()，同一 actor 同时只有一个 pull_and_run
      ▼
  actor_base::dispatch_envelope()
      │  (5) 本地消息直接 deliver()
      │      远端消息先按 (sender_node_id, msg_id) 占位，再 deliver()
      ▼
  处理函数
      │  (6) ok / dead_letter 之后才对远端发 0x06
      ▼
   完成
```

`send()` 把消息放进 mailbox 后返回，处理函数在线程池上跑。同一 actor 的消息不会并发进入处理函数。

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

// 1. 定义消息类型。要 trivially copyable，不能直接放 std::string。
struct ping_msg { int id; char text[32] = {}; };

// 2. 定义 Actor
class ping_actor : public actor<ping_actor> {
public:
    int m_count = 0;

    ping_actor() {
        // 注册消息处理器
        register_handler<ping_msg>([this](const ping_msg& m) {
            m_count++;
            (void)m;
        });
    }
};

// 3. 使用
int main() {
    actor_system system;                                    // 创建系统
    auto ref = system.spawn<ping_actor>("pinger-1");        // 创建 Actor
    ref.send(ping_msg{42, "hello"});                        // 进入 mailbox 后返回
    while (ref->m_count < 1) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
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
cfg.advertise_host = "192.168.1.10";   // 空则对外公布 127.0.0.1
cfg.remote_log_path = "/tmp/sender.log";     // 空则发送路径不落盘
cfg.receiver_log_path = "/tmp/receiver.log"; // 空则保持“确认后被杀死会丢”的窗口
cfg.dedup_cap = 1048576;
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

1. `find()` 使用 `"ultra://*/type_name/actor_name"` 或带节点号的 URI。`type_name` 是 `typeid(T).name()`，和编译器绑定，不能跨编译器当稳定名字。
2. 本地注册表没有时，会查 gossip 填进来的远端表，并创建 `remote_proxy`。对端还没公布位置时，`find` 暂时无效，需要等 gossip。
3. 非侵入式 Actor 的返回类型是 `actor_ref<plain_class>`，内部是 `actor_adapter<plain_class>`。`ref.get()` 走 `native_object()`。

---

### 2.4 发送消息

```cpp
// Fire-and-forget 发送（异步投递到 mailbox）
ref.send(ping_msg{1, "hello"});

// 非阻塞发送（mailbox 满时返回 false）
bool ok = ref.try_send(ping_msg{2, "world"});

// 跨 actor 发送（等价于 target.send(msg)）
src_ref.send_to(dst_ref, ping_msg{3, "hi"});

// 发送到无效 ref 立刻返回 false，不阻塞
actor_ref<handler_actor> empty_ref;
bool accepted = empty_ref.send(int_msg{42});
```

**发送语义**：

1. **异步投递**：`send()` 将消息推入 mailbox 后立即返回，actor 在线程池上异步处理
2. **背压处理**：mailbox 满时 `send()` 短暂阻塞（自适应退避），`try_send()` 立即返回 false
3. **消息类型**：编译期自动检测——trivially copyable 走 memcpy 快路径，有 `serialize()` 走自定义序列化

---

### 2.5 注册消息处理器

```cpp
class handler_actor : public actor<handler_actor> {
public:
    int m_int_count = 0;
    char m_last_text[64] = {};

    handler_actor() {
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

---

### 2.10 两进程远端

仓库里没有 `dist-bench`。两进程行为在 `tests/actor_dist_runtime_test.cc`。父进程 `fork` 后 `exec` 自己，角色包括普通收发、断连、ask、成员离开、accept 黑洞、发送方日志重放、接收方日志的两个 `SIGKILL` 点，以及 Docker 用的 `docker-recv` / `docker-send`。

容器要加 `--security-opt seccomp=unconfined`，否则 `io_uring_queue_init_params` 会 `Operation not permitted`。`advertise_host` 填这个容器里对端能连上的地址，不要依赖默认的 `127.0.0.1`。

`ask` 示例：

```cpp
struct ask_q { int value = 0; };
struct ask_a { int value = 0; };

register_handler<ask_q>([this](const ask_q& q) {
    reply(ask_a{q.value + 1});
});

auto got = ref.ask<ask_a>(ask_q{41}, std::chrono::milliseconds(1000));
```

监管：

```cpp
auto ref = sys.spawn_supervised<MyActor>("name", supervisor{3});
```

处理函数抛异常时会计入重启次数。超过 `max_restarts` 后 actor 进入关闭，不再收消息。

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
4. **消息类型**：trivially copyable 走 memcpy。另外提供 `serialize()` / `deserialize()` 的类型走自定义序列化。`std::string`、`std::vector` 不能直接当字段。当前线上类型比上面的 `0x01`/`0x02`/`0x03` 多了 `0x04`/`0x05`/`0x06`，见本节后面的决策表。

### Phase 4: Cluster + Transport 集成

**变更思路**：Phase 1-3 中 `cluster` 和 `transport` 是独立模块，需要调用者手动管理。Phase 4 将其完全集成到 `actor_system` 生命周期：

1. **同步 bind + 异步 serve**：构造函数中同步调用 `bind_socket()`（C socket API），获取实际端口后创建 `tcp_transport`，再将 `serve()` 提交到 thread pool 异步执行。
2. **`gossip_loop()`**：`actor_system` 构造时自动启动 gossip 协程。周期性从 `cluster::live_nodes()` 选随机节点，发送 gossip + actor_location 消息。
3. **`handle_inbound_message()`**：按类型字节分派。`0x04` 解出 `sender_node_id` 和 `msg_id`，确认在处理函数返回之后。`0x02` 仍是早期的 actor_message 路径。
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
| Wire type discriminator | `0x01` gossip，`0x02` actor_message，`0x03` actor_location，`0x04` 带 msg_id 的远端消息，`0x05` ask 答复，`0x06` 确认 |
| 长度前缀 | 本机序 `uint32`。负载字段走序列化器的大端 |
| 合并写 | 同一连接的写循环把当前队列里的帧拼成一次 `Write`，每帧仍是长度加负载 |
| 自定义序列化 | 消息提供 `serialize()` / `deserialize()` 即可，不必 trivially copyable |

---

## 第四部分：注意事项与陷阱

### 3.1 当前状态（2026-10-01）

本地调度和两端都活着时的至少一次送达已经有测试。接收日志能在进程被 `SIGKILL` 后把数据记录再执行一次。用户状态要自己 `save_snapshot`。两台物理机没有测。

### 3.1 功能清单

| 功能 | 状态 |
|------|------|
| `spawn` / `find` / `send` / `try_send` / `ask` | 已有。`send` 返回 bool |
| Mailbox 异步调度 + CAS 激活 + 背压 | 已有。单 actor 同时只有一个 `pull_and_run` |
| `serialize()` / `deserialize()` | 已有 |
| Gossip + TCP，长度前缀帧 | 已有 |
| `remote_proxy`，确认在处理函数之后 | 已有 |
| `spawn_supervised` | 已有。超过重启次数后关闭该 actor |
| 发送方日志 `remote_log_path` | 已有。`fsync` 后 `send` 才返回 true |
| 接收方日志 `receiver_log_path` | 已有，默认关。重启再执行数据记录 |
| 恰好一次、自动恢复 actor 字段 | 没有 |
| 两台物理机上的测量 | 没有 |

---

### 3.2 注意事项

**1. 消息必须是 trivially copyable 或提供 serialize/deserialize**

```cpp
// ✅ 正确：trivially copyable
struct ping { int id; char text[32]; };

// ✅ 正确：自定义序列化
struct order { std::string symbol;
    std::vector<uint8_t> serialize() const { ... }
    static order deserialize(const uint8_t*, size_t) { ... }
};

// ❌ 错误：含 std::string 且无序列化方法
struct bad { std::string x; };  // static_assert 拦截
```

**2. 使用 `operator->` 访问 actor 成员**

```cpp
auto ref = sys.spawn<MyActor>("name");
ref->received;  // ✅ 直接访问（替代 static_cast）
```

**3. 异步处理需要等待**

`send()` 是异步的，消息在线程池上处理。测试中需要适当等待（`sleep_for` 或轮询 `ref->received`）。

**4. 跨系统通信需要 gossip 发现**

远端 actor 查找依赖 gossip 协议，首次发现需要等待 gossip 周期（默认 1 秒）。

**2. find() 使用 typeid 名称**

```cpp
auto key = actor_uri::make_local(typeid(echo_actor).name(), "finder").to_string();
```

`typeid(T).name()` 在不同编译器上返回不同的名称（GCC 返回 mangled name，MSVC 返回 decorated name）。URI 不可跨编译器传递。

**3. actor_system 析构可能阻塞**

析构会先让 actor 停止收消息，再 `shutdown` 监听和已接受连接，等线程池里的任务结束，然后才丢掉线程池。`serve` / `gossip` / 确认超时循环看关闭标志退出。不要在这些协程还堵在不可取消的调用上时假设析构马上返回。

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
cmake -S . -B /tmp/ultranet-asan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
cmake --build /tmp/ultranet-asan --target actor_gtest actor_dist_runtime_test -j$(nproc)
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 /tmp/ultranet-asan/bin/actor_gtest
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 /tmp/ultranet-asan/bin/actor_dist_runtime_test
```

2026-10-01 的结果：`actor_gtest` 141 通过，4015 ms，退出码 0，stderr 为空。`actor_dist_runtime_test` 退出码 0，`dist_failures=0`，stderr 为空。不要把构建目录放在仓库里的 `build/`。

---

## 性能基准

| 场景 | 结果 | 条件 |
|------|------|------|
| 本地灌入 | 中位 11839924 条/秒，最慢 11220825，最快 17412502 | 2026-10-01，4 线程，10 万条 16 字节，1 次预热 + 12 次，中位是升序下标 6。不走 TCP |
| 同日 13:13 对 CAF | 灌入中位 13702384 对 1352905；ping-pong 中位 3285690 对 208066 | 合并写之后没有重跑 CAF 和 ping-pong |
| 容器对容器 | 16782、26572、20023 条/秒，中位 20023 | 各 2 线程，1 万条，不开日志，`seccomp=unconfined`，显式 `advertise_host`。同一内核上的两个网络命名空间 |

不要把本地灌入的条/秒写成网络吞吐。两台物理机未测。

## 相关文档

- [分布式运行时](superpowers/specs/2026-09-30-actor-distributed-runtime-design.md) — 第 9 节是当时的测量
- [剩余缺口](superpowers/specs/2026-10-01-actor-remaining-gaps-design.md) — 第 11 节是 13:10–13:16 的测量
- [契约与远端性能](superpowers/specs/2026-10-01-actor-contract-perf-design.md) — 第 9 节是接收日志和合并写之后的测量

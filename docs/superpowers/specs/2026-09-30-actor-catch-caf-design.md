# 本地 actor 灌入追上 CAF — 设计

日期：2026-09-30  
状态：阶段 A、B 已落地。2026-09-30 22:46 同次测量里 send() 中位 33 万，低于第 5 节 80 万的门，未开始 C。  
取代关系：实现以本文为准。`docs/superpowers/plans/2026-09-30-actor-local-throughput.md` 里的 150 万通过线是保守门槛，本文把同一次会话里的 CAF 中位设为通过线。

## 1. 要追上的数字

机器：Intel Core i5-8350U，4 核 8 线程，最高 3.6GHz。  
负载：4 个调度线程，主线程连续 `send` 10 万条 16 字节消息，一个 actor 用 `atomic` 计数。1 次预热，5 次取中位。Release `-O3`。CAF 是 libcaf 0.17.6，`scheduler.max-threads=4`，程序打印 `caf_workers=4`。`scheduler.policy` 设成字符串失败，用的是默认 work-stealing。

| 路径 | 中位 | 每条时间 |
|---|---|---|
| ultra `send()` | 36 万条/秒 | 2.8 μs |
| ultra `try_send`，满了只 yield | 95 万（最好 166 万） | 1.06 μs（最好 0.60 μs） |
| vector 信封，专用线程弹出 | 227 万 | 0.44 μs |
| 16 字节原样入队 | 6640 万 | 15 ns |
| CAF `anon_send` | 231 万（五次 164 万–283 万） | 0.43 μs |
| ultra ping-pong | 103 万条消息/秒 | 已与 CAF 同档 |
| CAF ping-pong | 97 万条消息/秒 | |
| ultra 每 512 条等一次 | 1.2 万 | 每条一次激活 |

目标：同一台机器、同一次会话里，ultra 的 `send()` 中位不低于 CAF `anon_send` 中位。ping-pong 消息中位不低于 80 万条/秒。6640 万不是目标，那是没有 actor 的队列。

实现完成的定义和目标分开。A、B、C 都做完并测完。C 的 `send()` 中位已经不低于这次 CAF，就不再做 D。低于 CAF 才做 D，再测一轮。两轮的数字都写入第 9 节。写完第 9 节，实现就结束，不管有没有追上。不改 `IoReactor::wait_for_events`，不把 `max_per_activation` 调大来凑数。第 2 节里“去掉分配后中位也许只有 160 万”是估计，用来解释为什么目标可能达不到；它不代替测量，也不把 160 万写成通过线。

## 2. 现有架构为什么够用

灌入时生产者比消费者快，邮箱会堆到一批。`try_activate` 用 CAS 保证同时只有一个 `pull_and_run`。`pull_and_run` 一次取 `min(max_per_activation, m_pending)`，基准里 `max_per_activation` 是 1024。第一条消息把 `pull_and_run` 提交到线程池，后面的消息只入队。10 万条大约 100 次激活。eventfd 和 io_uring 等待摊在这一批上。

所以 CAF 那种灌入不需要新调度器。每 512 条停下来等、只有 1.2 万条/秒，是 worker 排空后回到 `wait_for_events`。那是一条一条来的负载。本次 CAF 数字不是这种负载。ping-pong 一旦热起来，两个 actor 互相 `try_activate`，不再回到等待，所以已经和 CAF 同档。

每条时间的差：

- 原样入队 15 ns，vector 信封 441 ns。中间大约 426 ns 是堆分配和释放。
- `try_send` 最好 602 ns，比信封队列多大约 160 ns，这是激活、`unordered_map` 查找和 `std::function` 调用。
- `try_send` 中位 1056 ns，多出来的是队列满时的 yield，以及消费者还在分配时跟不上。
- `send()` 中位 2.8 μs，比 `try_send` 更慢，因为按值入队多一次拷贝，并且睡到 100μs。

从最好的 602 ns 里去掉大约 426 ns 的分配，剩下大约 180 ns，对应约 500 万条/秒，高于 CAF。从中位 1056 ns 里去掉同样的 426 ns，剩下大约 630 ns，对应约 160 万，低于 CAF。分配去掉之后消费者变快，队列不那么容易堆满，中位里的 yield 会下降。这是假设，不是测量。所以通过线用同一次重跑的 CAF 中位，而不是把 500 万写进验收。

## 3. 不改什么

- 不改 worker 循环，不把 `wait_for_events` 的 100ms 超时改成空转。
- 不增加线程，不改 `max_per_activation` 的默认值 256。基准程序自己设 1024，和现在的 `perf_bench` 一致。
- 不改远端缓冲满了丢新消息的策略。
- 不把 CAF 链进 CMake。
- 不改 HTTP、`IoOperation::cancel`、`config`。
- 单激活 CAS 保留。一个 actor 仍然只有一个 `pull_and_run` 在跑。
- 未经用户要求不提交。

## 4. 阶段 A — 成功才移走

### 4.1 根因

`mailbox::try_push` 按值接收：

```cpp
bool try_push(message_envelope env) {
    return m_queue.try_push(std::move(env));
}
```

`push_envelope` 把左值传进去。每次调用先拷贝整份信封。队列满时这份拷贝在形参里析构，下一次重试再分配。

`MpscQueue::try_push(T item)` 也按值接收。`thread_pool.hpp` 两处是 `try_push(std::move(task))`。队列满时 `UnifiedTask` 在形参里析构，循环重试的是空任务。这是现状 bug。修复后失败不移动 `task`，调用点不改。

`push_blocking(const message_envelope&)` 依赖“失败不改源对象”。`tests/core_mpsc_test.cc` 里 `const int value` 的 `try_push(value)` 同样不能要求把 const 左值移走。

### 4.2 队列接口

替换 `MpscQueue::try_push(T)`。三个重载都转给私有 `emplace(T&)`。`emplace` 只在 CAS 成功之后 `slot.data = std::move(item)`。`dif < 0` 时直接返回 false，不碰 `item`。`seq > pos` 时重读 `m_enqueue_pos`，这段保持现在的 Vyukov 逻辑。

```cpp
bool try_push(T& item) {
    return emplace(item);
}

bool try_push(T&& item) {
    return emplace(item);
}

bool try_push(const T& item) {
    T copy = item;
    return emplace(copy);
}
```

选择规则：

- 非 const 左值走 `T&`。成功后源是 moved-from。失败后源不变。
- 右值走 `T&&`。`try_push(std::move(task))` 失败时 `task` 还在。
- const 左值走 `const T&`。先拷到局部再 `emplace`。源永远不变。`int` 的移动等于拷贝，非 const 的 `int` 左值走 `T&` 之后循环变量的值还在。

`emplace` 放在类末尾的 `private:` 段，也就是 `empty()` 之后。不要把 `private:` 插在 `try_push` 和 `try_pop` 之间。`try_pop` 不动。

`const T&` 重载要求 `T` 可拷贝，但只有被调用时才实例化。`unique_ptr` 和 `UnifiedTask` 走 `T&` / `T&&`，不会实例化拷贝重载。

### 4.3 mailbox 接口

删掉按值的 `mailbox::try_push`。换成：

```cpp
bool try_push(message_envelope& env) {
    return m_queue.try_push(env);
}

bool try_push(message_envelope&& env) {
    return try_push(static_cast<message_envelope&>(env));
}
```

`push_envelope` 继续传左值。成功后 `env` 已被移走，函数返回，不再读 `env`。失败后 `env` 完整，可以重试，且不再分配。

`try_push_envelope` 里已有的 `m_mailbox.try_push(std::move(env))` 走右值重载，调用不改。

`push_blocking(const message_envelope&)` 不改。它命中 `const T&`，每次尝试拷贝一次。这只发生在 200 次退避之后。热路径禁止走这个重载。

### 4.4 测试

`tests/core_mpsc_test.cc` 增加容量为 2 的 `unique_ptr<int>` 用例：前两次成功，源变空；第三次失败，源仍拥有 `int(3)`；弹出的第一个值是 1。现有 `const int value` 的循环不改，它必须还能编译和通过。

先只加测试，确认对按值 `try_push` 编译失败，再改队列和 mailbox。

## 5. 阶段 B — 缩短退避

`push_envelope` 现在的阶梯：前 10 次忙等，然后 yield，然后 1μs、10μs、100μs，共 200 次，最后 `push_blocking`。10 万条灌入必超过 4096 的容量，100μs 睡眠会计入吞吐。

改成：前 64 次 `yield`，之后每次 `sleep_for(1us)`，仍最多 200 次，然后 `push_blocking`。成功顺序不变：`try_push` → `m_pending.fetch_add(1, release)` → `try_activate` → `return`。

不丢消息。满队列时生产者让出，消费者仍是唯一的 `pull_and_run`。200 次之后的 `push_blocking` 继续 yield，和现在一样可能占满一个核，直到消费者腾出槽。这是冷路径。

这一步的门：`send()` 中位至少 80 万条/秒。低于这个数说明拷贝和睡眠不是主因，停止，不开始阶段 C。80 万低于 `try_send` 中位 95 万，给方差留余量，又明显高于现在的 36 万。

## 6. 阶段 C — 32 字节内联缓冲

### 6.1 布局

`message_envelope` 不再暴露 `std::vector<uint8_t> data`。

```cpp
struct message_envelope {
    static constexpr size_t k_inline_capacity = 32;

    uint64_t msg_type = 0;
    uint32_t m_size = 0;
    alignas(std::max_align_t) uint8_t m_inline[k_inline_capacity];
    std::vector<uint8_t> m_heap;

    const uint8_t* bytes() const;
    uint8_t* bytes();
    size_t size() const { return m_size; }
    bool uses_heap() const { return m_size > k_inline_capacity; }

    void assign_bytes(const uint8_t* src, size_t n);
    void assign_vector(std::vector<uint8_t> v);
};
```

`bytes()`：`m_size <= 32` 时返回 `m_inline`，否则返回 `m_heap.data()`。  
`assign_bytes`：先写 `m_size = static_cast<uint32_t>(n)`。`n <= 32` 时 `m_heap.clear()`，再把 `n` 字节 `memcpy` 到 `m_inline`。`n > 32` 时 `m_heap.assign(src, src + n)`。  
`assign_vector`：`v.size() <= 32` 时调用 `assign_bytes(v.data(), v.size())`，否则 `m_size = static_cast<uint32_t>(v.size())` 且 `m_heap = std::move(v)`。

小消息结束时 `m_heap.empty()` 必须为真。否则拷贝会把 vector 容量带走并分配，内联就失效了。`clear()` 不释放容量，但拷贝一个 size 为 0 的 vector 不会分配。槽里的对象被反复移动，容量留在槽里的 moved-from vector 上。下一次小消息 `clear()` 即可。不要为了小消息 `shrink_to_fit`，那会再分配。

`alignas(std::max_align_t)` 是因为处理函数把 `bytes()` 转成 `const Msg*`。`uint8_t` 的 vector 在堆上通常按 malloc 对齐；内联缓冲必须自己对齐。16 字节的 `tick` 只要求 8 字节对齐，`max_align_t` 覆盖更宽的平凡消息。

每个邮箱槽大约多 32 字节。4096 槽大约多 128KB。一个 actor 一份，可以接受。

### 6.2 复制和移动

五个特殊成员都要手写。`MpscQueue` 入队是 `slot.data = std::move(item)`，这是移动赋值，不是移动构造。弹出才是 `T item = std::move(slot.data)`。只写移动构造时，编译器会删掉隐式移动赋值和拷贝赋值，阶段 C 编不过。默认的移动赋值也不会把源的 `m_size` 置 0。

拷贝构造和拷贝赋值：拷贝 `msg_type`、`m_size`，`memcpy` 32 字节 `m_inline`，拷贝 `m_heap`。小消息的 `m_heap` 是空的，这次拷贝不分配。赋值要处理自赋值。

移动构造和移动赋值：移走 `m_heap`，`memcpy` `m_inline`，拷贝 `msg_type` 和 `m_size`，然后把源的 `m_size` 和 `msg_type` 置 0。弹出之后 `bytes()` 指向这份对象自己的缓冲。移动赋值之后，槽里留下的源对象 `size()` 为 0，再读它的 `bytes()` 不得被当成一条有效消息。

测试：先 `assign_vector` 一份超过 32 字节的负载，再 `assign_bytes` 一份 16 字节负载，然后移动赋值进另一个信封。目标 `uses_heap()` 为假，字节能还原，源的 `size()` 为 0。

`make()`：可序列化消息 `assign_vector(msg.serialize())`。平凡消息 `assign_bytes(reinterpret_cast<const uint8_t*>(&msg), sizeof(Msg))`。不再 `resize`。

### 6.3 调用点

只改这些地方。`remote_proxy.h` 的 `buf.data` 是 `buffered_message` 自己的 vector，不改。

| 位置 | 现在 | 改成 |
|---|---|---|
| `actor_ref.h` `deliver` | `env.data.assign(...)` | `env.assign_bytes(...)` |
| `base_actor.h` `drain_pending` 和 `pull_and_run` | `env.data.data()` / `env.data.size()` | `env.bytes()` / `env.size()` |
| `actor_system.h` 入站 | `env.data = std::move(msg_payload)` | `env.assign_vector(std::move(msg_payload))` |
| `actor_gtest.cc` | `popped->data.data()`、`env.data.size()`、`env.data.data()` | `bytes()` / `size()` |
| `actor_mailbox_test.cc` | `popped->data.data()` | `bytes()` |
| `actor_robustness_test.cc` | `env.data.size()`、`env.data.data()` | `size()` / `bytes()` |

`local_actor_proxy::deliver` 仍然从原始指针拷进信封，再 `push_envelope`。阶段 A 之后这次拷贝是唯一的一次；阶段 C 之后 16 字节落在 `m_inline`，没有堆分配。

### 6.4 测试

`actor_gtest.cc` 增加两个用例：

- `tiny_msg` 两个 `uint64_t`。`static_assert` 它不大于 32。`make` 之后 `uses_heap()` 为假，`bytes()` 能还原两个字段。
- `wide_msg` 是 `unsigned char[64]`。`static_assert` 它大于 32。`uses_heap()` 为真，首尾字节能还原。

旧断言的期望值不变：小消息的字节数仍是 `sizeof` 那个类型。

## 7. 阶段 D — 只有一种消息时跳过 map

只在阶段 C 的重跑低于 CAF 中位时做。做之前把 C 的中位写下来。

`actor_base::deliver` 每次 `m_handlers.find`。基准 actor 只注册一种消息。`std::function` 在 libstdc++ 里对只捕获 `this` 的 lambda 走小对象优化，不再堆分配。要省的是哈希查找，不是再去掉 `std::function`。

在 `register_handler` 末尾：

- `m_handlers.size() == 1` 时，记下 `m_solo = m_handlers.begin()->second` 和 `m_solo_type = hash`，`m_solo_on = true`。
- 否则 `m_solo_on = false`。同一类型注册第二次，size 仍是 1，要更新 `m_solo`。

`deliver` 开头：`m_solo_on && msg_type == m_solo_type` 时走现有的 try/catch 调用 `m_solo`，然后返回。否则保持 map 查找。死信和异常限流两条路径不变。

`pull_and_run` 和 `drain_pending` 里用 `actor_base::deliver(...)` 限定调用，避免虚函数。全库没有类覆盖 `actor_base::deliver`。`actor_proxy::deliver` 是另一个函数，不在这条热路径上。`actor_base::deliver` 保持虚函数，供现有签名兼容；禁止派生类覆盖它。覆盖会被限定调用静默绕过。这个约束写进 `deliver` 上方的注释。

新增测试：一个 actor 注册一种消息，发一条，计数加一。再注册第二种，两条都能到。这保证 `m_solo_on` 在第二种类型出现后关掉。

阶段 D 再测一轮。仍低于 CAF 就停。

## 8. 测量方法

基准源码在 `/tmp/ultra_actor_bench.cc` 和 `/tmp/caf_actor_bench.cc`，不进仓库。改完头文件后重编：

```bash
g++ -O3 -DNDEBUG -std=c++23 -pthread -I/home/jwy/workspace/ultra-net/include \
  /tmp/ultra_actor_bench.cc -o /tmp/ultra_actor_bench -luring
g++ -O3 -DNDEBUG -std=c++17 -pthread /tmp/caf_actor_bench.cc -o /tmp/caf_actor_bench -lcaf_core
```

只读 `ultra_flood`（不带 `_clock`）和 `caf_flood`，以及 `ultra_pingpong_msgs`。预热一次已经在程序里。五次中位。CAF 和 ultra 在同一次会话里先后跑，中间不跑别的重负载。

正确性用已有的 ASAN 目录 `/tmp/ultranet-asan`：

```bash
cmake --build /tmp/ultranet-asan --target core_mpsc_test actor_gtest actor_mailbox_test actor_robustness_test -j$(nproc)
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/core_mpsc_test
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_gtest --gtest_brief=1
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_mailbox_test
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_robustness_test
```

`detect_leaks=0` 是因为进程退出时 `serve` / `gossip` 的协程帧会被 LeakSanitizer 报出，这在改动前就有。`halt_on_error=1` 仍然抓 use-after-free。本次改动前 `actor_gtest` 是 123 个通过。阶段 C 加 2 个，阶段 D 若做再加 1 个。验收是原先那些仍通过，加上新用例，不要求总数恰好是 123。

## 9. 结果（实现后填写）

测量：2026-09-30 22:46，同一台 i5-8350U。先跑 `/tmp/ultra_actor_bench`，再跑 `/tmp/caf_actor_bench`。Release `-O3 -DNDEBUG`。1 次预热，5 次中位。CAF 打印 `caf_workers=4`。

| 阶段 | send() 中位 | CAF 同次中位 | ping-pong | 是否继续 |
|---|---|---|---|---|
| A+B | 33.4 万条/秒 | 264 万条/秒 | 37.2 万条消息/秒 | 否。低于 80 万的门，未开始 C |
| C | 未做 | | | |
| D（若做了） | 未做 | | | |

`send()` 五次（千条/秒）：486、258、707、334、307。中位 334。最好一次 707，仍低于 80 万。

CAF `anon_send` 五次（千条/秒）：3130、1827、2687、2639、1919。中位 2639。

ping-pong 五次（千条消息/秒）：4086、372、3245、312、325。中位 372。两次在 300 万以上，三次在 37 万附近，中位落在慢的那一簇。改动前同程序中位是 103 万。这次低于第 1 节的 80 万下限。

同一次 ultra 程序里的对照，不经过 actor 调度：vector 信封专用消费者 198 万条/秒，多一次拷贝 243 万，16 字节原样入队 6550 万，只构造信封 8760 万。每 512 条等一次仍是 1.0 万。带时钟的 `send()` 中位 37.7 万。

正确性：`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1` 下 `core_mpsc_test` 通过，`actor_gtest` 123 个通过。单独反复跑 `ScheduleFunction` 时，断言 `ran` 仍会在约 110ms 失败；把 `try_push` 换回按值版本同样 5 次都失败，这不是这次入队改动引入的。全量那一次里这个用例是通过的。

实现时多修了一处设计里没写的释放：`WorkStealingQueue::pop` / `steal` 原先在 CAS 成功之前就把 `UnifiedTask` 移出槽位，CAS 失败时析构唯一的那份，另一个线程可能正在执行同一个 `std::function`。ASAN 报的是 `thread_pool.hpp` 里 `submit_function` 的 lambda 被 `steal` 释放。现在只有 CAS 成功才移动。worker 循环和 `wait_for_events` 没有改。

结论：阶段 A、B 没有把 `send()` 从改动前的 36 万拉开。按第 5 节，拷贝和 100μs 睡眠不是这次中位的主因，停止，不开始阶段 C 和 D。同一次 CAF 是 264 万，没有追上。16 字节消息仍然每次 `std::vector` 分配；专用消费者的邮箱路径是 198 万，和 CAF 仍在同一档，慢的是 actor 的 `send()`。第 1 节把“做完 C”写成实现完成的定义，第 5 节的门更具体，这次按第 5 节停。

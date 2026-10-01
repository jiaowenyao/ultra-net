# 本地 actor 吞吐对齐 CAF Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在这台机器上，把本地 `send()` 灌入 16 字节消息的中位吞吐从大约 36 万条/秒提到至少 150 万条/秒，ping-pong 不掉到 80 万条/秒以下。CAF 的 231 万是对照，不是本计划的通过条件。

**Architecture:** `send()` 比 `try_send()` 慢，是因为 `mailbox::try_push` 按值接收导致每次重试都拷贝 `vector`，以及 `push_envelope` 在队列满时睡到 100μs。`try_send()` 中位 95 万、最好 166 万，专用消费者把同一份 vector 信封跑到 227 万；这两条都有堆分配，相差的是 actor 调度。本计划只去掉多余拷贝、缩短退避、让 32 字节以内不再堆分配。不改 `wait_for_events`，因此不把“追上 CAF 的 231 万”写成通过条件。若 Task 3 之后仍低于 200 万，把数字记在计划末尾，另开调度计划。

**Tech Stack:** C++23、header-only、`ynet::actor`、现有 `MpscQueue`、Release `-O3`、ASAN `-fsanitize=address,undefined`。

## Global Constraints

- 注释使用中文；`if` / `for` / `while` 必须加大括号并换行；缩进 4 空格。
- 命名空间保持 `ynet::actor` 与 `ynet::async`。不新增类层次。
- 不引入 CAF 或其它第三方库进 CMake。CAF 只作为已经在这台机器上跑过的对照。
- 不改 `config`、HTTP body、`IoOperation::cancel`、远端缓冲满时丢新消息的策略。
- 未经用户明确要求，不要 `git commit`。
- 验收数字只对这台机器有效：Intel Core i5-8350U（4 核 8 线程，最高 3.6GHz），4 个 worker，Release，N=100000，1 次预热后 5 次，取中位。机器忙时只允许重跑一轮，不允许把门槛改低。

## 已经测到的数字（2026-09-30，同进程形态）

两边都是 4 个调度线程、主线程灌入、接收方用 `atomic` 计数、N=100000。CAF 是 apt 的 libcaf 0.17.6，`scheduler.max-threads=4`（程序打印 `caf_workers=4`）。`scheduler.policy` 设成字符串失败（`type_mismatch`），用的是 CAF 默认 work-stealing。ultra-net 是当前工作区，`max_per_activation=1024`。

| 路径 | 中位 | 五次（条/秒） |
|---|---|---|
| ultra `send()`，循环里不取时钟 | 356k | 414k, 439k, 202k, 356k, 305k |
| ultra `send()`，每条取 `steady_clock` | 512k | 512k, 699k, 470k, 431k, 755k |
| ultra `try_send()`，满了就 `yield` | 947k | 410k, 924k, 947k, 1090k, 1660k |
| ultra 信封队列，专用消费者，`yield` | 2.27M | 2.38M, 1.73M, 2.18M, 2.51M, 2.27M |
| ultra 再拷一次信封后入队 | 2.46M | 噪声内，不能当成“拷贝免费” |
| ultra 16 字节原样入队 | 66.4M | 56.7M–71.0M |
| ultra 只构造信封再析构 | 81.7M | 单线程 |
| CAF `anon_send` | 2.31M | 2.62M, 2.31M, 1.64M, 1.72M, 2.83M |
| CAF 每条取时钟 | 2.36M | 与不取时钟同一档 |
| ultra ping-pong（2 万个来回） | 1.03M 条消息/秒 | 来回中位 514k/秒；单次 156k–3.17M，方差大 |
| CAF ping-pong | 0.97M 条消息/秒 | 来回中位 484k/秒 |
| ultra 每 512 条等一次 | 12k | 这是“每条消息一次激活”的悬崖，不是灌入吞吐 |

对照关系：CAF 灌入和“vector 信封 + 专用消费者”在同一档（约 230 万）。`send()` 比这一档慢大约 6.5 倍。`try_send()` 已经是一次分配、满了只 yield，预热后最好 166 万，仍低于邮箱本身。16 字节原样队列是 6640 万，那是没有类型擦除、没有调度的上限，不是 CAF 的数字。

## 根因（按代码，不是猜测）

1. `local_actor_proxy::deliver`（`include/ultranet/actor/core/actor_ref.h`）对平凡消息 `assign` 进 `std::vector<uint8_t>`，16 字节也走堆。
2. 二次拷贝发生在 `mailbox::try_push(message_envelope env)`（`include/ultranet/actor/core/mailbox.h`）的按值形参，不是只发生在队列里。`push_envelope` 把左值 `env` 传给它，每次调用先拷贝整份信封；队列满时这次拷贝被丢掉，下一次重试再分配。`MpscQueue::try_push(T item)` 同样按值接收。只改队列、不改 `mailbox::try_push`，actor 路径的重试分配还在。`WorkStealingThreadPool` 里 `try_push(std::move(task))` 也是按值：队列满时任务在形参析构时丢掉，循环重试的是空任务。这是现状 bug，Task 1 要修掉。
3. 同一函数在第 50 次失败后 `sleep` 1μs，第 100 次后 10μs，第 150 次后 100μs。邮箱容量 4096，10 万条灌入必满。`try_send` 不走这段睡眠，所以它比 `send()` 高。
4. `message_envelope::make`（`include/ultranet/actor/core/mailbox.h`）对非序列化消息 `resize + memcpy`，同样强制堆分配。

明确不做：不让 worker 在 `wait_for_events` 里空转。ping-pong 已经和 CAF 同一档；`reactor.hpp` 里 100ms 超时是没被 eventfd 叫醒时的上界。每 512 条同步一次只有 1.2 万条/秒，是因为 worker 把一条消息处理完就清掉 `m_activated` 并回到 io_uring 等待。那是另一个改动，会碰到网络延迟，本计划不碰。

---

### Task 1: 入队只在成功时把元素移进槽

**Files:**
- Modify: `include/ultranet/coroutine/mpsc_queue.hpp`（`try_push`）
- Modify: `include/ultranet/actor/core/mailbox.h`（`mailbox::try_push` 不再按值接收）
- Modify: `tests/core_mpsc_test.cc`
- Test: `tests/core_mpsc_test.cc`

**Interfaces:**
- Consumes: `mailbox::try_push`、`mailbox::push_blocking`、`actor_base::try_push_envelope`、`actor_base::push_envelope`、`WorkStealingThreadPool` 里两处 `try_push(std::move(task))`、`tests/core_mpsc_test.cc` 里 `const int value` 的 `try_push(value)`。
- Produces: 三个重载，都转给私有 `bool emplace(T& item)`。
  - `bool try_push(T& item)`：成功则移走 `item`，失败则 `item` 不变。
  - `bool try_push(T&& item)`：同上。`try_push(std::move(task))` 在失败时任务还在，这是修复后的行为；现在按值版本会把任务丢掉。
  - `bool try_push(const T& item)`：先拷贝到局部，再 `emplace` 那个局部。成功或失败都不改源对象。`push_blocking(const message_envelope&)` 和 `const int` 测例走这条。热路径不要用它，否则每次重试仍会分配。

- [ ] **Step 1: 写会失败的测试**

在 `tests/core_mpsc_test.cc` 增加一个用例，队列容量 2，元素是 `std::unique_ptr<int>`。前两次推入成功，源指针变空。第三次队列已满，返回 false，源指针仍拥有 `int(3)`。现有“满队列拒绝”的 `int` 用例保持不变。

```cpp
void test_failed_push_keeps_source() {
    T("failed push keeps unique_ptr");
    MpscQueue<std::unique_ptr<int>, 2> q;
    auto a = std::make_unique<int>(1);
    auto b = std::make_unique<int>(2);
    auto c = std::make_unique<int>(3);
    CHECK(q.try_push(a), "first");
    CHECK(a == nullptr, "moved on success");
    CHECK(q.try_push(b), "second");
    CHECK(!q.try_push(c), "full");
    CHECK(c != nullptr, "kept on failure");
    CHECK(*c == 3, "value intact");
    auto p1 = q.try_pop();
    CHECK(p1.has_value() && *p1 && **p1 == 1, "pop first");
    PASS();
}
```

`main` 里调用 `test_failed_push_keeps_source()`。这个文件的 `CHECK` 失败会 `return`，所以断言必须放在 `PASS()` 之前。

- [ ] **Step 2: 跑测试，确认红**

先只改测试，不改队列。当前 `try_push` 按值接收，`try_push(a)` 对 `unique_ptr` 左值不能编译。

```bash
cmake --build /tmp/ultranet-asan --target core_mpsc_test -j$(nproc)
```

Expected: 编译失败，错误指向 `try_push(a)` 无法把 `unique_ptr` 左值拷进按值参数。

- [ ] **Step 3: 改 `try_push`**

用下面的实现替换 `mpsc_queue.hpp` 里的 `try_push`。失败分支（`dif < 0`）直接 `return false`，不要动 `item`。只有 CAS 成功后才 `slot.data = std::move(item)`。

同时把 `mailbox::try_push` 改成下面两个重载，删掉按值的那个。`push_envelope` 传左值，走引用重载，成功才把 `env` 移进队列。`try_push_envelope` 里已有的 `try_push(std::move(env))` 走右值重载，不用改调用。

```cpp
bool try_push(message_envelope& env) {
    return m_queue.try_push(env);
}

bool try_push(message_envelope&& env) {
    return try_push(static_cast<message_envelope&>(env));
}
```

`push_blocking(const message_envelope&)` 保持不动。它会命中队列的 `const T&` 重载，每次尝试拷贝一次；这只在 200 次失败之后发生。

队列实现：

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

bool emplace(T& item) {
    size_t pos = m_enqueue_pos.load(std::memory_order_relaxed);
    for (;;) {
        Slot& slot = m_buffer[pos & kMask];
        size_t seq = slot.sequence.load(std::memory_order_acquire);
        intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);
        if (dif == 0) {
            if (m_enqueue_pos.compare_exchange_weak(
                    pos, pos + 1, std::memory_order_relaxed)) {
                slot.data = std::move(item);
                slot.sequence.store(pos + 1, std::memory_order_release);
                return true;
            }
        } else if (dif < 0) {
            return false;
        } else {
            pos = m_enqueue_pos.load(std::memory_order_relaxed);
        }
    }
}
```

`MpscQueue` 在 `public:` 之前是默认私有数据。三个 `try_push` 重载留在现有 `public:` 段里，替换原来的按值 `try_push`。上面代码块里的 `emplace` 不要和它们放在一起还带着 `private:`。把 `emplace` 的函数体放到类的最后，也就是 `empty()` 之后，单独加一个 `private:`。不要把 `private:` 插在 `try_push` 和 `try_pop` 之间，否则 `try_pop` 会变成私有。`try_pop` 的函数体不动。

非 const 的 `int` 左值会绑到 `T&`。对 `int` 来说移动等于拷贝，循环变量的值还在。`const int` 绑到 `const T&`，源不变。不要改 `tests/core_mpsc_test.cc` 里 `const int value` 那次 `try_push(value)`。

`thread_pool.hpp` 的两处调用保持 `try_push(std::move(task))`。修复前，队列满时任务在按值形参里被析构。修复后，失败不移动 `task`，循环重试的是同一个任务。不要改这两处调用。

- [ ] **Step 4: 跑测试，确认绿**

```bash
cmake --build /tmp/ultranet-asan --target core_mpsc_test actor_gtest -j$(nproc)
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/core_mpsc_test
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_gtest --gtest_brief=1
```

Expected: `core_mpsc_test` 全部 PASSED。`actor_gtest` 原先通过的测试仍然通过（本次 ASAN 跑分是 123），没有 heap-use-after-free。ASAN 构建目录是 `/tmp/ultranet-asan`（Debug + ASan/UBSan）。若目录不存在，用：

```bash
cmake -S /home/jwy/workspace/ultra-net -B /tmp/ultranet-asan -DCMAKE_BUILD_TYPE=Debug \
  -DULTRANET_BUILD_EXAMPLES=OFF \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
```

---

### Task 2: `push_envelope` 在队列满时不要睡到 100μs

**Files:**
- Modify: `include/ultranet/actor/core/base_actor.h`（`push_envelope`）
- Test: 现有 `tests/actor_gtest.cc` 的发送用例，加上本任务的 Release 灌入

**Interfaces:**
- Consumes: Task 1 的 `mailbox::try_push(message_envelope&)`。`push_envelope` 继续把左值 `env` 传进去。失败时 `env` 仍完整，成功时 `env` 被移走，函数随后返回，不再读 `env`。这条语义在 Task 1 改掉 mailbox 的按值形参之后才成立。
- Produces: 无新函数。退避变为：前 64 次 `yield`，之后每次 `sleep_for(1us)`，循环仍最多 200 次，然后 `push_blocking`。

- [ ] **Step 1: 改退避**

替换 `push_envelope` 里 `for (int attempt = 0; attempt < 200; ++attempt)` 的循环体。成功分支的顺序保持 `try_push` → `m_pending.fetch_add` → `try_activate` → `return`。删掉 10μs 和 100μs 两档。

```cpp
for (int attempt = 0; attempt < 200; ++attempt) {
    if (m_mailbox.try_push(env)) {
        m_pending.fetch_add(1, std::memory_order_release);
        try_activate();
        return;
    }
    if (attempt < 64) {
        std::this_thread::yield();
    } else {
        std::this_thread::sleep_for(std::chrono::microseconds(1));
    }
}
m_mailbox.push_blocking(env);
m_pending.fetch_add(1, std::memory_order_release);
try_activate();
```

`push_blocking` 本身已经是 `yield` 循环，不要再给它加睡眠。

- [ ] **Step 2: 正确性**

```bash
cmake --build /tmp/ultranet-asan --target actor_gtest -j$(nproc)
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_gtest --gtest_brief=1
```

Expected: 原先通过的 `actor_gtest` 仍然全部通过（本次 ASAN 跑分是 123）。退出码 0，没有 heap-use-after-free。

- [ ] **Step 3: 灌入门槛（Task 3 之前的门）**

重编 Release 和 `/tmp/ultra_actor_bench.cc`（N=100000，4 线程，`max_per_activation=1024`，只看 `ultra_flood` 那一行）。1 次预热，5 次，取中位。

```bash
cmake --build /tmp/ultranet-rel -j$(nproc) --target perf_bench
g++ -O3 -DNDEBUG -std=c++23 -pthread -I/home/jwy/workspace/ultra-net/include \
  /tmp/ultra_actor_bench.cc -o /tmp/ultra_actor_bench -luring
/tmp/ultra_actor_bench 2>/dev/null | rg 'ultra_flood '
```

Expected: `ultra_flood`（不带 `_clock`）中位至少 100 万条/秒。这只说明睡眠和多余拷贝收掉了，还不要求超过 CAF。若中位低于 100 万，停在这里，不要开始 Task 3。

---

### Task 3: 32 字节以内的信封不再堆分配

**Files:**
- Modify: `include/ultranet/actor/core/mailbox.h`（`message_envelope`）
- Modify: `include/ultranet/actor/core/actor_ref.h`（`deliver` 里的 `env.data.assign`）
- Modify: `include/ultranet/actor/core/base_actor.h`（两处 `env.data.data()` / `env.data.size()`）
- Modify: `include/ultranet/actor/system/actor_system.h`（`env.data = std::move(msg_payload)`）
- Modify: `tests/actor_gtest.cc`、`tests/actor_mailbox_test.cc`、`tests/actor_robustness_test.cc` 里对 `message_envelope::data` 的访问
- Test: `tests/actor_gtest.cc` 新增小消息 / 大消息用例

**Interfaces:**
- Consumes: Task 1 的移动语义。槽里的信封被 `try_pop` 移出后，`bytes()` 仍指向有效存储。
- Produces:
  - `static constexpr size_t k_inline_capacity = 32`
  - `const uint8_t* bytes() const` / `uint8_t* bytes()`
  - `size_t size() const`
  - `bool uses_heap() const`（`m_size > k_inline_capacity`）
  - `void assign_bytes(const uint8_t* src, size_t n)`
  - `void assign_vector(std::vector<uint8_t> v)`
  - 删除对外的 `std::vector<uint8_t> data` 字段。调用方改走上面的函数。

布局：`uint64_t msg_type`，`uint32_t m_size`，`alignas(std::max_align_t) uint8_t m_inline[32]`，`std::vector<uint8_t> m_heap`。`bytes()` 在 `m_size <= 32` 时返回 `m_inline`，否则返回 `m_heap.data()`。

复制：默认拷贝会同时拷贝数组和 vector。小消息路径必须保证 `m_heap.empty()`，这样拷贝不分配。移动：手写移动构造和移动赋值，把 `m_inline` `memcpy` 过去，移走 `m_heap`，然后把源的 `m_size` 和 `msg_type` 置 0。定义了移动就必须同时定义拷贝。`assign_bytes` 先写 `m_size = static_cast<uint32_t>(n)`。`n <= 32` 时 `m_heap.clear()` 再 `memcpy` 到 `m_inline`；`n > 32` 时 `m_heap.assign(src, src + n)`。`assign_vector` 在 `v.size() <= 32` 时转 `assign_bytes(v.data(), v.size())`，否则 `m_size = static_cast<uint32_t>(v.size())` 且 `m_heap = std::move(v)`。小消息路径结束时 `m_heap.empty()` 必须为真，否则默认拷贝会把 vector 的容量一起带走并分配。每个邮箱槽会多出 32 字节内联缓冲，4096 槽大约多 128KB，可以接受。

`make()`：序列化结果走 `assign_vector`；平凡消息走 `assign_bytes(reinterpret_cast<const uint8_t*>(&msg), sizeof(Msg))`。不要再 `resize`。

`local_actor_proxy::deliver`：`env.msg_type = msg_type` 后 `env.assign_bytes(static_cast<const uint8_t*>(data), len)`。

`pull_and_run` 和 `drain_pending` 的 lambda：`deliver(env.msg_type, env.bytes(), env.size())`。

`actor_system` 入站：`env.assign_vector(std::move(msg_payload))`。

- [ ] **Step 1: 写会失败的测试**

在 `tests/actor_gtest.cc` 的 mailbox 测试附近增加：

```cpp
struct tiny_msg {
    static constexpr const char* actor_type = "tiny";
    uint64_t a;
    uint64_t b;
};
struct wide_msg {
    static constexpr const char* actor_type = "wide";
    unsigned char raw[64];
};

TEST(EnvelopeStorage, SmallMessageStaysInline) {
    auto env = message_envelope::make(tiny_msg{7, 9});
    EXPECT_EQ(env.size(), sizeof(tiny_msg));
    EXPECT_FALSE(env.uses_heap());
    tiny_msg out{};
    std::memcpy(&out, env.bytes(), sizeof(out));
    EXPECT_EQ(out.a, 7u);
    EXPECT_EQ(out.b, 9u);
}

TEST(EnvelopeStorage, WideMessageUsesHeap) {
    wide_msg in{};
    in.raw[0] = 1;
    in.raw[63] = 2;
    auto env = message_envelope::make(in);
    EXPECT_EQ(env.size(), sizeof(wide_msg));
    EXPECT_TRUE(env.uses_heap());
    wide_msg out{};
    std::memcpy(&out, env.bytes(), sizeof(out));
    EXPECT_EQ(out.raw[0], 1);
    EXPECT_EQ(out.raw[63], 2);
}
```

`static_assert(sizeof(tiny_msg) <= message_envelope::k_inline_capacity)` 和 `static_assert(sizeof(wide_msg) > message_envelope::k_inline_capacity)` 放在两个测试上面。

- [ ] **Step 2: 跑测试，确认红**

```bash
cmake --build /tmp/ultranet-asan --target actor_gtest -j$(nproc)
```

Expected: 编译失败，`uses_heap` / `bytes` 不存在，或旧的 `env.data` 仍在而新测试对不上。以新测试不能通过为准。同时把下列旧访问改成 `bytes()` / `size()`，否则它们会在字段改名后一起编译失败：

- `tests/actor_gtest.cc` 里 `popped->data.data()`、`env.data.size()`、`env.data.data()`
- `tests/actor_mailbox_test.cc` 里 `popped->data.data()`
- `tests/actor_robustness_test.cc` 里 `env.data.size()`、`env.data.data()`

这些旧断言的期望值不变：小消息的字节数仍是 `sizeof` 那个类型。

- [ ] **Step 3: 实现内联缓冲并改完全部 `message_envelope::data` 访问**

只改本任务 Files 列出的位置。`remote_proxy.h` 里的 `buf.data` 是 `buffered_message` 自己的 vector，不要改。

移动后的信封被 `try_pop` 拿出来时，`bytes()` 必须指向槽外这份对象自己的 `m_inline` 或 `m_heap`，不能指向源对象。移动构造里对 `m_inline` 的 `memcpy` 就是为了这个。

- [ ] **Step 4: 正确性 + 吞吐门槛**

```bash
cmake --build /tmp/ultranet-asan --target actor_gtest actor_mailbox_test actor_robustness_test core_mpsc_test -j$(nproc)
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_gtest --gtest_brief=1
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/core_mpsc_test
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_mailbox_test
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_robustness_test
cmake --build /tmp/ultranet-rel -j$(nproc) --target perf_bench
g++ -O3 -DNDEBUG -std=c++23 -pthread -I/home/jwy/workspace/ultra-net/include \
  /tmp/ultra_actor_bench.cc -o /tmp/ultra_actor_bench -luring
/tmp/ultra_actor_bench 2>/dev/null | rg 'ultra_flood |ultra_pingpong_msgs'
```

Expected:

- 上述测试退出码 0。原先通过的 `actor_gtest` 仍通过，并加上 `EnvelopeStorage` 两个用例。
- `ultra_flood` 中位至少 150 万条/秒。这是 `try_send` 最好一档（166 万）附近。CAF 的 231 万含调度，本任务不要求达到。
- `ultra_pingpong_msgs` 中位至少 80 万条/秒。
- 若灌入中位低于 150 万：停止。不要加 worker 空转，不要改 `max_per_activation` 来凑数字。把 Task 2 和 Task 3 的中位写进本计划末尾。低于 200 万的剩余差距记成调度后续，不在这里改 `wait_for_events`。

---

## 自检

- 覆盖：mailbox 按值拷贝和队列失败丢元素 → Task 1；100μs 退避 → Task 2；16 字节堆分配 → Task 3。ping-pong 回归门槛写在 Task 3。
- 没有把“每 512 条一次激活只有 1.2 万条/秒”塞进本计划。CAF 231 万不是通过条件。
- `try_push(T&)` 失败不消费源对象。`push_envelope` 走 `mailbox::try_push(message_envelope&)`，不走 `const T&`。`push_blocking` 仍走 `const T&`，每次尝试拷贝一次。
- 门槛是这台 i5-8350U 上的中位，不是 README 里的 P50 1μs。

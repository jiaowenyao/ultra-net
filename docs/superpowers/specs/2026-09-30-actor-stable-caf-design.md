# 稳定本地灌入到 CAF — 设计

日期：2026-09-30  
状态：阶段 1、2 已落地。2026-09-30 23:35 同次测量里 send() 中位 2427 万，高于 CAF 中位 274 万，最慢 2143 万。未做阶段 D。  
取代关系：实现以本文为准。`docs/superpowers/specs/2026-09-30-actor-catch-caf-design.md` 的阶段 A、B 已经落地，它的 80 万门没有过，阶段 C 没有做。那个文档禁止改 `wait_for_events`。本文只改“叫醒哪一个 worker”，100ms 超时的数值保持不变。

## 1. 要稳定的数字

机器：Intel Core i5-8350U，4 核 8 线程。调度器 `powersave`，一次采样里频率在 2.6–3.4GHz，没有掉到 400MHz。  
负载：4 个调度线程，主线程连续 `send` 10 万条 16 字节消息，一个 actor 用 `atomic` 计数。1 次预热不计入，之后 12 次。Release `-O3 -DNDEBUG`。CAF 是 libcaf 0.17.6，`scheduler.max-threads=4`，程序打印 `caf_workers=4`。

2026-09-30 23:00 之后同机测量：

| 路径 | 结果 |
|---|---|
| ultra `send()`，25 次去掉预热后的中位 | 44 万条/秒 |
| 同一次进程里的最好三次 | 252 万、245 万、178 万 |
| CAF `anon_send`，12 次去掉预热后的中位 | 232 万（157 万–310 万） |

中位用和现有基准相同的取法：升序排序后取 `v[v.size() / 2]`。12 次取下标 6。

目标：同一次会话里，ultra `send()` 的中位不低于 CAF `anon_send` 的中位，并且这 12 次里最慢的一次不低于 100 万条/秒。ping-pong 消息中位不低于 80 万条/秒；它不决定是否进入阶段 2。

实现完成的定义和目标分开。阶段 1 做完并测完。灌入的两个不等式都成立就停止，不开始阶段 2，也不做阶段 D。有一个不成立才做阶段 2，再测一轮。阶段 2 之后中位仍低于同一次 CAF，才做阶段 D 一次，再测一轮。阶段 2 之后中位已经不低于 CAF，不做阶段 D。每一轮的数字都写入第 8 节，写完就结束，不管有没有稳住。不把 `wait_for_events` 的 100ms 改小，不把 `max_per_activation` 调大，不让 worker 空转。

## 2. 慢的那一簇是怎么来的

快的一次大约 40ms（250 万条/秒）。慢的一次是 300–600ms。差出来的是若干次大约 100ms 的停顿，不是每条消息都贵了几微秒。

`pull_and_run` 在线程池上跑，不是每条消息一个协程。gcov（`--coverage`，生产者被插桩拖慢，只用来数分支）对 260 万条消息的计数：

- `local_actor_proxy::deliver` 里 `env.data.assign` 260 万次。16 字节仍然每次分配。
- `try_push` 第一次成功占 96%。`yield` 9.2 万次，睡 1μs 2.5 万次，`push_blocking` 173 次。阶段 A、B 改的拷贝和长睡眠不在这条热路径上。
- `try_activate` 260 万次，CAS 成功 2604 次，大约每 1000 条一次。
- 这 2604 次 `pull_and_run` 里，2578 次结束时 `m_pending > 0`，当场再调度。
- `wait_for_events` 657 次。

在同一个 worker 上时，`submit_function` 把下一次 `pull_and_run` 推进本地队列，不写 eventfd。worker 循环先排空本地队列，不会去 `wait_for_events`。这就是 250 万那一簇。

链断开的窗口在 `base_actor.h` 的 `pull_and_run`：先 `m_activated.store(false)`，再读 `m_pending`。这一瞬间是 0，就不再 `try_activate`。worker 排空本地队列之后进入 `wait_for_events`，超时 100ms。下一条 `send` 从主线程进来，`try_activate` 走 `enqueue_external`。

`enqueue_external` 用 `m_next_worker` 轮转，推进 `m_mpsc_queues[idx]`，然后 `wake_workers()`。`wake_workers` 对唯一一把 `m_event_fd` 做一次 `write`，写入的值是次数，不是写多次。eventfd 没有 `EFD_SEMAPHORE` 时，一次 `read` 把计数清零，四个 worker 里只有一个正在等待的 `io_uring` 读会完成。

`MpscQueue::try_pop` 用的 `m_dequeue_pos` 是普通 `size_t`，不是原子变量。只有下标为 `idx` 的那个 worker 可以弹出自己的队列。被叫醒的如果是别的 worker，它看自己的 MPSC，是空的，再回到 `wait_for_events`。任务留在主人的队列里，直到主人自己的 100ms 超时结束。一次这样的错过就把 40ms 的运行拉进慢的那一簇。

所以阶段 A、B 稳定不了中位：它们不决定这次唤醒打到谁。

## 3. 不改什么

- 不改 `IoReactor::wait_for_events` 的 100ms。
- 不在 worker 循环里空转。
- 不增加线程，不改 `max_per_activation`（默认 256，基准程序自己设 1024）。
- 不让别的 worker 去 `try_pop` 别人的 `MpscQueue`。`m_dequeue_pos` 不是原子的，多消费者是数据竞争。
- 不把每条消息改成协程，不为每条消息再提交一次 io_uring。那是挂起和恢复，灌入不会更快。
- 不改邮箱容量、不改阶段 A 的“成功才移动”、不改阶段 B 的退避。
- 阶段 1 不改 `message_envelope` 的 `vector` 布局。
- 不把 CAF 链进 CMake。
- 未经用户要求不提交。

## 4. 方案

三个做法里采用第一个。

1. 每个 worker 一把 eventfd。任务推进 `m_mpsc_queues[idx]` 之后，只写 `idx` 那把。主人正在 `wait_for_events` 时，它自己的读完成，回到循环，弹出自己的队列。100ms 不再决定这次灌入。写发生在进入 `wait_for_events` 之前时也不改 `IoReactor`：该 worker 上已经挂着一次 eventfd 读，这次写会完成那个 CQE，`wait_cqe` 立刻返回。推荐这个。
2. 谁被共享 eventfd 叫醒，谁就扫描全部 MPSC。做不到。`try_pop` 不是多消费者安全的。
3. 把 100ms 改成 1ms，或者在 `wait_for_events` 里空转。超时改小只是把错过唤醒的代价封顶，空转会占住原来等网络完成的核。两个都不做。

## 5. 阶段 1 — 按 worker 唤醒

只改 `include/ultranet/coroutine/thread_pool.hpp`。`IoReactor` 已经接收一个 eventfd，构造函数签名不动。

### 5.1 构造和析构

删掉单个 `m_event_fd`。换成 `std::vector<int> m_event_fds`。

在启动任何 worker 之前，为每个线程 `eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)`。某一把失败时，关掉已经创建的那些，再抛 `std::system_error`。向量在线程启动之后不再扩容，worker 只按下标读。

`worker_thread(i)` 里构造 `IoReactor` 时传入 `m_event_fds[i]`，不再传入同一把 fd。

析构顺序保持：`m_stop = true`，然后叫醒每一个 worker，再 `join`，最后关掉每一把 fd。现在的 `wake_workers(m_workers.size())` 只写一次共享 fd，并不会叫醒所有人；改完之后关闭路径会逐个写。

### 5.2 唤醒函数

删掉 `wake_workers(uint64_t count = 1)`。换成：

```cpp
void wake_worker(size_t worker_id) {
    if (worker_id >= m_event_fds.size()) {
        return;
    }
    uint64_t val = 1;
    // 不重试。EAGAIN 一般是计数溢出；别的错误也不在这里终止进程，析构还要 join。
    ssize_t written = ::write(m_event_fds[worker_id], &val, sizeof(val));
    (void)written;
}

void wake_all_workers() {
    for (size_t i = 0; i < m_event_fds.size(); ++i) {
        wake_worker(i);
    }
}
```

`write` 的返回值必须接住，避免现有的 `warn_unused_result`。不要因为 `EAGAIN` 或其他错误重试。溢出时计数已经极大，随后的读仍会把主人叫醒。

热路径上的本地入队不调用 `wake_worker`。worker 正在跑，任务已经在它的本地队列里。写发生在 `wait_for_events` 之前时，也不要改 `IoReactor` 或加空转：该线程的 eventfd 读已经提交，CQE 会让 `wait_cqe` 立刻返回。

### 5.3 调用点

`enqueue_external`：`try_push` 成功之后，对最终的 `idx` 调用 `wake_worker(idx)`。循环因为队列满而换 `idx` 时，叫醒的是成功的那一次，不是失败前的下标。

`submit_on_thread`：`try_push` 进 `m_mpsc_queues[worker_id]` 之后调用 `wake_worker(worker_id)`。协程仍然只在这个线程的 io_uring 上跑。

析构调用 `wake_all_workers()`。

`submit_function` 在本线程是池内 worker 时仍然只 `push` 本地队列，不写 eventfd。

### 5.4 测试

不新增会睡 100ms 的定时测试。用已经会暴露这个问题的用例。

`ActorSystemTest.ScheduleFunction` 在 `sys.schedule` 之后睡 **50ms** 再读 `ran`。这个睡眠必须保持远小于 100ms。改成 110ms 会让 `wait_for_events` 的超时先把任务跑完，`ran` 变成 true，测不出唤醒打错线程。改动前单独跑这个用例，经常在这 50ms 里看到 `ran == false`：共享 eventfd 的那一次写被别的 worker 读走了。这不是每次都失败，因为 `serve` / `gossip` 的 I/O 也可能在 50ms 内碰巧叫醒正确的线程。阶段 1 之后连续跑 20 次，20 次都通过。这 20 次是回归门，不是灌入达到 CAF 的证据。用 ASAN 二进制：

```bash
cmake --build /tmp/ultranet-asan --target actor_gtest -j$(nproc)
for i in $(seq 1 20); do
  ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 \
    /tmp/ultranet-asan/bin/actor_gtest \
    --gtest_filter='ActorSystemTest.ScheduleFunction' --gtest_brief=1
done
```

然后同一二进制跑完整 `actor_gtest`，原先通过的用例仍然通过。`detect_leaks=0` 是因为进程退出时 `serve` / `gossip` 的协程帧会被 LeakSanitizer 报出，这在改动前就有。`halt_on_error=1` 仍然抓 use-after-free。

`core_mpsc_test` 不覆盖线程池，但头文件被这次改动间接包含的测试要能链接。阶段 1 不改队列算法，不要求新的 MPSC 用例。

## 6. 阶段 2 — 只有阶段 1 没稳住才做

条件：阶段 1 测完，`send()` 中位低于同一次 CAF 中位，或者 12 次里最慢的一次低于 100 万。两个不等式都成立则不做本节。ping-pong 不触发本节。

做的内容和 `2026-09-30-actor-catch-caf-design.md` 第 6 节相同：`message_envelope` 32 字节内联缓冲，五个特殊成员，`assign_bytes` / `assign_vector` / `bytes()`，以及那里列出的调用点。测试仍是 `tiny_msg` 不走堆、`wide_msg` 走堆、先堆再短再移动赋值。

不做那个文档的阶段 D，除非阶段 2 测完中位仍低于 CAF。阶段 D 最多做一次：单处理函数跳过 `unordered_map`，`pull_and_run` 用 `actor_base::deliver` 限定调用。做完再测一轮。仍低于 CAF 就停。

阶段 2 和阶段 D 的实现细节以那个文档的第 6、7 节为准，本文不另写一套布局。那个文档第 5 节的 80 万门作废，不再使用。

## 7. 测量

基准源码留在 `/tmp`，不进仓库。阶段 1 改完头文件后重编。灌入只用 flood，不跑“每 512 条等一次”的 batched，避免 10 秒空等把频率和缓存带偏。

```bash
g++ -O3 -g -fno-omit-frame-pointer -DNDEBUG -std=c++23 -pthread \
  -I/home/jwy/workspace/ultra-net/include \
  /tmp/ultra_flood_only.cc -o /tmp/ultra_flood_only -luring
g++ -O3 -g -fno-omit-frame-pointer -DNDEBUG -std=c++17 -pthread \
  /tmp/caf_flood_only.cc -o /tmp/caf_flood_only -lcaf_core
```

两个程序都是 1 次预热加 12 次计入。先跑完 ultra，立刻跑 CAF，中间不跑别的重负载。ping-pong 用 `/tmp/ultra_actor_bench.cc` 和 `/tmp/caf_actor_bench.cc` 里已有的 ping 函数，另编只跑 ping 的程序，各 1 次预热加 12 次。ping 的消息速率是来回两条，和现有 `ultra_pingpong_msgs` 的算法一样：`40000 * 1000000 / median_us`。

通过条件：

- ultra `send()` 中位 `>=` 同一次 CAF `anon_send` 中位
- ultra 这 12 次的最小值 `>=` 100 万条/秒
- ultra ping-pong 消息中位 `>=` 80 万条/秒

前两个都成立，阶段 1 结束，不开始阶段 2。前两个有一个不成立，进入阶段 2，测完再看前两个。ping-pong 低于 80 万也写入第 8 节，但不因此进入阶段 2。若 ping-pong 低于阶段 1 之前测到的 37 万条消息/秒，先检查是不是这次把激活打到了错误的线程，修完只再测一次 ping，不靠空转或加大 `max_per_activation`。

## 8. 结果（实现后填写）

| 阶段 | send() 中位 | 最慢一次 | CAF 同次中位 | ping-pong | 是否继续 |
|---|---|---|---|---|---|
| 1 | 254 万条/秒 | 237 万条/秒 | 256 万条/秒 | 602 万条消息/秒 | 是。中位比 CAF 少 1.6 万条/秒 |
| 2 | 2427 万条/秒 | 2143 万条/秒 | 274 万条/秒 | 615 万条消息/秒 | 否。两个灌入不等式都成立，未做 D |
| D（若做了） | 未做 | | | | |

阶段 1 的 12 次（万条/秒）：248、237、244、255、262、244、262、254、254、259、251、256。中位取升序下标 6，是 254 万。CAF 同一次：255、259、230、259、312、154、144、241、287、316、205、256，中位 256 万。

阶段 2 的 12 次（万条/秒）：2220、2190、2300、2341、2379、2464、2586、2427、2620、2677、2709、2143。中位 2427 万，最慢 2143 万。CAF 同一次中位 274 万，范围 147 万–331 万。ping-pong 中位 615 万条消息/秒，最慢 363 万。

正确性：阶段 1 之前 `ScheduleFunction` 单独 20 次失败 13 次。按 worker 唤醒之后 20 次都通过。ASAN `actor_gtest` 在阶段 2 加了 3 个信封用例后是 126 个通过。`actor_mailbox_test`、`actor_robustness_test`、`core_mpsc_test` 通过。`ASAN_OPTIONS=detect_leaks=0:halt_on_error=1`。

结论：阶段 1 把慢的一簇收掉了，12 次都在 237 万以上，但中位仍比同一次 CAF 少大约 1.6 万条/秒，所以做了阶段 2。32 字节内联缓冲之后，同一次 `send()` 中位是 2427 万条/秒，最慢 2143 万，CAF 中位是 274 万。灌入已经稳定高于 CAF，没有做阶段 D。ping-pong 中位 615 万，高于 80 万。100ms 超时和 `max_per_activation` 没有改。

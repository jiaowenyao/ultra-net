# 分布式 actor 运行时 — 设计

日期：2026-09-30
状态：已按复核意见修改，待再次复核
本地灌入的测量以 `2026-09-30-actor-stable-caf-design.md` 为准。本文不重写那条热路径，也不把远端测试的数字做成比本地灌入更好看。

## 1. 做完之后必须为真的事

下面六件事都要用真实进程测，数字从程序输出里抄进第 9 节，不在代码里预先写好再打印。

1. 进程 B 上的 actor 收到进程 A 用 `actor_ref::send` 送出的消息。A 和 B 是两个操作系统进程，回环地址 `127.0.0.1`，各有自己的 `actor_system`。
2. `send` / `try_send` 能区分收下和没收下。两个进程都还活着时，已经收下的远端消息至少进入对端邮箱一次。重复次数单独计数。
3. `ask` 在本进程和两个进程之间都能等到 `reply`。对端不 `reply` 时，按调用方给出的超时返回失败。
4. 装了监管者的 actor，处理函数抛出 `std::exception` 时按次数调用 `on_restart()`。超过次数后停止，后面的消息不再进入业务计数。没装监管者的 actor 保持现在的行为：捕获异常并打日志。
5. 两个进程交换过 gossip 之后，把其中一个 `SIGKILL`。活着的那个在配置的节点超时之内，`live_nodes` 里不再有死者。
6. actor 主动写入并 `fsync` 的快照，在本进程被 `SIGKILL` 之后能被新进程读回。没有写进快照的在途远端消息，测到多少丢失就记多少，不要求是 0。

下面这些不是本轮的完成条件，测试和结论里不许写成已经做到：

- 恰好一次送达。
- `kill -9` 之后每条已经 `send` 返回 true 的远端消息仍能送达。那要每条 fsync，本轮不做。
- 监管树、事件溯源、跨机房。
- 远端吞吐超过本地灌入，或超过 CAF。

## 2. 现在代码里和这句话矛盾的地方

`remote_proxy::deliver`（`include/ultranet/actor/dist/remote_proxy.h`）在没有连接且 `m_buffered.size() == 1024` 时直接丢掉新消息。调用方的 `send` 是 `void`，看不出来。

`actor_ref::try_send` 在 `local_actor()` 为空时调用 `deliver` 然后 `return true`。队列满了也会返回 true。

`remote_send_impl` 只在连接还有效时 `co_await conn->send`。写失败只把 `outbound_conn::m_valid` 设为 false，消息不回到队列。

`outbound_conn` 只有 `send`。对端的 `handle_connection` 只读不写。因此“沿同一条 TCP 连接把答复写回去”在现有代码里没有读循环，也没有入站写接口。本轮要补的是这一条路径，不另做一套按地址回连的答复通道。

线格式 `0x02` 没有消息编号。帧长度是本机字节序的 `uint32_t`（`outbound_conn::send` 直接写 `sizeof(length)`），不是网络字节序。本轮不改 `0x02` 的布局，也不改帧长度的字节序。

`actor_base::deliver` 捕获异常后只打日志。

`actor_cluster_test` 在同一个进程里让两份 `cluster` 对象交换字节。没有“杀掉对端进程”的测试。

`message_envelope` 现在没有关联编号。本地灌入不使用关联编号。

## 3. 不改什么

- 不改 32 字节内联、按 worker 的 eventfd、`IoReactor::wait_for_events` 的 100ms、`max_per_activation`。
- 不改 `0x01`、`0x02`、`0x03` 的字段布局，不改帧长度的本机字节序。
- 不把 CAF 加进 CMake。
- 不为了远端数字去空转 `wait_for_events` 或加大 `max_per_activation`。
- 未经用户要求不提交。
- 不在仓库、测试或日志里写 sudo 密码。
- gossip 仍可以每轮额外拨一条短连接把 gossip 字节发出去。短连接不放进 `m_connections`。唯一改动的一点：`connect_to_node` 发现该 `node_id` 已有仍然有效的池化连接时，不覆盖它。池化连接无效时才换上新连接，并为这条新连接启动读循环，同时重发未确认消息。

## 4. 语义

### 4.1 收下

`actor_ref::send` 改为返回 `bool`。已有的 `ref.send(msg);` 仍然能编译。

- 代理为空：立刻返回 false，不等待。测试用一个找不到的 URI，墙钟必须小于 500ms。
- 本地：进入邮箱返回 true。`shutting_down` 时丢弃并返回 false。
- 远端 `try_send`：能放进未连接时的缓冲队列，或放进已连接时的未确认集合，返回 true。缓冲上限仍是 1024。放不进去返回 false，队列长度保持 1024。禁止在这条路径上丢弃。
- 远端 `send`：只在“已经有远端代理，但缓冲已满”时，在 `system_config::remote_accept_timeout_ms`（默认 5000）内重复尝试。超时返回 false，这条消息不在队列里。没有路由时走上面的“代理为空”，不占用这 5 秒。

`m_pool == nullptr` 时 `try_deliver` 返回 false，不提交协程，也不丢弃。

### 4.2 两个进程都还活着时的送达

远端消息使用新类型 `0x04`，带本进程单调递增的 `msg_id`。`0x02` 仍能解开，但新的 `send` / `try_send` 走 `0x04`。

接收进程在 `push_envelope` 成功之后，沿收下这条消息的那条已接受连接回写 `0x06`（只有 `msg_id`）。确认的含义是“已经放进目标邮箱”，不是“处理函数已经跑完”。处理函数随后抛异常，不撤销这条确认。

发送进程把已收下、尚未收到 `0x06` 的消息留在未确认集合里。出现下面任一情况就把该集合里的消息再发一次，`msg_id` 不变：

- 出站写失败，或 `outbound_conn` 变为无效。
- 出站读循环读到对端关闭。
- 距离上次发送超过 `remote_ack_timeout_ms`（默认 5000）仍没有 `0x06`。

`resent` 只统计前两种（写失败或对端关闭）。确认超时导致的重发计入 `ack_timeout_resend`，不计入 `resent`。

未确认的消息不因为重试次数到了就被删掉。本进程还活着，并且对端仍在 `live_nodes` 里，就继续留着。对端被判死后，这些消息留在内存里，测试打印 `unacked_after_peer_dead`，不把它们清零。

接收端不做业务去重。测试按 `msg_id` 统计 `unique` 和 `duplicates`。

断连用例把 `remote_ack_timeout_ms` 设为 60000，等待上限 10 秒。因此这 10 秒内确认超时不可能把 `resent` 抬上去。

通过条件同时满足：

- `unique` 等于本轮 `send` 返回 true 的条数。
- `resent > 0`。
- `drop_inbound_connections()` 的返回值（本次关闭的已接受 fd 数）大于 0。返回 0 则失败，原因是没有已接受连接可关。

`duplicates` 可以为 0 或大于 0，原样写入第 9 节。`ack_timeout_resend` 在这 10 秒内必须为 0，否则失败。

断连的做法：接收进程在成功入队 100 条业务消息之后，调用 `actor_system::drop_inbound_connections()`，关闭已接受的连接，监听套接字继续听。关闭监听套接字不算这条测试，因为它不断开已经接受的连接。

等待上限 10 秒。超时则失败，并打印 `unique`、`accepted`、`duplicates`、`resent`、关闭的 fd 数。不用固定睡眠当作通过。

### 4.3 进程被杀掉

快照只包含 actor 主动写入的字节。`SIGKILL` 之后，新进程只能读到最后一次文件和目录都 `fsync` 成功的快照。

在途远端消息另做一次测量，不作为“必须是 0”的断言：

- 发送进程每成功 `send` 100 条，把累计收下数 `fsync` 到文件。
- 父进程看到第一份这个文件之后发 `SIGKILL`。
- 接收进程再等 1 秒，写出自己的 `unique`。
- 第 9 节同时写下最后一次 `fsync` 的收下数和接收端的 `unique`。
- 若 `unique` 大于最后一次 `fsync` 的数，差额是两次 `fsync` 之间多送出的，不记成负数丢失，也不把它修成相等。

### 4.4 ask

```cpp
template <typename Reply, typename Msg>
std::optional<Reply> ask(const Msg& msg, std::chrono::milliseconds timeout);
```

超时、对端不 `reply`、代理为空，都返回 `nullopt`。

`actor_base::reply(const Reply&)` 只在当前线程正在执行该 actor 的处理函数时有效。`pull_and_run` 在调用处理函数之前，把本条 `message_envelope` 的 `correlation_id`、`reply_conn_id` 和 `flags` 写入线程局部，返回前清掉。入站协程里不设置这个线程局部，因为真正的 `deliver` 发生在稍后的 worker 上。

`correlation_id == 0` 时 `reply` 直接返回 false，不查表。本地灌入的信封这两个字段保持 0。

本地 `ask` 把等待项放进 `actor_system` 的表，键是 `correlation_id`，`reply_conn_id` 为 0。`reply` 看到 `reply_conn_id == 0` 且 `correlation_id != 0` 时，只完成这张表，不写套接字。

远端 `ask` 把 `0x04` 的 flags bit0 置上，并且 `0x04` 的 `msg_id` 就是发起端 ask 表里的那个 id。入站 `0x04` 入队时把线格式里的 `msg_id` 写入信封的 `correlation_id`，把 flags 写入 `flags`，并把当时那条已接受连接的 id 写入 `reply_conn_id`。`pull_and_run` 把这三个值放进线程局部。`reply` 用线程局部里的 `correlation_id` 作为 `0x05` 的 `msg_id`，用 `reply_conn_id` 查 `actor_system` 的入站连接表，提交一次 `0x05` 写协程，不在 actor 线程上阻塞写套接字。连接已经不在表里则 `reply` 返回 false，调用方的 `ask` 按超时结束。发起端在出站读循环里收到 `0x05`，按 `msg_id` 完成等待。没有第二条“按 host:port 再连回去”的答复路径。

禁止在目标 actor 的处理函数里对自己 `ask`。这条会死锁到超时，因为处理函数占着该 actor 的执行，自问的消息排在邮箱里。测试把 `ask` 超时设为 200ms，并用 `alarm(3)` 兜底。期望 `nullopt`，进程在 2 秒内结束。`alarm` 触发则测试失败。

对端不 `reply` 的远端 `ask` 同样用 200ms 超时，墙钟小于 2 秒。

### 4.5 监管

```cpp
struct supervisor {
    int max_restarts = 3;
};
```

新增 `spawn_supervised(name, supervisor, args...)`。现有 `spawn(name, args...)` 不增加参数，避免和用户构造函数的第一个参数冲突。

未使用 `spawn_supervised` 时，异常路径保持第 2 节的日志行为。

使用之后，处理函数抛出 `std::exception` 计一次。次数不超过 `max_restarts` 时调用虚函数 `on_restart()`，然后继续处理后面的消息。超过之后 `set_shutting_down(true)`，再来的消息不调用处理函数。测试 actor 在 `on_restart` 里把代数加一，并清掉导致抛异常的状态。断言代数，以及停止之后业务计数不再增加。

不销毁 `unique_ptr` 再 new 一个对象。`actor_ref` 持有的是原来的地址。

`catch (...)` 的未知异常同样计一次失败。不在日志之外再吞掉。

### 4.6 成员

`node_timeout_ms` 默认值保持 3000。成员测试把这项设为 1000，不把默认改成 0。

两个进程交换过 gossip 之后，父进程对其中一个发 `SIGKILL`。另一个在 `node_timeout_ms + 1500ms`（即 2500ms）内，`live_nodes` 不再包含死者的 id。用墙钟判断。析构同一个进程里的第二个 `actor_system` 不算这条测试。

## 5. 线格式

长度前缀仍是本机字节序的 4 字节，后面是负载。负载里的多字节整数继续用 `serializer` 的大端。

`message_type::routed = 0x04`

| 字段 | 宽度 |
|---|---|
| type | 1，值 0x04 |
| flags | 1，bit0 = 需要答复 |
| msg_id | 8，大端 |
| target uri | 字符串（已有的 `write_string`：u32 长度 + 字节） |
| msg_hash | 8，大端 |
| payload | u32 长度 + 字节 |

`message_type::reply = 0x05`

| 字段 | 宽度 |
|---|---|
| type | 1，值 0x05 |
| msg_id | 8，大端 |
| payload | u32 长度 + 字节 |

`message_type::ack = 0x06`

| 字段 | 宽度 |
|---|---|
| type | 1，值 0x06 |
| msg_id | 8，大端 |

`pack_routed` / `unpack_routed`、`pack_reply` / `unpack_reply`、`pack_ack` / `unpack_ack` 放在 `serialization.h`。`payload_len == 0` 对 `0x04` 和 `0x05` 拒绝，与 `0x02` 相同。`0x06` 没有 payload。

单元测试不启动网络：打包再解开，字段相等；一段现有的 `0x02` 字节仍能被 `unpack_actor_message` 解开。

`message_envelope` 增加 `uint64_t correlation_id`（默认 0）、`uint64_t reply_conn_id`（默认 0）和 `uint8_t flags`（默认 0）。这三个字段不放进 32 字节负载。本地灌入不设置它们。若它们让第 7.2 节的中位低于 1000 万，先去掉热路径上的额外拷贝，再继续远端工作。

现有 `msg_handler_t` 只接收负载，`client_fd` 出不了 `handle_connection`。本轮改成：入站帧处理持有一个可写的已接受连接对象，handler 签名带上这个对象（`handle_inbound_message(inbound_conn, payload)`）。入站 `handle_inbound_message` 处理 `0x04` 以及现有的 `0x01` / `0x02` / `0x03`。`0x04` 入队时把 `msg_id` 写入 `correlation_id`、flags 写入 `flags`、该连接在表中的 id 写入 `reply_conn_id`。`push_envelope` 成功之后，用调用当时手里的这个对象发送 `0x06`，不另拨连接。`0x05` 和 `0x06` 不由入站 handler 处理，也不进邮箱。

每次向 `m_connections` 插入或替换一条连接时，给这条新对象启动读循环，并作废被换下的旧对象（换下只发生在旧对象已经无效时，见第 3 节）。这条读循环只处理 `0x05` 和 `0x06`：`0x05` 完成 ask 表，`0x06` 从未确认集合删除 `msg_id`。读到 `0x01`、`0x02`、`0x03` 或 `0x04` 时不入队、不确认，打一条错误日志。业务 `0x04` 只从池化出站连接写出，由对端在已接受连接上读入，并在同一条已接受连接上写回 `0x06`。因此出站读循环不持有已接受连接，也不把负载交给 `handle_inbound_message`。gossip 额外拨出去、没有放进 `m_connections` 的短连接不启动读循环。

`tcp_transport` 记录已接受的连接对象。`drop_inbound_connections()` 关闭这些对象的 fd，返回本次关闭的个数，不关闭监听 fd。关闭后对象从表里去掉，随后的 `reply` 查不到 `reply_conn_id`。

## 6. 代码落点

| 文件 | 改动 |
|---|---|
| `include/ultranet/actor/dist/serialization.h` | `routed` / `reply` / `ack` 和对应 pack/unpack |
| `include/ultranet/actor/dist/remote_proxy.h` | 去掉满队列丢弃；`try_deliver`；未确认集合；写失败后重发 |
| `include/ultranet/actor/net/transport.h` | 可写的已接受连接对象；handler 带上该对象；池化出站连接每次插入时的读循环；`drop_inbound_connections` 返回关闭个数 |
| `include/ultranet/actor/core/mailbox.h` | 信封上的 `correlation_id`、`reply_conn_id` 和 `flags` |
| `include/ultranet/actor/core/actor_ref.h` | `send` 返回 `bool`；远端 `try_send` 使用 `try_deliver`；`ask` |
| `include/ultranet/actor/core/base_actor.h` | `reply`、`on_restart`、监管计数；无监管时异常路径不改 |
| `include/ultranet/actor/system/actor_system.h` | 两个超时配置；入站 `handle_inbound_message` 处理 `0x04` 和现有 `0x01`/`0x02`/`0x03`；出站读循环处理 `0x05`/`0x06`；`ask` 表；`spawn_supervised`；`drop_inbound_connections` |
| `include/ultranet/actor/core/snapshot.h` | `save_snapshot` / `load_snapshot`：写临时文件，`fsync`，再 `rename`，并 `fsync` 目录 |
| `tests/CMakeLists.txt` | 注册下面两个可执行文件，链接方式与 `actor_gtest` 相同 |
| `tests/actor_gtest.cc` | 线格式、本地 `ask`、对自己 `ask`、监管、无连接时第 1025 次 `try_deliver` 返回 false |
| `tests/actor_dist_runtime_test.cc` | 两个进程：送达、断连重发、远端 `ask`、成员 `SIGKILL`、在途条数测量 |
| `tests/actor_snapshot_crash.cc` | 快照之后父进程 `SIGKILL`，再启动恢复进程 |

`actor_dist_runtime_test` 用 `fork` + `exec` 自己，子进程通过参数区分角色，结果写到临时文件。父进程读文件再断言。不在同一个进程里放两个 `actor_system` 来代替这条测试。同一个进程里的两个系统只能作为调试，不能当作第 7.1 节的通过证据。

快照程序：子进程写快照，`fsync` 文件和目录，创建 `ready` 文件并 `fsync` 它所在目录，然后停在一个不会自己退出的循环里。父进程看到 `ready` 之后发 `SIGKILL`。随后以恢复参数再启动同一个程序，读出的计数必须等于快照里的值。`_exit(0)` 不算崩溃。

## 7. 验证

### 7.1 正确性

构建目录使用已有的 `/tmp/ultranet-asan`，不在源码树里的 `build/` 重编。

```bash
cmake --build /tmp/ultranet-asan --target actor_gtest actor_dist_runtime_test actor_snapshot_crash -j$(nproc)
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_gtest --gtest_brief=1
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_dist_runtime_test
ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 /tmp/ultranet-asan/bin/actor_snapshot_crash
```

`detect_leaks=0` 是因为进程退出时已有的 `serve` / `gossip` 协程帧会被 LSan 报泄漏。`halt_on_error=1`。沙箱里的 LSan “does not work under ptrace” 不算功能失败；这三条命令在沙箱外执行。

`actor_dist_runtime_test` 的通过条件：

- 对端在听。发送 1000 条。`unique == 1000`。打印 `duplicates`。
- 对端入队 100 条后 `drop_inbound_connections()`。该用例 `remote_ack_timeout_ms` 为 60000，等待上限 10 秒。最终 `unique == send 返回 true 的条数`，`resent > 0`，关闭的已接受 fd 数大于 0，`ack_timeout_resend == 0`。打印 `duplicates`。
- 找不到的 URI，`send` 返回 false，墙钟小于 500ms，对端计数为 0。
- 对端不 `reply` 的 `ask`，200ms 超时得到 `nullopt`，墙钟小于 2 秒。
- 200 次成功的远端 `ask`。p50 是排序后的下标 100，单位微秒。没有目标值。
- gossip 之后 `SIGKILL` 对端。`node_timeout_ms` 为 1000。2500ms 内 `live_nodes` 没有死者。打印 `unacked_after_peer_dead`。
- 第 4.3 节的在途测量打印两个数。这两个数不相等也不失败。

### 7.2 本地灌入

重编 `/tmp/ultra_flood_only` 和 `/tmp/caf_flood_only`。各 1 次预热，12 次。中位是升序后的下标 6。CAF 只作对照。

回归门：ultra 中位不低于 1000 万条/秒。上一轮中位是 2427 万。低于 1000 万就停，先修回归，不填写“本轮完成”。高于或低于 2427 万都原样写入第 9 节。

### 7.3 不许做的事

- 不把期望的 `unique`、p50、吞吐写进被测程序里再打印。
- 不把在途丢失修成 0。
- 不把 `node_timeout_ms` 设为 0。
- 不把成员测试改成析构同一个进程里的对象。
- 不把关闭监听套接字当作断连。

## 8. 实现顺序

每一步先写会失败的测试，看到失败，再写实现。

1. `0x04` / `0x05` / `0x06` 的打包测试。红灯是缺少 `pack_routed`。同时断言旧 `0x02` 仍能解开。
2. 无连接的 `remote_proxy` 塞进 1024 条之后，下一次 `try_deliver` 返回 false，`buffered_count()` 仍为 1024。
3. 两个进程、1000 条、`unique == 1000`。
4. `drop_inbound_connections()` 之后 `resent > 0` 且 `unique` 等于收下条数。
5. 本地 `ask`，然后对自己 `ask` 的超时，然后两个进程的 `ask` 和超时。
6. `spawn_supervised` 的重启代数，以及超过次数后业务计数停止增加。
7. 两个进程，`SIGKILL`，成员表在 2500ms 内去掉死者。
8. 快照 `SIGKILL`。
9. 在途条数测量，只记录，不要求为 0。
10. 重跑本地灌入。低于 1000 万则停。填写第 9 节。

成员超时是第 4.6 节那个等待。其余测试的失败条件是第 7.1 节的计数或墙钟上限，不是“睡一会儿再看”。

## 9. 结果（跑完再填）

数字来自 2026-10-01 00:46–00:48 这台 i5-8350U 上的进程输出。ASAN 是 `detect_leaks=0:halt_on_error=1`。本地灌入是 Release `-O3 -DNDEBUG`，先跑 ultra 再跑 CAF，1 次预热加 12 次，中位是升序后的下标 6。

| 项 | 结果 |
|---|---|
| actor_gtest | 134 tests，PASSED 134，1359 ms。没有 AddressSanitizer 报告 |
| 远端 1000 条 unique / duplicates | `flood unique=1000 accepted=1000 duplicates=0 resent=0 rc=0` |
| 断连后 unique / accepted / duplicates / resent | `drop unique=1000 accepted=1000 duplicates=1 resent=437 ack_timeout_resend=0 closed_fds=1 rc=0` |
| 找不到 URI 时 send 是否为 false，墙钟 | `missing_uri accepted=0 wall_ms=0` |
| ask 超时是否为 nullopt，墙钟 | `ask_timeout nullopt 200 rc=0` |
| 远端 ask p50 微秒 | `ask_p50 200 876 rc=0`。200 次都答对，p50 是 876 微秒 |
| 监管代数 / 停止后业务计数 | `SupervisorTest.RestartsThenStops` 通过。3 次异常后 `generation==3`，第 4 次后 `shutting_down`，之后 `business` 不再增加 |
| 成员是否在 2500ms 内消失 / unacked_after_peer_dead | `member gone 954 0 rc=0`。954 ms，unacked 为 0 |
| 快照恢复的值 | `restored 42`，随后 `snapshot sigkill ok` |
| 在途：最后一次 fsync 的收下数 / 接收 unique | `inflight fsynced_accepted=300 recv=166 166 saw_file=1`。接收 unique 是 166，比 fsync 记下的 300 少 134 |
| 本地灌入中位 / 最慢 / CAF 同次中位 | ultra 中位 23185717，最慢 21570319，最快 25773195。CAF 中位 2606610，最慢 1639747，最快 3011050 |

结论：两端进程都活着时，1000 条都送到了。断连之后重发把 unique 补回 1000，同时多出 1 条重复，`resent=437`。这是至少一次，不是恰好一次。`SIGKILL` 时 fsync 记下 300，接收端只留住 166，差额没有被改成 0。本地灌入中位 23185717，过了 1000 万的门，也低于上一轮的 24271844。CAF 同一次的中位是 2606610。

# 剩余缺口 — 设计

上一轮分布式运行时已经通过评审并测完。本文只处理那次测完后仍然成立的问题。不改 `IoReactor` 的 100ms 超时，不改 `max_per_activation`。本地灌入的门仍然是中位不低于 1000 万条/秒。低于这个数就停，先去掉热路径上的额外成本，再继续。

本文不声称恰好一次。未完成的日志残尾不计入 `complete_data`，也不要求 `torn` 为 0。已经 `fsync` 的完整记录，必须在发送进程被 `SIGKILL` 之后由重放进程送出并被处理。上一轮“内存收下数和接收 unique 可以不相等”测的是当时没有日志；那个用例由第 6.4 节替换，不再拿内存收下数冒充落盘条数。

## 1. 已记录的问题

证据是 2026-10-01 00:46–00:48 的输出。

| 编号 | 问题 | 证据 |
|---|---|---|
| G1 | 确认在进邮箱之后就发出。处理函数抛异常时，发送方已经收到确认，不会再送。同一 `msg_id` 的重发会再次进入处理函数。断连那次 `duplicates=1` | `drop unique=1000 accepted=1000 duplicates=1 resent=437`。`handle_inbound_message` 在 `push_envelope` 成功后立刻 `post_inbound(pack_ack)`。`deliver()` 自己吞掉异常 |
| G2 | 发送进程被 `SIGKILL` 时，已经 `send` 成功但还没到接收方的消息没有落盘 | `inflight fsynced_accepted=300 recv=166 166`。少 134 条。快照只保存调用方写下的字节，不保存发出去的帧 |
| G3 | 成员测试的 `unacked=0` 只说明那次没有业务消息。对端死亡后 `recover_node` 每 50ms 重连，不看 `live_nodes` | `member gone 954 0`。`recover_node` 的循环只在 `m_shutdown` 时退出 |
| G4 | 分区只测过一次立刻关闭已接受连接。没有测“连不上一段时间，节点仍算活着，之后补齐” | 断连用例的对端进程一直活着，并且马上可以重新 accept |
| G5 | 进程退出时 LSan 报 `serve` / `gossip` 协程帧泄漏，所以测试把 `detect_leaks` 设成 0 | 上一轮设计第 7.1 节写明了这个原因。还没有按泄漏栈修 |
| G6 | 本地灌入中位从 24271844 降到 23185717。ping-pong 这一轮没重跑 | 同一次 CAF 中位 2606610。源码在 `/tmp/ultra_ping_only.cc` 和 `/tmp/caf_ping_only.cc` |

## 2. 不做什么

- 不声称恰好一次。接收进程重启后，内存里的去重表消失，同一条消息可以再次进入处理函数。
- 不在接收端把去重记录 `fsync` 到磁盘。处理函数的副作用还在内存里。先把去重落盘、再发确认，会把“效果已经丢了”的消息挡掉，发送方不再重送。
- 不自动序列化 actor 的成员。调用方的 `save_snapshot` 保持原样。运行时只为远端发出的 `0x04` 帧加日志。
- 不把 `remote_log_path` 为空时的本地 `send` 写成磁盘。本地灌入不打开这个文件。
- 不使用 iptables。分区用传输层拒绝新的 accept，并关掉已接受连接。这不是跨机器的网络。
- 不把 `node_timeout_ms` 设成 0，不把成员测试改成析构同一进程里的第二个 `actor_system`。
- 不修改 `0x01` / `0x02` / `0x03` 的布局。长度前缀仍是本机字节序。
- 不去追 24271844。ping-pong 只重跑并记录，不设新的下限。

## 3. 确认改到处理函数之后，并按发送方去重

### 3.1 线上的 `0x04`

在现有字段里加发送方节点号。`0x05` / `0x06` 不变。

```text
type:1 flags:1 msg_id:8 sender_node_id:8 uri:string msg_hash:8 payload_len:4 payload
```

`payload_len == 0` 仍然拒绝。`sender_node_id` 允许为 0，不因此拒绝。`pack_routed` 增加参数 `uint64_t sender_node_id`。重放时直接把日志里的原始帧写出去，不再打包，因此帧里的节点号保持写下时的值。

### 3.2 信封

`message_envelope` 增加 `uint64_t sender_node_id`，默认 0。拷贝、移动和清空的路径与 `correlation_id` 相同。本地灌入保持 0。入站 `0x04` 把它从帧抄进信封。

### 3.3 去重状态

键是 `(sender_node_id, msg_id)`。三态：空、`in_progress`、`completed`。表放在 `actor_system`，用互斥锁保护。上限 1048576 条。超过上限时不淘汰旧记录，本条按没有见过处理，`dedup_overflow` 加 1。

入站 `0x04` 不再在 `push_envelope` 之后发 `0x06`。

- 状态已是 `completed`：不入队。若这条连接还在，再发一次 `0x06`。`wire_skips` 加 1。
- 状态已是 `in_progress`：不入队，不发确认。`wire_skips` 加 1。正在处理的那份负责确认或释放。
- 状态为空：入队。不在入站协程里改成 `in_progress`。两个副本可以都进邮箱。占位发生在 actor 线程、调用用户处理函数之前。

`deliver()` 改为返回 `deliver_result`：`ok`、`threw`、`dead_letter`。异常仍在 `deliver()` 内部捕获，监管路径不改。没有注册处理函数是 `dead_letter`，不是异常。

`pull_and_run` 在调用 `deliver` 之前对远端消息（`correlation_id != 0`）做占位：

- 占到 `in_progress`：调用 `deliver`。
- 占不到：不调用 `deliver`，`wire_skips` 加 1。若当前状态是 `completed`，再发一次 `0x06`。若是 `in_progress`，不发。

`deliver` 返回之后，调用 `actor_system::finish_remote_delivery(sender_node_id, msg_id, reply_conn_id, deliver_result)`：

- `ok` 或 `dead_letter`：状态改为 `completed`，然后用 `reply_conn_id` 发 `0x06`。确认没写出去也保持 `completed`。之后的重发只补确认，不再进处理函数。
- `threw`：状态改回空，不发确认。发送方会重送，处理函数可以再跑。

`correlation_id == 0` 的本地消息不进这张表，也不发 `0x06`。

`dead_letter` 要确认。否则未知类型会永远重送。这和现在“进邮箱就确认”对死信的结果一样。

### 3.4 断连用例要改的断言

接收端同时计 `unique`（集合大小）和 `handler_runs`（真正进入用户处理函数的次数）。`wire_skips` 由发送进程读不到，接收进程把它写进结果文件。

通过条件在原有条件上加一条：`handler_runs == unique`。`unique == accepted`、`resent > 0`、关闭的 fd 数大于 0、`ack_timeout_resend == 0` 都保持。`wire_skips` 只打印，允许为 0。

## 4. 对端从存活表消失后停止重连

`recover_node` 每一轮在 `connect_to_node` 之前调用 `cluster::live_nodes(node_timeout_ms)`。目标 `node_id` 不在返回列表里，就把 `m_recovering` 清掉并退出。不删除 `m_unacked`。这一轮不调用 `connect_to_node`，也不增加 `recover_attempts`。

`recover_attempts` 只在真正调用 `connect_to_node` 之前加 1。公开 `uint64_t recover_attempt_count() const`。

这只用于已经建立过池化连接之后的恢复。第一次 seed 拨号不走 `recover_node`。

断连用例的对端进程还活着，gossip 会刷新 `last_seen`，所以它仍在 `live_nodes` 里，重连继续。该用例的 `node_timeout_ms` 保持 3000。

新用例 `member-unacked`，两个进程：

- `node_timeout_ms` 为 1000，`remote_ack_timeout_ms` 为 60000。
- 接收角色 `die-on-first`：用户处理函数第一次被调用时调用 `raise(SIGKILL)`。确认发在 `deliver` 返回之后，所以这次杀死发生在确认之前。
- 发送方循环 `send`，直到收下数达到 20 或墙钟 3 秒。写下收下数。收下数为 0 则失败。
- 父进程等接收进程死亡，再等发送方写下成员行。
- 发送方看到节点不在 `live_nodes` 之后写下 `gone <elapsed_ms> <unacked> <recover_attempts>`，再睡 1000ms，写下第二次 `recover_attempts`。
- 通过：`gone`，`elapsed_ms <= 2500`，`unacked >= 1`，两次 `recover_attempts` 相等。
- `unacked >= 1` 是因为第一次处理在确认之前被杀死，至少这一条没有确认。它不是把丢失改成 0。

## 5. 短时拒绝 accept

`tcp_transport` 增加 `blackhole_accept_for(std::chrono::milliseconds)`。在截止时间之前，`serve` 仍 `accept`，但立刻 `close` 新 fd，不交给 `handle_connection`。已有连接由调用方先 `drop_inbound_connections()`。

新用例 `blackhole`：

- 发送方 `node_timeout_ms` 为 8000。黑洞只关掉接收方已经 accept 的连接，并拒绝接收方新的 accept。接收方已经知道发送方地址后，仍能向外拨号发 gossip，发送方的 `last_seen` 会继续刷新。8000ms 是刷新失败时的上限，不是靠多睡一会儿碰通过。
- 接收方在 `unique` 达到 100 时关闭已接受连接，并 `blackhole_accept_for(2000ms)`。
- 发送 1000 条。等待上限 15 秒。
- 通过：`unique == 1000`，`handler_runs == unique`，`resent > 0`，`recover` 没有因为“不在存活表”而退出（接收方把 `blackhole` 起止写进文件；发送方的结果里 `stopped_recover == 0`）。
- 打印 `wire_skips`。允许为 0。
- 若节点在这 2 秒内被判死，`stopped_recover` 会变成非 0，用例失败。这是 G3 和 G4 叠在一起的检查。

`stopped_recover` 在第 4 节的退出分支加 1。公开读取。

## 6. 发送方远端日志

只覆盖 `remote_log_path` 非空时的远端 `0x04`。路径为空时，`send` 的远端路径与现在相同，不打开文件。

### 6.1 文件格式

小端。每条记录：

```text
magic:4 type:1 msg_id:8 body_len:4 body:body_len crc32:4
```

- `magic` 为字节 `55 4C 52 31`（ASCII `ULR1`）。
- `type` 为 1 表示数据，2 表示确认。
- 数据记录的 `body` 是完整的 `0x04` 帧。确认记录的 `body_len` 为 0。
- CRC-32/ISO-HDLC：多项式 `0xEDB88320`，初值 `0xFFFFFFFF`，结果异或 `0xFFFFFFFF`。覆盖 `type`、`msg_id`、`body_len`、`body`，不覆盖 magic。

读取时按顺序校验。第一条 magic 不对、长度超出剩余字节、或 CRC 不对的记录，以及它后面的字节，都不再解析。若文件在最后一条完整记录之后还有剩余字节，`torn = 1`，否则 `torn = 0`。不把残尾修成一条假记录。

### 6.2 何时写、何时 fsync

远端 `send` / `try_send` 在返回 true 之前：若该 `msg_id` 还没有数据记录，追加一条数据记录，`fsync` 文件。`fsync` 失败则返回 false，不把这次算进收下数。同一 `msg_id` 的重试不再追加第二条数据记录。

`note_ack` 在从内存未确认表删除之前，追加确认记录并 `fsync`。`fsync` 失败则不删除内存项，留待下次确认或重放。

本地 `send` 不进入这个函数。

### 6.3 重放

新进程使用同一个路径。读出完整记录后，没有确认记录的 `msg_id` 保留其原始帧。看到 `ready` 之前不连接、不把帧放进写队列。看到 `ready` 之后，把这些原始帧用现有的单写者队列写出去，不调用 `pack_routed`。这些帧已经在日志里，重放不再追加数据记录。收到确认后按第 6.2 节追加确认记录。

重放进程的新 `self_node_id` 不写进旧帧。去重键用帧内的 `sender_node_id`。

### 6.4 SIGKILL 用例

替换现在的在途用例。接收进程保持存活。只比较最后的 `unique` 和 `complete_data` 不够：杀死前已经进邮箱或还在内核缓冲里的帧，会在放开之后被处理，重放一条不发也能相等。

接收角色 `hold`。在写下 `ready` 之前，读到的 `0x04` 直接丢掉，不入队、不确认。`0x01` gossip 仍按现有路径处理。这段时间内用户处理函数若被调用，`handled_before_go` 加 1，否则保持 0。

顺序：

1. 发送方设置 `remote_log_path`，循环发送。完整数据记录达到 50 后停止发送，从日志文件本身数出 `complete_data` 和 `torn`，用临时文件加 `rename` 写下结果，然后 `pause()`。
2. 父进程看到该结果后 `SIGKILL` 发送进程，并确认它死于信号。
3. 父进程以 `replay` 角色启动同一个程序、同一个日志路径。重放进程先读日志，写下 `replay_sent`（没有确认记录的数据记录数）并 `fsync` 该结果文件，然后等待 `ready`。看到 `ready` 之前不构造 `actor_system`，避免构造函数里的 gossip 提前拨号。
4. 父进程看到 `replay_sent` 之后写下 `replay_go`。
5. 接收方看到 `replay_go` 后，先 `drop_inbound_connections()`，丢掉旧连接和其中还没读完的字节，再写下 `ready`。`drop` 之前已经读进内存、尚未入队的 `0x04` 丢掉。`ready` 之后只入队在这次 `drop` 之后新 accept 的连接上的 `0x04`。
6. 重放进程看到 `ready` 后送出那些原始帧，然后退出。
7. 重放进程退出后再等 2 秒。接收方写下 `handled_before_go`、`unique`、`handler_runs`。

通过要同时满足：`handled_before_go == 0`，`replay_sent == complete_data`，`handler_runs == unique`，`unique == replay_sent`。

打印 `torn`。不要求 `torn == 0`。不拿杀死前内存里的收下数和 `unique` 比较。`unique < complete_data` 失败，表示一条已经 `fsync` 的完整记录没有被处理。残尾不在 `complete_data` 里。

接收进程若被杀死，去重表和用户状态都在内存里。本文不补这个洞，也不新增把该丢失打成 0 的断言。

## 7. 退出时的泄漏

实施的第一步，在改去重和日志之前：

```bash
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 /tmp/ultranet-asan/bin/actor_dist_runtime_test
```

把泄漏栈留在实现记录里。按栈修所有权，不用 suppression 文件，也不把 `detect_leaks` 继续设成 0 当作通过。

若栈显示 `~actor_system` 在 `serve` / `gossip` / `ack_timeout_loop` 结束前 `m_pool.reset()`，就先置关闭标志、让这些协程退出、再销毁线程池。栈若指向别处，按栈修，不按这句假设改。

通过标准：`actor_dist_runtime_test` 和 `actor_gtest` 在 `detect_leaks=1:halt_on_error=1` 下退出码为 0，且输出里没有 `LeakSanitizer`。

## 8. 本地灌入和 ping-pong

日志和去重都加上之后重编并测量。先 ultra 再 CAF，中间不跑别的重负载。各 1 次预热加 12 次，中位是升序后的下标 6。

```bash
g++ -O3 -g -fno-omit-frame-pointer -DNDEBUG -std=c++23 -pthread \
  -I/home/jwy/workspace/ultra-net/include \
  /tmp/ultra_flood_only.cc -o /tmp/ultra_flood_only -luring
g++ -O3 -g -fno-omit-frame-pointer -DNDEBUG -std=c++17 -pthread \
  /tmp/caf_flood_only.cc -o /tmp/caf_flood_only -lcaf_core
g++ -O3 -g -fno-omit-frame-pointer -DNDEBUG -std=c++23 -pthread \
  -I/home/jwy/workspace/ultra-net/include \
  /tmp/ultra_ping_only.cc -o /tmp/ultra_ping_only -luring
g++ -O3 -g -fno-omit-frame-pointer -DNDEBUG -std=c++17 -pthread \
  /tmp/caf_ping_only.cc -o /tmp/caf_ping_only -lcaf_core
```

灌入中位低于 10000000 就停。ping-pong 的中位、最慢、最快原样写入第 11 节，没有下限。

## 9. 文件

| 文件 | 改动 |
|---|---|
| `include/ultranet/actor/dist/serialization.h` | `0x04` 增加 `sender_node_id` |
| `include/ultranet/actor/core/mailbox.h` | 信封增加 `sender_node_id` |
| `include/ultranet/actor/core/base_actor.h` | `deliver` 返回 `deliver_result`；处理前占位，处理后确认或释放 |
| `include/ultranet/actor/core/remote_log.h` | 新文件。追加、`fsync`、按 CRC 读取，残尾停止 |
| `include/ultranet/actor/dist/remote_proxy.h` | 打包时写入发送方节点号；重放帧直接入写队列 |
| `include/ultranet/actor/system/actor_system.h` | 去重表、确认时机、`recover` 退出、日志、计数器 |
| `include/ultranet/actor/net/transport.h` | `blackhole_accept_for` |
| `tests/actor_gtest.cc` | 新 `0x04` 布局；日志残尾；`0x02` 仍能解开 |
| `tests/actor_dist_runtime_test.cc` | `handler_runs`、`member-unacked`、`blackhole`、日志重放 |

## 10. 实施顺序

1. 打开 `detect_leaks=1`，按栈修退出泄漏，再跑通第 7 节的两条命令。
2. 改 `0x04` 和解包测试。红灯是旧的 `pack_routed` 参数对不上。
3. 去重和确认后移。更新断连用例。`handler_runs == unique`。
4. `recover_node` 在节点不存活时退出。跑 `member-unacked`。
5. accept 黑洞 2 秒。跑 `blackhole`。
6. 远端日志和 `SIGKILL` 后重放。通过条件是第 6.4 节的四条，并打印 `torn`。
7. 重跑灌入和 ping-pong。灌入低于 1000 万则停。
8. 用第 11 节的命令输出填表。不手写期望数字。

不提交 git，除非调用方另外要求。

## 11. 结果（跑完再填）

| 项 | 结果 |
|---|---|
| detect_leaks=1 的 actor_gtest / actor_dist_runtime_test | `actor_gtest` 135 个通过，退出码 0，stderr 0 字节，3278 ms。`actor_dist_runtime_test` 退出码 0，`dist_failures=0`，stderr 0 字节。两边都没有 `LeakSanitizer`。2026-10-01 13:10 与 13:16 |
| 断连后 unique / handler_runs / wire_skips / resent / closed_fds | `drop unique=1000 accepted=1000 handler_runs=1000 wire_skips=1 duplicates=0 resent=761 ack_timeout_resend=0 closed_fds=1 rc=0` |
| member-unacked：elapsed / unacked / 两次 recover_attempts | `member-unacked gone 1004 20 17 17 rc=0` |
| blackhole：unique / handler_runs / resent / stopped_recover | `blackhole unique=1000 handler_runs=1000 resent=28450 stopped_recover=0 live rc=0` |
| 重放：complete_data / replay_sent / handled_before_go / unique / handler_runs / torn | `replay complete_data=50 replay_sent=50 handled_before_go=0 unique=50 handler_runs=50 torn=0 signaled=1 rc=0` |
| 本地灌入中位 / 最慢 / CAF 同次中位 | ultra 中位 13702384，最慢 12402331，最快 14158289。CAF 同次中位 1352905，最慢 867453，最快 1675041。2026-10-01 13:13，先 ultra 再 CAF |
| ping-pong 中位 / 最慢 / CAF 同次中位 | ultra 中位 3285690，最慢 2251618，最快 3562205。CAF 同次中位 208066，最慢 186430，最快 227751 |

多主机：未测。

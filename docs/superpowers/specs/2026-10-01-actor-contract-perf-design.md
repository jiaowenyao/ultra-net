# 契约边界与远端性能 — 设计

本文只处理两件事：接收进程被杀死后已经确认的消息会丢，以及两端都活着、不开日志时，容器对容器大约 1.5 万条/秒。不测两台物理机。不改 `IoReactor` 的 100ms 超时，不改 `max_per_activation`。

上一份已接受的设计是 `docs/superpowers/specs/2026-10-01-actor-remaining-gaps-design.md`。本文只改写它里面这些句子，其余仍然有效，包括发送方 `remote_log_path` 非空时，`send` 返回 true 之前必须 `fsync` 数据记录：

- 去重表满时不再把新消息当成没见过并再执行。
- 「接收进程被杀死这个洞一律不补」只保留在 `receiver_log_path` 为空时。路径非空时，重启用日志里的数据记录再执行处理函数。
- 「不要在接收端先把去重落盘再发确认」针对的是落盘之后就跳过执行。本文不那样做。已处理记录落盘只表示发送方可以忘掉这帧，以及本进程内不要执行第二次。重启不因已处理记录跳过数据记录。

## 1. 调研

### 1.1 现在的契约

两端进程都活着时是至少一次。确认在处理函数返回 `ok` 或 `dead_letter` 之后发出。处理函数抛异常则释放去重项，不确认。去重键是 `(sender_node_id, msg_id)`，只在内存里。

发送方日志（`remote_log_path`）在 `fsync` 成功之后才允许 `send` 返回 true。接收方没有对应的日志。因此存在这个窗口：接收方已经处理并确认，发送方已经把确认记录 `fsync` 并忘掉这帧，然后接收进程被杀死。重启后的接收方没有这帧，也没有用户状态。发送方不会再送。这不是至少一次能覆盖的丢失。

去重表上限 `1048576`。满了不淘汰，新消息按没见过执行，`dedup_overflow` 加 1，表里不留下这项。同一条消息之后可以无限次再进处理函数。

`receiver` 侧不自动序列化 actor 字段。`save_snapshot` 仍由调用方调用。

### 1.2 远端为什么慢

2026-10-01 13:41 左右，两个容器在网桥 `172.30.0.0/24` 上，Release `-O3 -DNDEBUG`，各 2 个调度线程，不开任何日志，`seccomp=unconfined`。10000 条 16 字节消息，从第一条 `send` 到接收端数满：

| 次数 | 耗时 | 条/秒 | ask 成功 | ask p50 |
|---|---|---|---|---|
| 1 | 673136 μs | 14856 | 50/50 | 235 μs |
| 2 | 642491 μs | 15564 | 50/50 | 222 μs |

同一时期本地灌入中位 13702384 条/秒。本地路径不走 TCP。

`write_frame` 对每一帧做两次 `Write`：先 4 字节本机序长度，再负载。`write_loop` 每次只从队列取出一帧再调用 `write_frame`。确认 `0x06` 在入站连接上再走一遍同样的路径。一条业务消息至少四次 io_uring 写。`read_frame` 按长度前缀逐帧读，TCP 上前后两帧连在一起仍然能拆开。

ask 的 p50 约 220 μs。若完全不流水线，吞吐大约是 4500 条/秒。实测约 1.5 万，说明发送没有等确认才发下一条，瓶颈在每条消息的写次数和分配，不在来回延迟本身。

本地灌入从 2026-10-01 00:48 的中位 23185717 降到 13702384。本文不把这个下降归因于某一个字段，也不去追回 23185717。本地 `send` 不进入 `write_frame`。

## 2. 不做什么

- 不声称恰好一次。接收日志打开时，处理函数返回之后、`handled` 记录 `fsync` 之前被杀死，重启会再执行一次处理函数。
- 不自动序列化 actor 字段，不改 `save_snapshot`。
- 不把接收日志默认打开。路径为空时，确认时机、内存去重、以及第 1.1 节的丢失窗口保持原样。
- 不放宽发送方日志的契约。`remote_log_path` 非空时，仍然是每条数据记录 `fsync` 成功之后 `send` 才返回 true。不把多条发送方记录合成一次 `fsync`。
- 不改 `0x04` / `0x05` / `0x06` 的字段布局，不新增确认类型。长度前缀仍是本机序 `uint32`。
- 不改 100ms 超时，不改 `max_per_activation`。
- 不测两台物理机，不改 Docker 的 seccomp。性能数字沿用 `seccomp=unconfined` 和显式 `advertise_host`。
- 不去追 23185717。本地灌入只设已有的门：12 次的中位不低于 10000000。
- 不为远端吞吐设一个新的目标条数。合并写之后，三次容器测量的中位必须高于 15564。不高于则撤回合并写，把三次数字写入第 9 节，不再加第二套优化。

## 3. 合并写

`write_frame` 改成一次 `Write` 写出 `长度 || 负载`。短写则从剩余字节继续，直到全部写完或失败。它只返回 false，不在这里把连接标死。

`outbound_conn::write_loop` 和 `inbound_conn::write_loop` 在持锁时把当前队列里的帧全部取出，拼成一段字节再写。每一帧仍然是 `本机序 uint32 长度 || 该帧字节`。队列在这次写完之前可以继续 `post_frame`。

写失败时两条路径分开，不写成同一种：

- 出站：清空尚未写完的队列，并把连接标死。与现在的 `outbound_conn::write_loop` 相同。本次已经取出的帧算在这次失败里，不塞回队列。
- 入站：本次已经取出的帧丢掉；锁外期间新进来、还没取出的帧留下；不把连接标死；`writing` 置 false。与现在的 `inbound_conn::write_loop` 相同。

本地 `send` 不调用这两条路径。

## 4. 去重表满

`system_config::dedup_cap`，默认 `1048576`。测试可以把它设小。

`dedup_cap` 只限制内存表。接收日志的索引不受这个上限约束。

接收日志关闭，且新键不在表里、表已达到上限时：

1. 有 `completed` 项就删掉 `msg_id` 最小的一项；`msg_id` 相同则删 `sender_node_id` 较小的一项。`dedup_evict` 加 1。新键记为 `in_progress` 并执行处理函数。被删掉的键再到达时再执行。
2. 没有 `completed` 项时，不执行，不确认，`dedup_overflow` 加 1。不把这个键写入表。发送方稍后重试。

接收日志打开时，内存表里的 `completed` 可以按同样的规则淘汰。索引保留该键。本进程内该键再到达时只确认，不因为缓存被淘汰而再执行。没有 `completed` 可淘汰时，仍然把新帧写成日志里的数据记录（索引变大），不写入内存表，也不把它标成「待投递且禁止再入队」。缓存有空位或进程重启时再投递。因满表没执行的键不得留下一条会吞掉重试的待投递记录。

不再把满表的新消息当成没见过却不写入表。公开 `dedup_evict_count()` 和已有的 overflow 计数。第 6.2 节只覆盖日志关闭。

## 5. 接收日志

`system_config::receiver_log_path`，默认空。空路径不创建文件，不改变第 1.1 节的行为。

与 `remote_log_path` 相同时不打开接收日志，`receiver_log_rejected` 加 1，行为与空路径相同。

### 5.1 文件格式

小端。magic 为字节 `52 43 56 31`（ASCII `RCV1`）。

```text
magic:4 type:1 sender_node_id:8 msg_id:8 body_len:4 body crc32:4
```

- `type` 为 1 表示数据。`body` 是完整的 `0x04` 帧。
- `type` 为 2 表示已处理。`body_len` 为 0。
- CRC-32/ISO-HDLC，多项式 `0xEDB88320`，初值与结果异或都是 `0xFFFFFFFF`。覆盖 `type || sender_node_id || msg_id || body_len || body`，不覆盖 magic。算法与发送方日志相同。

扫描在第一条坏 magic、短长度或 CRC 错误处停止。其后的字节不再解析。完整记录之后还有剩余字节则 `torn = 1`，否则 `torn = 0`。不把残尾修成记录。缺文件视为 `torn = 0`、数据记录 0 条。

启动时把扫描结果放进内存索引。每一条数据记录都要在本进程里执行一次，不管文件里有没有对应的已处理记录。已处理记录不阻止这次重建。只有已处理、没有数据的键忽略。重建完成之后，该键在本进程内视为已完成。

### 5.2 何时写、何时执行、何时确认

接收日志打开时，入站 `0x04` 的顺序是：

1. 本进程内该键已经执行过：只确认，不进处理函数，`wire_skips` 加 1。文件里的已处理记录不能单独触发这一步；必须是这次进程启动时的重建已经执行过，或本进程的处理函数已经返回。
2. 索引里已有数据记录、本进程还没执行：不再写第二条数据记录。还没 `push_envelope` 的，在缓存有空位时入队。已经入队的，重发不再入队。
3. 其他：追加一条数据记录到用户态缓冲。不立刻 `write`，也不进处理函数。
4. 凑满 32 条未 `fsync` 的记录，或距离这组第一条已过去 2ms，就把这组 `write` 之后 `fsync` 一次。`fsync` 返回成功之后，`receiver_fsynced_data_count()` 或 `receiver_fsynced_handled_count()` 才增加，分别对应本组里的数据记录和已处理记录。未 `fsync` 的字节留在用户态缓冲。2ms 由已有调度器上的休眠实现，不空转。
5. 数据记录 `fsync` 成功之后才 `push_envelope`。`fsync` 失败则不投递、不确认，索引里删掉这组还没落盘的键，留给发送方重试。
6. 处理函数返回 `ok` 或 `dead_letter` 后追加已处理记录，并进入下一组 `fsync`。这一组 `fsync` 成功之后才 `post_remote_ack`。抛异常则不写已处理记录，不确认，索引里删掉这个数据键，发送方可以重试。
7. 重启时第 5.1 节的重建会执行每一条已 `fsync` 的数据记录，包括已经有已处理记录的键。发送方是否还保留这帧不影响这次执行。本进程重建之后再到达的同一键走第 1 步。

`set_hold_handler(true)` 时，数据记录仍按第 4 步 `fsync`，但不 `push_envelope`，不写已处理记录，不确认。默认 false。

`spawn` 本地 actor 之后，把索引中 URI 指向该 actor 的数据记录各投递一次，包括已有已处理记录的键。同一进程里 `spawn` 只做这一次重建。

内存去重表是索引的缓存。缓存未命中时查索引。本进程尚未执行的数据记录可以再入队；本进程已经执行过的只确认。

## 6. 测试

### 6.1 合并写不改帧边界

一帧一次 `write` 和多帧拼成一次 `write`，读端按长度前缀都能拆开。因此“读到两帧”不能当作合并写的红灯。`actor_gtest` 用 `socketpair` 连续 `post_frame` 两帧，读端必须还原出原来的两段负载。该测试在改写循环前后都应通过。合并写是否保留，只由第 6.4 节的容器中位决定。

### 6.2 满表

`dedup_cap = 2`。同一发送方的 `msg_id` 1、2 处理完成。第 3 条必须执行，`dedup_evict >= 1`，被淘汰的是 `msg_id` 较小的那条。再送被淘汰的 `msg_id`，处理函数再执行一次。表满且两项都是 `in_progress` 时，新的一条不执行、不确认，`dedup_overflow` 加 1。

### 6.3 接收日志与 SIGKILL

本机进程，不使用 Docker。这两条都只覆盖进程被 `SIGKILL`，不覆盖掉电。通过条数只认 `fsync` 返回之后增加的 `receiver_fsynced_data_count()`，不认直接读文件。父进程在杀死前读到这个计数。

**还没执行：** 接收方 `set_hold_handler(true)`。发送方发送。2 秒内该计数达到至少 20，且 `receiver_fsynced_handled_count()` 为 0，处理函数次数为 0。不足 32 条时必须靠 2ms 那一组刷出。父进程 `SIGKILL` 接收进程并确认死于信号，然后停掉发送方，避免重发把次数搅乱。新接收进程同一路径、`set_hold_handler(false)`，`spawn` 后不再依赖发送方。通过：`handler_runs == unique ==` 杀死前的 `receiver_fsynced_data_count()`。打印 `torn`，不要求 `torn == 0`。

**已经确认：** `set_hold_handler(false)`。发送方等到自己的未确认数变为 0，且接收方 `receiver_fsynced_handled_count()` 至少 20。然后 `SIGKILL` 接收进程。发送方不再发送。新接收进程 `spawn` 之后必须再执行这些数据记录。通过：新进程的 `handler_runs == unique ==` 杀死前的 `receiver_fsynced_data_count()`。若实现因已处理记录跳过执行，这条的 `handler_runs` 为 0，必须失败。

不把内存里的用户字段说成被快照恢复。恢复的是这些帧在新进程里又进了一次处理函数。

另有一条：`receiver_log_path` 与 `remote_log_path` 设成同一个非空路径时，`receiver_log_rejected == 1`，并且不创建第二套语义。

### 6.4 性能

合并写进入二进制之后：

1. 本地灌入仍用 2026-10-01 剩余缺口设计第 8 节的 `g++ -O3` 命令，1 次预热加 12 次，中位是升序后的下标 6。中位低于 10000000 则停止，撤回合并写。
2. 容器测量与第 1.2 节相同：两个容器、各 2 线程、10000 条、不开日志、`seccomp=unconfined`、显式 `advertise_host`。连续三次，中间不跑别的重负载。中位是三次升序后的下标 1。中位不高于 15564 则撤回合并写，三次数字仍写入第 9 节。

不把本地灌入的条/秒和容器条/秒比成同一个指标。

## 7. 文件

| 文件 | 改动 |
|---|---|
| `include/ultranet/actor/net/transport.h` | 一次写出长度和负载；写循环排空队列 |
| `include/ultranet/actor/system/actor_system.h` | `dedup_cap`、淘汰、接收日志、`set_hold_handler` |
| `include/ultranet/actor/dist/receiver_log.h` | 新文件。追加、组 `fsync`、按 CRC 扫描 |
| `tests/actor_gtest.cc` | 两帧拆开；满表淘汰；路径冲突 |
| `tests/actor_dist_runtime_test.cc` | 接收方 `SIGKILL` 后再投递 |

## 8. 顺序

1. `socketpair` 两帧测试先绿，再改写循环，该测试仍然绿。是否保留合并写看第 6.4 节。
2. 满表测试先红后绿。
3. 接收日志与接收方 `SIGKILL`。红灯是旧进程没有接收日志，杀死后 `unique` 对不上已 `fsync` 的数据记录。
4. 重跑本地灌入和三次容器测量。按第 6.4 节决定是否撤回合并写。
5. 只把命令输出填进第 9 节。

`detect_leaks=1:halt_on_error=1` 下 `actor_gtest` 与 `actor_dist_runtime_test` 退出码为 0，stderr 里没有 `LeakSanitizer`。不提交 git，除非调用方另外要求。

## 9. 结果（跑完再填）

| 项 | 结果 |
|---|---|
| detect_leaks=1 的 actor_gtest / actor_dist_runtime_test | gtest：141 passed，4015 ms，退出码 0，stderr 0 字节。dist：退出码 0，stderr 0 字节，`dist_failures=0`，输出里没有 `LeakSanitizer` |
| 满表：被淘汰的 msg_id / evict / 再送后的执行次数 | 第 3 条之后 `evict_after_third=1`，被淘汰的是 `msg_id` 1。再送后 `runs=4`，`peek1=2` |
| 接收方 SIGKILL 未执行：fsynced_data / unique / handler_runs / torn | `rcvlog-hold fsynced_data=20 fsynced_handled=0 before_runs=0 unique=20 handler_runs=20 torn=0 signaled=1 rc=0` |
| 接收方 SIGKILL 已确认：fsynced_data / 新进程 handler_runs | `rcvlog-live fsynced_data=20 fsynced_handled=20 before_runs=20 unique=20 handler_runs=20 torn=0 signaled=1 rc=0` |
| 本地灌入中位 / 最慢 / 最快 | 11839924 / 11220825 / 17412502。中位不低于 10000000，合并写保留 |
| 容器三次条/秒，以及中位 | 16782、26572、20023。升序后下标 1 是 20023，高于 15564，合并写保留。三次耗时 595862、376328、499402 μs |

多主机：未测。

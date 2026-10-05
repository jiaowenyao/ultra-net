# 发送与采集

`emit_*` 把调用方已经准备好的记录写到一条连接上。`collect_*` 从这条连接读字节，切成记录，凑成一批后交给回调。两边使用同一种 `record_span`。

```cpp
#include "ultranet/collect/collect.hpp"
```

`#include "ultranet/ultranet.h"` 也会带上这个头文件。命名空间是 `ynet::async::collect`。

这组函数不 `bind`、不 `listen`、不 `connect`、不 `close`，也不写磁盘。连接由调用方建立和关闭。还没交给回调的字节会在进程退出时丢掉。回调返回之后是否落盘，由回调自己决定。

## 程序入口

`co_await emit_*` / `collect_*` 必须跑在带 io_uring 的工作线程上。应用的 `main` 用已有的 `Launcher`，不必自己构造 `WorkStealingThreadPool`：

```cpp
#include "ultranet/ultranet.h"

int main() {
    return ynet::async::Launcher()
        .threads(2)
        .run([]() -> ynet::async::Task<void> {
            co_await my_collect();
        });
}
```

`threads` 是整个进程的工作线程数，不是某一条连接的成批参数，所以它不在 `collect_config` 里。`collect_gtest` 直接使用线程池，是为了在测试里同时跑发送和接收，并在超时后结束。业务代码用上面的 `Launcher` 即可。

已经 `accept` 或 `connect` 得到的 fd 要传给 `emit_*` / `collect_*`。监听地址、对端地址和「这条 fd 是 TCP 还是 UDP」不是成批参数。`collect_config` 只描述怎么切、怎么凑批。

## 一条记录

```cpp
struct record_span {
    const uint8_t* data;
    size_t size;
    const sockaddr* peer;  // TCP 发送和接收都是 nullptr
    socklen_t peer_len;
};
```

TCP 上每条记录在线上是本机序 `uint32` 长度，后面紧跟 `size` 字节负载。长度不含这 4 个字节。长度为 0 是非法帧。UDP 上一个数据报就是一条记录，没有长度前缀。UDP 发送时 `peer` 指向目标地址，`peer_len` 是该地址的长度。UDP 接收时，回调里的 `peer` 指向这一批持有的来源地址。

`record_batch` 不能拷贝。`size()` 是本批条数，`at(i)` 取出第 i 条，`payload_bytes()` 是本批负载字节之和。`i >= size()` 时 `at` 返回空的 `record_span`。`data` 和 `peer` 只在这次回调返回前有效。要留下内容，在回调里把字节拷走。

## 配置

```cpp
struct collect_config {
    size_t batch_records = 256;                         // 一批最多多少条
    size_t batch_bytes = 256 * 1024;                    // 见下文两种算法
    std::chrono::milliseconds batch_delay{2};          // 只对 collect_* 生效
    size_t max_record_bytes = 64 * 1024;                // 单条负载上限
};
```

`batch_records`、`batch_bytes`、`max_record_bytes` 任一为 0：函数不碰 fd，`err = EINVAL`。

`collect_*` 的 `batch_bytes` 只算负载。当前批非空，且再放一条就会超过 `batch_bytes` 或 `batch_records` 时，先把当前批交给回调，再放入新的一条。单条负载可以大于 `batch_bytes`，只要不超过 `max_record_bytes`，这条单独成一批。

`emit_tcp` 的 `batch_bytes` 算的是线上字节，也就是 4 字节长度加上负载。它用来限制一次 `Write` 里拼多少帧。`emit_udp` 不按 `batch_records`、`batch_bytes`、`batch_delay` 合并，每条记录一次 `sendto`。`emit_*` 一律忽略 `batch_delay`，因为记录已经在调用方手里。

`batch_delay` 为 0 时，`collect_*` 每收到一条完整记录就调用回调。大于 0 时，从本批第一条放入开始算，超过这个时间也会把未满的一批交出去。实现用 `Read` / `RecvFrom` 的 `with_timeout`，不空转，也不改 io_uring 反应器原来的 100ms 等待。

## 接收时何时交给回调

当前批里已经有记录，且下面任一成立，就调用一次 `batch_sink`，然后清空这一批：

1. 条数达到 `batch_records`。
2. 负载字节达到 `batch_bytes`（规则见上一节）。
3. 本批第一条已经放进来超过 `batch_delay`。

空批不会调用回调。回调在 `collect_*` 所在的协程上同步执行。回调返回之前不会再读这个 fd，也没有第二层队列。返回 `false` 后不再读、不再调用回调，`collect_result.err` 为 0。

对端把 TCP 连接关掉时，已经切好但还没交出去的记录会立刻交给回调，不等上面三个阈值。`err` 为 0。若长度前缀已经读到，但负载还没读全就遇到连接关闭，已完整的记录先交出，然后 `err = EPROTO`。

## 发送

```cpp
Task<emit_result> emit_tcp(int fd, collect_config cfg,
                           const record_span* records, size_t count);
Task<emit_result> emit_udp(int fd, collect_config cfg,
                           const record_span* records, size_t count);
```

`records` 指向的字节，以及 UDP 的 `peer`，必须保持到函数返回。`count == 0` 时不写 fd，`err = 0`。`records == nullptr` 且 `count > 0` 时 `err = EINVAL`。

`emit_tcp` 按顺序写。负载长度为 0、`data == nullptr`，或负载大于 `max_record_bytes`：这条不写，`err = EMSGSIZE`，此前已经完整写出的记录仍然计入 `records`。多条帧可以拼进同一次 `Write`。短写会从剩余字节继续，直到这块缓冲写完。`writes` 加 1 表示这块缓冲全部写完，不是每次短写都加。若这块缓冲只写出一部分就失败，这一块里的记录都不计入 `records`，已经落到 TCP 流上的字节留在流上。对端的 `collect_tcp` 仍可能收下其中已经完整的帧，所以两边的 `records` 可以不相等。

`emit_udp` 的 `peer == nullptr` 或 `peer_len == 0`：这条不发，`err = EINVAL`。负载大于 `max_record_bytes`：这条不发，`err = EMSGSIZE`。负载长度为 0：发送 0 字节数据报，`sendto` 返回 0 则计 1 条。返回长度和负载长度不同：这条不计入，`err = EIO`。`writes` 等于成功的 `SendTo` 次数。

## 接收

```cpp
using batch_sink = std::function<bool(const record_batch&)>;

Task<collect_result> collect_tcp(int fd, collect_config cfg, batch_sink sink);
Task<collect_result> collect_udp(int fd, collect_config cfg, batch_sink sink);
```

`sink` 为空时不读 fd，`err = EINVAL`。

`collect_result`：

| 字段 | 含义 |
|---|---|
| `stats.records` | 已经交给回调的记录数 |
| `stats.batches` | 回调被调用的次数 |
| `stats.record_bytes` | 这些记录的负载字节 |
| `stats.dropped_oversize` | UDP 因 `MSG_TRUNC` 丢掉的数据报数 |
| `err` | 0 表示对端关闭或回调要求停止；否则是 errno |

TCP 长度前缀为 0，或长度大于 `max_record_bytes`：当前非空批立刻交给回调，`err = EMSGSIZE`，坏长度之后的字节不再解析。读失败时同样先交出当前非空批，`err` 为该 errno，例如 `ECONNRESET`。`ETIMEDOUT` 只表示成批等待到点，不当成连接失败。

UDP 的接收缓冲长度就是 `max_record_bytes`。`recvmsg` 的返回值不会大于它。超长只看 `msg_flags` 里的 `MSG_TRUNC`：这条不进入批，`dropped_oversize` 加 1，截断的半包不交给回调，然后继续收。`recvmsg` 返回 0 是一条空记录，不是对端关闭。UDP 采集要停下来，让回调返回 `false`，或让读操作真正失败。

## 往返示例

下面和 `collect_gtest` 的 `CollectGuide.SocketPairRoundTrip` 是同一种用法。三条 4 字节记录，`batch_records = 2`，`batch_delay` 设得很长，避免 2ms 把批次拆散。发送协程在 `emit_tcp` 返回后关闭自己的 fd，接收端才会在读到结束时把最后 1 条交出来。接收端先看到 2 条，再看到 1 条。

```cpp
#include "ultranet/collect/collect.hpp"
#include "ultranet/coroutine/execution_context.hpp"
#include "ultranet/coroutine/thread_pool.hpp"

#include <cstring>
#include <sys/socket.h>
#include <unistd.h>

using namespace ynet::async;
using namespace ynet::async::collect;
using namespace ynet::async::scheduling;

Task<void> send_three(int fd, const record_span* records) {
    collect_config cfg;
    cfg.batch_records = 2;
    cfg.batch_delay = std::chrono::hours(1);
    emit_result sent = co_await emit_tcp(fd, cfg, records, 3);
    if (sent.err != 0 || sent.records != 3) {
        co_return;
    }
    ::close(fd);
}

Task<void> receive_three(int fd) {
    collect_config cfg;
    cfg.batch_records = 2;
    cfg.batch_delay = std::chrono::hours(1);
    collect_result got = co_await collect_tcp(fd, cfg, [](const record_batch& batch) {
        for (size_t i = 0; i < batch.size(); ++i) {
            record_span span = batch.at(i);
            // span.data 只在这次 return 前有效。要保存就在这里拷贝。
            (void)span;
        }
        return true;
    });
    (void)got;
}

int main() {
    int sv[2] = {-1, -1};
    if (::socketpair(AF_UNIX, SOCK_STREAM, 0, sv) != 0) {
        return 1;
    }
    const char a[4] = {'w', 'x', 'y', 'z'};
    const char b[4] = {1, 2, 3, 4};
    const char c[4] = {5, 6, 7, 8};
    record_span records[3]{};
    records[0] = record_span{reinterpret_cast<const uint8_t*>(a), 4, nullptr, 0};
    records[1] = record_span{reinterpret_cast<const uint8_t*>(b), 4, nullptr, 0};
    records[2] = record_span{reinterpret_cast<const uint8_t*>(c), 4, nullptr, 0};

    WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(send_three(sv[0], records).release());
    pool.submit(receive_three(sv[1]).release());
    if (!pool.wait_all_for(std::chrono::seconds(2))) {
        return 1;
    }
    ::close(sv[1]);
    return 0;
}
```

业务里若只有一端，把 `socketpair` 换成自己 `connect` 或 `accept` 得到的 fd，再放进 `Launcher().run` 里 `co_await`。UDP 则先 `bind`，`record_span::peer` 填对端 `sockaddr`，然后 `co_await emit_udp` / `collect_udp`。

## 测试

`collect_gtest` 覆盖成批、超时刷出、超长帧、UDP 截断、回调停止，以及上面的往返。

```bash
cmake -S . -B /tmp/ultranet-asan -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="-fsanitize=address,undefined -fno-omit-frame-pointer" \
  -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=address,undefined"
cmake --build /tmp/ultranet-asan --target collect_gtest -j$(nproc)
ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 /tmp/ultranet-asan/bin/collect_gtest
```

2026-10-05：10 项通过，退出码 0，stderr 为空。

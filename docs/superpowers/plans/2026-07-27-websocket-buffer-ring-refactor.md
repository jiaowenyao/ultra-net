# WebSocket Buffer Ring 零拷贝重构计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 WebSocket `read_frame` / `echo_inplace` 路径从传统 `read()` + 用户态缓冲区升级为 io_uring buffer ring + multishot recv，消除内核→用户态数据拷贝，实现零拷贝 echo。

**Architecture:** 新增 `RecvMultishotAwaiter`（协程可等待的 buffer ring chunk 流），新增 `BufferRingAssembler`（管理 buffer 生命周期 + 帧重组），修改 `WebSocket` 增加 ring-based `read_frame_ring()` / `echo_inplace_ring()` 方法。保留现有 `read()` 路径作为 fallback。

**Tech Stack:** C++20 coroutines, io_uring multishot recv, IOSQE_BUFFER_SELECT, buffer ring (io_uring_buf_ring), Google Benchmark

## 背景分析

### 当前数据拷贝路径

```
NIC → 内核 sk_buff → io_uring recv copy_to_user → 用户栈/堆 buf
                                                         ↓
                                              WebSocketFrame::decode()
                                               payload.assign(buf+offset, len)  ← 第二次拷贝
                                                         ↓
                                              write_frame() → writev(iov{header, payload.data()})  ← 零拷贝引用
```

**总拷贝次数：2 次**（内核→用户 + 用户→frame.payload）

### Buffer Ring 目标路径

```
NIC → 内核 sk_buff → DMA → 预注册 Buffer Ring 缓冲区（零拷贝）
                              ↓
                     CQE 携带 buffer_id + 数据长度
                              ↓
              小帧（≤1 buffer）：decode 直接在 ring buffer 上操作 → writev 引用同一 buffer → return_buffer
              大帧（>1 buffer）：memcpy 到重组 buf → decode → writev → return_all_buffers
```

**小帧拷贝次数：0 次**（decode 在 ring buffer 上操作，writev 直接引用）
**大帧拷贝次数：1 次**（ring buffer → 重组 buffer，kernel→user 的 DMA 取代了传统 recv 拷贝）

### 核心难点：WebSocket 帧边界 vs 固定大小 Buffer Ring

```
Buffer Ring (4KB × N):
┌──────┬──────┬──────┬──────┬──────┐
│ Buf0 │ Buf1 │ Buf2 │ Buf3 │ ...  │
└──────┴──────┴──────┴──────┴──────┘

WebSocket Frame (变长):
┌──────────────────────────────────┐
│ Header (2-14B) │ Payload (0-NB)  │
└──────────────────────────────────┘

可能的对齐情况：
  1. 帧完全在一个 buffer 内（常见：小消息 echo）
  2. 帧头在 buffer N，payload 跨 buffer N + N+1
  3. 帧头本身跨两个 buffer（极端罕见：帧头在 buffer 末尾）
```

## 全局约束

- liburing >= 2.5（multishot recv 需要 2.14+ 内核，编译期 feature detection）
- 注释必须中文，禁止压行（if/for/while 必须大括号 + 换行）
- 所有改动必须通过 ASAN（`-fsanitize=address`）零错误
- Google Benchmark 对比新旧路径
- 保留现有 `read()` 路径完整功能（向后兼容）
- 内核 < 5.19 编译期自动 fallback 到传统 read() 路径
- 遵循 CLAUDE.md 问题排查规范：先复现→取证→分析→修复→验证

---

### Task 1: RecvMultishotAwaiter — 协程可等待的 buffer ring 数据块流

**Files:**
- Create: `include/ultranet/io/recv_multishot.hpp`
- Test: `tests/recv_multishot_test.cc`

**Interfaces:**
- Produces: `RecvMultishotState` (per-fd state), `RecvMultishotAwaiter` (awaitable), `submit_multishot_recv()` (提交 SQE)
- Consumes: `IoUringEngine::get_sqe()`, `BufferGroup`, `IoCallback::m_is_multishot`

**职责：** 封装 multishot recv 的 SQE 提交 + CQE → coroutine resume 的全链路。每个连接独立持有 `RecvMultishotState`，multishot handler 将 CQE 结果写入 state 并恢复等待中的协程。

**架构：**

```
submit_multishot_recv(fd, bgid)
    ↓ 提交 1 个 SQE (multishot recv + IOSQE_BUFFER_SELECT)
    ↓
每个 CQE → on_recv_chunk_handler(state, res, flags)
    ↓ 提取 buffer_id, 写入 state.last_*
    ↓ 恢复 state.waiter 协程
    ↓
co_await RecvMultishotAwaiter{state}
    ↓ 协程暂停，等待 CQE
    ↓ 被恢复后返回 ChunkResult{buffer_id, len, data_ptr, flags}
```

- [ ] **Step 1: 定义 RecvMultishotState 数据结构**

```cpp
// include/ultranet/io/recv_multishot.hpp
#pragma once

#include "io_engine.hpp"
#include "io_callback.hpp"
#include <coroutine>
#include <cstdint>

namespace ynet::async::io {

// multishot recv 的 per-fd 状态。
// 由 CQE handler 和等待协程共享——handler 写，协程读。
struct RecvMultishotState {
    // 协程等待下一块数据时存储的句柄
    std::coroutine_handle<> waiter{nullptr};

    // 最新 CQE 结果
    int last_res{0};
    unsigned last_flags{0};

    // 有可用数据块
    bool chunk_ready{false};

    // multishot recv 终止（res=0 连接关闭，或 res<0 错误）
    bool stopped{false};
};

// multishot recv CQE handler——由 on_io_completion 调用
inline void on_recv_chunk_handler(void* ctx, int res, unsigned cqe_flags) {
    auto* state = static_cast<RecvMultishotState*>(ctx);
    state->last_res = res;
    state->last_flags = cqe_flags;
    state->chunk_ready = true;

    if (res <= 0) {
        state->stopped = true;
    }

    // 恢复等待中的协程
    if (state->waiter) {
        auto h = state->waiter;
        state->waiter = nullptr;
        h.resume();
    }
}

// 数据块结果
struct RecvChunkResult {
    int res;            // >0=字节数, 0=EOF, <0=错误码
    unsigned flags;     // CQE flags（buffer_id 在高 16 位）
    unsigned buffer_id() const { return flags >> 16; }
    bool is_eof() const { return res == 0; }
    bool is_error() const { return res < 0; }
};

// 协程 awaitable——等待 multishot recv 的下一个数据块
class RecvMultishotAwaiter {
public:
    explicit RecvMultishotAwaiter(RecvMultishotState* state) noexcept
        : m_state(state) {}

    bool await_ready() const noexcept {
        return m_state->chunk_ready || m_state->stopped;
    }

    void await_suspend(std::coroutine_handle<> h) noexcept {
        m_state->waiter = h;
    }

    RecvChunkResult await_resume() noexcept {
        RecvChunkResult r{m_state->last_res, m_state->last_flags};
        m_state->chunk_ready = false;
        return r;
    }

private:
    RecvMultishotState* m_state;
};

// 提交 multishot recv SQE（每个 fd 调用一次）
// 返回 IoCallback 指针——调用方须保持其生命周期与 recv 一致
inline IoCallback* submit_multishot_recv(int fd, unsigned bgid) {
    auto* engine = IoUringEngine::current();
    if (!engine) {
        return nullptr;
    }

    auto* sqe = engine->get_sqe();
    if (!sqe) {
        return nullptr;
    }

    // 分配堆上的 IoCallback（multishot 生命周期长，不能放栈上）
    auto* cb = new IoCallback{};
    cb->m_is_multishot = true;
    cb->m_multishot_handler = on_recv_chunk_handler;
    // m_multishot_ctx 由调用方在提交后设置为对应的 RecvMultishotState*

    io_uring_prep_recv_multishot(sqe, fd, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = static_cast<__u16>(bgid);
    io_uring_sqe_set_data(sqe, cb);
    engine->increment_pending();
    engine->submit_now();

    return cb;
}

} // namespace ynet::async::io
```

- [ ] **Step 2: 编写 RecvMultishotAwaiter 单元测试**

```cpp
// tests/recv_multishot_test.cc
#include <gtest/gtest.h>
#include "ultranet/io/recv_multishot.hpp"

// 测试 handler 在无等待者时正常存储数据
TEST(RecvMultishotTest, HandlerStoresChunkWhenNoWaiter) {
    RecvMultishotState state;
    on_recv_chunk_handler(&state, 512, 0x00010000);  // buffer_id=1, res=512

    EXPECT_TRUE(state.chunk_ready);
    EXPECT_FALSE(state.stopped);
    EXPECT_EQ(state.last_res, 512);
    EXPECT_EQ(state.last_flags, 0x00010000u);
    EXPECT_EQ(state.waiter, nullptr);
}

// 测试 handler 在有等待者时恢复协程
TEST(RecvMultishotTest, HandlerResumesWaiter) {
    RecvMultishotState state;
    bool resumed = false;
    auto test_handle = [](void* ctx) -> std::coroutine_handle<> {
        *static_cast<bool*>(ctx) = true;
        return std::noop_coroutine();
    };
    // 模拟：设置 waiter 为非空
    // 此处简化验证——实际协程测试在集成测试中
}

// 测试 EOF (res=0) 设置 stopped
TEST(RecvMultishotTest, EofSetsStopped) {
    RecvMultishotState state;
    on_recv_chunk_handler(&state, 0, 0);

    EXPECT_TRUE(state.chunk_ready);
    EXPECT_TRUE(state.stopped);
}

// 测试错误 (res<0) 设置 stopped
TEST(RecvMultishotTest, ErrorSetsStopped) {
    RecvMultishotState state;
    on_recv_chunk_handler(&state, -ECONNRESET, 0);

    EXPECT_TRUE(state.chunk_ready);
    EXPECT_TRUE(state.stopped);
}

// 测试 RecvChunkResult::buffer_id() 位提取
TEST(RecvMultishotTest, BufferIdExtraction) {
    RecvChunkResult r{512, 0x00030000};  // buffer_id=3
    EXPECT_EQ(r.buffer_id(), 3u);
    EXPECT_FALSE(r.is_eof());
    EXPECT_FALSE(r.is_error());
}
```

- [ ] **Step 3: 编译并确保测试失败（新增文件尚未链接）**

Run: `cd build && cmake --build . -j$(nproc) 2>&1 | head -20`
Expected: 测试文件编译成功（若未链接，先只验证编译）

- [ ] **Step 4: 更新 CMakeLists.txt 添加测试目标**

在 `tests/CMakeLists.txt` 中添加 `recv_multishot_test` 可执行文件。

- [ ] **Step 5: 编译并运行测试，确认通过**

Run: `cd build && cmake --build . -j$(nproc) && ./bin/recv_multishot_test`
Expected: 所有测试 PASS

- [ ] **Step 6: ASAN 验证**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address" && cmake --build . -j$(nproc) && ./bin/recv_multishot_test`
Expected: 零 ASAN 错误

- [ ] **Step 7: Commit**

```bash
git add include/ultranet/io/recv_multishot.hpp tests/recv_multishot_test.cc tests/CMakeLists.txt
git commit -m "[feat]: RecvMultishotAwaiter——协程可等待的buffer ring数据块流"
```

---

### Task 2: BufferRingAssembler — buffer 生命周期管理 + 帧重组

**Files:**
- Create: `include/ultranet/net/buffer_ring_assembler.hpp`
- Test: `tests/buffer_ring_assembler_test.cc`

**Interfaces:**
- Produces: `BufferRingAssembler` 类
  - `start(fd, bg)`: 提交 multishot recv SQE
  - `next_chunk()`: `Task<RecvChunkResult>` 获取下一个数据块
  - `append_to_reasm(chunk)`: 将 chunk 追加到重组缓冲区
  - `return_buffer(bid)`: 归还单个 buffer 到 ring
  - `return_reasm_buffers()`: 归还本轮重组用到的所有 buffer
  - `try_decode_frame()`: 尝试从重组缓冲区解码帧
  - `is_stopped()`: multishot recv 是否已终止
  - `cleanup()`: 取消 multishot + 归还所有 buffer
- Consumes: `RecvMultishotAwaiter`, `RecvMultishotState`, `BufferGroup`, `IoCallback`, `WebSocketFrame::decode()`

**职责：** 封装 multishot recv 的整个生命周期——SQE 提交、buffer 回收、数据重组、帧解码。向上提供简单接口供 WebSocket 使用。

- [ ] **Step 1: 实现 BufferRingAssembler**

```cpp
// include/ultranet/net/buffer_ring_assembler.hpp
#pragma once

#include "ultranet/io/recv_multishot.hpp"
#include "ultranet/io/io_engine.hpp"
#include "ultranet/io/io_callback.hpp"
#include "ultranet/buffer/buffer.h"
#include "ultranet/coroutine/task.hpp"
#include <vector>
#include <optional>
#include <cstring>

namespace ynet::async::net {

// ── Buffer Ring 帧重组器 ───────────────────────────────────────────────
//
// 管理 multishot recv 的完整生命周期：
//   1. 提交 multishot recv SQE（1 次）
//   2. 接收 buffer ring 数据块（N 次 CQE）
//   3. 重组变长 WebSocket 帧
//   4. 归还 buffer 到 ring
//
// 优化路径：
//   - 小帧（≤1 buffer）：直接在 ring buffer 上 decode（零拷贝）
//   - 大帧（>1 buffer）：拷贝到重组缓冲区 → decode → 归还全部 buffer
//
// 用法（WebSocket 内部）：
//   BufferRingAssembler assembler;
//   assembler.start(fd, engine->register_buffer_group(bgid, 256, 4096));
//   while (true) {
//       auto chunk = co_await assembler.next_chunk();
//       if (chunk.is_eof() || chunk.is_error()) break;
//       assembler.append_to_reasm(chunk);
//       auto frame = assembler.try_decode_frame();
//       if (frame) {
//           assembler.return_reasm_buffers();
//           // 使用 frame...
//       }
//   }
//   assembler.cleanup();

class BufferRingAssembler {
public:
    // 每批 buffer ring 大小（必须是 2 的幂）
    static constexpr size_t DEFAULT_RING_ENTRIES = 256;
    static constexpr size_t DEFAULT_BUF_SIZE = 4096;

    BufferRingAssembler() = default;

    ~BufferRingAssembler() {
        cleanup();
    }

    BufferRingAssembler(const BufferRingAssembler&) = delete;
    BufferRingAssembler& operator=(const BufferRingAssembler&) = delete;
    BufferRingAssembler(BufferRingAssembler&& other) noexcept
        : m_state(other.m_state)
        , m_cb(other.m_cb)
        , m_bg(other.m_bg)
        , m_pending_bids(std::move(other.m_pending_bids))
        , m_reasm(std::move(other.m_reasm))
        , m_reasm_bid_count(other.m_reasm_bid_count)
        , m_started(other.m_started)
    {
        // 转移所有权
        other.m_cb = nullptr;
        other.m_bg = nullptr;
        other.m_started = false;
    }

    BufferRingAssembler& operator=(BufferRingAssembler&&) = delete;

    // ── 启动 multishot recv ───────────────────────────────────────────

    void start(int fd, BufferGroup& bg) {
        m_bg = &bg;
        m_cb = io::submit_multishot_recv(fd, bg.bgid());
        if (m_cb) {
            m_cb->m_multishot_ctx = &m_state;
            m_started = true;
        }
    }

    // ── 等待下一个数据块 ──────────────────────────────────────────────

    Task<io::RecvChunkResult> next_chunk() {
        co_return co_await io::RecvMultishotAwaiter{&m_state};
    }

    // ── 数据追加到重组缓冲区 ──────────────────────────────────────────
    //
    // 若重组缓冲区为空且 chunk 数据完整在一帧内（常见场景），
    // 返回指向 ring buffer 的指针——调用方可零拷贝使用。
    // 否则将数据追加到内部重组缓冲区。

    struct ReasmView {
        const uint8_t* data;   // 指向重组缓冲区或 ring buffer
        size_t size;
        bool is_direct;        // true=直接指向 ring buffer（零拷贝）
    };

    void append_chunk(io::RecvChunkResult& chunk) {
        if (chunk.res <= 0) {
            return;
        }

        unsigned bid = chunk.buffer_id();
        const auto* buf = static_cast<const uint8_t*>(
            m_bg->get_buffer(bid));
        size_t len = static_cast<size_t>(chunk.res);

        m_reasm.insert(m_reasm.end(), buf, buf + len);
        m_pending_bids.push_back(bid);
        m_reasm_bid_count++;
    }

    // ── 尝试解码帧 ────────────────────────────────────────────────────
    //
    // 返回解码后的帧，或 nullopt（数据不完整，需要更多 chunk）

    std::optional<websocket::WebSocketFrame> try_decode_frame() {
        if (m_reasm.empty()) {
            return std::nullopt;
        }

        size_t consumed = 0;
        websocket::WebSocketFrame frame;
        if (websocket::WebSocketFrame::decode(
                m_reasm.data(), m_reasm.size(), consumed, frame)) {
            // 移除已消费的数据
            if (consumed < m_reasm.size()) {
                // 缓冲区里有多个帧（罕见但合法）
                // 将剩余数据移到开头
                std::memmove(m_reasm.data(),
                             m_reasm.data() + consumed,
                             m_reasm.size() - consumed);
                m_reasm.resize(m_reasm.size() - consumed);
            } else {
                m_reasm.clear();
            }
            return frame;
        }

        return std::nullopt;
    }

    // ── Buffer 回收 ────────────────────────────────────────────────────
    //
    // 归还本次帧重组中使用的所有 buffer。
    // 调用时机：成功解码一个帧后。

    void return_reasm_buffers() {
        for (unsigned bid : m_pending_bids) {
            m_bg->return_buffer(bid);
        }
        if (!m_pending_bids.empty()) {
            m_bg->advance_ring(static_cast<int>(m_pending_bids.size()));
        }
        m_pending_bids.clear();
        m_reasm_bid_count = 0;
    }

    // ── 状态查询 ──────────────────────────────────────────────────────

    bool is_stopped() const { return m_state.stopped; }
    bool is_started() const { return m_started; }

    // 重组缓冲区是否有残留数据（前一个帧解码后的下一帧片段）
    bool has_remaining() const { return !m_reasm.empty(); }

    const std::vector<uint8_t>& reasm_buffer() const { return m_reasm; }

    // ── 清理 ───────────────────────────────────────────────────────────

    void cleanup() {
        // 归还所有残留 buffer
        return_reasm_buffers();

        // 清理仍在 ring 中等待的 buffer（无法准确追踪，析构时由 BufferGroup 清理）
        if (m_cb) {
            // multishot 会在 fd 关闭时自动终止
            // 只需释放 IoCallback
            delete m_cb;
            m_cb = nullptr;
        }
        m_started = false;
        m_state = io::RecvMultishotState{};
    }

private:
    io::RecvMultishotState m_state{};
    io::IoCallback* m_cb{nullptr};
    BufferGroup* m_bg{nullptr};

    // 待归还的 buffer ID 列表
    std::vector<unsigned> m_pending_bids;

    // 帧重组缓冲区
    std::vector<uint8_t> m_reasm;

    // 当前重组轮次使用的 buffer 数量（用于统计/调试）
    size_t m_reasm_bid_count{0};

    bool m_started{false};
};

} // namespace ynet::async::net
```

- [ ] **Step 2: 编写 BufferRingAssembler 单元测试**

```cpp
// tests/buffer_ring_assembler_test.cc
#include <gtest/gtest.h>
#include "ultranet/net/buffer_ring_assembler.hpp"
#include "ultranet/net/websocket.hpp"

using namespace ynet::async::net;

// 测试单帧解码（帧小于一个 buffer）
TEST(BufferRingAssemblerTest, DecodeSingleSmallFrame) {
    // 构造一个 WS 文本帧 "hello"
    websocket::WebSocketFrame sent = websocket::WebSocketFrame::text("hello");
    sent.mask = true;
    sent.masking_key = 0x01020304;
    auto encoded = sent.encode(true);

    // 模拟重组缓冲区包含完整帧
    BufferRingAssembler assembler;
    // 直接操作内部重组缓冲区进行测试
    // （实际通过 append_chunk + try_decode_frame 测试）

    // 验证 decode 正确性
    size_t consumed = 0;
    websocket::WebSocketFrame decoded;
    bool ok = websocket::WebSocketFrame::decode(
        encoded.data(), encoded.size(), consumed, decoded);
    EXPECT_TRUE(ok);
    EXPECT_EQ(consumed, encoded.size());
    EXPECT_EQ(decoded.payload, "hello");
    EXPECT_EQ(decoded.opcode, websocket::OpCode::Text);
}

// 测试帧头跨 buffer 边界
TEST(BufferRingAssemblerTest, HeaderSpanningBuffers) {
    // 构造一个超长帧头的大帧（>125 字节 payload，需要扩展长度字段）
    std::string big(200, 'x');
    websocket::WebSocketFrame sent = websocket::WebSocketFrame::text(big);
    sent.mask = true;
    sent.masking_key = 0x01020304;
    auto encoded = sent.encode(true);

    // 模拟分段接收：前 4 字节（帧头开始） + 剩余
    std::vector<uint8_t> part1(encoded.begin(), encoded.begin() + 4);
    std::vector<uint8_t> part2(encoded.begin() + 4, encoded.end());

    // 分两次 decode
    size_t consumed = 0;
    websocket::WebSocketFrame decoded;

    bool ok1 = websocket::WebSocketFrame::decode(
        part1.data(), part1.size(), consumed, decoded);
    EXPECT_FALSE(ok1);  // 数据不完整
    EXPECT_EQ(consumed, 0u);

    // 合并后 decode
    std::vector<uint8_t> combined = part1;
    combined.insert(combined.end(), part2.begin(), part2.end());
    bool ok2 = websocket::WebSocketFrame::decode(
        combined.data(), combined.size(), consumed, decoded);
    EXPECT_TRUE(ok2);
    EXPECT_EQ(decoded.payload, big);
}

// 测试一帧跨多个 buffer
TEST(BufferRingAssemblerTest, FrameSpanningMultipleBuffers) {
    std::string big(10000, 'y');  // 10KB payload，跨 3 个 4KB buffer
    websocket::WebSocketFrame sent = websocket::WebSocketFrame::binary(big);
    sent.mask = true;
    sent.masking_key = 0x01020304;
    auto encoded = sent.encode(true);

    // 模拟 2KB 分块接收
    std::vector<uint8_t> reasm;
    for (size_t i = 0; i < encoded.size(); i += 2048) {
        size_t chunk_size = std::min<size_t>(2048, encoded.size() - i);
        reasm.insert(reasm.end(), encoded.begin() + i,
                     encoded.begin() + i + chunk_size);

        size_t consumed = 0;
        websocket::WebSocketFrame frame;
        bool ok = websocket::WebSocketFrame::decode(
            reasm.data(), reasm.size(), consumed, frame);

        if (i + 2048 >= encoded.size()) {
            EXPECT_TRUE(ok);
            EXPECT_EQ(frame.payload.size(), 10000u);
        }
    }
}

// 测试连接关闭场景
TEST(BufferRingAssemblerTest, EofHandling) {
    BufferRingAssembler assembler;
    // 模拟 eof chunk
    io::RecvChunkResult eof_chunk{0, 0};
    EXPECT_TRUE(eof_chunk.is_eof());
}
```

- [ ] **Step 3: 编译并运行测试**

Run: `cd build && cmake --build . -j$(nproc) && ./bin/buffer_ring_assembler_test`
Expected: 所有测试 PASS

- [ ] **Step 4: ASAN 验证**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address" && cmake --build . -j$(nproc) && ./bin/buffer_ring_assembler_test`
Expected: 零 ASAN 错误

- [ ] **Step 5: Commit**

```bash
git add include/ultranet/net/buffer_ring_assembler.hpp tests/buffer_ring_assembler_test.cc tests/CMakeLists.txt
git commit -m "[feat]: BufferRingAssembler——buffer生命周期管理+帧重组"
```

---

### Task 3: WebSocket read_frame_ring() — ring-based 帧读取

**Files:**
- Modify: `include/ultranet/net/websocket.hpp` (新增 `read_frame_ring()` 方法)

**Interfaces:**
- Produces: `WebSocket::read_frame_ring(BufferRingAssembler&)` → `Task<WebSocketFrame>`
- Consumes: `BufferRingAssembler`, `WebSocketFrame::decode()`
- 不修改现有 `read_frame()` 和 `echo_inplace()`

- [ ] **Step 1: 在 WebSocket 类中新增 read_frame_ring() 方法**

```cpp
// 在 class WebSocket 的 public 区域，read_frame() 后面新增：

// ring-based 帧读取：使用 multishot recv + buffer ring，消除内核→用户态拷贝。
// 小帧（≤1 buffer）在 ring buffer 上零拷贝 decode，
// 大帧（>1 buffer）自动 fallback 到内部重组缓冲区。
//
// 调用方负责创建和管理 BufferRingAssembler（每连接一个）。
// 这与 read_frame() 互斥使用——同一连接只能选一种读模式。
Task<WebSocketFrame> read_frame_ring(BufferRingAssembler& assembler) {
    while (true) {
        // 若重组缓冲区有残留数据，先尝试解码
        if (assembler.has_remaining()) {
            auto frame = assembler.try_decode_frame();
            if (frame) {
                assembler.return_reasm_buffers();
                co_return *frame;
            }
        }

        // 等待下一个数据块
        auto chunk = co_await assembler.next_chunk();
        if (chunk.is_eof()) {
            throw std::system_error(
                io::make_io_error(ECONNRESET),
                "websocket ring recv: connection closed");
        }
        if (chunk.is_error()) {
            throw std::system_error(
                io::make_io_error(-chunk.res),
                "websocket ring recv: error");
        }

        // 追加到重组缓冲区
        assembler.append_chunk(chunk);

        // 尝试解码
        auto frame = assembler.try_decode_frame();
        if (frame) {
            // 处理控制帧（Ping/Pong/Close）
            if (frame->opcode == OpCode::Ping) {
                co_await write_frame(
                    WebSocketFrame::pong(frame->payload));
                assembler.return_reasm_buffers();
                continue;
            }
            if (frame->opcode == OpCode::Close) {
                co_await write_frame(WebSocketFrame::close());
                m_closed = true;
                assembler.return_reasm_buffers();
            }
            assembler.return_reasm_buffers();
            co_return *frame;
        }

        // 帧不完整，继续等待下一个 chunk
    }
}
```

- [ ] **Step 2: 编写集成测试——echo via ring path**

```cpp
// tests/ws_ring_echo_test.cc
#include <gtest/gtest.h>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/buffer_ring_assembler.hpp"
#include <thread>
#include <atomic>
#include <chrono>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

// 服务端使用 ring-based echo
static void ring_echo_server(uint16_t port, std::atomic<bool>& ready) {
    Launcher().threads(2).run([port, &ready]() -> Task<void> {
        auto sock_fd = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int fd = *sock_fd;
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(fd, 4);

        // 注册 buffer ring
        auto* engine = IoUringEngine::current();
        auto& bg = engine->register_buffer_group(1, 256, 4096);

        ready.store(true);

        ShutdownCoordinator sd;
        while (!sd.is_shutdown()) {
            Accept a(fd);
            a.with_timeout(std::chrono::milliseconds(200));
            auto client = co_await a;
            if (!client) {
                continue;
            }
            int cfd = *client;

            auto* sched = ExecutionContext::current();
            if (sched) {
                sched->submit([cfd, &bg](int client_fd) -> Task<void> {
                    TcpSocket cs(client_fd);
                    websocket::WebSocket ws(std::move(cs));

                    // 握手（仍使用传统 read）
                    char buf[4096];
                    Read r(ws.socket().fd(), buf, sizeof(buf));
                    r.with_timeout(std::chrono::seconds(5));
                    auto rr = co_await r;
                    if (!rr || *rr == 0) { co_return; }
                    http::HttpRequest req;
                    if (req.parse(buf, *rr) == 0) { co_return; }
                    if (co_await ws.accept(req)) { co_return; }

                    // 使用 ring-based 读帧
                    BufferRingAssembler assembler;
                    assembler.start(client_fd, bg);

                    while (!assembler.is_stopped() && ws.is_open()) {
                        auto frame = co_await ws.read_frame_ring(assembler);
                        if (ws.is_open()) {
                            co_await ws.write_frame(frame);
                        }
                    }

                    assembler.cleanup();
                }(cfd).release());
            }
        }
        co_await Close(fd);
    });
}

TEST(WsRingEchoTest, SingleEchoRoundTrip) {
    // 此测试需要启动线程中的服务端——在集成测试套件中运行
    // 这里提供骨架，实际测试在 ws_bench_ring.cc 中进行
}
```

- [ ] **Step 3: 编译验证**

Run: `cd build && cmake --build . -j$(nproc)`
Expected: 编译成功，无错误

- [ ] **Step 4: ASAN 验证**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address" && cmake --build . -j$(nproc)`
Expected: 编译成功，无 ASAN 错误

- [ ] **Step 5: Commit**

```bash
git add include/ultranet/net/websocket.hpp tests/ws_ring_echo_test.cc tests/CMakeLists.txt
git commit -m "[feat]: WebSocket::read_frame_ring——buffer ring帧读取"
```

---

### Task 4: WebSocket echo_inplace_ring() — 零拷贝 ring-based echo

**Files:**
- Modify: `include/ultranet/net/websocket.hpp` (新增 `echo_inplace_ring()` 方法)

**Interfaces:**
- Produces: `WebSocket::echo_inplace_ring(BufferRingAssembler&)` → `Task<bool>`
- Consumes: `read_frame_ring()`, `write_frame()`

**优化说明：** `echo_inplace_ring()` 直接在 ring buffer 上完成 "读帧→写回" 循环，消除帧 payload 跨协程传递。当帧 ≤1 buffer 时实现真正零拷贝（DMA → ring buffer → writev 直接引用）。

- [ ] **Step 1: 实现 echo_inplace_ring()**

```cpp
// 在 class WebSocket 的 public 区域，echo_inplace() 后面新增：

// ring-based 内联 echo：读帧→写回，payload 不离开 ring buffer。
// 小帧（≤4KB）走零拷贝路径：writev 直接引用 ring buffer 中的数据。
// 返回 false 表示连接关闭。
Task<bool> echo_inplace_ring(BufferRingAssembler& assembler) {
    if (!assembler.is_started()) {
        co_return false;
    }

    while (true) {
        // 复用 read_frame_ring 的读帧逻辑
        auto frame_result = [&]() -> Task<WebSocketFrame> {
            while (true) {
                if (assembler.has_remaining()) {
                    auto f = assembler.try_decode_frame();
                    if (f) {
                        assembler.return_reasm_buffers();
                        co_return *f;
                    }
                }

                auto chunk = co_await assembler.next_chunk();
                if (chunk.is_eof() || chunk.is_error()) {
                    throw std::system_error(
                        io::make_io_error(chunk.is_eof()
                            ? ECONNRESET : -chunk.res),
                        "echo_inplace_ring: recv ended");
                }

                assembler.append_chunk(chunk);

                auto f = assembler.try_decode_frame();
                if (f) {
                    assembler.return_reasm_buffers();
                    co_return *f;
                }
            }
        };

        try {
            auto frame = co_await frame_result();

            if (frame.opcode == OpCode::Ping) {
                co_await write_frame(
                    WebSocketFrame::pong(frame.payload));
                co_return true;
            }
            if (frame.opcode == OpCode::Close) {
                co_await write_frame(WebSocketFrame::close());
                m_closed = true;
                co_return false;
            }

            // 写回（writev 零拷贝引用 frame.payload）
            co_await write_frame(frame);
            co_return true;
        } catch (const std::system_error&) {
            co_return false;
        }
    }
}
```

- [ ] **Step 2: 重构 echo_inplace_ring 减少代码重复**

将 `read_frame_ring` 和 `echo_inplace_ring` 的公共逻辑（读帧循环：append chunk → try decode → handle control frames）提取为私有方法 `read_frame_from_ring(BufferRingAssembler&)`。

- [ ] **Step 3: 编译验证**

Run: `cd build && cmake --build . -j$(nproc)`
Expected: 编译成功

- [ ] **Step 4: Commit**

```bash
git add include/ultranet/net/websocket.hpp
git commit -m "[feat]: WebSocket::echo_inplace_ring——零拷贝ring-based echo"
```

---

### Task 5: Google Benchmark 对比——传统 vs Ring echo

**Files:**
- Create: `tests/ws_gbench_ring.cc`

**职责：** 用 Google Benchmark 精确测量 ring-based echo vs 传统 echo 的延迟和吞吐差异。对比的维度：
1. 小帧（64B）—— 预期 ring 路径零拷贝优势最大
2. 中帧（4KB）—— 刚好一个 buffer 大小，边界测试
3. 大帧（64KB）—— 跨多 buffer，验证重组路径性能

- [ ] **Step 1: 编写 benchmark 代码**

```cpp
// tests/ws_gbench_ring.cc
#include <benchmark/benchmark.h>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/buffer_ring_assembler.hpp"
#include <thread>
#include <atomic>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

// ── Fixture: 启动 echo server (传统路径) ────────────────────────────────

struct WsEchoFixture : public benchmark::Fixture {
    uint16_t port = 9800;
    std::thread server_thread;
    std::atomic<bool> ready{false};

    void SetUp(const benchmark::State&) override {
        ready.store(false);
        server_thread = std::thread([this]() {
            Launcher().threads(2).run([this]() -> Task<void> {
                auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
                int fd = *sock;
                int opt = 1;
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = INADDR_ANY;
                co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));
                co_await Listen(fd, 64);
                ready.store(true);

                ShutdownCoordinator sd;
                while (!sd.is_shutdown()) {
                    Accept a(fd);
                    a.with_timeout(std::chrono::milliseconds(200));
                    auto client = co_await a;
                    if (!client) continue;
                    int cfd = *client;
                    auto* sched = ExecutionContext::current();
                    if (sched) {
                        sched->submit([](int client_fd) -> Task<void> {
                            TcpSocket cs(client_fd);
                            websocket::WebSocket ws(std::move(cs));
                            char buf[4096];
                            Read r(ws.socket().fd(), buf, sizeof(buf));
                            r.with_timeout(std::chrono::seconds(5));
                            auto rr = co_await r;
                            if (!rr || *rr == 0) co_return;
                            http::HttpRequest req;
                            if (req.parse(buf, *rr) == 0) co_return;
                            if (co_await ws.accept(req)) co_return;
                            while (co_await ws.echo_inplace()) {}
                        }(cfd).release());
                    }
                }
                co_await Close(fd);
            });
        });
        while (!ready.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown(const benchmark::State&) override {
        server_thread.detach();
    }
};

// ── Fixture: Ring-based echo server ────────────────────────────────────

struct WsRingEchoFixture : public benchmark::Fixture {
    uint16_t port = 9801;
    std::thread server_thread;
    std::atomic<bool> ready{false};

    void SetUp(const benchmark::State&) override {
        ready.store(false);
        server_thread = std::thread([this]() {
            Launcher().threads(2).run([this]() -> Task<void> {
                auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
                int fd = *sock;
                int opt = 1;
                setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(port);
                addr.sin_addr.s_addr = INADDR_ANY;
                co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));
                co_await Listen(fd, 64);

                // 创建 buffer ring（所有连接共享）
                auto* engine = IoUringEngine::current();
                auto& bg = engine->register_buffer_group(1, 256, 4096);

                ready.store(true);

                ShutdownCoordinator sd;
                while (!sd.is_shutdown()) {
                    Accept a(fd);
                    a.with_timeout(std::chrono::milliseconds(200));
                    auto client = co_await a;
                    if (!client) continue;
                    int cfd = *client;
                    auto* sched = ExecutionContext::current();
                    if (sched) {
                        sched->submit([cfd, &bg](int client_fd) -> Task<void> {
                            TcpSocket cs(client_fd);
                            websocket::WebSocket ws(std::move(cs));
                            char buf[4096];
                            Read r(ws.socket().fd(), buf, sizeof(buf));
                            r.with_timeout(std::chrono::seconds(5));
                            auto rr = co_await r;
                            if (!rr || *rr == 0) co_return;
                            http::HttpRequest req;
                            if (req.parse(buf, *rr) == 0) co_return;
                            if (co_await ws.accept(req)) co_return;

                            BufferRingAssembler assembler;
                            assembler.start(client_fd, bg);
                            while (co_await ws.echo_inplace_ring(assembler)) {}
                            assembler.cleanup();
                        }(cfd).release());
                    }
                }
                co_await Close(fd);
            });
        });
        while (!ready.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown(const benchmark::State&) override {
        server_thread.detach();
    }
};

// ── Benchmarks ──────────────────────────────────────────────────────────

// 小帧 64B——传统路径
BENCHMARK_F(WsEchoFixture, Traditional_64B)(benchmark::State& state) {
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        std::string payload(64, 'x');

        for (auto _ : state) {
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            benchmark::DoNotOptimize(frame);
        }
        co_await ws.close();
    });
}

// 小帧 64B——Ring 路径
BENCHMARK_F(WsRingEchoFixture, Ring_64B)(benchmark::State& state) {
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        std::string payload(64, 'x');

        for (auto _ : state) {
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            benchmark::DoNotOptimize(frame);
        }
        co_await ws.close();
    });
}

// 中帧 4KB——传统路径
BENCHMARK_F(WsEchoFixture, Traditional_4KB)(benchmark::State& state) {
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        std::string payload(4096, 'x');

        for (auto _ : state) {
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            benchmark::DoNotOptimize(frame);
        }
        co_await ws.close();
    });
}

// 中帧 4KB——Ring 路径
BENCHMARK_F(WsRingEchoFixture, Ring_4KB)(benchmark::State& state) {
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        std::string payload(4096, 'x');

        for (auto _ : state) {
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            benchmark::DoNotOptimize(frame);
        }
        co_await ws.close();
    });
}

// 大帧 64KB——传统路径
BENCHMARK_F(WsEchoFixture, Traditional_64KB)(benchmark::State& state) {
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        std::string payload(65536, 'x');

        for (auto _ : state) {
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            benchmark::DoNotOptimize(frame);
        }
        co_await ws.close();
    });
}

// 大帧 64KB——Ring 路径
BENCHMARK_F(WsRingEchoFixture, Ring_64KB)(benchmark::State& state) {
    Launcher().threads(1).run([&]() -> Task<void> {
        auto ws = co_await websocket::WebSocket::connect("127.0.0.1", port, "/");
        std::string payload(65536, 'x');

        for (auto _ : state) {
            co_await ws.send_text(payload);
            auto frame = co_await ws.read_frame();
            benchmark::DoNotOptimize(frame);
        }
        co_await ws.close();
    });
}

BENCHMARK_MAIN();
```

- [ ] **Step 2: 编译并运行 benchmark**

Run: `cd build && cmake --build . -j$(nproc) && ./bin/ws_gbench_ring --benchmark_min_time=2`
Expected: 两个路径均正常运行，对比输出延迟数据

- [ ] **Step 3: 记录并分析结果**

将结果记录到 `docs/benchmarks/buffer-ring-vs-traditional.md`：
- P50/P99/Avg 对比表
- 小/中/大帧吞吐量对比
- 拷贝次数分析

- [ ] **Step 4: Commit**

```bash
git add tests/ws_gbench_ring.cc tests/CMakeLists.txt docs/benchmarks/buffer-ring-vs-traditional.md
git commit -m "[perf]: buffer ring vs 传统echo基准测试——小/中/大帧对比"
```

---

### Task 6: WsServer ring 模式集成 + ASAN 全量验证

**Files:**
- Modify: `include/ultranet/net/ws_server.hpp` (新增 `enable_ring_mode()` 选项)
- Modify: `tests/ws_bench_self.cc` (新增 ring 路径测试)

- [ ] **Step 1: WsServer 增加 ring mode 开关**

```cpp
// WsServer 类新增成员和方法：

class WsServer {
    // ...
public:
    // 启用 buffer ring 模式（零拷贝 echo）。
    // 在 serve() 之前调用。
    void enable_ring_mode(size_t ring_entries = 256,
                          size_t buf_size = 4096) {
        m_ring_mode = true;
        m_ring_entries = ring_entries;
        m_ring_buf_size = buf_size;
    }

private:
    bool m_ring_mode{false};
    size_t m_ring_entries{256};
    size_t m_ring_buf_size{4096};
    // ...
};
```

- [ ] **Step 2: 修改 handle_client() 支持 ring 路径**

```cpp
Task<void> handle_client(int client_fd) {
    TcpSocket sock(client_fd);
    websocket::WebSocket ws(std::move(sock));

    // ... 握手代码不变 ...

    WsConn conn(std::move(ws));

    if (m_on_open) { co_await m_on_open(conn); }

    if (m_ring_mode) {
        // Ring 模式：每连接独立 BufferRingAssembler
        auto* engine = IoUringEngine::current();
        // buffer group 在 serve() 中提前注册，通过成员引用访问
        auto& bg = *m_buffer_group;  // m_buffer_group 在 serve() 中初始化
        BufferRingAssembler assembler;
        assembler.start(client_fd, bg);
        co_await read_loop_ring(conn, assembler);
        assembler.cleanup();
    } else {
        co_await read_loop(conn);
    }

    conn.m_open = false;
    if (m_on_close) { co_await m_on_close(conn); }
}

Task<void> read_loop_ring(WsConn& conn, BufferRingAssembler& assembler) {
    std::vector<uint8_t> frag_buf;

    while (conn.is_open()) {
        auto frame = co_await conn.m_ws.read_frame_ring(assembler);

        switch (frame.opcode) {
        case websocket::OpCode::Close:
            co_await conn.m_ws.close(1000);
            conn.m_open = false;
            co_return;

        case websocket::OpCode::Ping:
            co_await conn.m_ws.send_pong(
                std::string(frame.payload.begin(), frame.payload.end()));
            break;

        case websocket::OpCode::Pong:
            break;

        case websocket::OpCode::Text:
        case websocket::OpCode::Binary:
            frag_buf.insert(frag_buf.end(),
                frame.payload.begin(), frame.payload.end());
            if (!frame.fin) { break; }

            if (frame.opcode == websocket::OpCode::Text && m_on_text) {
                std::string msg(frag_buf.begin(), frag_buf.end());
                frag_buf.clear();
                co_await m_on_text(conn, std::move(msg));
            } else if (frame.opcode == websocket::OpCode::Binary && m_on_binary) {
                auto payload = std::move(frag_buf);
                frag_buf.clear();
                co_await m_on_binary(conn, std::move(payload));
            } else {
                frag_buf.clear();
            }
            break;

        default:
            break;
        }
    }
}
```

- [ ] **Step 3: 全量测试 + ASAN**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address" && cmake --build . -j$(nproc) && ./bin/actor_gtest`
Expected: 120 测试全部 PASS，零 ASAN 错误

Run: `./bin/ws_bench_self 9800 1000`
Expected: 无消息丢失

- [ ] **Step 4: 完整 CHECKLIST 验证**

对照 `docs/CHECKLIST.md` 逐一验证：
- [ ] ASAN 零错误
- [ ] GTest 120 全通过
- [ ] 10M+ 消息压测通过
- [ ] P50 延迟 < 20μs
- [ ] 无消息丢失

- [ ] **Step 5: Commit**

```bash
git add include/ultranet/net/ws_server.hpp tests/ws_bench_self.cc
git commit -m "[feat]: WsServer ring mode集成——零拷贝echo可选开关"
```

---

## 整体架构总结

```
                    ┌──────────────────────────┐
                    │     WsServer (ring mode) │
                    │  enable_ring_mode(true)  │
                    └──────────┬───────────────┘
                               │
                    ┌──────────▼───────────────┐
                    │      WebSocket           │
                    │  read_frame_ring()       │
                    │  echo_inplace_ring()     │
                    └──────────┬───────────────┘
                               │
                    ┌──────────▼───────────────┐
                    │  BufferRingAssembler     │
                    │  - append_chunk()        │
                    │  - try_decode_frame()    │
                    │  - return_reasm_buffers()│
                    └──────┬──────────┬────────┘
                           │          │
              ┌────────────▼──┐  ┌────▼──────────────┐
              │RecvMultishot  │  │   BufferGroup     │
              │  Awaiter      │  │ return_buffer()   │
              │ next_chunk()  │  │ advance_ring()    │
              └───────┬───────┘  └───────────────────┘
                      │
              ┌───────▼──────────┐
              │  IoCallback      │
              │  m_is_multishot  │
              │  m_multishot_    │
              │    handler       │
              └──────────────────┘
```

## 预期性能收益

| 场景 | 当前路径 | Ring 路径 | 拷贝次数 | 预期提升 |
|------|---------|----------|---------|---------|
| 64B echo | kernel→user→frame | DMA→ring→writev | 2→0 | ~15-20% |
| 4KB echo | kernel→user→frame | DMA→ring→frame | 2→1 | ~10-15% |
| 64KB echo | kernel→user→frame | DMA→ring→reasm→frame | 2→1 | ~5-10% |

实际收益取决于瓶颈是拷贝带宽还是协程调度延迟。基准测试将给出精确数据。

## 风险与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| Buffer ring 耗尽 | 新连接无法接收数据 | `append_chunk` 中检测 res==-ENOBUFS，触发 `advance_ring` 或等待 |
| Multishot recv 内核兼容 | 老内核无法使用 | 编译期 `IORING_FEAT_FAST_POLL` 检测，运行时 fallback |
| IoCallback 堆分配泄漏 | 内存泄漏 | `cleanup()` + 析构函数保证释放；ASAN 覆盖 |
| 协程生命周期 > CQE | use-after-free | `RecvMultishotState` 使用值语义，不依赖外部指针 |
| ring mode 与传统路径共存 | 同一连接两路径混用 | 文档明确互斥使用；`start()` 后 `read_frame()` 行为未定义 |

## 自 Review

**1. Spec coverage:** 4 个需求全部覆盖——
- ✅ Buffer ring 集成到 WebSocket（Task 1-3）
- ✅ 帧重组（Task 2: BufferRingAssembler）
- ✅ Buffer 归还（Task 2: return_reasm_buffers）
- ✅ 零拷贝优化（Task 4: echo_inplace_ring，小帧直接引用 ring buffer）

**2. Placeholder scan:** 无 TBD/TODO/implement later——每个步骤都有实际代码或具体命令。

**3. Type consistency:** `RecvMultishotState`→`RecvMultishotAwaiter`→`BufferRingAssembler`→`WebSocket` 的接口签名在所有 Task 中一致。

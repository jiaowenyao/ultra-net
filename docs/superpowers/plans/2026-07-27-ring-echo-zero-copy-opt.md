# Ring Echo 零拷贝优化计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 消除 ring echo 路径的两次用户态拷贝，实现单 buffer 帧的真正零拷贝 echo，将 P50 延迟从 221μs 降至 ≤174μs（与传统路径持平或更优）。

**Architecture:** 在 `echo_inplace_ring()` 内新增单 buffer 快速路径——直接从 ring buffer 解析帧头、原地解掩码、writev 写回。跳过 `append_chunk`（ring→m_reasm 拷贝）和 `WebSocketFrame::decode`（m_reasm→payload 拷贝）。多 buffer 帧保持现有 assembler 慢路径。

**Tech Stack:** C++20 coroutines, io_uring, writev, WebSocket RFC 6455 frame format

## 全局约束

- 注释中文，禁止压行（if/for/while 必须大括号 + 换行）
- 所有改动必须通过 ASAN 零错误 + 152 个已有测试
- `ws_ring_echo_self` 端到端验证 1000/1000 无消息丢失
- 多 buffer 帧路径保持不变（向后兼容）
- 内核 < 5.19 时编译期 fallback 到传统路径（已有）

---

### Task 1: write_frame_raw — 绕过 WebSocketFrame 的直接写帧方法

**Files:**
- Modify: `include/ultranet/net/websocket.hpp`（在 `WebSocket` 类中新增 `write_frame_raw`）

**Interfaces:**
- Produces: `Task<void> write_frame_raw(OpCode opcode, bool fin, const uint8_t* payload, size_t payload_len)` — 直接从裸指针写 WebSocket 帧，无需构造 `WebSocketFrame` 对象
- Consumes: `io::Writev`（已有），栈帧头编码逻辑（从 `write_frame` 中提取）

**职责：** 提供一条不经过 `WebSocketFrame` 对象分配 + `std::string` payload 拷贝的写帧路径。帧头直接在栈上编码（≤14 字节），payload 通过 writev 的 iovec 引用外部内存。

- [ ] **Step 1: 从 write_frame 中提取帧头编码逻辑为私有辅助方法**

当前 `write_frame()` 中的帧头编码逻辑（397-429 行）可直接复用。新增 `write_frame_raw` 方法：

```cpp
// 零拷贝写帧——帧头栈编码 + payload 原位引用，不经过 WebSocketFrame/std::string。
// payload 指向的内存必须在 writev 完成前保持有效。
Task<void> write_frame_raw(OpCode opcode, bool fin,
                           const uint8_t* payload, size_t payload_len) {
    // 栈编码帧头（最多 14 字节：2B base + 8B 扩展长度 + 4B mask key）
    uint8_t hdr[14];
    uint8_t* p = hdr;
    *p++ = static_cast<uint8_t>((fin ? 0x80 : 0x00) |
                                 (static_cast<uint8_t>(opcode) & 0x0F));
    if (payload_len < 126) {
        *p++ = static_cast<uint8_t>(payload_len | (m_masked ? 0x80 : 0x00));
    } else if (payload_len <= 65535) {
        *p++ = static_cast<uint8_t>(126 | (m_masked ? 0x80 : 0x00));
        *p++ = static_cast<uint8_t>(payload_len >> 8);
        *p++ = static_cast<uint8_t>(payload_len);
    } else {
        *p++ = static_cast<uint8_t>(127 | (m_masked ? 0x80 : 0x00));
        for (int i = 7; i >= 0; --i) {
            *p++ = static_cast<uint8_t>(payload_len >> (i * 8));
        }
    }
    if (m_masked) {
        std::random_device rd;
        uint32_t mk = static_cast<uint32_t>(rd()) ^
                      (static_cast<uint32_t>(rd()) << 16);
        *p++ = static_cast<uint8_t>(mk >> 24);
        *p++ = static_cast<uint8_t>(mk >> 16);
        *p++ = static_cast<uint8_t>(mk >> 8);
        *p++ = static_cast<uint8_t>(mk);
    }
    size_t hdr_len = static_cast<size_t>(p - hdr);

    iovec iov[2];
    iov[0].iov_base = hdr;
    iov[0].iov_len = hdr_len;
    iov[1].iov_base = const_cast<uint8_t*>(payload);
    iov[1].iov_len = payload_len;

    auto w = co_await io::Writev(m_socket.fd(), iov, 2);
    if (!w) {
        throw std::system_error(w.error(), "websocket write failed");
    }
}
```

注意：服务端 `m_masked` 为 `false`（`ws.accept()` 中设置），所以 mask 分支实际不执行。这意味着 `write_frame_raw` 对于服务端只写 2-10 字节帧头 + 直接引用 payload。

- [ ] **Step 2: 编译验证**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="" && cmake --build . --target actor_gtest -j$(nproc)`
Expected: 编译成功

- [ ] **Step 3: 运行已有测试确认无回归**

Run: `./bin/actor_gtest`
Expected: 120 tests PASS

- [ ] **Step 4: Commit**

```bash
git add include/ultranet/net/websocket.hpp
git commit -m "[perf]: write_frame_raw——绕过WebSocketFrame的栈编码零拷贝写帧"
```

---

### Task 2: echo_inplace_ring 单 buffer 快速路径

**Files:**
- Modify: `include/ultranet/net/websocket.hpp`（重写 `echo_inplace_ring` 方法）
- Modify: `include/ultranet/net/buffer_ring_assembler.hpp`（新增 `get_buffer_group()` 方法）

**Interfaces:**
- Consumes: `BufferRingAssembler::next_chunk()`, `BufferGroup::get_buffer()`, `write_frame_raw()`
- Produces: 修改后的 `echo_inplace_ring()` —— 快速路径在单 buffer 帧时直接解析+写回

**职责：** 在 `echo_inplace_ring()` 中增加快速路径检测。若当前 chunk 包含完整帧（帧头可解析 + payload 不超出 chunk 边界），则：
1. 直接从 ring buffer 解析帧头（opcode, fin, mask, payload_offset, payload_len）
2. 原地解掩码（xor ring buffer 中的 payload 区域）
3. 调用 `write_frame_raw(opcode, fin, ring_ptr + payload_offset, payload_len)` 写回
4. 归还 buffer 到 ring
5. 跳过 m_reasm 拷贝 + WebSocketFrame 对象创建

若帧跨 buffer 边界，fallback 到现有的 assembler 慢路径。

- [ ] **Step 1: 在 BufferRingAssembler 中新增 buffer 访问方法**

当前 `append_chunk` 内部调用 `m_bg->get_buffer(bid)` 获取 buffer 指针，但该指针没有对外暴露。新增一个 getter：

```cpp
// 在 BufferRingAssembler class 中：
io::BufferGroup* buffer_group() const { return m_bg; }
```

- [ ] **Step 2: 重写 echo_inplace_ring 增加快速路径**

核心逻辑：收到 chunk 后，先尝试在 ring buffer 上直接解析帧头。若帧完整且不跨 buffer，走快速路径；否则走慢路径。

```cpp
Task<bool> echo_inplace_ring(BufferRingAssembler& assembler) {
    // 慢路径 lambda：走原有 assembler 重组 + decode 流程
    auto slow_path = [&]() -> Task<std::optional<WebSocketFrame>> {
        auto& buf = assembler.reasm_buffer();
        if (buf.empty()) { co_return std::nullopt; }
        size_t consumed = 0;
        WebSocketFrame frame;
        if (WebSocketFrame::decode(buf.data(), buf.size(), consumed, frame)) {
            assembler.consume_bytes(consumed);
            assembler.return_reasm_buffers();
            co_return frame;
        }
        co_return std::nullopt;
    };

    while (true) {
        // 慢路径：重组缓冲区有残留时先解码
        if (assembler.has_remaining()) {
            auto frame = co_await slow_path();
            if (frame) {
                if (frame->opcode == OpCode::Ping) {
                    co_await write_frame(WebSocketFrame::pong(frame->payload));
                    co_return true;
                }
                if (frame->opcode == OpCode::Close) {
                    co_await write_frame(WebSocketFrame::close());
                    m_closed = true;
                    co_return false;
                }
                co_await write_frame(*frame);
                co_return true;
            }
        }

        // 等待下一个 ring buffer 数据块
        auto chunk = co_await assembler.next_chunk();
        if (chunk.is_eof() || chunk.is_error()) {
            co_return false;
        }

        // ═══ 快速路径：尝试直接从 ring buffer 解析帧 ═══
        auto* bg = assembler.buffer_group();
        if (bg && chunk.res >= 2) {
            auto* raw = static_cast<const uint8_t*>(
                bg->get_buffer(chunk.buffer_id()));
            size_t len = static_cast<size_t>(chunk.res);

            // 解析帧头
            bool fin = (raw[0] & 0x80) != 0;
            auto opcode = static_cast<OpCode>(raw[0] & 0x0F);
            bool masked = (raw[1] & 0x80) != 0;
            uint64_t plen = raw[1] & 0x7F;
            size_t pos = 2;

            // 扩展长度
            if (plen == 126) {
                if (len < 4) { goto slow_path_label; }
                plen = (static_cast<uint64_t>(raw[2]) << 8) | raw[3];
                pos = 4;
            } else if (plen == 127) {
                if (len < 10) { goto slow_path_label; }
                plen = 0;
                for (int i = 0; i < 8; ++i) {
                    plen = (plen << 8) | raw[2 + i];
                }
                pos = 10;
            }

            // 掩码键
            uint32_t mask_key = 0;
            if (masked) {
                if (len < pos + 4) { goto slow_path_label; }
                mask_key = (static_cast<uint32_t>(raw[pos]) << 24)
                         | (static_cast<uint32_t>(raw[pos + 1]) << 16)
                         | (static_cast<uint32_t>(raw[pos + 2]) << 8)
                         | raw[pos + 3];
                pos += 4;
            }

            // 检查 payload 是否完整在此 buffer 内
            if (pos + plen > len) {
                // 帧跨 buffer——走慢路径
                goto slow_path_label;
            }

            // ── 快速路径核心：零拷贝 echo ──────────────────────────
            const uint8_t* payload_ptr = raw + pos;

            // 处理控制帧
            if (opcode == OpCode::Ping) {
                // Ping→Pong，需要拷贝 payload（小数据，可接受）
                std::string ping_data(
                    reinterpret_cast<const char*>(payload_ptr), plen);
                if (masked) {
                    for (size_t i = 0; i < plen; ++i) {
                        ping_data[i] ^= static_cast<char>(
                            (mask_key >> (8 * (3 - (i % 4)))) & 0xFF);
                    }
                }
                co_await write_frame(WebSocketFrame::pong(ping_data));
                bg->return_buffer(chunk.buffer_id());
                bg->advance_ring(1);
                co_return true;
            }

            if (opcode == OpCode::Close) {
                co_await write_frame(WebSocketFrame::close());
                m_closed = true;
                bg->return_buffer(chunk.buffer_id());
                bg->advance_ring(1);
                co_return false;
            }

            // Text/Binary：写回（需要先解掩码）
            if (masked && plen > 0) {
                // 原地解掩码——ring buffer 数据在 return 前不会被内核触碰
                auto* mutable_payload = const_cast<uint8_t*>(payload_ptr);
                for (size_t i = 0; i < plen; ++i) {
                    mutable_payload[i] ^= static_cast<uint8_t>(
                        (mask_key >> (8 * (3 - (i % 4)))) & 0xFF);
                }
            }

            // writev 直接引用 ring buffer 中的 payload（零拷贝）
            co_await write_frame_raw(opcode, fin, payload_ptr, plen);

            // 归还 buffer
            bg->return_buffer(chunk.buffer_id());
            bg->advance_ring(1);
            co_return true;
        }

    slow_path_label:
        // ── 慢路径：帧跨 buffer，走 assembler 重组 ────────────────
        assembler.append_chunk(chunk);
        auto frame = co_await slow_path();
        if (frame) {
            if (frame->opcode == OpCode::Ping) {
                co_await write_frame(WebSocketFrame::pong(frame->payload));
                co_return true;
            }
            if (frame->opcode == OpCode::Close) {
                co_await write_frame(WebSocketFrame::close());
                m_closed = true;
                co_return false;
            }
            co_await write_frame(*frame);
            co_return true;
        }
        // 帧不完整，继续等待更多 chunk
    }
}
```

**注意：** `goto` 用于快速路径失败时跳转到慢路径。在 C++ 中 `goto` 跨越 `co_await` 是非法的，但这里 `goto` 的目标在同一个 `while` 迭代内、`co_await` 之前，所以是合法的。

- [ ] **Step 3: 编译并运行已有测试**

Run: `cd build && cmake --build . --target actor_gtest ws_ring_echo_self -j$(nproc)`
Expected: 编译成功

Run: `./bin/actor_gtest`
Expected: 120 tests PASS

- [ ] **Step 4: 端到端 echo 验证**

Run: `./bin/ws_ring_echo_self 9802 1000`
Expected: 1000/1000 messages, no loss

- [ ] **Step 5: 性能对比**

Run: `./bin/ws_bench_self 9800 1000` (传统) vs `./bin/ws_ring_echo_self 9802 1000` (ring fast path)
Expected: Ring P50 ≤ 传统 P50

- [ ] **Step 6: ASAN 验证**

Run: `cmake .. -DCMAKE_CXX_FLAGS="-fsanitize=address" && cmake --build . --target actor_gtest ws_ring_echo_self -j$(nproc) && ASAN_OPTIONS=detect_leaks=0 ./bin/actor_gtest`
Expected: 零 ASAN 错误

- [ ] **Step 7: Commit**

```bash
git add include/ultranet/net/websocket.hpp include/ultranet/net/buffer_ring_assembler.hpp
git commit -m "[perf]: echo_inplace_ring单buffer快速路径——零拷贝ring echo"
```

---

### Task 3: 批量 buffer 回收优化（P1）

**Files:**
- Modify: `include/ultranet/net/websocket.hpp`（快速路径已直接调用 `return_buffer`，无需此优化针对快速路径）

**分析：** 快速路径中每帧只有一个 buffer（单 buffer 帧），`return_buffer(bid)` + `advance_ring(1)` 开销极小。批量回收主要受益于多 buffer 帧的慢路径，但慢路径是低频路径。此 Task 的投入产出比不高，**跳过**——如果后续 benchmark 显示多 buffer 帧瓶颈再回来做。

---

### Task 4: Release 模式性能终验

**职责：** Release 编译下跑完整性能对比，确认优化效果。

- [ ] **Step 1: Release 编译**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && cmake --build . --target ws_bench_self ws_ring_echo_self -j$(nproc)`

- [ ] **Step 2: 传统 vs Ring 对比 (1000 msg, 64B)**

Run: `./bin/ws_bench_self 9800 1000` 和 `./bin/ws_ring_echo_self 9802 1000`
Expected: Ring P50 ≤ 传统 P50（174μs）

- [ ] **Step 3: 大帧对比 (4KB)**

手工测试或增加 benchmark 参数支持。

- [ ] **Step 4: 记录结果到 docs/benchmarks/**

```bash
git add docs/benchmarks/ring-optimization-results.md
git commit -m "[docs]: ring echo零拷贝优化性能结果"
```

---

## 架构总结

```
echo_inplace_ring()
    │
    ├─ 快速路径（单 buffer 帧，如 64B echo）
    │   ├─ 直接从 ring buffer 解析帧头（无拷贝）
    │   ├─ 原地解掩码（xor ring buffer payload）
    │   ├─ write_frame_raw(opcode, fin, ring_ptr, len)
    │   │   └─ writev(栈帧头, ring_buffer_payload)  ← 零拷贝
    │   └─ return_buffer + advance_ring
    │
    └─ 慢路径（多 buffer 帧，如 >4KB payload）
        ├─ append_chunk → m_reasm（拷贝 1）
        ├─ WebSocketFrame::decode → frame.payload（拷贝 2）
        └─ write_frame → writev(栈帧头, payload.data())
```

### 预期性能提升

| 场景 | 当前 Ring | 优化后 Ring | 拷贝次数 | 预期 P50 |
|------|----------|------------|---------|----------|
| 64B echo | 2 copies + ring mgmt | 0 copies (fast path) | 2→0 | ≤174μs |
| 4KB echo | 2 copies + ring mgmt | 0 copies (fast path) | 2→0 | 显著改善 |
| 64KB echo | 2 copies + ring mgmt | 2 copies (slow path) | 无变化 | 持平 |

## 自 Review

**1. Spec coverage:** 覆盖了核心优化目标——消除小帧拷贝。大帧慢路径保持不变。

**2. Placeholder scan:** 无 TBD/TODO。

**3. Type consistency:** `write_frame_raw(OpCode, bool, const uint8_t*, size_t)` 签名在 Task 1 定义、Task 2 使用，一致。

**4. Risk assessment:**
- `goto` 跨 `co_await`：已验证——goto 目标在同一个 while 迭代的 co_await 之前，合法
- 原地解掩码修改 ring buffer：ring buffer 数据在 CQE 之后、return_buffer 之前属于用户，修改安全
- 快速路径帧头解析与 `WebSocketFrame::decode` 一致性：逻辑直接复制自 decode 实现，保持同步

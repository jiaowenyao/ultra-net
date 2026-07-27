# Multishot Accept + Buffer Ring Recv 集成计划

> 参考: liburing 2.14 官方 proxy.c 示例 (2528行), BufferGroup 已有 buffer ring 注册

## 现状

- `BufferGroup` (buffer.h) 已实现 `io_uring_register_buf_ring()`, 提供 buffer ring 注册
- `IoCallback` (io_callback.hpp) 已支持 `m_is_multishot` + `m_multishot_handler`
- 缺少: multishot recv 的 buffer 回收机制 (内核取走buffer → 用户处理 → 归还到ring)

## 架构

```
┌─ 注册 Buffer Ring (N × 4KB, bgid=1) ─┐
│  io_uring_register_buf_ring()          │
└────────────────────────────────────────┘
         ↓
┌─ 提交 multishot recv SQE ──────────────┐
│  io_uring_prep_recv_multishot(fd)      │
│  IOSQE_BUFFER_SELECT | bgid=1          │
└────────────────────────────────────────┘
         ↓ (kernel fills buffers)
┌─ CQE 回调 ─────────────────────────────┐
│  buffer_id = cqe->flags >> 16          │
│  data = buffer_group.get_buffer(bid)   │
│  → echo/send the data                  │
│  → buf_ring_add(bid) // 归还buffer     │
│  → buf_ring_advance(1)                 │
└────────────────────────────────────────┘
```

## 实施任务

### Task 1: BufferGroup 增加回收方法
- `return_buffer(bid)` — 将 buffer 归还到 ring
- `advance(count)` — 通知内核新增可用 buffer

### Task 2: Multishot Accept 集成到 Accept IoOperation
- Accept 增加 multishot 模式: 一次 submit, 多次 CQE
- 每个 CQE 的 fd 提交给 handler 协程

### Task 3: Multishot Recv WebSocket echo 服务端
- 使用 buffer ring + multishot recv 实现零拷贝 echo
- Google Benchmark 对比传统 read+write vs multishot

## 自Review
- ✅ buffer ring 注册已由 BufferGroup 实现
- ✅ IoCallback multishot 框架已就绪
- ✅ 参考官方 proxy.c (2528行) 验证方案可行性
- ⚠️ multishot recv 需要 WebSocket 帧解码适配 buffer ring 的固定大小 buffer

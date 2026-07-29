# Ring 多 Segment 零拷贝优化

## 证据

uWS 源码 (`src/WebSocket.h:send`, `src/WebSocketProtocol.h:consume`):
- 接收: 直接在 recv buffer 上解析帧头，零拷贝
- 发送: `us_socket_write2(header, payload_ptr)` — 栈帧头 + payload 指针，零拷贝
- 大消息路径 (>16KB): 绕过内部缓冲，直接 send()，接近裸 TCP 性能

ultra-net 当前 ring 慢路径: 2 次 64KB 拷贝 + 16 次额外 CQE 往返 → 这是 4.3× 差距的根因。

## 方案

修改 `echo_inplace_ring` 慢路径：不拷贝 chunk 到 m_reasm，保持 segment 列表，用多 iovec writev 直接发送。

修改 `write_frame_raw` 接受多 iovec payload。

## 实现

### 改动文件
- `include/ultranet/net/websocket.hpp`: 新增 `write_frame_raw_multi()`，重写 `echo_inplace_ring()` 慢路径
- `include/ultranet/net/buffer_ring_assembler.hpp`: 新增 segment 跟踪方法

### 目标
- 16 连接 × 64KB P50: 182μs → <50μs (追赶 uWS 的 42μs)
- 4 连接 × 4KB P50: 44μs → <24μs (反超 uWS)

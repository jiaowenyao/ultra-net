# Echo路径优化计划 (自Review通过)

> **Goal:** 减少WS echo路径中不必要的payload拷贝和协程往返，缩小大帧性能差距。

## 三方案评估

### 方案A: WebSocket::echo_inplace() — ✅ 采纳
**实现:** 在WebSocket类增加 `Task<void> echo_inplace()` 方法，内部读帧→直接写回，不返回frame给调用方。
**收益:** 消除decode拷贝(64KB ~5μs) + 减少write_frame协程创建(复用read_frame的协程上下文)
**风险:** 低——仅增加新方法，不改变现有API
**影响:** WsServer::read_loop从 `frame=read→write(frame)` 改为 `echo_inplace()`

### 方案B: write_frame(const char*, size_t) — ❌ 不采纳
**理由:** 不改API表面就能让read_frame内部直接传buffer给write，但无法解决"buffer生命周期"问题——read_frame的buffer在返回时销毁，调用方无法持有引用。

### 方案C: payload改vector<uint8_t> — ❌ 不采纳
**理由:** send_text用std::string→转vector需拷贝，用户API受影响。仅服务端read路径受益，客户端send路径反而变慢。

## 实施计划

### Task 1: WebSocket::echo_inplace()
**Files:** Modify `include/ultranet/net/websocket.hpp`
**实现:** 新增方法，读帧→直接写回，frame.payload不离开read_frame的协程上下文

### Task 2: WsServer使用echo_inplace
**Files:** Modify `include/ultranet/net/ws_server.hpp`
**实现:** read_loop中替换为echo_inplace()

### Task 3: Google Benchmark验证
**Files:** Modify `tests/ws_gbench_full.cc`
**实现:** 增加echo_inplace路径的benchmark对比

## 自Review结果
- ✅ 不改变现有public API
- ✅ 仅新增方法，向后兼容
- ✅ echo_inplace在read_frame协程帧内完成，buffer生命周期安全
- ✅ writev零拷贝仍然生效（echo_inplace调write_frame）
- ✅ 120 GTest必须继续通过

---

# CLAUDE.md — ultra-net 项目约定与开发规范

## 代码风格

- **注释必须使用中文**（专业术语如 CAS、mailbox、gossip、CRTP、io_uring 等保留英文）
- **禁止压行**：所有 `if`/`for`/`while` 必须使用大括号并换行，即使只有一行语句
  ```cpp
  // ❌ 错误
  if (cond) return;
  if (cond) { return; }

  // ✅ 正确
  if (cond) {
      return;
  }
  ```
- 缩进使用 4 空格
- 命名空间：`ynet::actor`（actor 框架）、`ynet::async`（协程运行时）

## 设计哲学

- **零成本抽象**：编译期 `if constexpr` 自动选择最优路径，不使用的特性零开销
- **Header-only**：纯头文件库，`add_library(ultranet INTERFACE)`
- **侵入式扩展**：序列化通过消息 struct 上的 `serialize()/deserialize()` 方法
- **不做过度设计**：删除死代码（metrics/trace），简化枚举（ShutdownPhase→atomic<bool>），移除冗余抽象（IoUringEngine::Scope）
- **协程生命周期所有权**：`Task<T>` 为唯一所有者，`UnifiedTask` 只执行不销毁，`TaskFinalAwaiter` 无父者自毁

## 消息类型规范

- 默认路径：trivially copyable → `memcpy` 快路径（P50=1μs）
- 自定义序列化：提供 `serialize()/deserialize()` → 编译期自动选择
- `static_assert(serializable_msg<Msg> || is_trivially_copyable_v<Msg>)` 编译期拦截
- `std::string`、`std::vector` 等禁止直接用作消息字段，使用 `char[N]` 替代或自定义序列化

## Git 规范

- Commit 格式：`[type]: 简短中文描述`（30 字以内）
- Type：`feat`/`fix`/`refactor`/`style`/`docs`/`chore`/`perf`/`test`
- 配置：`git config user.email "jiaowenyao163@163.com"` / `git config user.name "jiaowenyao"`
- `.gitignore` 包含 AI 辅助文件夹：`.claude/`、`.codebase-memory/`、`.cursor/`、`.copilot/`

## 测试规范

- 内存安全：必须通过 ASAN（`-fsanitize=address`）验证，零 use-after-free / double-free / leak
- 框架：优先使用 GTest（`actor_gtest.cc`），旧测试逐步迁移
- 压测标准：10M+ 消息吞吐量、5 分钟持久化、RSS 内存监控、checksum 校验
- 性能基线：单 Actor 吞吐 >200K msg/s、P50 延迟 <20μs、无消息丢失

## 禁止事项

## 问题排查规范

**必须先定位真正的根因，持有证据后才能开始修复。** 禁止猜测性修改。

排查流程：
1. **复现** — 稳定复现问题，明确触发条件
2. **取证** — 使用 ASAN（`-fsanitize=address`）获取精确的堆栈追踪，定位"分配点"和"释放点"
3. **分析** — 根据证据分析数据流，找到真正的根因，而非表象
4. **修复** — 针对根因做最小化修复，不引入新抽象或标记位（flag）
5. **验证** — 确认修复后 ASAN 零错误、全量测试通过

禁止行为：
- ❌ 猜测根因而未取证直接修改代码
- ❌ 加标记位/flag 绕过问题（会累积技术债）
- ❌ 修改多处同时"尝试修好"（无法确认哪处是真正修复）
- ❌ 修复后未跑 ASAN 验证就声称修好了

反例教训：协程 double-free 问题——最初猜测是 `WsServer::Close` 重复关闭 fd，修改后问题依旧；实际根因是 `UnifiedTask` 和 `~Task()` 双重销毁协程帧，通过 ASAN 堆栈才精确定位。

## 禁止事项

- 禁止 `std::cout`/`std::cerr`（使用 `ULTRA_LOG_*` 宏）
- 禁止裸 owning 指针（使用 `std::unique_ptr`）
- 禁止过度抽象（新增类/层需明确收益）
- 禁止未经 ASAN 验证的协程生命周期修改
- 禁止在协程中使用 `ASSERT_*`（GTest 宏使用 `return` 非法），改用 `EXPECT_*` + `co_return`

## 关键文件

| 文件 | 职责 |
|------|------|
| `coroutine/task.hpp` | Task/Promise/co_await/生命周期 |
| `coroutine/thread_pool.hpp` | WorkStealingThreadPool + io_uring CQE |
| `coroutine/unified_task.hpp` | 协程/回调统一任务包装 |
| `actor/core/base_actor.h` | Actor 基类 + mailbox 调度 |
| `actor/core/actor_ref.h` | actor_ref + local_actor_proxy |
| `actor/system/actor_system.h` | actor_system + spawn/find/gossip |
| `net/ws_server.hpp` | 开箱即用 WebSocket 服务器 |
| `lifecycle/shutdown.hpp` | ShutdownCoordinator |

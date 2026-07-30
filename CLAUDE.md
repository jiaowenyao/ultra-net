# CLAUDE.md — ultra-net 项目约定与开发规范

## Skills 触发规则

以下场景**必须**先调用对应 skill，再开始工作：

| 场景 | 必须调用的 Skill | 触发条件 |
|------|-----------------|---------|
| 修复 Bug / 排查崩溃 | `systematic-debugging` | 出现 crash、double-free、use-after-free、超时 |
| 声称"修好了/完成了" | `verification-before-completion` | 提交前、声称通过前——必须跑测试并展示证据 |
| 多步骤实现（>3 步） | `writing-plans` | 涉及 ≥3 个文件 或 ≥2 个模块的改动 |
| 按计划逐步实现 | `executing-plans` | 已有书面计划，分检查点执行 |
| 新增功能/特性 | `test-driven-development` | 任何新增 API、类、模块、优化 |
| 设计讨论/创意工作 | `brainstorming` | 架构决策、API 设计、优化方向 |
| 代码审查（提交前自查） | `requesting-code-review` | 完成一个阶段后自我审查 |
| 启动新对话 | `using-superpowers` | 新会话开始，建立 skills 上下文 |
| 多独立任务并行 | `dispatching-parallel-agents` | ≥2 个互不依赖的任务同时推进 |
| 架构讨论/术语对齐 | `grill-with-docs` | 设计模式、术语、抽象层次讨论 |

**原则**：宁可多调用一次 skill，不要跳过流程。skill 是流程保障，不是负担。

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
- **禁止在实际运行代码中使用假数据/硬编码 mock 值**——测试和真实运行必须用真实数据，否则无法发现问题
- **禁止未经用户明确要求进行 git commit/push**——用户主动要求后才能提交，防止假修复污染仓库

## 性能优化工作流

每次性能优化遵循三层基准对比法：

```
1. 理论极限 (raw io_uring, 无协程)  →  设定天花板
2. 协程路径 (当前 ultra-net)        →  量化协程开销
3. 对比差异                         →  定位瓶颈层
```

优化优先级：
- **P0**：消除 io_uring syscall 延迟（`submit_now()` 立即刷新，消除 `wait_for_events` 等待）
- **P1**：协程帧池化（线程局部复用，减少 malloc/free。注意：必须全量重编避免 ABI 不一致）
- **P2**：零拷贝路径（栈编码 + writev，已部分实现）
- **P3**：多核扩展（SO_REUSEPORT + 独立 io_uring ring）

每次优化后必跑 `tests/CHECKLIST.md` 全部项目。

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

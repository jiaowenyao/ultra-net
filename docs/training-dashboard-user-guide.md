# Training Dashboard — 用户手册

> 面向使用者。如需开发文档，参见 [架构与开发指南](training-dashboard-architecture.md)。

## 目录

1. [快速开始](#1-快速开始)
2. [界面布局](#2-界面布局)
3. [连接 PS 节点](#3-连接-ps-节点)
4. [面板详解](#4-面板详解)
   - [节点拓扑图](#41-节点拓扑图)
   - [训练进度面板](#42-训练进度面板)
   - [Actor 监控表](#43-actor-监控表)
   - [网络流量图](#44-网络流量图)
   - [告警面板](#45-告警面板)
5. [告警规则说明](#5-告警规则说明)
6. [命令行参考](#6-命令行参考)
7. [WSL2 参数限制](#7-wsl2-参数限制)
8. [故障排除](#8-故障排除)

---

## 1. 快速开始

### 启动 PS 节点（含 Metrics 服务）

```bash
# 方式 1: tcp 模式 — PS + Workers 同进程（开发/测试用）
./bin/dist-bench tcp <workers> <params> <steps> <ps_port> <metrics_port>

# 推荐参数（WSL2 验证通过）:
./bin/dist-bench tcp 2 500 20 18001 18080
#                    │  │    │   │     └── metrics HTTP 端口
#                    │  │    │   └── PS TCP 端口
#                    │  │    └── 每个 Worker 的训练步数
#                    │  └── 参数量（浮点数个数）
#                    └── Worker 数量

# 方式 2: ps 模式 — 仅 PS（生产用，Worker 另启进程）
./bin/dist-bench ps 18001 2 500 20 18080
```

启动后应有输出：
```
[metrics] exporter listening on :18080
=== TCP Loopback ===
  workers:    2
  steps:      40
  checksum:   0x5bc55510
  ...
[metrics] exporter stopped
```

> **注意**: 如果看到 `bind failed: Address already in use`，说明端口被占用（通常是之前 GDB 调试或崩溃残留的进程）。换一个端口号即可。

### 启动 Dashboard

```bash
# Linux / WSL2
DISPLAY=:0 ./bin/training-dashboard http://127.0.0.1:18080

# 或先启动后手动连接
DISPLAY=:0 ./bin/training-dashboard
# 然后在界面地址栏输入 http://127.0.0.1:18080，点击 Connect
```

### 验证连通性

```bash
# 在另一个终端验证 PS metrics 端点
curl http://127.0.0.1:18080/health
# → {"status":"ok"}

curl http://127.0.0.1:18080/api/v1/training
# → {"timestamp_ms":..., "total_steps":40, "checksum":"0x5bc55510", ...}
```

---

## 2. 界面布局

```
┌──────────────────────────────────────────────────────────────────┐
│  Ultra-Net Training Dashboard                         ● Connected │
├──────────────────────────┬───────────────────────────────────────┤
│                          │                                       │
│  ┌────────────────────┐  │  ┌─────────────────────────────────┐  │
│  │ http://127.0.0.1   │  │  │  THROUGHPUT     │ STEPS         │  │
│  │ [Connect][Disconn] │  │  │  7.6 MB/s       │ 40            │  │
│  └────────────────────┘  │  │  P50 LATENCY    │ P99 LATENCY   │  │
│                          │  │  0.029 ms       │ 0.043 ms      │  │
│  ┌────────────────────┐  │  │  NODES          │ CHECKSUM      │  │
│  │  NETWORK TOPOLOGY  │  │  │  3 / 3          │ 0x5bc55510   │  │
│  │                    │  │  └─────────────────────────────────┘  │
│  │     [W0]           │  │                                       │
│  │      │             │  │  ┌─────────────────────────────────┐  │
│  │   ───[PS]───       │  │  │  TRAINING PROGRESS              │  │
│  │      │             │  │  │  ╱╲    ╱╲    ╱╲                 │  │
│  │     [W1]           │  │  │ ╱  ╲──╱  ╲──╱  ╲──              │  │
│  │                    │  │  │  TP: 7.6 MB/s | P50:0.03 ms     │  │
│  └────────────────────┘  │  └─────────────────────────────────┘  │
│                          │                                       │
│  ┌────────────────────┐  │  ┌─────────────────────────────────┐  │
│  │  ACTOR MONITOR     │  │  │  NETWORK TRAFFIC                │  │
│  │  Name   Queue Msg/s│  │  │  Send: 12.5 MB/s                │  │
│  │  PS     0     100  │  │  │  Recv: 250.0 MB/s               │  │
│  │  W0     3      50  │  │  │  Conns: 4                       │  │
│  │  W1     2      45  │  │  │  ████████████████████            │  │
│  └────────────────────┘  │  └─────────────────────────────────┘  │
│                          │                                       │
│  ┌────────────────────┐  │                                       │
│  │  ALERTS            │  │                                       │
│  │  ✓ No active alerts│  │                                       │
│  └────────────────────┘  │                                       │
├──────────────────────────┴───────────────────────────────────────┤
│  Status: Connected — polling metrics at 1 Hz                     │
└──────────────────────────────────────────────────────────────────┘
```

| 区域 | 位置 | 更新频率 | 说明 |
|------|------|---------|------|
| 连接栏 | 左上 | 手动 | 输入 PS URL，Connect/Disconnect |
| 指标概览 | 右上 | 5Hz | 6 个关键指标的数字显示 |
| 节点拓扑 | 左中 | 1Hz | 彩色节点图，显示在线状态 |
| 训练曲线 | 右中 | 1Hz | 吞吐量 + P50/P99 延迟曲线 |
| Actor 表 | 左下 | 1Hz | 各 Actor 队列深度和消息速率 |
| 网络流量 | 右下 | 1Hz | Send/Recv 带宽曲线 |
| 告警面板 | 左下 | 实时 | 活跃告警列表 |
| 状态栏 | 底部 | 实时 | 连接状态和错误信息 |

---

## 3. 连接 PS 节点

### 3.1 界面操作

1. 在顶部地址栏输入 PS 的 metrics URL（默认 `http://127.0.0.1:18080`）
2. 点击 **Connect** 按钮
3. 状态栏显示 "Connected — polling metrics at 1 Hz"
4. 各面板开始实时更新

### 3.2 断开连接

点击 **Disconnect** 按钮。Dashboard 停止拉取数据，但保留已加载的历史数据。

### 3.3 自动重连

Dashboard 不自动重连。如果 PS 重启，需要手动点击 Connect。

### 3.4 时序说明

PS benchmark 完成极快（10ms 量级）。Dashboard 以 1Hz 频率轮询，benchmark 可能在 Dashboard 收到第一个数据点之前就已经结束。这是正常的——Dashboard 设计用于持续运行的分布式训练场景，而非微型 benchmark。

**验证技巧**: 如果 benchmark 太快来不及观测，增加 step 数量或参数规模可延长运行时间。

---

## 4. 面板详解

### 4.1 节点拓扑图

**位置**: 左侧中部

**功能**: 可视化训练集群的节点拓扑。

```
         [W0]
          │
    ─────[PS]─────
          │
         [W1]
```

**颜色编码**:

| 颜色 | 含义 |
|------|------|
| 🟢 绿色 | Online — 节点正常运行 |
| 🟡 黄色 | Degraded — 性能下降 |
| 🔴 红色 | Offline — 节点离线 |

**交互**: 鼠标拖拽平移视图。

**节点信息**（每个节点）：
- 标签：PS / W0 / W1 / ...
- 连接线：表示与 PS 的通信链路
- 发光环：增强视觉效果

### 4.2 训练进度面板

**位置**: 右侧上部

**指标数字**（6 个关键指标，5Hz 刷新）：

| 指标 | 格式 | 说明 |
|------|------|------|
| THROUGHPUT | `7.6 MB/s` | 总数据吞吐量 |
| STEPS | `40` | 累计训练步数 |
| P50 LATENCY | `0.029 ms` | 中位数延迟 |
| P99 LATENCY | `0.043 ms` | 99 分位延迟 |
| NODES | `3 / 3` | 在线节点数 / 总节点数 |
| CHECKSUM | `0x5bc55510` | CRC32 数据校验和（非零 = 数据完整） |

**训练曲线**（1Hz 追加数据点）：
- **上半部分**（青色曲线）：吞吐量变化趋势
- **下半部分**（绿色曲线）：P50 延迟；**红色曲线**：P99 延迟
- 横轴：时间（最近 120 个采样点）
- 纵轴：自适应缩放
- 底部图例：当前最新值

**读取曲线**：
- 吞吐量上升 → 训练加速
- P99 延迟突然上升 → 可能存在网络拥塞或节点瓶颈
- P50 与 P99 差距增大 → 延迟分布变宽，存在抖动

### 4.3 Actor 监控表

**位置**: 左侧下部

**功能**: 监控每个 Actor 的运行状态。

**表格列**：

| 列 | 含义 | 颜色编码 |
|----|------|---------|
| Name | Actor 名称 | — |
| Queue | 消息队列深度 | >50 红, >10 黄, 正常 灰白 |
| Msg/s | 每秒处理消息数 | — |
| Lat(ms) | 平均处理延迟 | — |
| State | 运行状态 | blocked=红, idle=黄, running=绿 |

**异常指示器**（标题栏右侧）：
- `✓ All normal` — 所有 Actor 正常
- `⚠ N anomalous` — N 个 Actor 存在异常

**异常判定标准**：
- 队列深度 > 10
- 状态为 "blocked"

### 4.4 网络流量图

**位置**: 右侧下部

**功能**: 显示网络收发带宽。

**曲线**：
- 青色：发送速率 (MB/s)
- 绿色：接收速率 (MB/s)

**汇总数据**（图表下方）：
- `Send: 12.5 MB/s`
- `Recv: 250.0 MB/s`
- `Conns: 4`

**典型场景解读**：
- 分布式训练中 Recv >> Send（PS 接收梯度远多于发送确认）
- Send/Recv 比例接近 1:1 → 可能是数据并行同步
- 流量突然降为零 → 训练可能已暂停或完成

### 4.5 告警面板

**位置**: 左侧底部

**功能**: 显示活跃告警。

**告警条目格式**：
```
🔴 [CRITICAL] Node Offline
    Node worker-3 is offline — 14:32:05
```

**严重级别**：

| 图标 | 级别 | 背景色 |
|------|------|--------|
| 🔴 | CRITICAL | 深红 |
| 🟡 | WARNING | 深黄 |
| ℹ️ | INFO | 默认 |

**状态栏**：
- `✓ No active alerts` — 一切正常（绿色）
- `🔴 2 critical, 3 warnings` — 有告警（红色，加粗）
- `🟡 1 warnings` — 有警告（黄色，加粗）

---

## 5. 告警规则说明

Dashboard 内置 5 条告警规则，在每次数据更新时自动评估。

| 规则 ID | 告警标题 | 触发条件 | 严重级别 | 冷却期 |
|---------|---------|---------|---------|--------|
| `node_offline` | Node Offline | 任一节点状态为 offline | 🔴 Critical | 30s |
| `high_latency` | High Latency Detected | P99 延迟 > 10ms | 🟡 Warning | 60s |
| `throughput_drop` | Throughput Drop | 吞吐量下降超过 50% | 🟡 Warning | 120s |
| `actor_queue_full` | Actor Queue Full | 队列深度 > 100 | 🟡 Warning | 30s |
| `checksum_mismatch` | Checksum Mismatch | CRC32 校验失败 | 🔴 Critical | 15s |

**冷却期**：告警清除后在冷却期内不会再次触发，避免告警风暴。

---

## 6. 命令行参考

### dist-bench（PS 节点）

```bash
# 本地共享内存模式（无 TCP，无 metrics）
dist-bench local <workers> <params> <steps>

# TCP loopback 模式（PS + Workers 同进程）
dist-bench tcp <workers> <params> <steps> [ps_port] [metrics_port]

# PS 独立模式（Worker 需另外启动进程）
dist-bench ps <port> <workers> <params> <steps> [metrics_port]

# Worker 独立模式（连接到指定 PS）
dist-bench worker <addr> <id> <steps> [batch] [params]
```

**默认值**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `workers` | 4 | Worker 数量 |
| `params` | 10000 | 参数量（浮点数个数） |
| `steps` | 100 | 每个 Worker 的训练步数 |
| `ps_port` (tcp 模式) | 18001 | PS TCP 监听端口 |
| `metrics_port` | 0 (禁用) | Metrics HTTP 端口 |

**验证过的参数组合**（WSL2 环境）：

```bash
# 轻量测试 — Dashboard 功能验证
dist-bench tcp 1 500 20 18001 18080

# 标准测试 — 2 workers, 中等规模
dist-bench tcp 2 500 20 18001 18080

# 多 worker（注意 WSL2 上限见第 7 节）
dist-bench tcp 2 500 200 18001 18080

# 仅 PS（等待外部 Worker）
dist-bench ps 18001 2 500 20 18080

# 启动一个 Worker（连接到远程 PS）
dist-bench worker 192.168.1.100:18001 0 200 1 50000
```

### training-dashboard

```bash
training-dashboard [ps_url]
```

**参数**：
- `ps_url`（可选）：启动后自动连接到此 PS URL

**环境变量**：
- `DISPLAY`：X11 显示器地址（WSL2 设为 `:0`）

---

## 7. WSL2 参数限制

WSL2 的 io_uring 提交队列（SQ）容量有限，超出会导致进程崩溃。以下为实测结果：

### 参数矩阵

| Workers | Params | Steps | 数据量 | WSL2 结果 |
|---------|--------|-------|--------|-----------|
| 1 | 500 | 20 | 4 KB/step | ✅ 通过 |
| 2 | 500 | 20 | 4 KB/step | ✅ 通过（P50=0.029ms） |
| 2 | 500 | 200 | 4 KB/step | ✅ 通过（需 ~500ms） |
| 2 | 1000 | 20 | 8 KB/step | ✅ 通过 |
| 4 | 500 | 20 | 4 KB/step | ⚠️ 偶发 ENOBUFS |
| 4 | 50000 | 200 | 400 KB/step | ❌ SIGSEGV (SQ 溢出) |

### 崩溃症状

```
coroutine exception: No buffer space available   ← ENOBUFS
timeout: the monitored command dumped core       ← 静默 SIGSEGV
```

### 解决方案

1. **减少并发**: 降低 Worker 数量至 ≤2
2. **减小数据量**: 降低 params 或 steps
3. **裸机 Linux**: 无此限制，可随意扩展
4. **分步测试**: 先用小参数验证连通性，再逐步放大

```bash
# WSL2 安全上限（推荐）
dist-bench tcp 2 500 200 18001 18080

# 如果仍然崩溃，继续减小
dist-bench tcp 1 500 100 18001 18080
```

---

## 8. 故障排除

### 8.1 Dashboard 无法启动

**症状**: 启动后无窗口显示，命令行无输出。

**排查**:
```bash
# 检查 Qt 平台插件
ls ~/anaconda3/plugins/platforms/libqxcb.so

# 检查 X11 连接
xdpyinfo -display :0 | head -3

# 手动测试 X11
xeyes -display :0
```

**解决**:
```bash
# 如果缺少 X11，WSL2 用户确认 WSLg 已安装：
# Windows 端：wsl --update

# 确保 DISPLAY 已设置
export DISPLAY=:0
```

### 8.2 端口被占用

**症状**: PS 启动后输出 `bind failed: Address already in use`，metrics exporter 监听在随机端口而非指定端口。

```
[metrics] bind failed: Address already in use
[metrics] exporter listening on :45117   ← 随机端口，不是 18080
```

**排查**:
```bash
# 查看占用端口的进程
ss -tlnp | grep 18080

# 查看残留的 dist-bench 进程（含 GDB 会话）
ps aux | grep dist-bench | grep -v grep
```

**解决**:
```bash
# 终止残留进程
kill -9 $(ss -tlnp | grep 18080 | grep -oP 'pid=\K[0-9]+')

# 或直接换一个端口
dist-bench tcp 2 500 20 18001 18081
```

**常见原因**:
- 上一次运行通过 GDB 调试，GDB 暂停了进程但未释放端口
- 上一次运行崩溃（SIGSEGV），进程未正常退出
- 多个终端同时运行了 dist-bench

### 8.3 连接 PS 失败

**症状**: Dashboard 状态栏显示 "Error: Connection refused"。

**排查**:
```bash
# 确认 PS 进程在运行
ps aux | grep dist-bench

# 确认 metrics 端口在监听
ss -tlnp | grep 18080

# 测试 HTTP 连通性
curl -v http://127.0.0.1:18080/health
```

**解决**:
- 检查 PS 启动时是否传入了 `metrics_port` 参数（最后一个参数）
- 确认端口号正确（默认可在 Dashboard 地址栏修改）
- 如果 PS 已结束，重新启动 PS 后点击 Connect

### 8.4 数据不更新

**症状**: Dashboard 已连接但所有数值显示为 "—"。

**排查**:
```bash
# 手动拉取 API 确认数据
curl http://127.0.0.1:18080/api/v1/training

# 检查 PS 是否有 Worker 连接
# 在 PS 输出中查找 "workers:" 行 — 0 表示无 Worker
```

**解决**:
- 确认有 Worker 正在运行并向 PS 发送数据
- TCP loopback 模式下，确认 benchmark 参数足够大以至于有足够时间拉取数据
- 如果 benchmark 太快（< 1 秒），Dashboard 来不及拉取，增大 step 数量

### 8.5 训练曲线无数据

**症状**: Dashboard 图表显示 "Waiting for data..."。

**原因**: Dashboard 需要至少 2 个数据点才能绘制曲线（1Hz 拉取 = 至少需要 2 秒的 benchmark）。

**解决**: 增大 step 数量或参数量以延长 benchmark 运行时间，例如：
```bash
dist-bench tcp 2 500 200 18001 18080   # 200 steps ≈ 500ms+
```

### 8.6 界面显示异常

**症状**: 字体不清晰、颜色不对、布局错乱。

**排查**:
```bash
# 确认 QSS 样式文件存在
ls app/training-dashboard/resources/style.qss
```

**解决**:
- 确保从 `build/` 目录运行（QSS 文件路径相对 `../app/...`）
- 或确保 Qt 资源文件已编译进二进制

### 8.7 Dashboard 占用过高 CPU

**症状**: Dashboard 进程 CPU >10%。

**解决**:
- Dashboard 设计目标 <5% CPU @ 5Hz UI 刷新
- 如果超标，检查是否有大量数据点渲染
- 图表保留最近 120 个点，超过自动淘汰

### 8.8 GDB 调试后端口残留

**症状**: GDB 调试 dist-bench 后，端口一直被占用，新进程无法 bind。

**原因**: GDB 暂停（stop）进程时，已绑定的端口不会释放。即使 GDB quit，如果被调试进程处于 traced/stopped 状态（`ps aux` 显示 `tl`），端口也不会释放。

**解决**:
```bash
# 找到被 GDB 暂停的进程
ps aux | grep "tl" | grep dist-bench

# 强制终止
kill -9 <PID>

# 验证端口已释放
ss -tlnp | grep 18080
```

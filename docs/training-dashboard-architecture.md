# Training Dashboard — 架构与开发指南

> 面向开发者和维护者。如需使用说明，参见 [用户手册](training-dashboard-user-guide.md)。

## 目录

1. [系统概览](#1-系统概览)
2. [MVVM 架构哲学](#2-mvvm-架构哲学)
3. [Model 层](#3-model-层)
4. [ViewModel 层](#4-viewmodel-层)
5. [View 层](#5-view-层)
6. [数据流全链路](#6-数据流全链路)
7. [PS 端 MetricsExporter](#7-ps-端-metricsexporter)
8. [Agent 可读接口](#8-agent-可读接口)
9. [扩展指南](#9-扩展指南)
10. [构建与测试](#10-构建与测试)
11. [常见陷阱](#11-常见陷阱)

---

## 1. 系统概览

```
┌──────────────────────────────────────────────────────────────────┐
│                    Qt5 Dashboard (独立进程)                       │
│                                                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │
│  │ Topology │ │ Training │ │  Actor   │ │  Alert   │           │
│  │  View    │ │  Chart   │ │  Table   │ │  Panel   │  ← View   │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └────┬─────┘           │
│       │             │            │            │                  │
│  ┌────┴─────────────┴────────────┴────────────┴─────┐           │
│  │              DashboardViewModel                    │ ← VM     │
│  │  ├── ActorListViewModel                           │          │
│  │  └── AlertViewModel                               │          │
│  └──────────────────────┬────────────────────────────┘          │
│                         │                                       │
│  ┌──────────────────────┴──────────────────────────┐           │
│  │  MetricsClient (HTTP 1Hz)                        │ ← Model  │
│  │  ├── NodeRegistry                                │          │
│  │  ├── TrainingHistory                             │          │
│  │  ├── ActorListModel                              │          │
│  │  ├── NetworkStatsRegistry                        │          │
│  │  └── AlertEngine                                 │          │
│  └──────────────────────────────────────────────────┘          │
└──────────────────────────────┬───────────────────────────────────┘
                               │ HTTP GET /api/v1/* (JSON)
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│              PS Node (actor_system / dist-bench)                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  MetricsExporter (协程 HTTP server, 端口 18080)             │  │
│  │  /health             → {"status":"ok"}                     │  │
│  │  /api/v1/nodes       → 节点拓扑 JSON                        │  │
│  │  /api/v1/training    → 训练指标 JSON                        │  │
│  │  /api/v1/actors      → Actor 状态 JSON                     │  │
│  │  /api/v1/network     → 网络流量 JSON                       │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

**两个独立进程：**

| 进程 | 可执行文件 | 角色 |
|------|-----------|------|
| PS (Parameter Server) | `dist-bench` | 运行训练任务，暴露 metrics HTTP 端点 |
| Dashboard | `training-dashboard` | Qt5 桌面应用，拉取并可视化 metrics |

---

## 2. MVVM 架构哲学

### 为什么选 MVVM 而非 MVC

传统 MVC 中 View 直接持有 Model 引用，导致 UI 与数据格式紧耦合。MVVM 在二者之间插入 ViewModel 层：

```
MVC:   Model ←──→ View       (View 知道 Model 结构)
MVVM:  Model ←→ ViewModel ←→ View   (View 只绑定 ViewModel 属性)
```

**核心收益：**

1. **View 零业务逻辑** — View 只负责渲染，不包含任何数据转换。测试可以完全绕过 View。
2. **ViewModel 可独立测试** — 不需要启动 GUI 即可验证数据转换逻辑。
3. **Agent 可读** — ViewModel 暴露 `agent_state()` 接口，Agent/脚本可直接读取结构化状态，无需解析 UI 控件。
4. **替换 View 容易** — 同一个 ViewModel 可以驱动 Qt Widgets、QML、或 CLI 输出。

### 本项目的 MVVM 实现

所有代码在 `app/training-dashboard/` 下，header-only 设计：

```
app/training-dashboard/
├── main.cc                          ← 入口，创建 VM + View
├── models/                          ← 纯数据，无 Qt 依赖
│   ├── node_info.h                  ← NodeInfo, NodeRegistry
│   ├── training_metrics.h           ← TrainingSnapshot, TrainingHistory
│   ├── actor_status.h               ← ActorStatus, ActorListModel
│   ├── network_stats.h              ← NetworkStats, NetworkStatsRegistry
│   ├── alert_rule.h                 ← Alert, AlertRule, AlertEngine
│   ├── data_buffer.h                ← RingBuffer<T> 泛型环形缓冲
│   └── metrics_client.h             ← HTTP 客户端 (QNetworkAccessManager)
├── viewmodels/
│   └── dashboard_viewmodel.h        ← DashboardVM + ActorListVM + AlertVM
├── views/
│   ├── main_window.h                ← 主窗口，组装所有子 View
│   ├── topology_widget.h            ← 节点拓扑图 (QGraphicsView)
│   ├── training_chart_widget.h      ← 训练曲线 (QPainter)
│   ├── actor_table_widget.h         ← Actor 状态表 (QTableWidget)
│   ├── network_widget.h             ← 网络流量图 (QPainter)
│   └── alert_panel_widget.h         ← 告警面板 (QListWidget)
└── resources/
    ├── resources.qrc
    └── style.qss                    ← 暗色科技风主题
```

### 类图（核心关系）

```
MetricsClient ──信号──▶ DashboardViewModel ──属性绑定──▶ MainWindow
     │                        │                                │
     │ 拉取 HTTP              │ 持有子 VM                      │ 持有子 View
     │                        │                                │
     ▼                        ▼                                ▼
  NodeRegistry          ActorListViewModel              TopologyWidget
  TrainingHistory       AlertViewModel                 TrainingChartWidget
  ActorListModel                                       ActorTableWidget
  NetworkStatsRegistry                                 NetworkWidget
  AlertEngine                                          AlertPanelWidget
```

---

## 3. Model 层

Model 层是纯数据结构，**不依赖 Qt 元对象系统**（除 `MetricsClient` 外）。每个 Model 负责单一领域的数据表示和 JSON 反序列化。

### 3.1 `node_info.h` — 节点信息

```cpp
enum class NodeStatus { Online, Degraded, Offline };

struct NodeInfo {
    QString   node_id;        // "ps-0", "worker-0"
    QString   address;        // "127.0.0.1:18001"
    NodeStatus status;
    double    cpu_percent;
    int64_t   memory_mb;
    int64_t   uptime_seconds;
    QDateTime last_seen;

    static NodeInfo from_json(const QJsonObject&);  // JSON → 结构体
};

struct NodeRegistry {
    QVector<NodeInfo> nodes;
    void update_from_json_array(const QJsonArray&);
    int online_count() const;
};
```

**设计要点**：`from_json` 静态工厂方法封装 JSON 解析，Model 使用者不需要知道 JSON 结构。

### 3.2 `training_metrics.h` — 训练指标

```cpp
struct WorkerSample {
    int    worker_id;
    int    steps;
    double avg_latency_ms;
    double p99_latency_ms;
};

struct TrainingSnapshot {
    int64_t  timestamp_ms;
    int      total_steps;
    int64_t  total_bytes;
    double   throughput_mbps;
    double   latency_p50_ms;
    double   latency_p99_ms;
    QString  checksum;
    QVector<WorkerSample> workers;

    static TrainingSnapshot from_json(const QJsonObject&);
};

class TrainingHistory {          // 固定容量环形缓冲
    static constexpr size_t kDefaultCapacity = 3600;  // 1小时@1Hz
    void push(TrainingSnapshot);
    TrainingSnapshot latest() const;
    double avg_throughput_last_n(size_t n) const;
};
```

**设计要点**：`TrainingHistory` 是指定容量的 deque，自动淘汰旧数据，自动计算派生指标。

### 3.3 `actor_status.h` — Actor 状态

```cpp
struct ActorStatus {
    QString  name;               // "ps", "worker-0"
    int      queue_size;         // 当前消息队列深度
    double   msg_rate_per_sec;   // 消息速率
    int64_t  total_messages;     // 累计消息数
    double   avg_latency_ms;     // 平均处理延迟
    QString  state;              // "running", "idle", "blocked"

    static ActorStatus from_json(const QJsonObject&);
};

struct ActorListModel {
    QVector<ActorStatus> actors;
    int total_queue_depth() const;
    QVector<ActorStatus> filtered_by_state(const QString&) const;
    QVector<ActorStatus> anomalous(int threshold = 10) const;  // 异常检测
};
```

### 3.4 `network_stats.h` — 网络统计

```cpp
struct NetworkStats {
    QString  peer;               // 对端标识
    int64_t  bytes_sent;
    int64_t  bytes_recv;
    double   send_rate_mbps;
    double   recv_rate_mbps;
    int      active_connections;
};

struct NetworkStatsRegistry {
    QVector<NetworkStats> stats;
    double total_send_mbps() const;
    double total_recv_mbps() const;
};
```

### 3.5 `alert_rule.h` — 告警规则引擎

```cpp
enum class AlertSeverity { Info, Warning, Critical };

struct Alert {
    QString       id;
    QString       title;
    QString       message;
    AlertSeverity severity;
    QDateTime     raised_at;
    QDateTime     cleared_at;
    bool          active;
};

struct AlertRule {
    QString       rule_id;          // 唯一标识
    QString       title_template;   // 告警标题模板
    AlertSeverity severity;
    int           cooldown_seconds; // 冷却期（避免重复触发）
};

class AlertEngine {
    void add_rule(const AlertRule&);
    void evaluate(std::function<QString(const AlertRule&)> evaluator);
    const QVector<Alert>& active_alerts() const;
    int active_count() const;
    int critical_count() const;
};
```

**预置告警规则**（`default_alert_rules()`）：

| rule_id | 条件 | 严重级别 |
|---------|------|---------|
| `node_offline` | 任一节点离线 | Critical |
| `high_latency` | P99 > 10ms | Warning |
| `throughput_drop` | 吞吐量下降 >50% | Warning |
| `actor_queue_full` | 队列深度 >100 | Warning |
| `checksum_mismatch` | 数据校验失败 | Critical |

### 3.6 `data_buffer.h` — 泛型环形缓冲

```cpp
template <typename T>
class RingBuffer {
    static constexpr size_t kDefaultCapacity = 3600;
    void push(T&&);
    void push(const T&);
    T latest() const;
    std::vector<T> last_n(size_t n) const;
    template <typename Reducer>
    auto reduce_last_n(size_t n, Reducer&&, auto initial) const;
    void resize(size_t);
};
```

独立于 `TrainingHistory` 的泛型版本，可用于存储任意时序数据（如网络流量历史）。

### 3.7 `metrics_client.h` — HTTP 客户端

```cpp
class MetricsClient : public QObject {
    Q_OBJECT
    void set_base_url(const QString&);
    void start(int interval_ms = 1000);
    void stop();

signals:
    void nodes_updated(const QVector<NodeInfo>&);
    void training_updated(const TrainingSnapshot&);
    void actors_updated(const QVector<ActorStatus>&);
    void network_updated(const QVector<NetworkStats>&);
    void connection_error(const QString&);
    void connected_changed(bool);
};
```

**设计要点**：
- 定时器驱动（默认 1Hz），避免阻塞 UI 线程
- 每个端点独立 `QNetworkAccessManager::get()`，错误隔离
- actors 和 network 端点为可选（404 不触发 error）
- `connected_changed` 信号驱动 UI 状态栏更新

---

## 4. ViewModel 层

ViewModel 是 MVVM 的核心——它持有 Model 数据，通过 Qt 属性绑定驱动 View 更新。

### 4.1 DashboardViewModel — 顶层协调器

```cpp
class DashboardViewModel : public QObject {
    Q_OBJECT
    // 属性绑定 — View 通过 Q_PROPERTY 自动更新
    Q_PROPERTY(bool connected READ is_connected NOTIFY connected_changed)
    Q_PROPERTY(int online_nodes READ online_nodes NOTIFY nodes_changed)
    Q_PROPERTY(double throughput READ throughput NOTIFY training_changed)
    Q_PROPERTY(int total_steps READ total_steps NOTIFY training_changed)
    Q_PROPERTY(double latency_p50 READ latency_p50 NOTIFY training_changed)
    Q_PROPERTY(double latency_p99 READ latency_p99 NOTIFY training_changed)

public:
    ActorListViewModel* actor_vm() const;    // 子 VM
    AlertViewModel* alert_vm() const;        // 子 VM
    const QVector<NodeInfo>& nodes() const;
    const QVector<NetworkStats>& network_stats() const;

    // Agent 可读接口
    QVariantMap agent_state() const;

public slots:
    void connect_to(const QString& url);
    void disconnect();
};
```

**数据流**：`MetricsClient` 信号 → `DashboardViewModel` 槽函数 → 更新 Model → emit 通知信号 → View 刷新。

### 4.2 ActorListViewModel — Actor 列表管理

```cpp
class ActorListViewModel : public QObject {
    Q_OBJECT
    Q_PROPERTY(int total_actors READ total_actors NOTIFY actors_changed)
    Q_PROPERTY(int total_queue_depth READ total_queue_depth NOTIFY actors_changed)
    Q_PROPERTY(int anomalous_count READ anomalous_count NOTIFY actors_changed)

    QVector<ActorStatus> sorted_by_queue() const;       // 按队列深度排序
    QVector<ActorStatus> anomalous_actors(int threshold) const;  // 异常 Actor
    QVariantMap agent_state() const;
};
```

### 4.3 AlertViewModel — 告警管理

```cpp
class AlertViewModel : public QObject {
    Q_OBJECT
    Q_PROPERTY(int active_alerts READ active_alerts NOTIFY alerts_changed)
    Q_PROPERTY(int critical_alerts READ critical_alerts NOTIFY alerts_changed)

    void evaluate(nodes, training, actors);  // 评估所有规则
    QVariantMap agent_state() const;
};
```

告警在每次 `training_updated` 时自动评估。

---

## 5. View 层

View 层只负责渲染，不包含任何数据处理逻辑。所有 View 都通过 Qt 信号/槽连接 ViewModel。

### 5.1 MainWindow — 主窗口布局

```
┌──────────────────────┬──────────────────────────────┐
│  LEFT (40%)          │  RIGHT (60%)                  │
│                      │                               │
│  [Connection Bar]    │  ┌───────────────────────┐    │
│                      │  │  THROUGHPUT  │ STEPS  │    │
│  ┌────────────────┐  │  │  P50 LATENCY │ P99    │    │
│  │   TOPOLOGY     │  │  │  NODES       │ CHECKSUM│   │
│  │   (QGraphics)  │  │  └───────────────────────┘    │
│  └────────────────┘  │                               │
│                      │  ┌───────────────────────┐    │
│  ┌────────────────┐  │  │  TRAINING CHART       │    │
│  │  ACTOR TABLE   │  │  │  (Throughput + P50/   │    │
│  │  (QTableWidget)│  │  │   P99 latency curves) │    │
│  └────────────────┘  │  └───────────────────────┘    │
│                      │                               │
│  ┌────────────────┐  │  ┌───────────────────────┐    │
│  │  ALERT PANEL   │  │  │  NETWORK TRAFFIC      │    │
│  │  (QListWidget) │  │  │  (Send/Recv curves)   │    │
│  └────────────────┘  │  └───────────────────────┘    │
├──────────────────────┴──────────────────────────────┤
│  Status: Connected — polling metrics at 1 Hz        │
└─────────────────────────────────────────────────────┘
```

每个子 View 都暴露 `agent_state()` 方法供外部读取。

### 5.2 TopologyWidget — 节点拓扑图

- 基于 `QGraphicsView` + `QGraphicsScene`
- PS 居中，Worker 环形排列
- 颜色编码：绿 = Online，黄 = Degraded，红 = Offline
- 半透明发光环 + 连接线
- 支持拖拽平移

### 5.3 TrainingChartWidget — 训练曲线

- 纯 `QPainter` 渲染（无 Qt Charts 依赖）
- 上半部分：吞吐量曲线（青色）
- 下半部分：P50（绿色）和 P99（红色）延迟曲线叠加
- 保留最近 120 个数据点
- 自适应 Y 轴缩放

### 5.4 ActorTableWidget — Actor 状态表

- `QTableWidget` 五列：Name, Queue, Msg/s, Lat(ms), State
- 队列深度 >50 红色高亮，>10 黄色高亮
- State 列颜色编码：blocked=红, idle=黄, running=绿
- 异常计数显示在标题栏

### 5.5 NetworkWidget — 网络流量图

- 纯 `QPainter` 渲染
- Send（青色）和 Recv（绿色）双曲线
- 底部汇总：Send/Recv MB/s，活跃连接数

### 5.6 AlertPanelWidget — 告警面板

- `QListWidget`，按严重级别着色背景
- Critical = 深红，Warning = 深黄，Info = 默认
- 状态栏概括：🔴 N critical, 🟡 M warnings

---

## 6. 数据流全链路

```
PS Node (每秒)
  │
  ├── bench_metrics 原子变量持续更新
  │
  ▼
MetricsExporter (HTTP server, 协程)
  │
  ├── GET /api/v1/nodes    → build_nodes_json()
  ├── GET /api/v1/training → build_training_json(snapshot)
  ├── GET /api/v1/actors   → build_actors_json()
  ├── GET /api/v1/network  → build_network_json()
  │
  ▼  (HTTP response, JSON)
Dashboard 进程
  │
  ▼
MetricsClient (QTimer 1Hz)
  │
  ├── fetch_nodes()    → parse JSON → emit nodes_updated(nodes)
  ├── fetch_training() → parse JSON → emit training_updated(snap)
  ├── fetch_actors()   → parse JSON → emit actors_updated(actors)
  ├── fetch_network()  → parse JSON → emit network_updated(stats)
  │
  ▼
DashboardViewModel (槽函数)
  │
  ├── on_nodes_updated()    → m_registry.nodes = nodes → emit nodes_changed()
  ├── on_training_updated() → m_history.push(snap)     → emit training_changed()
  ├── on_actors_updated()   → actor_vm->on_actors_updated() → alert eval
  ├── on_network_updated()  → m_network_registry.stats = stats → emit
  │
  ▼
View 层 (信号/槽自动更新)
  │
  ├── TopologyWidget::update_nodes()
  ├── TrainingChartWidget::add_data_point()
  ├── ActorTableWidget::update_actors()
  ├── NetworkWidget::update_stats()
  └── AlertPanelWidget::update_alerts()
```

**刷新频率**：
- 数据拉取：1Hz（MetricsClient 定时器）
- UI 渲染：5Hz（MainWindow 定时器）
- 图表数据点：每次 `training_updated` 追加 1 个点

---

## 7. PS 端 MetricsExporter

`include/ultranet/actor/system/metrics_exporter.h`

### 7.1 API 端点

所有端点返回 `Content-Type: application/json`，支持 CORS。

| 端点 | 方法 | 返回 |
|------|------|------|
| `/health` | GET | `{"status":"ok"}` |
| `/api/v1/nodes` | GET | 节点拓扑数组 |
| `/api/v1/training` | GET | 训练指标对象 |
| `/api/v1/actors` | GET | Actor 状态数组 |
| `/api/v1/network` | GET | 网络统计数组 |

### 7.2 JSON Schema

**GET /api/v1/training**
```json
{
  "timestamp_ms": 1781195806463,
  "total_steps": 800,
  "total_bytes": 32000000,
  "throughput_mbps": 251.2,
  "latency_p50_ms": 1.19,
  "latency_p99_ms": 1.52,
  "checksum": "0x14f7d308",
  "workers": [
    {"id": 0, "steps": 200, "avg_latency_ms": 1.20, "p99_latency_ms": 1.52},
    {"id": 1, "steps": 200, "avg_latency_ms": 1.21, "p99_latency_ms": 1.49}
  ]
}
```

**GET /api/v1/nodes**
```json
[
  {"node_id":"ps-0", "address":"127.0.0.1:18001", "status":"online",
   "cpu_percent":45.2, "memory_kb":262144, "uptime_seconds":3600},
  {"node_id":"worker-0", "address":"127.0.0.1:0", "status":"online",
   "cpu_percent":82.1, "memory_kb":131072, "uptime_seconds":3598}
]
```

**GET /api/v1/actors**
```json
[
  {"name":"ps", "queue_size":0, "msg_rate_per_sec":100.5,
   "total_messages":800, "avg_latency_ms":0.05, "state":"running"},
  {"name":"worker-0", "queue_size":3, "msg_rate_per_sec":50.2,
   "total_messages":400, "avg_latency_ms":1.20, "state":"running"}
]
```

**GET /api/v1/network**
```json
[
  {"peer":"workers", "bytes_sent":16000000, "bytes_recv":32000000,
   "send_rate_mbps":12.5, "recv_rate_mbps":250.0, "active_connections":4}
]
```

### 7.3 架构设计

```
metrics_exporter
├── json_provider = std::function<std::string()>  ← 每个端点一个回调
├── set_nodes_provider(json_provider)              ← 注册回调
├── set_training_provider(json_provider)
├── set_actors_provider(json_provider)
├── set_network_provider(json_provider)
├── set_port(uint16_t)
├── stop()                                         ← 关闭 listen socket
└── serve() → Task<void>                           ← 协程 HTTP server
```

**生命周期管理**：
- 继承 `std::enable_shared_from_this`
- `serve()` 和 `handle_client()` 各自持有 `shared_from_this()` 引用
- `stop()` 关闭 listen socket 唤醒 Accept 循环
- 同步 socket 创建（`::socket` / `::bind` / `::listen`）避免 io_uring 竞争

### 7.4 集成到 dist-bench

```bash
# tcp 模式（PS + workers 同进程）
./bin/dist-bench tcp 4 50000 200 18001 18080
#                              │     │     └── metrics 端口
#                              │     └── steps
#                              └── workers

# ps 模式（仅 PS）
./bin/dist-bench ps 18001 4 50000 200 18080
#                   │     │  │      │    └── metrics 端口
#                   │     │  │      └── steps
#                   │     │  └── params
#                   │     └── workers
#                   └── PS 端口
```

代码中通过 provider lambda 将 `bench_metrics` 数据桥接到 JSON 端点：

```cpp
auto exporter = std::make_shared<metrics_exporter>();
exporter->set_training_provider([&]() -> std::string {
    training_snapshot_data snap;
    snap.total_steps = state.m.total_steps.load();
    snap.latency_p50_ms = state.m.latency.p50();
    // ...
    return build_training_json(snap);
});
exporter->set_port(18080);
sched->submit(exporter->serve().release());
```

---

## 8. Agent 可读接口

为了支持 Agent/脚本读取 Dashboard 状态，所有核心组件都暴露了 `agent_state()` 方法。

### 8.1 调用方式

**DashboardViewModel::agent_state()** — 最全面的状态快照：

```cpp
QVariantMap state = view_model.agent_state();
// 返回结构:
{
  "connected": true,
  "base_url": "http://127.0.0.1:18080",
  "nodes": [
    {"node_id":"ps-0", "address":"...", "status":"online", ...}
  ],
  "online_nodes": 5,
  "total_nodes": 5,
  "training": {
    "timestamp_ms": 1781195806463,
    "total_steps": 800,
    "throughput_mbps": 251.2,
    "latency_p50_ms": 1.19,
    "latency_p99_ms": 1.52,
    "checksum": "0x14f7d308",
    "history_size": 3600
  },
  "network": [...],
  "total_send_mbps": 12.5,
  "total_recv_mbps": 250.0
}
```

**MainWindow::agent_state_json()** — JSON 字符串，可直接被外部进程读取：

```cpp
QString json = main_window.agent_state_json();
```

**MainWindow::agent_state()** — 包含所有子 View 状态：

```cpp
QVariantMap full_state = main_window.agent_state();
// 包含 dashboard, actor_table, network_view, alert_panel, ui 子对象
```

### 8.2 子组件 agent_state

| 组件 | 方法 | 返回内容 |
|------|------|---------|
| `ActorListViewModel` | `agent_state()` | actors 数组, total_queue_depth, anomalous_count |
| `AlertViewModel` | `agent_state()` | alerts 数组, active_count, critical_count |
| `ActorTableWidget` | `agent_state()` | 表格行数据 |
| `NetworkWidget` | `agent_state()` | send_mbps, recv_mbps, connections |
| `AlertPanelWidget` | `agent_state()` | 告警文本列表, alert_count, status_text |

### 8.3 直接在代码中访问 UI 控件

MainWindow 暴露了所有子 View 的裸指针（用于测试和 Agent 访问）：

```cpp
// MainWindow 公共访问器
TopologyWidget*     topology();
TrainingChartWidget* chart();
ActorTableWidget*   actor_table();
NetworkWidget*      network();
AlertPanelWidget*   alert_panel();
QLabel*             lbl_throughput();
QLabel*             lbl_steps();
QLabel*             lbl_p50();
// ... 等
```

---

## 9. 扩展指南

### 9.1 添加新的 Metrics 端点

**Step 1: PS 端** — 在 `build_xxx_json()` 中添加新函数：

```cpp
// 在 metrics_exporter.h 中添加
struct custom_metrics_data { /* ... */ };
inline std::string build_custom_json(const custom_metrics_data& data) {
    // 返回 JSON 字符串
}
```

**Step 2: PS 端** — 注册 provider：

```cpp
exporter->set_custom_provider([&]() -> std::string {
    custom_metrics_data d;
    // 填充数据...
    return build_custom_json(d);
});
```

**Step 3: Dashboard Model** — 添加数据结构：

```cpp
// models/custom_metrics.h
struct CustomMetrics {
    static CustomMetrics from_json(const QJsonObject&);
};
```

**Step 4: Dashboard Model** — 在 `MetricsClient` 添加拉取逻辑：

```cpp
// models/metrics_client.h
signals:
    void custom_updated(const CustomMetrics& data);

private:
    void fetch_custom() { /* GET /api/v1/custom → parse → emit */ }
```

**Step 5: ViewModel** — 添加属性和槽：

```cpp
// viewmodels/dashboard_viewmodel.h
Q_PROPERTY(double custom_value READ custom_value NOTIFY custom_changed)
```

**Step 6: View** — 添加展示控件。

### 9.2 添加新的 View 面板

```cpp
// views/my_new_widget.h
class MyNewWidget : public QWidget {
    Q_OBJECT
public:
    QVariantMap agent_state() const;  // Agent 接口
public slots:
    void update_data(const QVector<MyData>& data);
};
```

然后在 `MainWindow::setup_ui()` 中将新面板加入布局，在 `connect_signals()` 中连接 ViewModel 信号。

### 9.3 添加新的告警规则

```cpp
// 在 alert_rule.h 的 default_alert_rules() 中添加
{
    AlertRule r;
    r.rule_id         = "my_new_rule";
    r.title_template  = "My New Alert";
    r.severity        = AlertSeverity::Warning;
    r.cooldown_seconds = 60;
    rules.append(r);
}
```

然后在 `AlertViewModel::evaluate()` 中添加评估逻辑：

```cpp
if (rule.rule_id == "my_new_rule") {
    if (/* 触发条件 */) {
        return QString("Alert message with details");
    }
    return {};
}
```

---

## 10. 构建与测试

### 10.1 构建

```bash
# 构建目录
mkdir -p build && cd build

# CMake 配置
cmake .. -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_COMPILER=/home/jwy/anaconda3/envs/ultranet/bin/x86_64-conda-linux-gnu-g++ \
    -Dliburing_INCLUDE_DIR=/home/jwy/anaconda3/envs/ultranet/include \
    -Dliburing_LIBRARY=/home/jwy/anaconda3/envs/ultranet/lib/liburing.so

# 构建
make training-dashboard -j4   # Dashboard (需要 Qt5)
make dist-bench -j4           # PS + 基准测试
```

### 10.2 运行

```bash
# 终端 1: 启动 PS (含 metrics exporter)
./bin/dist-bench tcp 4 50000 200 18001 18080

# 终端 2: 启动 Dashboard (WSL2 需要 DISPLAY)
DISPLAY=:0 ./bin/training-dashboard http://127.0.0.1:18080
```

### 10.3 验证 Metrics API

```bash
# 健康检查
curl http://127.0.0.1:18080/health
# → {"status":"ok"}

# 训练指标
curl http://127.0.0.1:18080/api/v1/training | python3 -m json.tool

# 节点拓扑
curl http://127.0.0.1:18080/api/v1/nodes

# Actor 状态
curl http://127.0.0.1:18080/api/v1/actors
```

---

## 11. 常见陷阱

### 11.1 GCC 13.2 协程 Lambda 捕获破坏

**现象**：lambda `[=]` 按值捕获的变量在协程恢复后变成垃圾值。

**解决方案**：
- 用普通协程函数替代 lambda（见 `tcp_worker_run`）
- 用成员变量传递参数（见 `metrics_exporter::set_port()`）
- 用 `shared_from_this()` 替代 `this` 裸指针

### 11.2 WSL2 io_uring SQ 容量限制

**现象**：大量并发 io_uring 操作导致 ENOBUFS。

**解决方案**：
- 减少并发 io_uring 操作
- Metrics 服务器使用同步 socket 创建（`::socket` / `::bind` / `::listen`）
- 减少 worker 数量 ≤ 2，step 数量适度
- 生产环境部署在裸机 Linux 上不存在此限制

### 11.3 AUTOMOC 不处理 header-only Q_OBJECT

**现象**：链接时 `vtable undefined` 或 `staticMetaObject undefined`。

**解决方案**：在 `CMakeLists.txt` 中显式列出含 `Q_OBJECT` 的 header：

```cmake
add_executable(target
    main.cc
    models/metrics_client.h      # ← 必须列出
    viewmodels/dashboard_viewmodel.h
    views/main_window.h           # ← 及所有 Q_OBJECT 头文件
)
```

### 11.4 Qt5 在 WSL2 的 OpenGL 依赖

Qt5Gui → libGL.so.1。在 WSL2 中需要显式链接系统 GL：

```cmake
target_link_options(target PRIVATE
    -L/usr/lib/x86_64-linux-gnu
    -Wl,-rpath-link,/usr/lib/x86_64-linux-gnu
)
target_link_libraries(target PRIVATE Qt5::Core Qt5::Widgets Qt5::Network GL)
```

### 11.5 协程生命周期与 shared_ptr

MetricsExporter 被多个协程共享，必须用 `enable_shared_from_this`：

```cpp
class metrics_exporter : public std::enable_shared_from_this<metrics_exporter> {
    Task<void> serve() {
        auto self = shared_from_this();  // ← 持有引用到此协程退出
        // ...
    }
    Task<void> handle_client(int fd) {
        auto self = shared_from_this();  // ← 每个客户端也要持有
        // ...
    }
};
```

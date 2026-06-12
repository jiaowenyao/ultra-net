// ViewModel: Central coordinator connecting Model → View.
// Updated with actor monitoring, network stats, and alert engine.
#pragma once

#include <QObject>
#include <QVector>

#include "../models/metrics_client.h"
#include "../models/node_info.h"
#include "../models/training_metrics.h"
#include "../models/actor_status.h"
#include "../models/network_stats.h"
#include "../models/alert_rule.h"
#include "../models/data_buffer.h"

namespace dashboard::viewmodel {

using model::MetricsClient;
using model::NodeInfo;
using model::NodeRegistry;
using model::TrainingSnapshot;
using model::TrainingHistory;
using model::ActorStatus;
using model::ActorListModel;
using model::NetworkStats;
using model::NetworkStatsRegistry;
using model::Alert;
using model::AlertEngine;
using model::AlertRule;
using model::AlertSeverity;
using model::RingBuffer;

// ── Actor list ViewModel ──────────────────────────────────────────────

class ActorListViewModel : public QObject {
    Q_OBJECT
    Q_PROPERTY(int total_actors READ total_actors NOTIFY actors_changed)
    Q_PROPERTY(int total_queue_depth READ total_queue_depth NOTIFY actors_changed)
    Q_PROPERTY(int anomalous_count READ anomalous_count NOTIFY actors_changed)

public:
    explicit ActorListViewModel(QObject* parent = nullptr)
        : QObject(parent) {}

    int total_actors() const { return m_actor_list.actors.size(); }
    int total_queue_depth() const { return m_actor_list.total_queue_depth(); }
    int anomalous_count() const { return m_actor_list.anomalous().size(); }

    const QVector<ActorStatus>& actors() const { return m_actor_list.actors; }

    // Sorted view for display.
    QVector<ActorStatus> sorted_by_queue() const {
        auto list = m_actor_list.actors;
        std::sort(list.begin(), list.end(), [](const ActorStatus& a, const ActorStatus& b) {
            return a.queue_size > b.queue_size;
        });
        return list;
    }

    // Anomalous actors for highlighting.
    QVector<ActorStatus> anomalous_actors(int threshold = 10) const {
        return m_actor_list.anomalous(threshold);
    }

    // Agent-readable: export current actor state as a structured map.
    QVariantMap agent_state() const {
        QVariantMap map;
        QVariantList list;
        for (const auto& a : m_actor_list.actors) {
            QVariantMap am;
            am["name"]              = a.name;
            am["queue_size"]        = a.queue_size;
            am["msg_rate_per_sec"]  = a.msg_rate_per_sec;
            am["total_messages"]    = static_cast<qlonglong>(a.total_messages);
            am["avg_latency_ms"]    = a.avg_latency_ms;
            am["state"]             = a.state;
            list.append(am);
        }
        map["actors"]          = list;
        map["total_queue_depth"] = m_actor_list.total_queue_depth();
        map["anomalous_count"]   = m_actor_list.anomalous().size();
        return map;
    }

public slots:
    void on_actors_updated(const QVector<ActorStatus>& actors) {
        m_actor_list.actors = actors;
        emit actors_changed();
    }

signals:
    void actors_changed();

private:
    ActorListModel m_actor_list;
};

// ── Alert ViewModel ───────────────────────────────────────────────────

class AlertViewModel : public QObject {
    Q_OBJECT
    Q_PROPERTY(int active_alerts READ active_alerts NOTIFY alerts_changed)
    Q_PROPERTY(int critical_alerts READ critical_alerts NOTIFY alerts_changed)

public:
    explicit AlertViewModel(QObject* parent = nullptr)
        : QObject(parent) {
        for (const auto& rule : model::default_alert_rules()) {
            m_engine.add_rule(rule);
        }
    }

    int active_alerts() const { return m_engine.active_count(); }
    int critical_alerts() const { return m_engine.critical_count(); }

    const QVector<Alert>& alerts() const { return m_engine.active_alerts(); }

    // Evaluate rules against current system state.
    void evaluate(const QVector<NodeInfo>& nodes,
                  const TrainingSnapshot& training,
                  const QVector<ActorStatus>& actors)
    {
        m_engine.evaluate([&](const AlertRule& rule) -> QString {
            if (rule.rule_id == "node_offline") {
                for (const auto& n : nodes) {
                    if (n.status == model::NodeStatus::Offline) {
                        return QString("Node %1 is offline").arg(n.node_id);
                    }
                }
                return {};
            }
            if (rule.rule_id == "high_latency") {
                if (training.latency_p99_ms > 10.0) {
                    return QString("P99 latency %.2f ms exceeds 10 ms threshold")
                        .arg(training.latency_p99_ms);
                }
                return {};
            }
            if (rule.rule_id == "throughput_drop") {
                if (m_last_throughput > 0 &&
                    training.throughput_mbps < m_last_throughput * 0.5) {
                    return QString("Throughput dropped from %.1f to %.1f MB/s")
                        .arg(m_last_throughput).arg(training.throughput_mbps);
                }
                return {};
            }
            if (rule.rule_id == "actor_queue_full") {
                for (const auto& a : actors) {
                    if (a.queue_size > 100) {
                        return QString("Actor %1 queue at %2 messages")
                            .arg(a.name).arg(a.queue_size);
                    }
                }
                return {};
            }
            if (rule.rule_id == "checksum_mismatch") {
                // Checksum alert based on external verification.
                return {};
            }
            return {};
        });

        m_last_throughput = training.throughput_mbps;
        emit alerts_changed();
    }

    // Agent-readable: export current alert state.
    QVariantMap agent_state() const {
        QVariantMap map;
        QVariantList list;
        for (const auto& a : m_engine.active_alerts()) {
            QVariantMap am;
            am["id"]       = a.id;
            am["title"]    = a.title;
            am["message"]  = a.message;
            am["severity"] = a.severity_string();
            am["active"]   = a.active;
            list.append(am);
        }
        map["alerts"]          = list;
        map["active_count"]    = m_engine.active_count();
        map["critical_count"]  = m_engine.critical_count();
        return map;
    }

signals:
    void alerts_changed();

private:
    AlertEngine m_engine;
    double m_last_throughput = 0.0;
};

// ── Dashboard ViewModel (central coordinator) ─────────────────────────

class DashboardViewModel : public QObject {
    Q_OBJECT
    Q_PROPERTY(bool connected READ is_connected NOTIFY connected_changed)
    Q_PROPERTY(int online_nodes READ online_nodes NOTIFY nodes_changed)
    Q_PROPERTY(int total_nodes READ total_nodes NOTIFY nodes_changed)
    Q_PROPERTY(double throughput READ throughput NOTIFY training_changed)
    Q_PROPERTY(int total_steps READ total_steps NOTIFY training_changed)
    Q_PROPERTY(double latency_p50 READ latency_p50 NOTIFY training_changed)
    Q_PROPERTY(double latency_p99 READ latency_p99 NOTIFY training_changed)

public:
    explicit DashboardViewModel(QObject* parent = nullptr)
        : QObject(parent)
        , m_client(new MetricsClient(this))
        , m_actor_vm(new ActorListViewModel(this))
        , m_alert_vm(new AlertViewModel(this))
    {
        QObject::connect(m_client, &MetricsClient::nodes_updated,
                         this, &DashboardViewModel::on_nodes_updated);
        QObject::connect(m_client, &MetricsClient::training_updated,
                         this, &DashboardViewModel::on_training_updated);
        QObject::connect(m_client, &MetricsClient::actors_updated,
                         this, &DashboardViewModel::on_actors_updated);
        QObject::connect(m_client, &MetricsClient::network_updated,
                         this, &DashboardViewModel::on_network_updated);
        QObject::connect(m_client, &MetricsClient::connection_error,
                         this, &DashboardViewModel::on_connection_error);
        QObject::connect(m_client, &MetricsClient::connected_changed,
                         this, &DashboardViewModel::connected_changed);
    }

    // ── Properties ──────────────────────────────────────────────────

    bool is_connected() const { return m_client->is_running(); }
    int online_nodes() const { return m_registry.online_count(); }
    int total_nodes() const { return m_registry.nodes.size(); }
    double throughput() const { return m_history.latest().throughput_mbps; }
    int total_steps() const { return m_history.latest().total_steps; }
    double latency_p50() const { return m_history.latest().latency_p50_ms; }
    double latency_p99() const { return m_history.latest().latency_p99_ms; }

    // ── Sub-ViewModel accessors ─────────────────────────────────────

    ActorListViewModel* actor_vm() const { return m_actor_vm; }
    AlertViewModel* alert_vm() const { return m_alert_vm; }

    // ── Data accessors ──────────────────────────────────────────────

    const NodeRegistry& registry() const { return m_registry; }
    const TrainingHistory& history() const { return m_history; }
    const QVector<NodeInfo>& nodes() const { return m_registry.nodes; }
    const QVector<NetworkStats>& network_stats() const { return m_network_registry.stats; }

    QVector<NodeInfo> nodes_for_qml() const { return m_registry.nodes; }

    QVector<model::WorkerSample> worker_samples() const {
        return m_history.latest().workers;
    }

    // ── Agent-readable: full dashboard state snapshot ───────────────
    //
    // Returns a QVariantMap that can be read by an agent or script to
    // understand the current dashboard state without needing to parse
    // individual UI widgets.

    QVariantMap agent_state() const {
        QVariantMap map;

        // Connection info.
        map["connected"]    = is_connected();
        map["base_url"]     = m_client->base_url();

        // Node topology.
        QVariantList nodes_list;
        for (const auto& n : m_registry.nodes) {
            QVariantMap nm;
            nm["node_id"]     = n.node_id;
            nm["address"]     = n.address;
            nm["status"]      = (n.status == model::NodeStatus::Online) ? "online"
                              : (n.status == model::NodeStatus::Degraded) ? "degraded"
                              : "offline";
            nm["cpu_percent"] = n.cpu_percent;
            nm["memory_mb"]   = static_cast<qlonglong>(n.memory_mb);
            nodes_list.append(nm);
        }
        map["nodes"]       = nodes_list;
        map["online_nodes"] = online_nodes();
        map["total_nodes"]  = total_nodes();

        // Training progress.
        auto snap = m_history.latest();
        QVariantMap training;
        training["timestamp_ms"]   = static_cast<qlonglong>(snap.timestamp_ms);
        training["total_steps"]    = snap.total_steps;
        training["total_bytes"]    = static_cast<qlonglong>(snap.total_bytes);
        training["throughput_mbps"] = snap.throughput_mbps;
        training["latency_p50_ms"]  = snap.latency_p50_ms;
        training["latency_p99_ms"]  = snap.latency_p99_ms;
        training["checksum"]       = snap.checksum;
        training["history_size"]   = static_cast<qlonglong>(m_history.size());
        map["training"] = training;

        // Network stats.
        QVariantList net_list;
        for (const auto& s : m_network_registry.stats) {
            QVariantMap ns;
            ns["peer"]         = s.peer;
            ns["bytes_sent"]   = static_cast<qlonglong>(s.bytes_sent);
            ns["bytes_recv"]   = static_cast<qlonglong>(s.bytes_recv);
            ns["send_mbps"]    = s.send_rate_mbps;
            ns["recv_mbps"]    = s.recv_rate_mbps;
            ns["active_conns"] = s.active_connections;
            net_list.append(ns);
        }
        map["network"]         = net_list;
        map["total_send_mbps"] = m_network_registry.total_send_mbps();
        map["total_recv_mbps"] = m_network_registry.total_recv_mbps();

        return map;
    }

    // ── Slots ───────────────────────────────────────────────────────

public slots:
    void connect_to(const QString& url) { m_client->connect_to(url); }
    void disconnect() { m_client->stop(); }

signals:
    void nodes_changed();
    void training_changed();
    void actors_changed();
    void network_changed();
    void connection_error(const QString& error);
    void connected_changed(bool connected);

private slots:
    void on_nodes_updated(const QVector<NodeInfo>& nodes) {
        m_registry.nodes = nodes;
        emit nodes_changed();
    }

    void on_training_updated(const TrainingSnapshot& snap) {
        m_history.push(snap);
        emit training_changed();
        // Trigger alert evaluation on each training update.
        m_alert_vm->evaluate(m_registry.nodes, snap, m_actor_vm->actors());
    }

    void on_actors_updated(const QVector<ActorStatus>& actors) {
        m_actor_vm->on_actors_updated(actors);
        emit actors_changed();
    }

    void on_network_updated(const QVector<NetworkStats>& stats) {
        m_network_registry.stats = stats;
        emit network_changed();
    }

    void on_connection_error(const QString& err) {
        emit connection_error(err);
    }

private:
    MetricsClient* m_client;
    NodeRegistry m_registry;
    TrainingHistory m_history;
    NetworkStatsRegistry m_network_registry;
    ActorListViewModel* m_actor_vm;
    AlertViewModel* m_alert_vm;
};

} // namespace dashboard::viewmodel

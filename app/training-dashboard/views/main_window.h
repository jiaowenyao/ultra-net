// View: Main dashboard window — assembles all sub-views.
// Provides agent-readable state inspection interface.
#pragma once

#include <QMainWindow>
#include <QStatusBar>
#include <QHBoxLayout>
#include <QVBoxLayout>
#include <QGridLayout>
#include <QLabel>
#include <QFrame>
#include <QLineEdit>
#include <QPushButton>
#include <QGroupBox>
#include <QFile>
#include <QTimer>
#include <QScrollArea>
#include <QJsonDocument>
#include <QJsonObject>
#include <QJsonArray>

#include "topology_widget.h"
#include "training_chart_widget.h"
#include "actor_table_widget.h"
#include "network_widget.h"
#include "alert_panel_widget.h"
#include "../viewmodels/dashboard_viewmodel.h"

namespace dashboard::view {

using viewmodel::DashboardViewModel;

class MainWindow : public QMainWindow {
    Q_OBJECT
public:
    explicit MainWindow(DashboardViewModel* vm, QWidget* parent = nullptr)
        : QMainWindow(parent), m_vm(vm)
    {
        setWindowTitle("Ultra-Net Training Dashboard");
        resize(1400, 900);
        setup_ui();
        setup_style();
        connect_signals();
        update_stats();
    }

    // ── Public accessors for testing / agent inspection ─────────────

    TopologyWidget* topology() { return m_topology; }
    TrainingChartWidget* chart() { return m_chart; }
    ActorTableWidget* actor_table() { return m_actor_table; }
    NetworkWidget* network() { return m_network; }
    AlertPanelWidget* alert_panel() { return m_alert_panel; }
    QLabel* lbl_throughput() { return m_lbl_throughput; }
    QLabel* lbl_steps() { return m_lbl_steps; }
    QLabel* lbl_p50() { return m_lbl_p50; }
    QLabel* lbl_p99() { return m_lbl_p99; }
    QLabel* lbl_nodes() { return m_lbl_nodes; }
    QLabel* lbl_checksum() { return m_lbl_checksum; }
    QLineEdit* addr_input() { return m_addr_input; }

    // ── Agent-readable: full dashboard state as JSON ────────────────
    //
    // Returns a JSON string containing all visible dashboard state.
    // An agent or script can call this to understand the current
    // dashboard without needing to parse individual Qt widgets.

    QString agent_state_json() const {
        QJsonObject root;

        // Connection.
        root["connected"] = m_vm->is_connected();
        root["base_url"]  = m_addr_input->text();

        // Metrics.
        QJsonObject metrics;
        metrics["throughput_mbps"] = m_lbl_throughput->text();
        metrics["total_steps"]     = m_lbl_steps->text();
        metrics["p50_latency"]     = m_lbl_p50->text();
        metrics["p99_latency"]     = m_lbl_p99->text();
        metrics["nodes"]           = m_lbl_nodes->text();
        metrics["checksum"]        = m_lbl_checksum->text();
        root["metrics"] = metrics;

        // Status bar.
        root["status_bar"] = statusBar()->currentMessage();

        return QJsonDocument(root).toJson(QJsonDocument::Compact);
    }

    // Agent-readable: full state as QVariantMap (including sub-views).
    QVariantMap agent_state() const {
        QVariantMap map;
        map["connected"]    = m_vm->is_connected();
        map["base_url"]     = m_addr_input->text();
        map["status_bar"]   = statusBar()->currentMessage();

        // ViewModel state (includes training, nodes, network).
        map["dashboard"]    = m_vm->agent_state();

        // Sub-view states.
        map["actor_table"]  = m_actor_table->agent_state();
        map["network_view"] = m_network->agent_state();
        map["alert_panel"]  = m_alert_panel->agent_state();

        // Current UI values.
        QVariantMap ui;
        ui["throughput"] = m_lbl_throughput->text();
        ui["steps"]      = m_lbl_steps->text();
        ui["p50"]        = m_lbl_p50->text();
        ui["p99"]        = m_lbl_p99->text();
        ui["nodes"]      = m_lbl_nodes->text();
        ui["checksum"]   = m_lbl_checksum->text();
        map["ui"] = ui;

        return map;
    }

private:
    void setup_ui() {
        auto* central = new QWidget(this);
        auto* main_layout = new QHBoxLayout(central);
        main_layout->setContentsMargins(8, 8, 8, 8);
        main_layout->setSpacing(8);

        // ── Left panel ─────────────────────────────────────────────
        auto* left_scroll = new QScrollArea();
        left_scroll->setWidgetResizable(true);
        left_scroll->setStyleSheet(
            "QScrollArea { border: none; background: transparent; }");

        auto* left_container = new QWidget();
        auto* left = new QVBoxLayout(left_container);
        left->setSpacing(6);
        left->setContentsMargins(0, 0, 0, 0);

        // Connection bar.
        auto* conn_layout = new QHBoxLayout();
        m_addr_input = new QLineEdit("http://127.0.0.1:18080");
        m_addr_input->setStyleSheet(
            "background:#12171f;color:#bfc7d5;border:1px solid #1a2332;"
            "border-radius:3px;padding:4px;font-family:Consolas;");
        auto* btn_connect = new QPushButton("Connect");
        btn_connect->setStyleSheet(
            "background:#1f3a4a;color:#39bae6;border:none;border-radius:3px;"
            "padding:4px 12px;font-weight:bold;");
        QObject::connect(btn_connect, &QPushButton::clicked, this, [this]() {
            m_vm->connect_to(m_addr_input->text());
        });
        auto* btn_disconnect = new QPushButton("Disconnect");
        btn_disconnect->setStyleSheet(
            "background:#2d1518;color:#f26d78;border:none;border-radius:3px;"
            "padding:4px 12px;font-weight:bold;");
        QObject::connect(btn_disconnect, &QPushButton::clicked, this, [this]() {
            m_vm->disconnect();
        });
        conn_layout->addWidget(m_addr_input, 1);
        conn_layout->addWidget(btn_connect);
        conn_layout->addWidget(btn_disconnect);
        left->addLayout(conn_layout);

        // Topology.
        auto* topo_group = new QGroupBox("NETWORK TOPOLOGY");
        auto* topo_layout = new QVBoxLayout(topo_group);
        m_topology = new TopologyWidget();
        topo_layout->addWidget(m_topology);
        left->addWidget(topo_group, 2);

        // Actor table.
        auto* actor_group = new QGroupBox("ACTOR STATUS");
        auto* actor_layout = new QVBoxLayout(actor_group);
        m_actor_table = new ActorTableWidget();
        actor_layout->addWidget(m_actor_table);
        left->addWidget(actor_group, 1);

        // Alert panel.
        auto* alert_group = new QGroupBox("ALERTS");
        auto* alert_layout = new QVBoxLayout(alert_group);
        m_alert_panel = new AlertPanelWidget();
        alert_layout->addWidget(m_alert_panel);
        left->addWidget(alert_group, 1);

        left_scroll->setWidget(left_container);
        main_layout->addWidget(left_scroll, 2);

        // ── Right panel ────────────────────────────────────────────
        auto* right = new QVBoxLayout();
        right->setSpacing(6);

        // Status grid.
        auto* status_frame = new QFrame();
        status_frame->setObjectName("panel");
        auto* status_grid = new QGridLayout(status_frame);
        auto add_metric = [&](int row, int col, const QString& label, QLabel*& val) {
            auto* l = new QLabel(label);
            l->setObjectName("metric_label");
            status_grid->addWidget(l, row, col * 2);
            val = new QLabel("—");
            val->setObjectName("value");
            status_grid->addWidget(val, row, col * 2 + 1);
        };
        add_metric(0, 0, "THROUGHPUT", m_lbl_throughput);
        add_metric(0, 1, "STEPS", m_lbl_steps);
        add_metric(1, 0, "P50 LATENCY", m_lbl_p50);
        add_metric(1, 1, "P99 LATENCY", m_lbl_p99);
        add_metric(2, 0, "NODES", m_lbl_nodes);
        add_metric(2, 1, "CHECKSUM", m_lbl_checksum);
        right->addWidget(status_frame);

        // Training chart.
        m_chart = new TrainingChartWidget();
        right->addWidget(m_chart, 3);

        // Network traffic.
        auto* net_group = new QGroupBox("NETWORK TRAFFIC");
        auto* net_layout = new QVBoxLayout(net_group);
        net_layout->setContentsMargins(4, 4, 4, 4);
        m_network = new NetworkWidget();
        net_layout->addWidget(m_network);
        right->addWidget(net_group, 1);

        main_layout->addLayout(right, 3);

        setCentralWidget(central);
        statusBar()->showMessage("Not connected — enter PS URL and click Connect");
    }

    void setup_style() {
        QString qss_path = "../app/training-dashboard/resources/style.qss";
        QFile file(qss_path);
        if (file.open(QFile::ReadOnly)) {
            setStyleSheet(file.readAll());
        }
    }

    void connect_signals() {
        auto* timer = new QTimer(this);
        // 5 Hz UI refresh for smooth updates, data fetches at 1 Hz.
        timer->start(200);
        QObject::connect(timer, &QTimer::timeout, this, &MainWindow::update_stats);

        // ── Nodes ──────────────────────────────────────────────────
        QObject::connect(m_vm, &DashboardViewModel::nodes_changed, this, [this]() {
            m_topology->update_nodes(m_vm->nodes());
        });

        // ── Training ───────────────────────────────────────────────
        QObject::connect(m_vm, &DashboardViewModel::training_changed, this, [this]() {
            m_chart->add_data_point(m_vm->history().latest());
        });

        // ── Actors ─────────────────────────────────────────────────
        QObject::connect(m_vm, &DashboardViewModel::actors_changed, this, [this]() {
            m_actor_table->update_actors(m_vm->actor_vm()->sorted_by_queue());
        });

        // ── Network ────────────────────────────────────────────────
        QObject::connect(m_vm, &DashboardViewModel::network_changed, this, [this]() {
            m_network->update_stats(m_vm->network_stats());
        });

        // ── Connection state ───────────────────────────────────────
        QObject::connect(m_vm, &DashboardViewModel::connected_changed, this, [this](bool c) {
            statusBar()->showMessage(c ? "Connected — polling metrics at 1 Hz"
                                       : "Disconnected");
        });

        // ── Connection errors ──────────────────────────────────────
        QObject::connect(m_vm, &DashboardViewModel::connection_error, this, [this](const QString& err) {
            statusBar()->showMessage("Error: " + err);
        });

        // ── Alerts (from AlertViewModel) ───────────────────────────
        QObject::connect(m_vm->alert_vm(), &viewmodel::AlertViewModel::alerts_changed,
                         this, [this]() {
            m_alert_panel->update_alerts(m_vm->alert_vm()->alerts());
        });
    }

private slots:
    void update_stats() {
        auto snap = m_vm->history().latest();
        m_lbl_throughput->setText(QString::number(snap.throughput_mbps, 'f', 1) + " MB/s");
        m_lbl_steps->setText(QString::number(snap.total_steps));
        m_lbl_p50->setText(QString::number(snap.latency_p50_ms, 'f', 2) + " ms");
        m_lbl_p99->setText(QString::number(snap.latency_p99_ms, 'f', 2) + " ms");
        m_lbl_nodes->setText(QString("%1 / %2")
            .arg(m_vm->online_nodes()).arg(m_vm->total_nodes()));
        m_lbl_checksum->setText(snap.checksum.isEmpty() ? "—" : snap.checksum.left(10));
    }

private:
    DashboardViewModel* m_vm;
    TopologyWidget* m_topology;
    TrainingChartWidget* m_chart;
    ActorTableWidget* m_actor_table;
    NetworkWidget* m_network;
    AlertPanelWidget* m_alert_panel;
    QLineEdit* m_addr_input;
    QLabel *m_lbl_throughput, *m_lbl_steps, *m_lbl_p50, *m_lbl_p99, *m_lbl_nodes, *m_lbl_checksum;
};

} // namespace dashboard::view

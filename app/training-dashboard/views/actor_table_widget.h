// View: Actor status table — shows per-actor queue depth, message rate, and latency.
#pragma once

#include <QWidget>
#include <QVBoxLayout>
#include <QHBoxLayout>
#include <QLabel>
#include <QTableWidget>
#include <QHeaderView>
#include <QPushButton>
#include <QLineEdit>
#include <QTimer>

#include "../models/actor_status.h"
#include "../viewmodels/dashboard_viewmodel.h"

namespace dashboard::view {

using model::ActorStatus;
using viewmodel::ActorListViewModel;

class ActorTableWidget : public QWidget {
    Q_OBJECT
public:
    explicit ActorTableWidget(QWidget* parent = nullptr)
        : QWidget(parent)
    {
        auto* layout = new QVBoxLayout(this);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(4);

        // ── Header ──────────────────────────────────────────────────
        auto* header = new QHBoxLayout();
        auto* title = new QLabel("ACTOR MONITOR");
        title->setStyleSheet(
            "color:#39bae6;font-size:14px;font-weight:bold;font-family:Consolas;");
        header->addWidget(title);

        m_anomaly_label = new QLabel("");
        m_anomaly_label->setStyleSheet(
            "color:#f26d78;font-size:11px;font-family:Consolas;");
        header->addWidget(m_anomaly_label);
        header->addStretch();

        layout->addLayout(header);

        // ── Table ───────────────────────────────────────────────────
        m_table = new QTableWidget(0, 5, this);
        m_table->setHorizontalHeaderLabels(
            {"Name", "Queue", "Msg/s", "Lat(ms)", "State"});
        m_table->horizontalHeader()->setStretchLastSection(true);
        m_table->horizontalHeader()->setSectionResizeMode(
            0, QHeaderView::Stretch);
        m_table->horizontalHeader()->setSectionResizeMode(
            1, QHeaderView::ResizeToContents);
        m_table->horizontalHeader()->setSectionResizeMode(
            2, QHeaderView::ResizeToContents);
        m_table->horizontalHeader()->setSectionResizeMode(
            3, QHeaderView::ResizeToContents);
        m_table->horizontalHeader()->setSectionResizeMode(
            4, QHeaderView::ResizeToContents);
        m_table->setSelectionBehavior(QAbstractItemView::SelectRows);
        m_table->setEditTriggers(QAbstractItemView::NoEditTriggers);
        m_table->setShowGrid(true);
        m_table->setAlternatingRowColors(true);
        m_table->verticalHeader()->setVisible(false);
        m_table->setMinimumHeight(120);

        layout->addWidget(m_table);
    }

    // Public accessors for agent inspection.
    QTableWidget* table() { return m_table; }
    QLabel* anomaly_label() { return m_anomaly_label; }

    // Agent-readable: export visible table state.
    QVariantMap agent_state() const {
        QVariantMap map;
        QVariantList rows;
        for (int r = 0; r < m_table->rowCount(); ++r) {
            QVariantMap row;
            row["name"]    = m_table->item(r, 0)
                           ? m_table->item(r, 0)->text() : "";
            row["queue"]   = m_table->item(r, 1)
                           ? m_table->item(r, 1)->text().toInt() : 0;
            row["msg_rate"] = m_table->item(r, 2)
                           ? m_table->item(r, 2)->text().toDouble() : 0.0;
            row["latency"] = m_table->item(r, 3)
                           ? m_table->item(r, 3)->text().toDouble() : 0.0;
            row["state"]   = m_table->item(r, 4)
                           ? m_table->item(r, 4)->text() : "";
            rows.append(row);
        }
        map["rows"] = rows;
        map["row_count"] = m_table->rowCount();
        return map;
    }

public slots:
    void update_actors(const QVector<ActorStatus>& actors) {
        m_table->setRowCount(actors.size());

        int anomaly_count = 0;
        for (int i = 0; i < actors.size(); ++i) {
            const auto& a = actors[i];

            auto set_cell = [&](int col, const QString& text,
                                const QColor& fg = QColor(0xbf, 0xc7, 0xd5)) {
                auto* item = m_table->item(i, col);
                if (!item) {
                    item = new QTableWidgetItem(text);
                    m_table->setItem(i, col, item);
                } else {
                    item->setText(text);
                }
                item->setForeground(fg);
            };

            set_cell(0, a.name);

            // Queue depth: highlight if high.
            QColor queue_color = (a.queue_size > 50) ? QColor(0xf2, 0x6d, 0x78)
                               : (a.queue_size > 10) ? QColor(0xff, 0xcc, 0x66)
                               : QColor(0xbf, 0xc7, 0xd5);
            set_cell(1, QString::number(a.queue_size), queue_color);

            set_cell(2, QString::number(a.msg_rate_per_sec, 'f', 1));
            set_cell(3, QString::number(a.avg_latency_ms, 'f', 2));

            // State: color-coded.
            QColor state_color = (a.state == "blocked") ? QColor(0xf2, 0x6d, 0x78)
                               : (a.state == "idle")    ? QColor(0xff, 0xcc, 0x66)
                               : QColor(0x7f, 0xd9, 0x62);
            set_cell(4, a.state, state_color);

            if (a.queue_size > 10 || a.state == "blocked") {
                ++anomaly_count;
            }
        }

        // Update anomaly indicator.
        if (anomaly_count > 0) {
            m_anomaly_label->setText(
                QString("⚠ %1 anomalous").arg(anomaly_count));
        } else {
            m_anomaly_label->setText("✓ All normal");
            m_anomaly_label->setStyleSheet(
                "color:#7fd962;font-size:11px;font-family:Consolas;");
        }
    }

private:
    QTableWidget* m_table;
    QLabel* m_anomaly_label;
};

} // namespace dashboard::view

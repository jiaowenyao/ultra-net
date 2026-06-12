// View: Alert panel — shows active alerts with severity indicators.
#pragma once

#include <QWidget>
#include <QVBoxLayout>
#include <QLabel>
#include <QListWidget>
#include <QListWidgetItem>
#include <QDateTime>

#include "../models/alert_rule.h"
#include "../viewmodels/dashboard_viewmodel.h"

namespace dashboard::view {

using model::Alert;
using model::AlertSeverity;
using viewmodel::AlertViewModel;

class AlertPanelWidget : public QWidget {
    Q_OBJECT
public:
    explicit AlertPanelWidget(QWidget* parent = nullptr)
        : QWidget(parent)
    {
        auto* layout = new QVBoxLayout(this);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(4);

        // ── Header ──────────────────────────────────────────────────
        m_title = new QLabel("ALERTS");
        m_title->setStyleSheet(
            "color:#39bae6;font-size:14px;font-weight:bold;font-family:Consolas;padding:4px;");
        layout->addWidget(m_title);

        // ── Status bar ──────────────────────────────────────────────
        m_status = new QLabel("✓ No active alerts");
        m_status->setStyleSheet(
            "color:#7fd962;font-size:11px;font-family:Consolas;padding:2px;");
        layout->addWidget(m_status);

        // ── Alert list ──────────────────────────────────────────────
        m_list = new QListWidget();
        m_list->setStyleSheet(
            "QListWidget {"
            "  background-color: #12171f;"
            "  border: 1px solid #1a2332;"
            "  border-radius: 4px;"
            "  color: #bfc7d5;"
            "  font-family: Consolas;"
            "  font-size: 11px;"
            "}"
            "QListWidget::item {"
            "  padding: 6px;"
            "  border-bottom: 1px solid #1a2332;"
            "}");
        m_list->setMinimumHeight(80);
        m_list->setMaximumHeight(200);
        layout->addWidget(m_list);
    }

    // Public accessors for agent inspection.
    QListWidget* list_widget() { return m_list; }
    QLabel* status_label() { return m_status; }
    QLabel* title_label() { return m_title; }

    // Agent-readable: export current alert state.
    QVariantMap agent_state() const {
        QVariantMap map;
        QVariantList alerts;
        for (int i = 0; i < m_list->count(); ++i) {
            auto* item = m_list->item(i);
            if (item) {
                alerts.append(item->text());
            }
        }
        map["alerts"]       = alerts;
        map["alert_count"]  = m_list->count();
        map["status_text"]  = m_status->text();
        return map;
    }

public slots:
    void update_alerts(const QVector<Alert>& alerts) {
        m_list->clear();

        if (alerts.isEmpty()) {
            m_status->setText("✓ No active alerts");
            m_status->setStyleSheet(
                "color:#7fd962;font-size:11px;font-family:Consolas;padding:2px;");
            return;
        }

        int critical = 0;
        int warning = 0;

        for (const auto& a : alerts) {
            if (!a.active) {
                continue;
            }

            if (a.severity == AlertSeverity::Critical) {
                ++critical;
            } else if (a.severity == AlertSeverity::Warning) {
                ++warning;
            }

            QString icon = (a.severity == AlertSeverity::Critical) ? "🔴"
                         : (a.severity == AlertSeverity::Warning)  ? "🟡"
                         : "ℹ️";

            QString text = QString("%1 [%2] %3\n    %4 — %5")
                .arg(icon)
                .arg(a.severity_string())
                .arg(a.title)
                .arg(a.message)
                .arg(a.raised_at.toString("hh:mm:ss"));

            auto* item = new QListWidgetItem(text);

            QColor bg = (a.severity == AlertSeverity::Critical)
                      ? QColor(0x2d, 0x15, 0x18)
                      : (a.severity == AlertSeverity::Warning)
                      ? QColor(0x2d, 0x25, 0x10)
                      : QColor(0x12, 0x17, 0x1f);
            item->setBackground(bg);
            m_list->addItem(item);
        }

        // Update status bar.
        QString status_text;
        if (critical > 0) {
            status_text = QString("🔴 %1 critical, %2 warnings").arg(critical).arg(warning);
            m_status->setStyleSheet(
                "color:#f26d78;font-size:11px;font-weight:bold;font-family:Consolas;padding:2px;");
        } else if (warning > 0) {
            status_text = QString("🟡 %1 warnings").arg(warning);
            m_status->setStyleSheet(
                "color:#ffcc66;font-size:11px;font-weight:bold;font-family:Consolas;padding:2px;");
        }
        m_status->setText(status_text);
    }

private:
    QLabel* m_title;
    QLabel* m_status;
    QListWidget* m_list;
};

} // namespace dashboard::view

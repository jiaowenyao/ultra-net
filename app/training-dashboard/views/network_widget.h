// View: Network traffic chart — shows per-peer send/recv bandwidth.
#pragma once

#include <QWidget>
#include <QVBoxLayout>
#include <QLabel>
#include <QPainter>
#include <deque>
#include <algorithm>

#include "../models/network_stats.h"

namespace dashboard::view {

using model::NetworkStats;

class NetworkCanvas : public QWidget {
    Q_OBJECT
public:
    explicit NetworkCanvas(QWidget* parent = nullptr)
        : QWidget(parent)
    {
        setAutoFillBackground(true);
        QPalette pal = palette();
        pal.setColor(QPalette::Base, QColor(0x12, 0x17, 0x1f));
        setPalette(pal);
        setMinimumHeight(180);
    }

    void add_data(double send_mbps, double recv_mbps) {
        m_send_history.push_back(send_mbps);
        m_recv_history.push_back(recv_mbps);
        if (m_send_history.size() > 120) {
            m_send_history.pop_front();
            m_recv_history.pop_front();
        }
        update();
    }

protected:
    void paintEvent(QPaintEvent*) override {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing);

        int w = width() - 50;
        int h = height() - 40;
        int ox = 45;
        int oy = 15;

        if (m_send_history.size() < 2) {
            p.setPen(QColor(0x5c, 0x67, 0x73));
            p.drawText(rect(), Qt::AlignCenter, "Waiting for network data...");
            return;
        }

        double ymax = std::max(
            std::max(1.0, *std::max_element(m_send_history.begin(), m_send_history.end())),
            *std::max_element(m_recv_history.begin(), m_recv_history.end()));
        ymax *= 1.1;  // 10% headroom.

        // Grid.
        p.setPen(QPen(QColor(0x1a, 0x23, 0x32), 1));
        for (int i = 0; i <= 4; ++i) {
            int y = oy + h * i / 4;
            p.drawLine(ox, y, ox + w, y);
        }

        // Send (cyan).
        draw_series(p, m_send_history, ox, oy, w, h, ymax, QColor(0x39, 0xba, 0xe6));

        // Recv (green).
        draw_series(p, m_recv_history, ox, oy, w, h, ymax, QColor(0x7f, 0xd9, 0x62));

        // Legend.
        p.setPen(QColor(0x5c, 0x67, 0x73));
        p.setFont(QFont("Consolas", 8));
        p.drawText(ox, oy + h + 16,
            QString("Send: %1 MB/s  |  Recv: %2 MB/s")
                .arg(m_send_history.back(), 0, 'f', 1)
                .arg(m_recv_history.back(), 0, 'f', 1));

        // Y-axis labels.
        p.setPen(QColor(0x5c, 0x67, 0x73));
        p.setFont(QFont("Consolas", 7));
        for (int i = 0; i <= 4; ++i) {
            double val = ymax * (4 - i) / 4;
            p.drawText(0, oy + h * i / 4 + 4,
                       QString::number(val, 'f', 0));
        }
    }

private:
    void draw_series(QPainter& p, const std::deque<double>& data,
                     int ox, int oy, int w, int h,
                     double ymax, QColor color)
    {
        if (data.size() < 2) {
            return;
        }
        p.setPen(QPen(color, 2));
        double xs = static_cast<double>(w)
                  / std::max(static_cast<int>(data.size()) - 1, 1);
        for (size_t i = 1; i < data.size(); ++i) {
            int x1 = ox + static_cast<int>((i - 1) * xs);
            int y1 = oy + h - static_cast<int>(data[i - 1] / ymax * h);
            int x2 = ox + static_cast<int>(i * xs);
            int y2 = oy + h - static_cast<int>(data[i] / ymax * h);
            p.drawLine(x1, y1, x2, y2);
        }
    }

    std::deque<double> m_send_history;
    std::deque<double> m_recv_history;
};

class NetworkWidget : public QWidget {
    Q_OBJECT
public:
    explicit NetworkWidget(QWidget* parent = nullptr)
        : QWidget(parent)
    {
        auto* layout = new QVBoxLayout(this);
        layout->setContentsMargins(0, 0, 0, 0);
        layout->setSpacing(4);

        m_title = new QLabel("NETWORK TRAFFIC");
        m_title->setStyleSheet(
            "color:#39bae6;font-size:14px;font-weight:bold;font-family:Consolas;padding:4px;");
        layout->addWidget(m_title);

        m_canvas = new NetworkCanvas();
        layout->addWidget(m_canvas);

        // Summary labels.
        auto* summary = new QHBoxLayout();
        m_send_label = new QLabel("Send: — MB/s");
        m_send_label->setStyleSheet("color:#39bae6;font-size:11px;font-family:Consolas;");
        m_recv_label = new QLabel("Recv: — MB/s");
        m_recv_label->setStyleSheet("color:#7fd962;font-size:11px;font-family:Consolas;");
        m_conns_label = new QLabel("Conns: 0");
        m_conns_label->setStyleSheet("color:#5c6773;font-size:11px;font-family:Consolas;");
        summary->addWidget(m_send_label);
        summary->addWidget(m_recv_label);
        summary->addStretch();
        summary->addWidget(m_conns_label);
        layout->addLayout(summary);
    }

    NetworkCanvas* canvas() { return m_canvas; }
    QLabel* send_label() { return m_send_label; }
    QLabel* recv_label() { return m_recv_label; }
    QLabel* conns_label() { return m_conns_label; }

    // Agent-readable: export current network state.
    QVariantMap agent_state() const {
        QVariantMap map;
        map["send_mbps"] = m_send_label->text();
        map["recv_mbps"] = m_recv_label->text();
        map["connections"] = m_conns_label->text();
        return map;
    }

public slots:
    void update_stats(const QVector<NetworkStats>& stats) {
        double total_send = 0.0;
        double total_recv = 0.0;
        int total_conns = 0;

        for (const auto& s : stats) {
            total_send += s.send_rate_mbps;
            total_recv += s.recv_rate_mbps;
            total_conns += s.active_connections;
        }

        m_canvas->add_data(total_send, total_recv);

        m_send_label->setText(
            QString("Send: %1 MB/s").arg(total_send, 0, 'f', 1));
        m_recv_label->setText(
            QString("Recv: %1 MB/s").arg(total_recv, 0, 'f', 1));
        m_conns_label->setText(
            QString("Conns: %1").arg(total_conns));
    }

private:
    QLabel* m_title;
    NetworkCanvas* m_canvas;
    QLabel* m_send_label;
    QLabel* m_recv_label;
    QLabel* m_conns_label;
};

} // namespace dashboard::view

// View: Training progress charts — pure QPainter rendering.
#pragma once

#include <QWidget>
#include <QPainter>
#include <QPen>
#include <QFont>
#include <QVBoxLayout>
#include <QLabel>
#include <deque>
#include <algorithm>

#include "../models/training_metrics.h"

namespace dashboard::view {

using model::TrainingSnapshot;

class ChartCanvas : public QWidget {
    Q_OBJECT
public:
    explicit ChartCanvas(QWidget* parent = nullptr) : QWidget(parent) {
        setAutoFillBackground(true);
        QPalette pal = palette();
        pal.setColor(QPalette::Base, QColor(0x12, 0x17, 0x1f));
        setPalette(pal);
        setMinimumHeight(280);
    }

    void add_point(double throughput, double p50, double p99) {
        m_throughput.push_back(throughput);
        m_p50.push_back(p50);
        m_p99.push_back(p99);
        if (m_throughput.size() > 120) {
            m_throughput.pop_front();
            m_p50.pop_front();
            m_p99.pop_front();
        }
        update();
    }

protected:
    void paintEvent(QPaintEvent*) override {
        QPainter p(this);
        p.setRenderHint(QPainter::Antialiasing);
        int w = width() - 60, h = height() - 50, ox = 50, oy = 20;

        if (m_throughput.size() < 2) {
            p.setPen(QColor(0x5c, 0x67, 0x73));
            p.drawText(rect(), Qt::AlignCenter, "Waiting for data...");
            return;
        }

        double tp_max = std::max(1.0, *std::max_element(m_throughput.begin(), m_throughput.end()));
        double lat_max = std::max(1.0, std::max(
            *std::max_element(m_p50.begin(), m_p50.end()),
            *std::max_element(m_p99.begin(), m_p99.end())));

        // Grid lines
        p.setPen(QPen(QColor(0x1a, 0x23, 0x32), 1));
        for (int i = 0; i <= 4; ++i) {
            int y = oy + h * i / 4;
            p.drawLine(ox, y, ox + w, y);
        }

        // Throughput (cyan) - scale to top half
        draw_series(p, m_throughput, ox, oy, w, h / 2, tp_max, QColor(0x39, 0xba, 0xe6));

        // P50 (green) - scale to bottom half
        draw_series(p, m_p50, ox, oy + h / 2, w, h / 2, lat_max, QColor(0x7f, 0xd9, 0x62));

        // P99 (red) - overlay on same scale
        draw_series(p, m_p99, ox, oy + h / 2, w, h / 2, lat_max, QColor(0xf2, 0x6d, 0x78));

        // Legend
        p.setPen(QColor(0x5c, 0x67, 0x73));
        p.setFont(QFont("Consolas", 8));
        p.drawText(ox, oy + h + 18, QString("TP: %1 MB/s  |  P50: %2 ms  |  P99: %3 ms")
            .arg(m_throughput.back(), 0, 'f', 1)
            .arg(m_p50.back(), 0, 'f', 2)
            .arg(m_p99.back(), 0, 'f', 2));
    }

private:
    void draw_series(QPainter& p, const std::deque<double>& data,
                     int ox, int oy, int w, int h, double ymax, QColor color) {
        if (data.size() < 2) return;
        p.setPen(QPen(color, 2));
        double xs = (double)w / std::max((int)data.size() - 1, 1);
        for (size_t i = 1; i < data.size(); ++i) {
            int x1 = ox + (int)((i - 1) * xs);
            int y1 = oy + h - (int)(data[i-1] / ymax * h);
            int x2 = ox + (int)(i * xs);
            int y2 = oy + h - (int)(data[i] / ymax * h);
            p.drawLine(x1, y1, x2, y2);
        }
    }

    std::deque<double> m_throughput;
    std::deque<double> m_p50;
    std::deque<double> m_p99;
};

class TrainingChartWidget : public QWidget {
    Q_OBJECT
public:
    explicit TrainingChartWidget(QWidget* parent = nullptr) : QWidget(parent) {
        auto* layout = new QVBoxLayout(this);
        layout->setContentsMargins(0, 0, 0, 0);
        m_title = new QLabel("TRAINING PROGRESS");
        m_title->setStyleSheet("color:#39bae6;font-size:14px;font-weight:bold;font-family:Consolas;padding:4px;");
        layout->addWidget(m_title);
        m_canvas = new ChartCanvas();
        layout->addWidget(m_canvas);
    }

    ChartCanvas* canvas() { return m_canvas; }

public slots:
    void add_data_point(const TrainingSnapshot& snap) {
        m_canvas->add_point(snap.throughput_mbps, snap.latency_p50_ms, snap.latency_p99_ms);
    }

private:
    QLabel* m_title;
    ChartCanvas* m_canvas;
};

} // namespace dashboard::view

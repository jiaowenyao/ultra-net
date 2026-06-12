// Model: Training metrics for progress charts.
#pragma once

#include <QString>
#include <QJsonObject>
#include <QJsonArray>
#include <QVector>
#include <deque>
#include <algorithm>
#include <numeric>

namespace dashboard::model {

struct WorkerSample {
    int    worker_id;
    int    steps;
    double avg_latency_ms;
    double p99_latency_ms;
};

struct TrainingSnapshot {
    int64_t  timestamp_ms = 0;
    int      total_steps   = 0;
    int64_t  total_bytes   = 0;
    double   throughput_mbps = 0.0;
    double   latency_p50_ms  = 0.0;
    double   latency_p99_ms  = 0.0;
    QString  checksum;
    QVector<WorkerSample> workers;

    static TrainingSnapshot from_json(const QJsonObject& json) {
        TrainingSnapshot s;
        s.timestamp_ms    = static_cast<int64_t>(json["timestamp_ms"].toDouble());
        s.total_steps     = json["total_steps"].toInt();
        s.total_bytes     = static_cast<int64_t>(json["total_bytes"].toDouble());
        s.throughput_mbps = json["throughput_mbps"].toDouble();
        s.latency_p50_ms  = json["latency_p50_ms"].toDouble();
        s.latency_p99_ms  = json["latency_p99_ms"].toDouble();
        s.checksum        = json["checksum"].toString();
        for (const auto& wv : json["workers"].toArray()) {
            QJsonObject w = wv.toObject();
            WorkerSample ws;
            ws.worker_id      = w["id"].toInt();
            ws.steps          = w["steps"].toInt();
            ws.avg_latency_ms = w["avg_latency_ms"].toDouble();
            ws.p99_latency_ms = w["p99_latency_ms"].toDouble();
            s.workers.append(ws);
        }
        return s;
    }
};

// Ring buffer for training history (configurable capacity).
class TrainingHistory {
public:
    static constexpr size_t kDefaultCapacity = 3600;

    explicit TrainingHistory(size_t capacity = kDefaultCapacity)
        : m_capacity(capacity) {}

    void push(TrainingSnapshot snap) {
        if (m_data.size() >= m_capacity) m_data.pop_front();
        m_data.push_back(std::move(snap));
    }

    const std::deque<TrainingSnapshot>& data() const { return m_data; }

    TrainingSnapshot latest() const {
        return m_data.empty() ? TrainingSnapshot{} : m_data.back();
    }

    // Compute derived statistics.
    double avg_throughput_last_n(size_t n) const {
        if (m_data.empty()) return 0;
        size_t count = std::min(n, m_data.size());
        auto start = m_data.end() - static_cast<ssize_t>(count);
        double sum = 0;
        for (auto it = start; it != m_data.end(); ++it) sum += it->throughput_mbps;
        return sum / count;
    }

    void clear() { m_data.clear(); }
    size_t size() const { return m_data.size(); }

private:
    std::deque<TrainingSnapshot> m_data;
    size_t m_capacity;
};

} // namespace dashboard::model

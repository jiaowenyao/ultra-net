// Model: Actor status for the actor monitoring table.
#pragma once

#include <QString>
#include <QJsonObject>
#include <QVector>

namespace dashboard::model {

struct ActorStatus {
    QString  name;
    int      queue_size       = 0;
    double   msg_rate_per_sec = 0.0;
    int64_t  total_messages   = 0;
    double   avg_latency_ms   = 0.0;
    QString  state            = "running";  // running, idle, blocked, stopped

    static ActorStatus from_json(const QJsonObject& json) {
        ActorStatus s;
        s.name             = json["name"].toString();
        s.queue_size       = json["queue_size"].toInt();
        s.msg_rate_per_sec = json["msg_rate_per_sec"].toDouble();
        s.total_messages   = static_cast<int64_t>(json["total_messages"].toDouble());
        s.avg_latency_ms   = json["avg_latency_ms"].toDouble();
        s.state            = json["state"].toString();
        if (s.state.isEmpty()) {
            s.state = "running";
        }
        return s;
    }
};

// Sort/filter helpers exposed for ViewModel use.
enum class ActorSortKey { Name, QueueSize, MsgRate, Latency };

struct ActorListModel {
    QVector<ActorStatus> actors;

    void update_from_json_array(const QJsonArray& arr) {
        actors.clear();
        for (const auto& v : arr) {
            actors.append(ActorStatus::from_json(v.toObject()));
        }
    }

    int total_queue_depth() const {
        int total = 0;
        for (const auto& a : actors) {
            total += a.queue_size;
        }
        return total;
    }

    QVector<ActorStatus> filtered_by_state(const QString& state) const {
        QVector<ActorStatus> result;
        for (const auto& a : actors) {
            if (a.state == state) {
                result.append(a);
            }
        }
        return result;
    }

    // Return actors with queue depth above threshold (anomaly detection).
    QVector<ActorStatus> anomalous(int queue_threshold = 10) const {
        QVector<ActorStatus> result;
        for (const auto& a : actors) {
            if (a.queue_size > queue_threshold || a.state == "blocked") {
                result.append(a);
            }
        }
        return result;
    }
};

} // namespace dashboard::model

// Model: Network statistics for bandwidth monitoring.
#pragma once

#include <QString>
#include <QJsonObject>
#include <QJsonArray>
#include <QVector>

namespace dashboard::model {

struct NetworkStats {
    QString  peer;
    int64_t  bytes_sent     = 0;
    int64_t  bytes_recv     = 0;
    double   send_rate_mbps = 0.0;
    double   recv_rate_mbps = 0.0;
    int      active_connections = 0;

    static NetworkStats from_json(const QJsonObject& json) {
        NetworkStats s;
        s.peer                = json["peer"].toString();
        s.bytes_sent          = static_cast<int64_t>(json["bytes_sent"].toDouble());
        s.bytes_recv          = static_cast<int64_t>(json["bytes_recv"].toDouble());
        s.send_rate_mbps      = json["send_rate_mbps"].toDouble();
        s.recv_rate_mbps      = json["recv_rate_mbps"].toDouble();
        s.active_connections  = json["active_connections"].toInt();
        return s;
    }
};

struct NetworkStatsRegistry {
    QVector<NetworkStats> stats;

    void update_from_json_array(const QJsonArray& arr) {
        stats.clear();
        for (const auto& v : arr) {
            stats.append(NetworkStats::from_json(v.toObject()));
        }
    }

    double total_send_mbps() const {
        double total = 0.0;
        for (const auto& s : stats) {
            total += s.send_rate_mbps;
        }
        return total;
    }

    double total_recv_mbps() const {
        double total = 0.0;
        for (const auto& s : stats) {
            total += s.recv_rate_mbps;
        }
        return total;
    }

    int64_t total_bytes_sent() const {
        int64_t total = 0;
        for (const auto& s : stats) {
            total += s.bytes_sent;
        }
        return total;
    }

    int64_t total_bytes_recv() const {
        int64_t total = 0;
        for (const auto& s : stats) {
            total += s.bytes_recv;
        }
        return total;
    }
};

} // namespace dashboard::model

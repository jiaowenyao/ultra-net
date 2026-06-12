// Model: Node information for topology display.
#pragma once

#include <QString>
#include <QDateTime>
#include <QJsonObject>
#include <QVector>

namespace dashboard::model {

enum class NodeStatus { Online, Degraded, Offline };

struct NodeInfo {
    QString   node_id;
    QString   address;
    NodeStatus status = NodeStatus::Offline;
    double    cpu_percent = 0.0;
    int64_t   memory_mb = 0;
    int64_t   uptime_seconds = 0;
    QDateTime last_seen;

    static NodeInfo from_json(const QJsonObject& json) {
        NodeInfo info;
        info.node_id   = json["node_id"].toString();
        info.address   = json["address"].toString();
        info.cpu_percent = json["cpu_percent"].toDouble();
        info.memory_mb   = static_cast<int64_t>(json["memory_kb"].toDouble() / 1024.0);
        info.uptime_seconds = static_cast<int64_t>(json["uptime_seconds"].toDouble());
        info.last_seen  = QDateTime::currentDateTime();
        QString s = json["status"].toString();
        if (s == "online")  info.status = NodeStatus::Online;
        else if (s == "degraded") info.status = NodeStatus::Degraded;
        else info.status = NodeStatus::Offline;
        return info;
    }
};

struct NodeRegistry {
    QVector<NodeInfo> nodes;

    void update_from_json_array(const QJsonArray& arr) {
        nodes.clear();
        for (const auto& v : arr) {
            nodes.append(NodeInfo::from_json(v.toObject()));
        }
    }

    int online_count() const {
        int n = 0;
        for (auto& node : nodes)
            if (node.status == NodeStatus::Online) ++n;
        return n;
    }
};

} // namespace dashboard::model

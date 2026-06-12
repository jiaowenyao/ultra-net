// Model: HTTP client for fetching metrics from PS node.
#pragma once

#include <QObject>
#include <QNetworkAccessManager>
#include <QNetworkReply>
#include <QTimer>
#include <QJsonDocument>
#include <QJsonArray>
#include <QJsonObject>

#include "node_info.h"
#include "training_metrics.h"
#include "actor_status.h"
#include "network_stats.h"

namespace dashboard::model {

class MetricsClient : public QObject {
    Q_OBJECT
public:
    explicit MetricsClient(QObject* parent = nullptr)
        : m_http(new QNetworkAccessManager(this))
        , m_timer(new QTimer(this)) {
        QObject::connect(m_timer, &QTimer::timeout, this, &MetricsClient::fetch_all);
    }

    void set_base_url(const QString& url) { m_base_url = url; }
    QString base_url() const { return m_base_url; }
    bool is_running() const { return m_timer->isActive(); }

    void start(int interval_ms = 1000) {
        if (m_base_url.isEmpty()) return;
        m_timer->start(interval_ms);
        m_connected = true;
        emit connected_changed(true);
    }

    void stop() {
        m_timer->stop();
        m_connected = false;
        emit connected_changed(false);
    }

signals:
    void nodes_updated(const QVector<NodeInfo>& nodes);
    void training_updated(const TrainingSnapshot& metrics);
    void actors_updated(const QVector<ActorStatus>& actors);
    void network_updated(const QVector<NetworkStats>& stats);
    void connection_error(const QString& error);
    void connected_changed(bool connected);

public slots:
    void connect_to(const QString& url) {
        set_base_url(url);
        start();
    }

private slots:
    void fetch_all() {
        fetch_nodes();
        fetch_training();
        fetch_actors();
        fetch_network();
    }

    void fetch_nodes() {
        QNetworkRequest req(QUrl(m_base_url + "/api/v1/nodes"));
        auto* reply = m_http->get(req);
        QObject::connect(reply, &QNetworkReply::finished, this, [this, reply]() {
            reply->deleteLater();
            if (reply->error() != QNetworkReply::NoError) {
                emit connection_error(reply->errorString());
                return;
            }
            auto doc = QJsonDocument::fromJson(reply->readAll());
            QVector<NodeInfo> nodes;
            for (const auto& v : doc.array()) {
                nodes.append(NodeInfo::from_json(v.toObject()));
            }
            emit nodes_updated(nodes);
        });
    }

    void fetch_training() {
        QNetworkRequest req(QUrl(m_base_url + "/api/v1/training"));
        auto* reply = m_http->get(req);
        QObject::connect(reply, &QNetworkReply::finished, this, [this, reply]() {
            reply->deleteLater();
            if (reply->error() != QNetworkReply::NoError) {
                emit connection_error(reply->errorString());
                return;
            }
            auto doc = QJsonDocument::fromJson(reply->readAll());
            emit training_updated(TrainingSnapshot::from_json(doc.object()));
        });
    }

    void fetch_actors() {
        QNetworkRequest req(QUrl(m_base_url + "/api/v1/actors"));
        auto* reply = m_http->get(req);
        QObject::connect(reply, &QNetworkReply::finished, this, [this, reply]() {
            reply->deleteLater();
            if (reply->error() != QNetworkReply::NoError) {
                // Actors endpoint is optional — don't error if missing.
                return;
            }
            auto doc = QJsonDocument::fromJson(reply->readAll());
            QVector<ActorStatus> actors;
            for (const auto& v : doc.array()) {
                actors.append(ActorStatus::from_json(v.toObject()));
            }
            emit actors_updated(actors);
        });
    }

    void fetch_network() {
        QNetworkRequest req(QUrl(m_base_url + "/api/v1/network"));
        auto* reply = m_http->get(req);
        QObject::connect(reply, &QNetworkReply::finished, this, [this, reply]() {
            reply->deleteLater();
            if (reply->error() != QNetworkReply::NoError) {
                // Network endpoint is optional — don't error if missing.
                return;
            }
            auto doc = QJsonDocument::fromJson(reply->readAll());
            QVector<NetworkStats> stats;
            for (const auto& v : doc.array()) {
                stats.append(NetworkStats::from_json(v.toObject()));
            }
            emit network_updated(stats);
        });
    }

private:
    QNetworkAccessManager* m_http;
    QTimer* m_timer;
    QString m_base_url;
    bool m_connected = false;
};

} // namespace dashboard::model

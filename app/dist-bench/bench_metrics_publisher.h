// bench_metrics_publisher.h — simplified metrics exporter integration.
#pragma once

#include <memory>
#include <string>
#include <vector>
#include <chrono>

#include "ultranet/actor/system/metrics_exporter.h"
#include "bench_common.h"

class bench_metrics_publisher {
public:
    bench_metrics_publisher(uint16_t ps_port, int num_workers)
        : m_ps_port(ps_port)
        , m_num_workers(num_workers) {}

    void set_state(bench_state* state) { m_state = state; }

    // Attach this publisher to a metrics_exporter.
    // Sets up providers for /api/v1/nodes, /training, /actors, /network.
    void attach(ynet::actor::metrics_exporter& exporter) {
        auto t_start = std::chrono::steady_clock::now();

        exporter.set_nodes_provider([this]() -> std::string {
            std::vector<std::pair<std::string, uint16_t>> workers;
            std::vector<bool> online;
            for (int i = 0; i < m_num_workers; ++i) {
                workers.emplace_back("127.0.0.1", static_cast<uint16_t>(0));
                online.push_back(true);
            }
            return ynet::actor::build_nodes_json(
                "127.0.0.1", m_ps_port, workers, online);
        });

        exporter.set_training_provider([this, t_start]() -> std::string {
            ynet::actor::training_snapshot_data snap;
            snap.timestamp_ms = std::chrono::duration_cast<
                std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
            snap.total_steps   = static_cast<int>(m_state->total_steps.load());
            snap.total_bytes   = static_cast<int64_t>(m_state->total_bytes.load());
            snap.latency_p50_ms = m_state->latency.p50();
            snap.latency_p99_ms = m_state->latency.p99();
            snap.checksum       = m_state->checksum.load();
            snap.num_workers    = m_num_workers;

            auto now = std::chrono::steady_clock::now();
            double elapsed_s = std::chrono::duration<double>(
                now - t_start).count();
            if (elapsed_s > 0.001) {
                double data_mb = m_state->total_bytes.load()
                               / 1024.0 / 1024.0;
                snap.throughput_mbps = data_mb / elapsed_s;
            }

            for (int i = 0; i < m_num_workers; ++i) {
                ynet::actor::training_snapshot_data::worker_data wd;
                wd.id = i;
                wd.steps = m_num_steps;
                wd.avg_latency_ms = m_state->latency.avg();
                wd.p99_latency_ms = m_state->latency.p99();
                snap.workers.push_back(wd);
            }
            return ynet::actor::build_training_json(snap);
        });

        exporter.set_actors_provider([this]() -> std::string {
            std::vector<ynet::actor::actor_status_data> actors;
            {
                ynet::actor::actor_status_data a;
                a.name = "ps";
                a.total_messages = m_state->total_messages.load();
                actors.push_back(a);
            }
            for (int i = 0; i < m_num_workers; ++i) {
                ynet::actor::actor_status_data a;
                a.name = "worker-" + std::to_string(i);
                a.total_messages = static_cast<uint64_t>(m_num_steps);
                actors.push_back(a);
            }
            return ynet::actor::build_actors_json(actors);
        });

        exporter.set_network_provider([this]() -> std::string {
            std::vector<ynet::actor::network_stats_data> stats;
            ynet::actor::network_stats_data ns;
            ns.peer = "workers";
            ns.bytes_recv = static_cast<int64_t>(m_state->total_bytes.load());
            ns.bytes_sent = static_cast<int64_t>(
                m_state->total_bytes.load() * 0.01);
            ns.recv_rate_mbps = 0;
            ns.send_rate_mbps = 0;
            ns.active_connections = m_connected;
            stats.push_back(ns);
            return ynet::actor::build_network_json(stats);
        });
    }

    // Store final snapshots and switch to snapshot mode.
    void freeze(ynet::actor::metrics_exporter& exporter) {
        exporter.store_nodes_snapshot(
            ynet::actor::build_nodes_json("127.0.0.1", m_ps_port,
                std::vector<std::pair<std::string, uint16_t>>(
                    m_num_workers, {"127.0.0.1", 0}),
                std::vector<bool>(m_num_workers, true)));

        ynet::actor::training_snapshot_data snap;
        snap.timestamp_ms = std::chrono::duration_cast<
            std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        snap.total_steps   = static_cast<int>(m_state->total_steps.load());
        snap.total_bytes   = static_cast<int64_t>(m_state->total_bytes.load());
        snap.checksum      = m_state->checksum.load();
        snap.latency_p50_ms = m_state->latency.p50();
        snap.latency_p99_ms = m_state->latency.p99();

        double elapsed_s = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - m_t_start).count();
        if (elapsed_s > 0.001) {
            double data_mb = m_state->total_bytes.load() / 1048576.0;
            snap.throughput_mbps = data_mb / elapsed_s;
        }
        for (int i = 0; i < m_num_workers; ++i) {
            ynet::actor::training_snapshot_data::worker_data wd;
            wd.id = i;
            wd.steps = m_num_steps;
            wd.avg_latency_ms = m_state->latency.avg();
            wd.p99_latency_ms = m_state->latency.p99();
            snap.workers.push_back(wd);
        }
        exporter.store_training_snapshot(
            ynet::actor::build_training_json(snap));

        std::vector<ynet::actor::actor_status_data> actors;
        {
            ynet::actor::actor_status_data a;
            a.name = "ps";
            a.total_messages = m_state->total_messages.load();
            actors.push_back(a);
        }
        for (int i = 0; i < m_num_workers; ++i) {
            ynet::actor::actor_status_data a;
            a.name = "worker-" + std::to_string(i);
            a.total_messages = static_cast<uint64_t>(m_num_steps);
            actors.push_back(a);
        }
        exporter.store_actors_snapshot(
            ynet::actor::build_actors_json(actors));

        std::vector<ynet::actor::network_stats_data> net;
        {
            ynet::actor::network_stats_data ns;
            ns.peer = "workers";
            ns.bytes_recv = static_cast<int64_t>(m_state->total_bytes.load());
            ns.bytes_sent = static_cast<int64_t>(
                m_state->total_bytes.load() * 0.01);
            ns.active_connections = m_connected;
            if (elapsed_s > 0.001) {
                ns.recv_rate_mbps = (m_state->total_bytes.load()
                                    / 1048576.0) / elapsed_s;
            }
            net.push_back(ns);
        }
        exporter.store_network_snapshot(
            ynet::actor::build_network_json(net));

        exporter.use_snapshots();
    }

    void set_connected(int c) { m_connected = c; }
    void set_num_steps(int s) { m_num_steps = s; }
    void set_t_start(std::chrono::steady_clock::time_point t) { m_t_start = t; }

private:
    uint16_t m_ps_port;
    int m_num_workers;
    int m_connected = 0;
    int m_num_steps = 0;
    bench_state* m_state = nullptr;
    std::chrono::steady_clock::time_point m_t_start;
};

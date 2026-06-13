// bench_tcp.h — TCP benchmark mode (single-process PS + coroutine workers).
#pragma once

#include <iostream>
#include <chrono>

#include "ultranet/ultranet.h"
#include "bench_common.h"
#include "bench_output.h"
#include "bench_worker.h"
#include "bench_metrics_publisher.h"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

// Handle one client connection: receive gradients, update weights, ack.
inline Task<void> tcp_handle_client(int client_fd, bench_state& state) {
    tune_tcp(client_fd);
    auto recv_buf = std::make_unique<float[]>(state.param_count);
    size_t vec_bytes = state.param_count * sizeof(float);
    float* recv = recv_buf.get();
    float* w = state.weights.get();
    float lr = 0.01f;

    while (true) {
        size_t offset = 0;
        while (offset < vec_bytes) {
            Read reader(client_fd,
                       reinterpret_cast<uint8_t*>(recv) + offset,
                       vec_bytes - offset);
            reader.with_timeout(std::chrono::seconds(30));
            auto rr = co_await reader;
            if (!rr || *rr == 0) {
                co_await Close(client_fd);
                co_return;
            }
            offset += *rr;
        }

        auto t0 = std::chrono::steady_clock::now();
        for (size_t i = 0; i < state.param_count; ++i) {
            w[i] -= lr * recv[i];
        }
        state.checksum.fetch_add(crc32(recv, vec_bytes),
                                  std::memory_order_relaxed);
        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(
            t1 - t0).count();
        state.latency.record(ms);

        state.total_bytes += vec_bytes;
        state.total_steps++;
        state.total_messages++;

        uint8_t ack = 0x01;
        auto wr = co_await Write(client_fd, &ack, 1);
        if (!wr) {
            break;
        }
    }
    co_await Close(client_fd);
}

// TCP PS main: listen, accept workers, collect metrics, return result.
// If launch_workers is true, spawn internal coroutine workers (tcp/ps modes).
// If false, only accept external connections (used by multi-process ps mode).
inline Task<void> tcp_ps_main(const bench_config& cfg,
                               bench_metrics_publisher* pub,
                               bench_result* result,
                               bool launch_workers = true) {
    bench_state state;
    state.param_count = cfg.num_params;
    state.weights = init_weights(cfg.num_params);

    if (pub) {
        pub->set_state(&state);
    }

    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    int listen_fd = *sock;
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg.ps_port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    co_await Listen(listen_fd, cfg.num_workers * 2);

    state.cpu_start = cpu_snapshot::now();

    int connected = 0;

    // ── Metrics exporter (optional) ────────────────────────────────
    std::shared_ptr<ynet::actor::metrics_exporter> exporter;
    if (cfg.metrics_port > 0 && pub) {
        exporter = std::make_shared<ynet::actor::metrics_exporter>();
        pub->set_num_steps(cfg.num_steps);
        pub->attach(*exporter);
        exporter->set_port(cfg.metrics_port);
        exporter->start();
        co_await sleep_for(std::chrono::milliseconds(50));
    }

    // ── Launch internal workers (if requested) ────────────────────
    if (launch_workers) {
        auto* worker_sched = ExecutionContext::current();
        for (int i = 0; i < cfg.num_workers; ++i) {
            auto task = tcp_worker_run(i, cfg.ps_port,
                                       cfg.num_params, cfg.num_steps);
            worker_sched->submit(task.release());
            // Pace submissions to avoid io_uring ENOBUFS in WSL2.
            co_await sleep_for(std::chrono::milliseconds(200));
        }
    }

    // ── Accept workers ────────────────────────────────────────────
    auto accept_deadline = std::chrono::steady_clock::now()
                         + std::chrono::seconds(30);
    while (connected < cfg.num_workers) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            accept_deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) {
            std::cerr << "Error: timeout waiting for workers ("
                      << connected << "/" << cfg.num_workers
                      << " connected)" << std::endl;
            break;
        }

        Accept a(listen_fd);
        a.with_timeout(std::min(remaining, std::chrono::milliseconds(5000)));
        auto c = co_await a;
        if (!c) {
            continue;
        }

        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->submit(tcp_handle_client(*c, state).release());
        }
        ++connected;
        if (pub) {
            pub->set_connected(connected);
        }
    }

    // ── Wait for completion ───────────────────────────────────────
    auto t_xfer_start = std::chrono::steady_clock::now();
    if (pub) {
        pub->set_t_start(t_xfer_start);
    }
    int expected = connected * cfg.num_steps;
    // Use a generous timeout: WSL2 TCP loopback is ~0.7 MB/s.
    // 4 workers × 50000 params × 200 steps × 4 bytes = 160 MB
    // at 0.7 MB/s ≈ 229 seconds.  Use 600s for safety margin.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(600);

    while (static_cast<int>(state.total_steps.load()) < expected) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::cerr << "Error: timeout waiting for steps ("
                      << state.total_steps.load() << "/" << expected
                      << ")" << std::endl;
            break;
        }
        co_await sleep_for(std::chrono::milliseconds(10));
    }

    auto t_xfer_end = std::chrono::steady_clock::now();
    state.cpu_end = cpu_snapshot::now();

    double wall_ms = std::chrono::duration<double, std::milli>(
        t_xfer_end - t_xfer_start).count();
    double cpu_ms = state.cpu_start.elapsed_ms(state.cpu_end);
    double data_mb = state.total_bytes.load() / 1024.0 / 1024.0;

    bench_result r;
    r.mode_label = "TCP Loopback (127.0.0.1)";
    r.total_steps = static_cast<int>(state.total_steps.load());
    r.total_bytes = static_cast<int64_t>(state.total_bytes.load());
    r.checksum = state.checksum.load();
    r.throughput_mbps = (wall_ms > 0) ? data_mb / (wall_ms / 1000.0) : 0;
    r.latency_p50_ms = state.latency.p50();
    r.latency_p99_ms = state.latency.p99();
    r.latency_max_ms = state.latency.max_latency();
    r.wall_ms = wall_ms;
    r.cpu_ms = cpu_ms;
    r.connected_workers = connected;
    r.num_workers = cfg.num_workers;
    r.num_params = cfg.num_params;
    print_results(r);

    if (result) {
        *result = r;
    }

    co_await Close(listen_fd);

    // ── Metrics exporter shutdown ─────────────────────────────────
    if (exporter) {
        if (pub) {
            pub->set_connected(connected);
            pub->freeze(*exporter);
        } else {
            exporter->stop();
            co_await sleep_for(std::chrono::milliseconds(300));
        }
    }
}

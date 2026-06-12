// bench_local.h — local (shared-memory) benchmark mode.
#pragma once

#include <vector>
#include <thread>
#include <iostream>

#include "bench_common.h"
#include "bench_output.h"

inline void local_worker_run(bench_state& ps, int worker_id, int num_steps,
                              size_t param_count, latency_tracker& wm) {
    auto grads = init_gradients(param_count, worker_id);
    size_t vec_bytes = param_count * sizeof(float);
    float lr = 0.01f;
    float* w = ps.weights.get();

    for (int step = 0; step < num_steps; ++step) {
        auto t0 = std::chrono::steady_clock::now();

        for (size_t i = 0; i < param_count; ++i) {
            w[i] -= lr * grads[i];
        }

        ps.checksum.fetch_add(crc32(grads.get(), vec_bytes),
                              std::memory_order_relaxed);

        auto t1 = std::chrono::steady_clock::now();
        double ms = std::chrono::duration<double, std::milli>(
            t1 - t0).count();

        ps.latency.record(ms);
        wm.record(ms);
        ps.total_bytes += vec_bytes;
        ps.total_steps++;
        ps.total_messages++;
    }
}

inline bench_result run_local_bench(const bench_config& cfg) {
    bench_state ps;
    ps.param_count = cfg.num_params;
    ps.weights = init_weights(cfg.num_params);
    ps.cpu_start = cpu_snapshot::now();

    std::vector<std::thread> threads;
    std::vector<latency_tracker> worker_metrics(cfg.num_workers);

    for (int i = 0; i < cfg.num_workers; ++i) {
        threads.emplace_back([&, i]() {
            local_worker_run(ps, i, cfg.num_steps,
                            cfg.num_params, worker_metrics[i]);
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    ps.cpu_end = cpu_snapshot::now();

    double cpu_ms = ps.cpu_start.elapsed_ms(ps.cpu_end);
    double data_mb = ps.total_bytes.load() / 1024.0 / 1024.0;

    bench_result r;
    r.mode_label = "Local (Shared Memory)";
    r.total_steps = static_cast<int>(ps.total_steps.load());
    r.total_bytes = static_cast<int64_t>(ps.total_bytes.load());
    r.checksum = ps.checksum.load();
    r.throughput_mbps = (cpu_ms > 0) ? data_mb / (cpu_ms / 1000.0) : 0;
    r.latency_p50_ms = ps.latency.p50();
    r.latency_p99_ms = ps.latency.p99();
    r.latency_max_ms = ps.latency.max_latency();
    r.wall_ms = cpu_ms;
    r.cpu_ms = cpu_ms;
    r.connected_workers = cfg.num_workers;
    r.num_workers = cfg.num_workers;
    r.num_params = cfg.num_params;

    for (int i = 0; i < cfg.num_workers; ++i) {
        r.worker_avg_latency.push_back(worker_metrics[i].avg());
        r.worker_p99_latency.push_back(worker_metrics[i].p99());
        r.worker_counts.push_back(worker_metrics[i].count());
    }

    print_results(r);
    return r;
}

// bench_output.h — unified result printing for dist-bench.
#pragma once

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>

#include "bench_common.h"

struct bench_result {
    std::string mode_label;
    int total_steps = 0;
    int64_t total_bytes = 0;
    uint32_t checksum = 0;
    double throughput_mbps = 0.0;
    double latency_p50_ms = 0.0;
    double latency_p99_ms = 0.0;
    double latency_max_ms = 0.0;
    double wall_ms = 0.0;
    double cpu_ms = 0.0;
    int connected_workers = 0;
    int num_workers = 0;
    size_t num_params = 0;
    std::vector<double> worker_avg_latency;
    std::vector<double> worker_p99_latency;
    std::vector<size_t> worker_counts;
};

inline void print_results(const bench_result& r) {
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "\n=== " << r.mode_label << " ===" << std::endl;
    std::cout << "  workers:    " << r.connected_workers << std::endl;
    std::cout << "  params:     " << r.num_params << std::endl;
    std::cout << "  steps:      " << r.total_steps << std::endl;

    double data_mb = r.total_bytes / 1024.0 / 1024.0;
    std::cout << "  data:       " << std::fixed << std::setprecision(1)
              << data_mb << " MB" << std::endl;

    std::cout << "  throughput: " << std::fixed << std::setprecision(1)
              << r.throughput_mbps << " MB/s" << std::endl;

    std::cout << "  checksum:   0x" << std::hex << r.checksum
              << std::dec << std::endl;

    if (r.wall_ms > 0) {
        std::cout << "  wall time:  " << std::fixed << std::setprecision(0)
                  << r.wall_ms << " ms" << std::endl;
        std::cout << "  CPU time:   " << std::fixed << std::setprecision(0)
                  << r.cpu_ms << " ms" << std::endl;
        std::cout << "  CPU util:   " << std::fixed << std::setprecision(0)
                  << (r.cpu_ms / r.wall_ms * 100.0) << "%" << std::endl;
    }

    std::cout << "  latency P50:" << std::fixed << std::setprecision(3)
              << r.latency_p50_ms << " ms" << std::endl;
    std::cout << "  latency P99:" << std::fixed << std::setprecision(3)
              << r.latency_p99_ms << " ms" << std::endl;
    std::cout << "  latency max:" << std::fixed << std::setprecision(3)
              << r.latency_max_ms << " ms" << std::endl;

    // Per-worker breakdown.
    if (!r.worker_avg_latency.empty()) {
        std::cout << "  per-worker latency:" << std::endl;
        for (size_t i = 0; i < r.worker_avg_latency.size(); ++i) {
            std::cout << "    W" << i << ": avg=" << std::fixed
                      << std::setprecision(3) << r.worker_avg_latency[i]
                      << "ms  p99=" << r.worker_p99_latency[i]
                      << "ms  count=" << r.worker_counts[i] << std::endl;
        }
    }
}

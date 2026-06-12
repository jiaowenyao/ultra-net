// bench_common.h — shared state, configuration, and helpers for dist-bench.
#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <memory>
#include <random>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <iostream>
#include <iomanip>
#include <chrono>
#include <sys/resource.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

// ── CRC32 checksum ─────────────────────────────────────────────────────

inline uint32_t crc32(const void* data, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    auto* p = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
        crc ^= p[i];
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }
    return crc ^ 0xFFFFFFFF;
}

// ── CPU usage ──────────────────────────────────────────────────────────

struct cpu_snapshot {
    unsigned long long utime = 0;
    unsigned long long stime = 0;

    static cpu_snapshot now() {
        std::ifstream f("/proc/self/stat");
        std::string line;
        std::getline(f, line);
        cpu_snapshot s{};
        size_t pos = 0;
        int idx = 0;
        while (pos < line.size() && idx < 14) {
            pos = line.find(' ', pos);
            if (pos != std::string::npos) { ++pos; ++idx; }
        }
        if (pos < line.size()) {
            char* end = nullptr;
            s.utime = strtoull(line.c_str() + pos, &end, 10);
            s.stime = strtoull(end + 1, nullptr, 10);
        }
        return s;
    }

    double elapsed_ms(const cpu_snapshot& other) const {
        return (utime - other.utime + stime - other.stime)
             * 1000.0 / sysconf(_SC_CLK_TCK);
    }
};

// ── Latency tracker ────────────────────────────────────────────────────

class latency_tracker {
public:
    void record(double ms) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_samples.push_back(ms);
    }

    double p50() const { return percentile(0.50); }
    double p99() const { return percentile(0.99); }
    double p999() const { return percentile(0.999); }

    double avg() const {
        if (m_samples.empty()) { return 0; }
        return std::accumulate(m_samples.begin(), m_samples.end(), 0.0)
             / m_samples.size();
    }

    size_t count() const { return m_samples.size(); }

    double max_latency() const {
        return m_samples.empty() ? 0
            : *std::max_element(m_samples.begin(), m_samples.end());
    }

private:
    double percentile(double p) const {
        if (m_samples.empty()) { return 0; }
        auto s = m_samples;
        std::sort(s.begin(), s.end());
        return s[static_cast<size_t>(s.size() * p)];
    }
    std::vector<double> m_samples;
    mutable std::mutex m_mutex;
};

// ── Shared benchmark state ─────────────────────────────────────────────

struct bench_state {
    std::unique_ptr<float[]> weights;
    size_t param_count = 0;
    std::atomic<size_t> total_bytes{0};
    std::atomic<size_t> total_steps{0};
    std::atomic<size_t> total_messages{0};
    std::atomic<uint32_t> checksum{0};
    latency_tracker latency;
    cpu_snapshot cpu_start;
    cpu_snapshot cpu_end;
};

// ── Benchmark configuration ────────────────────────────────────────────

struct bench_config {
    std::string mode;
    int num_workers = 4;
    size_t num_params = 10000;
    int num_steps = 100;
    int batch_size = 1;
    uint16_t ps_port = 18001;
    int metrics_port = 0;
    std::string worker_addr = "127.0.0.1:18001";
    int worker_id = 0;

    static bench_config parse(int argc, char* argv[]);
    static void print_usage();
};

// ── TCP tuning ─────────────────────────────────────────────────────────

inline void tune_tcp(int fd) {
    int buf = 256 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
}

// ── Gradient initialization ────────────────────────────────────────────

inline std::unique_ptr<float[]> init_gradients(size_t param_count,
                                                int worker_id) {
    auto grads = std::make_unique<float[]>(param_count);
    for (size_t i = 0; i < param_count; ++i) {
        grads[i] = 0.1f + (static_cast<float>(worker_id) * 0.01f);
    }
    return grads;
}

// ── Weight initialization ──────────────────────────────────────────────

inline std::unique_ptr<float[]> init_weights(size_t param_count) {
    auto weights = std::make_unique<float[]>(param_count);
    std::mt19937 rng(42);
    std::normal_distribution<float> dist(0.0f, 1.0f);
    for (size_t i = 0; i < param_count; ++i) {
        weights[i] = dist(rng);
    }
    return weights;
}

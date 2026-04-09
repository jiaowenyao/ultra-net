// benchmarks/benchmark.hpp - Common benchmark utilities
#pragma once

#include <chrono>
#include <iostream>
#include <iomanip>
#include <atomic>
#include <vector>
#include <thread>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>

struct BenchmarkResult {
    std::string name;
    double duration_sec;
    uint64_t total_ops;
    uint64_t total_bytes;
    double ops_per_sec;
    double mb_per_sec;
    double avg_latency_us;
    double min_latency_us;
    double max_latency_us;
    double p99_latency_us;

    void print() const {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << "=== " << name << " ===" << std::endl;
        std::cout << "  Duration:      " << duration_sec << " s" << std::endl;
        std::cout << "  Total ops:     " << total_ops << std::endl;
        std::cout << "  Ops/sec:       " << std::setprecision(0) << ops_per_sec << std::endl;
        std::cout << "  Throughput:    " << std::setprecision(2) << mb_per_sec << " MB/s" << std::endl;
        std::cout << "  Avg latency:   " << std::setprecision(2) << avg_latency_us << " us" << std::endl;
        std::cout << "  P99 latency:   " << p99_latency_us << " us" << std::endl;
        std::cout << "  Max latency:   " << max_latency_us << " us" << std::endl;
        std::cout << std::endl;
    }
};

inline void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

inline int create_server_socket(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    if (listen(fd, 128) < 0) {
        close(fd);
        return -1;
    }

    set_nonblocking(fd);
    return fd;
}

inline int create_client_socket(const char* host, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    set_nonblocking(fd);
    return fd;
}

// 计算延迟百分位数
inline double calc_percentile(std::vector<double>& latencies, double percentile) {
    if (latencies.empty()) return 0;
    std::sort(latencies.begin(), latencies.end());
    size_t idx = static_cast<size_t>(latencies.size() * percentile);
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx];
}
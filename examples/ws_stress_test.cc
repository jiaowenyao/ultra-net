#include "ultranet/ultranet.h"
#include <iostream>
#include <vector>
#include <atomic>
#include <chrono>
#include <thread>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <signal.h>
#include <unistd.h>
#include <dirent.h>
#include <mutex>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net::websocket;

// Bounded, thread-safe latency sample buffer for long-running tests.
struct LatencyCollector {
    static constexpr size_t MAX_SAMPLES = 100000;
    mutable std::mutex mtx;
    std::vector<int64_t> samples;

    void push_back(int64_t us) { push(us); }
    void push(int64_t us) {
        std::lock_guard<std::mutex> lk(mtx);
        if (samples.size() >= MAX_SAMPLES) {
            // keep most recent 75%
            size_t keep = MAX_SAMPLES * 3 / 4;
            std::copy(samples.end() - keep, samples.end(), samples.begin());
            samples.resize(keep);
        }
        samples.push_back(us);
    }

    std::vector<int64_t> snapshot() const {
        std::lock_guard<std::mutex> lk(mtx);
        return samples;
    }
};

struct WsStressConfig {
    std::string name = "default";
    std::string host = "127.0.0.1";
    int port = 9001;
    int concurrency = 16;
    int messages = 100;
    int payload_size = 64;
    int duration_sec = 0;
    int timeout_ms = 5000;
    bool record_latency = true;
    bool verbose = false;
};

struct WsTestReport {
    std::string name;
    double wall_sec = 0;
    int64_t total_ops = 0, total_errors = 0, total_timeouts = 0;
    double qps = 0;
    double avg_lat_us = 0, p50_us = 0, p99_us = 0;
    int64_t rss_before_kb = 0, rss_after_kb = 0, rss_delta_kb = 0;
    int fd_before = 0, fd_after = 0, fd_delta = 0;
};

static int64_t get_rss_kb() {
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line)) {
        if (line.starts_with("VmRSS:")) {
            std::istringstream iss(line);
            std::string t; int64_t v; std::string u;
            iss >> t >> v >> u;
            return v;
        }
    }
    return -1;
}

static int count_fds() {
    int n = 0;
    DIR* d = opendir("/proc/self/fd");
    if (!d) return -1;
    while (readdir(d)) ++n;
    closedir(d);
    return n - 2;
}

// === WS Client Coroutine (shared implementation) ===
template<typename LatSink>
Task<void> ws_client_impl(int id, const WsStressConfig& cfg,
                          std::atomic<int64_t>& ops, std::atomic<int64_t>& errs,
                          std::atomic<int64_t>& tos,
                          LatSink& lat_sink) {
    try {
        auto ws = co_await WebSocket::connect(cfg.host, cfg.port, "/",
            std::chrono::milliseconds(cfg.timeout_ms));
        if (!ws.is_open()) { errs.fetch_add(cfg.messages); co_return; }

        std::string payload(cfg.payload_size, 'A' + (id % 26));
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(cfg.duration_sec);
        bool time_bounded = (cfg.duration_sec > 0);
        int count = 0;

        while (true) {
            if (!time_bounded && count >= cfg.messages) break;
            if (time_bounded && std::chrono::steady_clock::now() >= deadline) break;

            auto t0 = std::chrono::steady_clock::now();

            WebSocketFrame frame;
            if (cfg.payload_size <= 64)
                frame = WebSocketFrame::text(payload);
            else
                frame = WebSocketFrame::binary(payload);

            try {
                co_await ws.write_frame(frame);
                auto resp = co_await ws.read_frame();
                if (resp.opcode == OpCode::Close) { errs.fetch_add(1); break; }
                ops.fetch_add(1);

                if (cfg.record_latency) {
                    auto t1 = std::chrono::steady_clock::now();
                    auto us = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0).count();
                    lat_sink.push_back(us);
                }
                ++count;
            } catch (const std::system_error& e) {
                if (e.code().value() == ETIMEDOUT) tos.fetch_add(1);
                else errs.fetch_add(1);
                break;
            }
        }

        try { co_await ws.close(); } catch (...) {}
    } catch (const std::system_error& e) {
        if (e.code().value() == ETIMEDOUT) tos.fetch_add(cfg.messages);
        else errs.fetch_add(cfg.messages);
    } catch (...) {
        errs.fetch_add(cfg.messages);
    }
}

static WsTestReport run_test(const WsStressConfig& cfg) {
    WsTestReport r;
    r.name = cfg.name;
    r.rss_before_kb = get_rss_kb();
    r.fd_before = count_fds();

    std::atomic<int64_t> ops{0}, errs{0}, tos{0};
    std::vector<std::vector<int64_t>> per_thread_latencies(cfg.concurrency);
    std::vector<std::thread> threads;

    auto t0 = std::chrono::steady_clock::now();

    for (int i = 0; i < cfg.concurrency; ++i) {
        threads.emplace_back([&, i] {
            IoUringEngine::Scope engine_scope;
            scheduling::WorkStealingThreadPool pool(1);
            ExecutionContext::Scope exec_scope(&pool);
            auto& lat = per_thread_latencies[i];
            pool.submit(ws_client_impl(i, cfg, ops, errs, tos, lat).release());
            pool.wait_all();
        });
    }
    for (auto& t : threads) t.join();

    auto t1 = std::chrono::steady_clock::now();
    r.wall_sec = std::chrono::duration<double>(t1 - t0).count();
    r.total_ops = ops.load();
    r.total_errors = errs.load();
    r.total_timeouts = tos.load();
    r.rss_after_kb = get_rss_kb();
    r.rss_delta_kb = r.rss_after_kb - r.rss_before_kb;
    r.fd_after = count_fds();
    r.fd_delta = r.fd_after - r.fd_before;

    if (r.wall_sec > 0) r.qps = static_cast<double>(r.total_ops) / r.wall_sec;

    // Merge per-thread latencies
    std::vector<int64_t> all_latencies;
    for (auto& lat : per_thread_latencies)
        all_latencies.insert(all_latencies.end(), lat.begin(), lat.end());

    if (!all_latencies.empty()) {
        std::sort(all_latencies.begin(), all_latencies.end());
        int64_t sum = 0;
        for (auto v : all_latencies) sum += v;
        r.avg_lat_us = static_cast<double>(sum) / all_latencies.size();
        r.p50_us = all_latencies[all_latencies.size() / 2];
        r.p99_us = all_latencies[all_latencies.size() * 99 / 100];
    }

    return r;
}

static void print_separator() {
    std::cout << std::string(72, '-') << std::endl;
}

static void print_report(const WsTestReport& r) {
    print_separator();
    std::cout << "  " << r.name << std::endl;
    print_separator();
    std::cout << std::left << std::setw(20) << "Wall time:" << std::fixed << std::setprecision(2) << r.wall_sec << " s" << std::endl;
    std::cout << std::left << std::setw(20) << "Total ops:" << r.total_ops << std::endl;
    std::cout << std::left << std::setw(20) << "Errors:" << r.total_errors << std::endl;
    std::cout << std::left << std::setw(20) << "Timeouts:" << r.total_timeouts << std::endl;
    std::cout << std::left << std::setw(20) << "QPS:" << std::fixed << std::setprecision(0) << r.qps << std::endl;
    std::cout << std::left << std::setw(20) << "Latency avg:" << std::fixed << std::setprecision(0) << r.avg_lat_us << " us" << std::endl;
    std::cout << std::left << std::setw(20) << "Latency p50:" << std::fixed << std::setprecision(0) << r.p50_us << " us" << std::endl;
    std::cout << std::left << std::setw(20) << "Latency p99:" << std::fixed << std::setprecision(0) << r.p99_us << " us" << std::endl;
    std::cout << std::left << std::setw(20) << "RSS:" << r.rss_before_kb << " -> " << r.rss_after_kb << " KB (d=" << r.rss_delta_kb << ")" << std::endl;
    std::cout << std::left << std::setw(20) << "FDs:" << r.fd_before << " -> " << r.fd_after << " (d=" << r.fd_delta << ")" << std::endl;
    print_separator();
}

// === Scenarios ===

void run_burst(int port) {
    std::cout << "\n=== WebSocket Burst Throughput ===\n";
    std::vector<int> cons = {1, 2, 4, 8, 16, 32, 64};
    for (int c : cons) {
        WsStressConfig cfg;
        cfg.name = "burst-c" + std::to_string(c);
        cfg.port = port; cfg.concurrency = c; cfg.messages = 500; cfg.payload_size = 64;
        auto r = run_test(cfg);
        double err_pct = (r.total_ops + r.total_errors + r.total_timeouts) > 0
            ? 100.0 * (r.total_errors + r.total_timeouts) / (r.total_ops + r.total_errors + r.total_timeouts) : 0;
        std::cout << "c=" << c << "  QPS=" << std::fixed << std::setprecision(0) << r.qps
                  << "  err=" << std::fixed << std::setprecision(1) << err_pct << "%"
                  << "  p50=" << std::fixed << std::setprecision(0) << r.p50_us << "us"
                  << "  p99=" << r.p99_us << "us"
                  << "  FD_d=" << r.fd_delta
                  << "  RSS_d=" << r.rss_delta_kb << "KB";
        if (r.fd_delta > 10 || err_pct > 5.0) std::cout << "  **ISSUE**";
        std::cout << std::endl;
    }
}

void run_conn_storm(int port) {
    std::cout << "\n=== WebSocket Connection Storm ===\n";
    std::vector<int> cons = {32, 64, 128, 256};
    for (int c : cons) {
        WsStressConfig cfg;
        cfg.name = "conn-storm-c" + std::to_string(c);
        cfg.port = port; cfg.concurrency = c; cfg.messages = 1; cfg.payload_size = 64;
        auto r = run_test(cfg);
        int64_t total = r.total_ops + r.total_errors + r.total_timeouts;
        double ok_pct = total > 0 ? 100.0 * r.total_ops / total : 100;
        std::cout << "c=" << c << "  ok=" << std::fixed << std::setprecision(1) << ok_pct << "%"
                  << "  ops=" << r.total_ops << "  errs=" << r.total_errors
                  << "  FD_d=" << r.fd_delta;
        if (r.fd_delta > 20 || ok_pct < 90.0) std::cout << "  **ISSUE**";
        std::cout << std::endl;
    }
}

void run_msg_sizes(int port) {
    std::cout << "\n=== WebSocket Message Size Variation ===\n";
    std::vector<int> sizes = {64, 256, 1024, 4096, 16384, 65536};
    for (int sz : sizes) {
        WsStressConfig cfg;
        cfg.name = "msg-" + std::to_string(sz) + "B";
        cfg.port = port; cfg.concurrency = 8; cfg.messages = 50; cfg.payload_size = sz;
        auto r = run_test(cfg);
        double bandwidth = r.qps * sz / (1024.0 * 1024.0);
        std::cout << "sz=" << sz << "B  QPS=" << std::fixed << std::setprecision(0) << r.qps
                  << "  BW=" << std::setprecision(1) << bandwidth << " MB/s"
                  << "  p50=" << std::setprecision(0) << r.p50_us << "us"
                  << "  p99=" << r.p99_us << "us" << std::endl;
    }
}

void run_longhaul(int port) {
    std::cout << "\n=== WebSocket Long-Haul Stability (10s sustained) ===\n";
    for (int c : {1, 4, 16, 32}) {
        WsStressConfig cfg;
        cfg.name = "longhaul-c" + std::to_string(c) + "-10s";
        cfg.port = port; cfg.concurrency = c; cfg.duration_sec = 10; cfg.payload_size = 256;
        auto r = run_test(cfg);
        print_report(r);
    }
}

void run_endurance(int port) {
    constexpr int DURATION_SEC = 3600;
    constexpr int REPORT_SEC = 300; // every 5 minutes
    constexpr int CONCURRENCY = 8;
    constexpr int PAYLOAD_SIZE = 256;

    std::cout << "\n=== WebSocket 1-Hour Endurance Test ===\n";
    std::cout << "Duration: " << DURATION_SEC / 60 << " min | "
              << "Concurrency: " << CONCURRENCY << " | "
              << "Payload: " << PAYLOAD_SIZE << "B | "
              << "Report every: " << REPORT_SEC / 60 << " min\n\n";

    int64_t rss_start = get_rss_kb();
    int fd_start = count_fds();

    std::atomic<int64_t> ops{0}, errs{0}, tos{0};
    std::vector<LatencyCollector> lat_cols(CONCURRENCY);
    std::vector<std::thread> threads;
    std::atomic<bool> monitor_running{true};

    auto test_start = std::chrono::steady_clock::now();

    for (int i = 0; i < CONCURRENCY; ++i) {
        threads.emplace_back([&, i] {
            IoUringEngine::Scope engine_scope;
            scheduling::WorkStealingThreadPool pool(1);
            ExecutionContext::Scope exec_scope(&pool);
            WsStressConfig cfg;
            cfg.host = "127.0.0.1";
            cfg.port = port;
            cfg.duration_sec = DURATION_SEC;
            cfg.payload_size = PAYLOAD_SIZE;
            cfg.timeout_ms = 5000;
            cfg.record_latency = true;
            pool.submit(ws_client_impl(i, cfg, ops, errs, tos, lat_cols[i]).release());
            pool.wait_all();
        });
    }

    // Background monitor: reports cumulative metrics every REPORT_SEC
    std::thread monitor([&] {
        int64_t prev_ops = 0;
        auto prev_time = test_start;
        int tick = 0;

        while (monitor_running.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(REPORT_SEC));
            if (!monitor_running.load()) break;
            ++tick;

            auto now = std::chrono::steady_clock::now();
            double total_elapsed = std::chrono::duration<double>(now - test_start).count();
            double intv_sec = std::chrono::duration<double>(now - prev_time).count();

            int64_t cur_ops = ops.load();
            int64_t cur_errs = errs.load();
            int64_t intv_ops = cur_ops - prev_ops;
            double intv_qps = intv_sec > 0 ? intv_ops / intv_sec : 0;
            double avg_qps = total_elapsed > 0 ? cur_ops / total_elapsed : 0;

            int64_t rss_mb = get_rss_kb() / 1024;
            int fds = count_fds();

            // Snapshot and merge latency samples from all threads
            std::vector<int64_t> lat_snap;
            for (auto& lc : lat_cols) {
                auto s = lc.snapshot();
                lat_snap.insert(lat_snap.end(), s.begin(), s.end());
            }
            int64_t p50 = 0, p99 = 0;
            if (!lat_snap.empty()) {
                std::sort(lat_snap.begin(), lat_snap.end());
                p50 = lat_snap[lat_snap.size() / 2];
                p99 = lat_snap[lat_snap.size() * 99 / 100];
            }

            std::cout << "[" << std::setw(2) << tick * (REPORT_SEC / 60) << "m] "
                      << "cum_ops=" << std::setw(10) << cur_ops << "  "
                      << "intv_qps=" << std::fixed << std::setprecision(0) << std::setw(7) << intv_qps << "  "
                      << "avg_qps=" << std::setw(7) << avg_qps << "  "
                      << "p50=" << std::setw(5) << p50 << "us  "
                      << "p99=" << std::setw(6) << p99 << "us  "
                      << "errs=" << cur_errs << "  "
                      << "tos=" << tos.load() << "  "
                      << "RSS=" << rss_mb << "MB  "
                      << "FDs=" << fds
                      << std::endl;

            prev_ops = cur_ops;
            prev_time = now;
        }
    });

    for (auto& t : threads) t.join();
    monitor_running.store(false);
    monitor.join();

    // Final report
    auto test_end = std::chrono::steady_clock::now();
    double total_sec = std::chrono::duration<double>(test_end - test_start).count();

    int64_t total_ops = ops.load();
    int64_t total_errs = errs.load();
    int64_t total_tos = tos.load();
    int64_t rss_end = get_rss_kb();
    int fd_end = count_fds();

    // Final latency snapshot
    std::vector<int64_t> final_lat;
    for (auto& lc : lat_cols) {
        auto s = lc.snapshot();
        final_lat.insert(final_lat.end(), s.begin(), s.end());
    }
    int64_t final_p50 = 0, final_p99 = 0;
    if (!final_lat.empty()) {
        std::sort(final_lat.begin(), final_lat.end());
        final_p50 = final_lat[final_lat.size() / 2];
        final_p99 = final_lat[final_lat.size() * 99 / 100];
    }

    std::cout << "\n";
    print_separator();
    std::cout << "  1-HOUR ENDURANCE TEST — FINAL REPORT\n";
    print_separator();
    std::cout << std::left << std::setw(20) << "Wall time:" << std::fixed << std::setprecision(1) << total_sec << " s"
              << " (" << std::setprecision(0) << total_sec / 60 << " min)\n";
    std::cout << std::left << std::setw(20) << "Total ops:" << total_ops << "\n";
    std::cout << std::left << std::setw(20) << "Total errors:" << total_errs << "\n";
    std::cout << std::left << std::setw(20) << "Total timeouts:" << total_tos << "\n";
    std::cout << std::left << std::setw(20) << "Average QPS:" << std::fixed << std::setprecision(0)
              << (total_sec > 0 ? total_ops / total_sec : 0) << "\n";
    std::cout << std::left << std::setw(20) << "Error rate:" << std::fixed << std::setprecision(4)
              << (total_ops + total_errs > 0 ? 100.0 * total_errs / (total_ops + total_errs) : 0) << "%\n";
    std::cout << std::left << std::setw(20) << "p50 latency:" << final_p50 << " us\n";
    std::cout << std::left << std::setw(20) << "p99 latency:" << final_p99 << " us\n";
    std::cout << std::left << std::setw(20) << "Latency samples:" << final_lat.size() << "\n";
    std::cout << std::left << std::setw(20) << "RSS:"
              << rss_start << " -> " << rss_end << " KB"
              << " (delta=" << (rss_end - rss_start) / 1024 << " MB"
              << ", rate=" << std::fixed << std::setprecision(2)
              << (total_sec > 0 ? (rss_end - rss_start) / total_sec : 0) << " KB/s)\n";
    std::cout << std::left << std::setw(20) << "FDs:"
              << fd_start << " -> " << fd_end
              << " (delta=" << (fd_end - fd_start) << ")\n";

    double err_pct = total_ops + total_errs > 0 ? 100.0 * total_errs / (total_ops + total_errs) : 0;
    if (total_errs > 0 || err_pct > 0.01 || (fd_end - fd_start) > 5) {
        std::cout << "\n  ** WARNING: anomalies detected! **\n";
    } else {
        std::cout << "\n  ** PASS: No anomalies detected **\n";
    }
    print_separator();
}

void run_concurrent_connections(int port) {
    std::cout << "\n=== WebSocket Concurrent Connections ===\n";
    std::vector<int> cons = {8, 16, 32, 64};
    for (int c : cons) {
        WsStressConfig cfg;
        cfg.name = "concurrent-" + std::to_string(c);
        cfg.port = port; cfg.concurrency = c; cfg.messages = 10; cfg.payload_size = 128;
        auto r = run_test(cfg);
        double err_pct = (r.total_ops + r.total_errors + r.total_timeouts) > 0
            ? 100.0 * r.total_errors / (r.total_ops + r.total_errors + r.total_timeouts) : 0;
        std::cout << "c=" << c << "  QPS=" << std::fixed << std::setprecision(0) << r.qps
                  << "  err=" << std::setprecision(1) << err_pct << "%"
                  << "  avg_lat=" << std::setprecision(0) << r.avg_lat_us << "us"
                  << "  p99=" << r.p99_us << "us"
                  << "  FD_d=" << r.fd_delta
                  << "  RSS_d=" << r.rss_delta_kb << "KB" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    std::string scenario = "all";
    int port = 9001;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--scenario" && i + 1 < argc) scenario = argv[++i];
        else if (arg == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
    }

    std::cout << "ultranet WebSocket stress test\n";
    std::cout << "target: 127.0.0.1:" << port << "\n";
    std::cout << "cpu: 2 cores | memory: 2GB\n";
    std::cout << "scenario: " << scenario << "\n";

    // Wait for server to be ready
    std::cout << "\nWaiting for server..." << std::flush;
    for (int i = 0; i < 20; ++i) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) == 0) { ::close(fd); break; }
        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
    }
    std::cout << " ready\n";

    if (scenario == "all" || scenario == "burst") run_burst(port);
    if (scenario == "all" || scenario == "conn_storm") run_conn_storm(port);
    if (scenario == "all" || scenario == "msg_sizes") run_msg_sizes(port);
    if (scenario == "all" || scenario == "longhaul") run_longhaul(port);
    if (scenario == "all" || scenario == "concurrent") run_concurrent_connections(port);
    if (scenario == "endurance") run_endurance(port);

    std::cout << "\nAll WebSocket stress tests complete.\n";
    return 0;
}

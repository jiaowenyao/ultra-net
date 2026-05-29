#include "ultranet/ultranet.h"
#include <iostream>
#include <vector>
#include <atomic>
#include <chrono>
#include <thread>
#include <fstream>
#include <sstream>
#include <signal.h>
#include <unistd.h>
#include <dirent.h>

using namespace ynet::async;
using namespace ynet::async::io;

struct StressConfig {
    std::string name = "default";
    int port = 18080;
    int concurrency = 16;
    int messages = 100;
    int payload_size = 64;
    int duration_sec = 0;
    int timeout_ms = 0;
    bool verbose = false;
};

struct TestReport {
    std::string name;
    double wall_sec = 0;
    int64_t total_ops = 0, total_errors = 0, total_timeouts = 0;
    double qps = 0;
    double p50 = 0, p90 = 0, p99 = 0, p999 = 0, avg_lat = 0;
    int64_t rss_before_kb = 0, rss_after_kb = 0;
    int fd_before = 0, fd_after = 0;
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

static ynet::metrics::Histogram* g_lat = nullptr;
static ynet::metrics::Counter* g_ops_c = nullptr;
static ynet::metrics::Counter* g_err_c = nullptr;
static ynet::metrics::Counter* g_to_c = nullptr;

static void init_metrics() {
    auto& r = ynet::metrics::MetricRegistry::instance();
    r.reset();
    g_lat = r.histogram("latency_seconds", "Request latency");
    g_ops_c = r.counter("ops_total", "Successful operations");
    g_err_c = r.counter("errors_total", "Failed operations");
    g_to_c = r.counter("timeouts_total", "Timeout operations");
}

Task<void> stress_client(int id, const StressConfig& cfg,
                         std::atomic<int64_t>& ops, std::atomic<int64_t>& errs,
                         std::atomic<int64_t>& tos) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) { errs.fetch_add(cfg.messages); g_err_c->inc(cfg.messages); co_return; }
    int fd = *sock;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(static_cast<uint16_t>(cfg.port));

    auto conn = Connect(fd, (sockaddr*)&addr, sizeof(addr));
    if (cfg.timeout_ms > 0) conn.with_timeout(std::chrono::milliseconds(cfg.timeout_ms));
    auto cr = co_await conn;
    if (!cr) {
        errs.fetch_add(cfg.messages); g_err_c->inc(cfg.messages);
        co_await Close(fd);
        co_return;
    }

    std::string payload(cfg.payload_size, 'A' + (id % 26));
    std::vector<char> recv_buf(cfg.payload_size);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(cfg.duration_sec);
    bool time_bounded = (cfg.duration_sec > 0);
    int count = 0;

    while (true) {
        if (!time_bounded && count >= cfg.messages) break;
        if (time_bounded && std::chrono::steady_clock::now() >= deadline) break;

        auto t0 = std::chrono::steady_clock::now();

        auto writer = Write(fd, payload.data(), payload.size());
        if (cfg.timeout_ms > 0) writer.with_timeout(std::chrono::milliseconds(cfg.timeout_ms));
        auto w = co_await writer;
        if (!w) {
            if (w.error().value() == static_cast<int>(std::errc::timed_out)) {
                tos.fetch_add(1); g_to_c->inc();
            } else {
                errs.fetch_add(1); g_err_c->inc();
            }
            break;
        }

        auto reader = Read(fd, recv_buf.data(), recv_buf.size());
        if (cfg.timeout_ms > 0) reader.with_timeout(std::chrono::milliseconds(cfg.timeout_ms));
        auto r = co_await reader;
        if (!r) {
            if (r.error().value() == static_cast<int>(std::errc::timed_out)) {
                tos.fetch_add(1); g_to_c->inc();
            } else {
                errs.fetch_add(1); g_err_c->inc();
            }
            break;
        }

        g_lat->observe(std::chrono::duration<double>(
            std::chrono::steady_clock::now() - t0).count());
        ops.fetch_add(1); g_ops_c->inc();
        ++count;
    }

    co_await Close(fd);
    co_return;
}

static void run_client_sequential(const StressConfig& cfg,
                                   std::atomic<int64_t>& ops, std::atomic<int64_t>& errs,
                                   std::atomic<int64_t>& tos) {
    IoUringEngine::Scope engine_scope;
    for (int i = 0; i < cfg.concurrency; ++i) {
        auto task = stress_client(i, cfg, ops, errs, tos);
        task.resume();
        while (!task.handle().done()) {
            if (auto* eng = IoUringEngine::current()) {
                eng->for_each_cqe([](io_uring_cqe* cqe) {
                    auto* cb = reinterpret_cast<IoCallback*>(io_uring_cqe_get_data(cqe));
                    if (cb) { cb->m_result = cqe->res; cb->m_completed = true; }
                });
            }
            task.handle().resume();
        }
    }
}

static TestReport run_test(const StressConfig& cfg) {
    init_metrics();
    TestReport r;
    r.name = cfg.name;
    r.rss_before_kb = get_rss_kb();
    r.fd_before = count_fds();

    std::atomic<int64_t> ops{0}, errs{0}, tos{0};

    auto t0 = std::chrono::steady_clock::now();

    run_client_sequential(cfg, ops, errs, tos);

    auto t1 = std::chrono::steady_clock::now();
    r.wall_sec = std::chrono::duration<double>(t1 - t0).count();
    r.total_ops = ops.load();
    r.total_errors = errs.load();
    r.total_timeouts = tos.load();
    r.rss_after_kb = get_rss_kb();
    r.fd_after = count_fds();

    if (r.wall_sec > 0) {
        r.qps = static_cast<double>(r.total_ops) / r.wall_sec;
    }
    r.p50 = g_lat->p50();
    r.p90 = g_lat->p90();
    r.p99 = g_lat->p99();
    r.p999 = g_lat->p999();
    r.avg_lat = g_lat->count() > 0 ? g_lat->sum() / g_lat->count() : 0;

    return r;
}

static void print_report(const TestReport& r) {
    std::cout << "\n============================================================\n";
    std::cout << "  " << r.name << "\n";
    std::cout << "============================================================\n";
    std::cout << "Wall:    " << r.wall_sec << " s\n";
    std::cout << "Ops:     " << r.total_ops << "\n";
    std::cout << "Errors:  " << r.total_errors << "\n";
    std::cout << "Timeouts:" << r.total_timeouts << "\n";
    std::cout << "QPS:     " << r.qps << "\n";
    std::cout << "Latency: p50=" << r.p50 << " p90=" << r.p90
              << " p99=" << r.p99 << " p999=" << r.p999
              << " avg=" << r.avg_lat << "\n";
    std::cout << "RSS:     " << r.rss_before_kb << " -> " << r.rss_after_kb
              << " KB (d=" << (r.rss_after_kb - r.rss_before_kb) << ")\n";
    std::cout << "FDs:     " << r.fd_before << " -> " << r.fd_after
              << " (d=" << (r.fd_after - r.fd_before) << ")\n";
    std::cout << "============================================================\n";
}

// === Scenarios ===

void run_burst(int port) {
    std::cout << "\n--- Burst Throughput (clients vs echo_server) ---\n";
    std::vector<int> cons = {1, 2, 4, 8, 16, 32, 64};

    for (int c : cons) {
        StressConfig cfg;
        cfg.name = "burst-c" + std::to_string(c);
        cfg.port = port;
        cfg.concurrency = c;
        cfg.messages = 500;
        cfg.payload_size = 64;

        auto r = run_test(cfg);

        int64_t total = r.total_ops + r.total_errors + r.total_timeouts;
        double e_pct = total > 0 ? 100.0 * (r.total_errors + r.total_timeouts) / total : 0;
        int fd_delta = r.fd_after - r.fd_before;
        std::cout << "c=" << c << "  QPS=" << r.qps << "  err=" << e_pct << "%"
                  << "  p50=" << r.p50 << "  p99=" << r.p99
                  << "  RSS_d=" << (r.rss_after_kb - r.rss_before_kb) << "KB"
                  << "  FD_d=" << fd_delta;
        if (fd_delta > 5 || e_pct > 5.0) std::cout << "  **ISSUE**";
        std::cout << "\n";
    }
}

void run_conn_storm(int port) {
    std::cout << "\n--- Connection Storm ---\n";
    std::vector<int> cons = {32, 64, 128, 256};

    for (int c : cons) {
        StressConfig cfg;
        cfg.name = "conn_storm-c" + std::to_string(c);
        cfg.port = port;
        cfg.concurrency = c;
        cfg.messages = 1;
        cfg.payload_size = 64;

        auto r = run_test(cfg);
        int64_t total = r.total_ops + r.total_errors + r.total_timeouts;
        double ok_pct = total > 0 ? 100.0 * r.total_ops / total : 100;
        int fd_delta = r.fd_after - r.fd_before;
        std::cout << "c=" << c << "  ok=" << ok_pct << "%"
                  << "  errors=" << r.total_errors
                  << "  FD_d=" << fd_delta;
        if (fd_delta > 10 || ok_pct < 95.0) std::cout << "  **ISSUE**";
        std::cout << "\n";
    }
}

void run_backpressure(int port) {
    std::cout << "\n--- Backpressure (tight loop, no timeout) ---\n";
    StressConfig cfg;
    cfg.name = "backpressure";
    cfg.port = port;
    cfg.concurrency = 128;
    cfg.messages = 2000;
    cfg.payload_size = 16;
    cfg.timeout_ms = 0;

    auto r = run_test(cfg);
    print_report(r);
}

void run_longhaul(int port) {
    std::cout << "\n--- Long-Haul Stability (2s sustained, 1 client) ---\n";
    StressConfig cfg;
    cfg.name = "longhaul-2s";
    cfg.port = port;
    cfg.concurrency = 1;
    cfg.duration_sec = 2;
    cfg.payload_size = 256;

    auto r = run_test(cfg);
    print_report(r);
}

void run_timeout_stress(int port) {
    std::cout << "\n--- Timeout Stress ---\n";
    std::vector<int> tos = {1, 5, 10, 50};

    for (int t : tos) {
        StressConfig cfg;
        cfg.name = "timeout-" + std::to_string(t) + "ms";
        cfg.port = port;
        cfg.concurrency = 32;
        cfg.messages = 100;
        cfg.payload_size = 4096;
        cfg.timeout_ms = t;

        auto r = run_test(cfg);
        int64_t total = r.total_ops + r.total_errors + r.total_timeouts;
        std::cout << "t=" << t << "ms"
                  << "  ops=" << r.total_ops
                  << "  tos=" << r.total_timeouts
                  << "  errs=" << r.total_errors
                  << "  avg_lat=" << r.avg_lat << "\n";
    }
}

void run_mixed(int port) {
    std::cout << "\n--- Mixed Workload (small vs large payload) ---\n";

    StressConfig cfg_small;
    cfg_small.name = "mixed-small";
    cfg_small.port = port;
    cfg_small.concurrency = 16;
    cfg_small.messages = 200;
    cfg_small.payload_size = 64;

    StressConfig cfg_large;
    cfg_large.name = "mixed-large";
    cfg_large.port = port;
    cfg_large.concurrency = 16;
    cfg_large.messages = 200;
    cfg_large.payload_size = 4096;

    std::atomic<int64_t> ops_s{0}, errs_s{0}, tos_s{0};
    std::atomic<int64_t> ops_l{0}, errs_l{0}, tos_l{0};

    init_metrics();
    auto t0 = std::chrono::steady_clock::now();

    run_client_sequential(cfg_small, ops_s, errs_s, tos_s);
    run_client_sequential(cfg_large, ops_l, errs_l, tos_l);

    auto t1 = std::chrono::steady_clock::now();
    double wall = std::chrono::duration<double>(t1 - t0).count();

    std::cout << "small(64B):  ops=" << ops_s.load()
              << "  errs=" << errs_s.load() << "\n";
    std::cout << "large(4K):   ops=" << ops_l.load()
              << "  errs=" << errs_l.load() << "\n";
    std::cout << "wall=" << wall << "s"
              << "  p50=" << g_lat->p50() << "  p99=" << g_lat->p99() << "\n";
}

int main(int argc, char* argv[]) {
    std::string scenario = "all";
    std::string server_host = "127.0.0.1";
    int port = 18080;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--scenario" && i + 1 < argc) scenario = argv[++i];
        else if (arg == "--port" && i + 1 < argc) port = std::stoi(argv[++i]);
        else if (arg == "--host" && i + 1 < argc) server_host = argv[++i];
    }

    std::cout << "ultranet stress client\n";
    std::cout << "target: " << server_host << ":" << port << "\n";
    std::cout << "scenario: " << scenario << "\n";

    if (scenario == "all" || scenario == "burst")       run_burst(port);
    if (scenario == "all" || scenario == "conn_storm")  run_conn_storm(port);
    if (scenario == "all" || scenario == "backpressure") run_backpressure(port);
    if (scenario == "all" || scenario == "longhaul")    run_longhaul(port);
    if (scenario == "all" || scenario == "timeout")     run_timeout_stress(port);
    if (scenario == "all" || scenario == "mixed")       run_mixed(port);

    std::cout << "\nAll stress tests complete.\n";
    return 0;
}

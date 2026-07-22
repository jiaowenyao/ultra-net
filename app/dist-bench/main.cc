// Distributed Training Benchmark v5 — modular, clean CLI, proper error handling.
//
// Modes:
//   local   — single-process shared-memory (no TCP)
//   tcp     — single-process TCP PS + coroutine workers
//   ps      — standalone PS process
//   worker  — standalone worker process (connect to remote PS)
//   serve   — run benchmark + keep metrics server alive for Dashboard
//
// Usage:
//   dist-bench local  <workers> <params> <steps>
//   dist-bench tcp    <workers> <params> <steps> [ps_port] [metrics_port]
//   dist-bench ps     <workers> <params> <steps> <port> [metrics_port]
//   dist-bench worker <addr> <id> <steps> [batch] [params]
//   dist-bench serve  <metrics_port> [workers] [params] [steps] [ps_port]

#include <iostream>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <thread>
#include <chrono>

#include "bench_common.h"
#include "bench_output.h"
#include "bench_local.h"
#include "bench_tcp.h"
#include "bench_worker.h"
#include "bench_metrics_publisher.h"

#include "ultranet/ultranet.h"

using namespace ynet::async;
using namespace ynet::async::lifecycle;

// ── bench_config::parse ─────────────────────────────────────────────────

bench_config bench_config::parse(int argc, char* argv[]) {
    bench_config cfg;

    if (argc < 2) {
        print_usage();
        std::exit(1);
    }

    cfg.mode = argv[1];

    auto parse_int = [](const char* s, const char* name) -> int {
        char* end = nullptr;
        long v = std::strtol(s, &end, 10);
        if (end == s || *end != '\0' || v < 0) {
            std::cerr << "Error: invalid " << name << " '" << s << "'"
                      << std::endl;
            std::exit(1);
        }
        return static_cast<int>(v);
    };

    auto parse_size = [](const char* s, const char* name) -> size_t {
        char* end = nullptr;
        unsigned long long v = std::strtoull(s, &end, 10);
        if (end == s || *end != '\0') {
            std::cerr << "Error: invalid " << name << " '" << s << "'"
                      << std::endl;
            std::exit(1);
        }
        return static_cast<size_t>(v);
    };

    if (cfg.mode == "local") {
        if (argc > 2) { cfg.num_workers = parse_int(argv[2], "workers"); }
        if (argc > 3) { cfg.num_params = parse_size(argv[3], "params"); }
        if (argc > 4) { cfg.num_steps  = parse_int(argv[4], "steps"); }
    } else if (cfg.mode == "tcp") {
        if (argc > 2) { cfg.num_workers = parse_int(argv[2], "workers"); }
        if (argc > 3) { cfg.num_params = parse_size(argv[3], "params"); }
        if (argc > 4) { cfg.num_steps  = parse_int(argv[4], "steps"); }
        if (argc > 5) { cfg.ps_port     = static_cast<uint16_t>(parse_int(argv[5], "ps_port")); }
        if (argc > 6) { cfg.metrics_port = parse_int(argv[6], "metrics_port"); }
    } else if (cfg.mode == "ps") {
        if (argc > 2) { cfg.num_workers = parse_int(argv[2], "workers"); }
        if (argc > 3) { cfg.num_params = parse_size(argv[3], "params"); }
        if (argc > 4) { cfg.num_steps  = parse_int(argv[4], "steps"); }
        if (argc > 5) { cfg.ps_port     = static_cast<uint16_t>(parse_int(argv[5], "port")); }
        if (argc > 6) { cfg.metrics_port = parse_int(argv[6], "metrics_port"); }
    } else if (cfg.mode == "worker") {
        if (argc > 2) { cfg.worker_addr = argv[2]; }
        if (argc > 3) { cfg.worker_id   = parse_int(argv[3], "worker_id"); }
        if (argc > 4) { cfg.num_steps   = parse_int(argv[4], "steps"); }
        if (argc > 5) { cfg.batch_size  = parse_int(argv[5], "batch"); }
        if (argc > 6) { cfg.num_params  = parse_size(argv[6], "params"); }
    } else if (cfg.mode == "serve") {
        if (argc > 2) { cfg.metrics_port = parse_int(argv[2], "metrics_port"); }
        if (argc > 3) { cfg.num_workers  = parse_int(argv[3], "workers"); }
        if (argc > 4) { cfg.num_params   = parse_size(argv[4], "params"); }
        if (argc > 5) { cfg.num_steps    = parse_int(argv[5], "steps"); }
        if (argc > 6) { cfg.ps_port      = static_cast<uint16_t>(parse_int(argv[6], "ps_port")); }
    } else if (cfg.mode == "--help" || cfg.mode == "-h" || cfg.mode == "help") {
        print_usage();
        std::exit(0);
    } else {
        std::cerr << "Error: unknown mode '" << cfg.mode << "'" << std::endl;
        print_usage();
        std::exit(1);
    }

    return cfg;
}

void bench_config::print_usage() {
    std::cout << R"(Usage: dist-bench <mode> [options]

Modes:
  local   <workers> <params> <steps>
          Shared-memory benchmark (no network)

  tcp     <workers> <params> <steps> [ps_port=18001] [metrics_port=0]
          Single-process TCP PS with coroutine workers

  ps      <workers> <params> <steps> <port> [metrics_port=18080]
          Standalone PS process (for multi-process testing)

  worker  <addr> <id> <steps> [batch=1] [params=10000]
          Standalone worker process (connect to remote PS)

  serve   <metrics_port> [workers=2] [params=500] [steps=20] [ps_port=18001]
          Run short benchmark + keep metrics server alive for Dashboard

  --help  Show this help
)";
}

// ── Main ───────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    std::signal(SIGPIPE, SIG_IGN);

    bench_config cfg = bench_config::parse(argc, argv);

    if (cfg.mode == "local") {
        run_local_bench(cfg);
        return 0;
    }

    if (cfg.mode == "tcp") {
        int threads = cfg.num_workers + 2;
        return Launcher().threads(threads).run(
            [cfg](ShutdownCoordinator&) -> Task<void> {
                bench_metrics_publisher pub(cfg.ps_port, cfg.num_workers);
                co_await tcp_ps_main(cfg, (cfg.metrics_port > 0 ? &pub : nullptr),
                                     nullptr);
            });
    }

    if (cfg.mode == "ps") {
        int threads = cfg.num_workers + 2;
        return Launcher().threads(threads).run(
            [cfg](ShutdownCoordinator&) -> Task<void> {
                bench_metrics_publisher pub(cfg.ps_port, cfg.num_workers);
                co_await tcp_ps_main(cfg, (cfg.metrics_port > 0 ? &pub : nullptr),
                                     nullptr, /*launch_workers=*/false);
            });
    }

    if (cfg.mode == "worker") {
        return Launcher().threads(2).run(
            [cfg](lifecycle::ShutdownCoordinator&) -> Task<void> {
                co_await worker_mode_run(cfg);
            });
    }

    if (cfg.mode == "serve") {
        int threads = cfg.num_workers + 2;
        std::cout << "[serve] Starting benchmark + metrics on :"
                  << cfg.metrics_port << std::endl;
        std::cout << "[serve] Dashboard URL: http://127.0.0.1:"
                  << cfg.metrics_port << std::endl;
        std::cout << "[serve] Press Ctrl-C to stop" << std::endl;

        return Launcher().threads(threads).run(
            [cfg](ShutdownCoordinator&) -> Task<void> {
                bench_result result;
                bench_metrics_publisher pub(cfg.ps_port, cfg.num_workers);

                // Phase 1: Run benchmark (no metrics interference).
                co_await tcp_ps_main(
                    bench_config{
                        .mode = "tcp",
                        .num_workers = cfg.num_workers,
                        .num_params = cfg.num_params,
                        .num_steps = cfg.num_steps,
                        .ps_port = cfg.ps_port,
                        .metrics_port = 0,  // no metrics during benchmark
                    },
                    nullptr, &result);

                // Phase 2: Start metrics server with snapshot data.
                auto exporter = std::make_shared<
                    ynet::actor::metrics_exporter>();
                exporter->set_port(cfg.metrics_port);

                auto t_now = std::chrono::steady_clock::now();

                // Build snapshots from result.
                {
                    std::vector<std::pair<std::string, uint16_t>> wrk;
                    std::vector<bool> online;
                    for (int i = 0; i < cfg.num_workers; ++i) {
                        wrk.emplace_back("127.0.0.1",
                                         static_cast<uint16_t>(0));
                        online.push_back(true);
                    }
                    exporter->store_nodes_snapshot(
                        ynet::actor::build_nodes_json(
                            "127.0.0.1", cfg.ps_port, wrk, online));
                }

                {
                    ynet::actor::training_snapshot_data snap;
                    snap.timestamp_ms = std::chrono::duration_cast<
                        std::chrono::milliseconds>(
                        std::chrono::system_clock::now()
                            .time_since_epoch()).count();
                    snap.total_steps    = result.total_steps;
                    snap.total_bytes    = result.total_bytes;
                    snap.throughput_mbps = result.throughput_mbps;
                    snap.latency_p50_ms  = result.latency_p50_ms;
                    snap.latency_p99_ms  = result.latency_p99_ms;
                    snap.checksum        = result.checksum;
                    for (int i = 0; i < cfg.num_workers; ++i) {
                        ynet::actor::training_snapshot_data::worker_data wd;
                        wd.id = i;
                        wd.steps = cfg.num_steps;
                        wd.avg_latency_ms = result.latency_p50_ms;
                        wd.p99_latency_ms = result.latency_p99_ms;
                        snap.workers.push_back(wd);
                    }
                    exporter->store_training_snapshot(
                        ynet::actor::build_training_json(snap));
                }

                {
                    std::vector<ynet::actor::actor_status_data> actors;
                    ynet::actor::actor_status_data a;
                    a.name = "ps";
                    a.total_messages = static_cast<uint64_t>(
                        result.total_steps);
                    actors.push_back(a);
                    for (int i = 0; i < cfg.num_workers; ++i) {
                        ynet::actor::actor_status_data wa;
                        wa.name = "worker-" + std::to_string(i);
                        wa.total_messages = static_cast<uint64_t>(
                            cfg.num_steps);
                        actors.push_back(wa);
                    }
                    exporter->store_actors_snapshot(
                        ynet::actor::build_actors_json(actors));
                }

                {
                    std::vector<ynet::actor::network_stats_data> net;
                    ynet::actor::network_stats_data ns;
                    ns.peer = "workers";
                    ns.bytes_recv = result.total_bytes;
                    ns.bytes_sent = result.total_bytes / 100;
                    ns.recv_rate_mbps = result.throughput_mbps;
                    ns.active_connections = result.connected_workers;
                    net.push_back(ns);
                    exporter->store_network_snapshot(
                        ynet::actor::build_network_json(net));
                }

                exporter->use_snapshots();
                exporter->start();

                std::cout << "[serve] Benchmark complete. Metrics at "
                          << "http://127.0.0.1:" << cfg.metrics_port
                          << std::endl;
                std::cout << "[serve] Press Ctrl-C to stop" << std::endl;

                while (true) {
                    std::this_thread::sleep_for(
                        std::chrono::seconds(1));
                }
            });
    }

    return 1;
}

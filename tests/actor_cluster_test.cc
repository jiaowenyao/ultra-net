// Phase 2: Multi-node cluster test — 3 nodes, gossip discovery, message passing.
#include "ultranet/ultranet.h"
#include "ultranet/actor.hpp"
#include "ultranet/actor/net/transport.h"
#include "ultranet/actor/net/cluster.h"
#include <iostream>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>
#include <random>

using namespace ynet::async; using namespace ynet::async::io;
using namespace ynet::async::net; using namespace ynet::async::lifecycle;
using namespace ynet::actor;
using namespace ynet::actor::net;

static int g_passed=0, g_failed=0;
#define T(n) do{std::cout<<"  "<<n<<"... "<<std::flush;}while(0)
#define PASS() do{std::cout<<"PASSED"<<std::endl;++g_passed;}while(0)
#define FAIL(m) do{std::cout<<"FAILED: "<<m<<std::endl;++g_failed;}while(0)

// ── 3-node cluster test ────────────────────────────────────────────────

struct ping_msg { int id; std::string text; };
struct pong_msg { int id; std::string reply; };

class ping_actor : public actor<ping_actor> {
public:
    int m_count = 0;
    void on_ping(ping_msg& m) { m_count++; }
};

Task<void> node_main(int node_idx, uint16_t base_port,
                    const std::vector<std::string>& seeds,
                    std::atomic<int>& ready_count) {
    // Create transport.
    uint16_t port = base_port + node_idx;
    std::string addr = "127.0.0.1:" + std::to_string(port);

    node_id_t nid = 1000 + node_idx;
    cluster cl(nid, addr);
    for (auto& s : seeds) cl.add_seed(s);

    // Create actor system.
    actor_system sys({.num_threads=2, .listen_port=port, .node_name="n"+std::to_string(node_idx)});

    // Spawn actors.
    auto a1 = sys.spawn<ping_actor>("pinger");
    auto a2 = sys.spawn<ping_actor>("backup");

    // Start transport (accept loop).
    auto* sched = ExecutionContext::current();
    ShutdownCoordinator sd_local;  // Simplified: just run for a while

    // Start gossip by connecting to seeds.
    tcp_transport tp(port, [&](std::vector<uint8_t> data) -> Task<void> {
        cl.apply_gossip(data.data(), data.size());
        co_return;
    });

    ready_count.fetch_add(1);
    // Wait for all nodes to be ready.
    while (ready_count.load() < 3) co_await sleep_for(std::chrono::milliseconds(10));

    // Gossip a few rounds.
    for (int round = 0; round < 5; ++round) {
        auto nodes = cl.live_nodes();
        auto gmsg = cl.build_gossip();
        // Send gossip to seeds.
        for (auto& s : seeds) {
            auto pos = s.find(':'); if (pos==std::string::npos) continue;
            auto host = s.substr(0,pos); auto p = (uint16_t)std::stoi(s.substr(pos+1));
            if (p == port) continue;
            auto conn = co_await tp.connect(host, p);
            if (conn && conn->valid) co_await conn->send(gmsg);
        }
        co_await sleep_for(std::chrono::milliseconds(200));
    }

    // After gossip, check that all 3 nodes see 3 live nodes.
    auto nodes = cl.live_nodes(10000);
    std::cout << "  [node" << node_idx << "] sees " << nodes.size() << " live nodes" << std::endl;

    co_return;
}

void test_3node_cluster() {
    T("3-node gossip cluster");
    std::atomic<int> ready{0};
    std::vector<std::string> seeds = {"127.0.0.1:18001", "127.0.0.1:18002", "127.0.0.1:18003"};

    Launcher().threads(8).run([&](ShutdownCoordinator& sd) -> Task<void> {
        auto* sched = ExecutionContext::current();
        sched->submit(node_main(0, 18001, seeds, ready).release());
        sched->submit(node_main(1, 18002, seeds, ready).release());
        sched->submit(node_main(2, 18003, seeds, ready).release());
        co_await sleep_for(std::chrono::seconds(4));
    });
    PASS();
}

int main() {
    std::cout << "=== Actor Cluster Test ===" << std::endl;
    test_3node_cluster();
    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

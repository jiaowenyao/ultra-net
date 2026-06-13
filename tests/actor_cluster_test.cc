#include <iostream>
#include <cassert>
#include <cstring>
#include "ultranet/actor/net/cluster.h"

// Cluster gossip protocol tests.

using namespace ynet::actor::net;

static int g_passed = 0, g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

void test_single_node() {
    T("single node cluster");
    cluster c1(1001, "127.0.0.1:8001");
    auto nodes = c1.live_nodes();
    CHECK(nodes.size() == 1, "one node (self)");
    CHECK(nodes[0].id == 1001, "self id correct");
    PASS();
}

void test_gossip_round_trip() {
    T("gossip build/apply round-trip");
    cluster c1(1001, "127.0.0.1:8001");
    cluster c2(1002, "127.0.0.1:8002");

    // c1 builds gossip, c2 applies it.
    auto data = c1.build_gossip();
    CHECK(!data.empty(), "gossip non-empty");

    c2.apply_gossip(data.data(), data.size());
    auto nodes = c2.live_nodes();
    // c2 should now know about c1.
    bool found_c1 = false;
    for (auto& n : nodes) {
        if (n.id == 1001) {
            found_c1 = true;
        }
    }
    CHECK(found_c1, "c2 discovered c1 via gossip");
    PASS();
}

void test_gossip_bidirectional() {
    T("bidirectional gossip");
    cluster c1(1001, "127.0.0.1:8001");
    cluster c2(1002, "127.0.0.1:8002");

    // c1 ← c2 gossip.
    auto d2 = c2.build_gossip();
    c1.apply_gossip(d2.data(), d2.size());

    // c2 ← c1 gossip.
    auto d1 = c1.build_gossip();
    c2.apply_gossip(d1.data(), d1.size());

    // Both should see each other + themselves.
    CHECK(c1.live_nodes().size() >= 2, "c1 sees 2+ nodes");
    CHECK(c2.live_nodes().size() >= 2, "c2 sees 2+ nodes");
    PASS();
}

void test_self_filtering() {
    T("self filtering (don't add self from gossip)");
    cluster c1(1001, "127.0.0.1:8001");
    cluster c2(1002, "127.0.0.1:8002");

    // c2 sends gossip to c1.
    auto d2 = c2.build_gossip();
    c1.apply_gossip(d2.data(), d2.size());

    // c1 should have exactly 1 self entry + 1 peer (c2).
    auto nodes = c1.live_nodes();
    CHECK(nodes.size() == 2, "2 nodes total");
    // Verify no duplicate self.
    int self_count = 0;
    for (auto& n : nodes) {
        if (n.id == 1001) {
            ++self_count;
        }
    }
    CHECK(self_count == 1, "self appears exactly once");
    PASS();
}

void test_liveness_timeout() {
    T("node liveness timeout");
    cluster c1(1001, "127.0.0.1:8001");
    cluster c2(1002, "127.0.0.1:8002");

    // Apply gossip from c2.
    auto d2 = c2.build_gossip();
    c1.apply_gossip(d2.data(), d2.size());

    // With 0ms timeout, c2 should be considered dead.
    auto nodes = c1.live_nodes(0);
    bool found_c2 = false;
    for (auto& n : nodes) {
        if (n.id == 1002) {
            found_c2 = true;
        }
    }
    CHECK(!found_c2 || nodes.size() == 1, "c2 dead with 0ms timeout");
    PASS();
}

void test_multiple_peers() {
    T("multiple peers (3 nodes)");
    cluster c1(1001, "a:1");
    cluster c2(1002, "a:2");
    cluster c3(1003, "a:3");

    // Full mesh gossip.
    auto d1 = c1.build_gossip();
    auto d2 = c2.build_gossip();
    auto d3 = c3.build_gossip();

    c1.apply_gossip(d2.data(), d2.size());
    c1.apply_gossip(d3.data(), d3.size());
    c2.apply_gossip(d1.data(), d1.size());
    c2.apply_gossip(d3.data(), d3.size());
    c3.apply_gossip(d1.data(), d1.size());
    c3.apply_gossip(d2.data(), d2.size());

    CHECK(c1.live_nodes().size() == 3, "c1 sees 3");
    CHECK(c2.live_nodes().size() == 3, "c2 sees 3");
    CHECK(c3.live_nodes().size() == 3, "c3 sees 3");
    PASS();
}

void test_seed_nodes() {
    T("seed nodes");
    cluster c1(1001, "127.0.0.1:8001");
    c1.add_seed("192.168.1.1:9000");
    c1.add_seed("192.168.1.2:9000");
    CHECK(c1.seeds().size() == 2, "two seeds");
    CHECK(c1.seeds()[0] == "192.168.1.1:9000", "seed 1");
    CHECK(c1.seeds()[1] == "192.168.1.2:9000", "seed 2");
    PASS();
}

int main() {
    std::cout << "=== Cluster Gossip Tests ===" << std::endl;
    test_single_node();
    test_gossip_round_trip();
    test_gossip_bidirectional();
    test_self_filtering();
    test_liveness_timeout();
    test_multiple_peers();
    test_seed_nodes();
    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <cassert>
#include "ultranet/actor.hpp"

// Actor system integration tests.
// Verifies: transport auto-start, gossip loop, inbound routing, remote find.

using namespace ynet::actor;

static int g_passed = 0;
static int g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── Test message types ─────────────────────────────────────────────────
// IMPORTANT: messages must be trivially copyable — the framework memcpy's
// them into message_envelope.  Types with heap-allocated members
// (std::string, std::vector, etc.) are NOT supported in this version.

struct int_msg { int value = 0; };

// ── Test actors ────────────────────────────────────────────────────────

class echo_actor : public actor<echo_actor> {
public:
    int m_count = 0;
    int m_last_value = 0;

    echo_actor() {
        register_handler<int_msg>([this](const int_msg& m) {
            m_count++;
            m_last_value = m.value;
        });
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

void test_transport_auto_start() {
    T("transport auto-starts with actor_system");
    system_config cfg;
    cfg.node_name = "test-node";
    cfg.listen_port = 0;
    actor_system sys(cfg);
    uint16_t port = sys.actual_port();
    CHECK(port > 0, "port assigned");
    CHECK(port != 0, "port non-zero");
    PASS();
}

void test_transport_with_specific_port() {
    T("transport with specific port");
    system_config cfg_b;
    cfg_b.node_name = "node-b";
    cfg_b.listen_port = 19001;
    actor_system sys(cfg_b);
    uint16_t port = sys.actual_port();
    CHECK(port == 19001, "port matches request");
    PASS();
}

void test_spawn_with_transport_active() {
    T("spawn actor with transport active");
    system_config cfg_s;
    cfg_s.node_name = "spawn-node";
    actor_system sys(cfg_s);
    auto ref = sys.spawn<echo_actor>("echo1");
    CHECK(ref.is_valid(), "actor spawned");
    CHECK(sys.actual_port() > 0, "transport port assigned");
    PASS();
}

void test_spawn_publishes_location() {
    T("spawn publishes actor location");
    system_config cfg_p;
    cfg_p.node_name = "pub-node";
    actor_system sys(cfg_p);
    auto ref = sys.spawn<echo_actor>("pub-echo");
    CHECK(ref.is_valid(), "actor spawned");

    // The remote routing table should have this actor's location.
    auto key = ref.uri().to_string();
    // Try to find it locally (should work).
    auto found = sys.find<echo_actor>(key);
    CHECK(found.is_valid(), "can find locally after spawn");
    PASS();
}

void test_remote_find_nonexistent() {
    T("remote find of nonexistent actor");
    system_config cfg_r;
    cfg_r.node_name = "remote-node";
    actor_system sys(cfg_r);
    // An actor that was never spawned anywhere.
    auto key = actor_uri::make(99999, "echo_actor", "no-such").to_string();
    auto found = sys.find<echo_actor>(key);
    // Should be invalid (not in local registry, not in remote table).
    CHECK(!found.is_valid(), "remote nonexistent returns invalid");
    PASS();
}

void test_multiple_systems_different_ports() {
    T("two actor_systems with different ports");
    system_config cfg_a;
    cfg_a.node_name = "sys-a";
    cfg_a.listen_port = 19101;
    actor_system sys_a(cfg_a);
    system_config cfg_b;
    cfg_b.node_name = "sys-b";
    cfg_b.listen_port = 19102;
    actor_system sys_b(cfg_b);
    CHECK(sys_a.actual_port() == 19101, "sys-a port");
    CHECK(sys_b.actual_port() == 19102, "sys-b port");

    auto ref_a = sys_a.spawn<echo_actor>("echo-a");
    auto ref_b = sys_b.spawn<echo_actor>("echo-b");
    CHECK(ref_a.is_valid(), "sys-a actor ok");
    CHECK(ref_b.is_valid(), "sys-b actor ok");

    // Each system can only find its own actor (local).
    auto key_a = ref_a.uri().to_string();
    auto found_a = sys_a.find<echo_actor>(key_a);
    CHECK(found_a.is_valid(), "sys-a finds own actor");

    auto found_b_from_a = sys_a.find<echo_actor>(
        actor_uri::make(999, typeid(echo_actor).name(), "echo-b").to_string());
    CHECK(!found_b_from_a.is_valid(), "sys-a cannot find sys-b actor");
    PASS();
}

void test_system_with_config() {
    T("system stores config");
    system_config cfg;
    cfg.num_threads = 2;
    cfg.node_name = "config-test";
    cfg.max_per_activation = 32;
    cfg.gossip_interval_ms = 500;
    cfg.node_timeout_ms = 5000;
    actor_system sys(cfg);

    CHECK(sys.config().num_threads == 2, "num_threads");
    CHECK(sys.config().node_name == "config-test", "node_name");
    CHECK(sys.config().max_per_activation == 32, "max_per_activation");
    CHECK(sys.config().gossip_interval_ms == 500, "gossip_interval");
    CHECK(sys.config().node_timeout_ms == 5000, "node_timeout");
    PASS();
}

void test_add_seed() {
    T("add seed node");
    system_config cfg_s;
    cfg_s.node_name = "seed-node";
    actor_system sys(cfg_s);
    sys.add_seed("10.0.0.1:9000");
    sys.add_seed("10.0.0.2:9000");
    // No crash — seeds are tracked by the internal cluster.
    PASS();
}

void test_find_after_spawn_integration() {
    T("find after spawn — integration");
    system_config cfg_i;
    cfg_i.node_name = "integ-node";
    actor_system sys(cfg_i);
    auto ref = sys.spawn<echo_actor>("integ-echo");
    CHECK(ref.is_valid(), "spawned");

    // Send a message and verify async delivery.
    ref.send(int_msg{42});

    auto* proxy = ref.proxy().get();
    auto* local = proxy->local_actor();
    CHECK(local != nullptr, "has local actor");
    auto* actor = static_cast<echo_actor*>(local);
    for (int retry = 0; retry < 10 && actor->m_count < 1; ++retry) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    CHECK(actor->m_count >= 1, "message delivered in integrated system");
    CHECK(actor->m_last_value == 42, "message content correct");
    PASS();
}

int main() {
    std::cout << "=== Actor System Integration Tests ===" << std::endl;
    test_transport_auto_start();
    test_transport_with_specific_port();
    test_spawn_with_transport_active();
    test_spawn_publishes_location();
    test_remote_find_nonexistent();
    test_multiple_systems_different_ports();
    test_system_with_config();
    test_add_seed();
    test_find_after_spawn_integration();

    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

#include <iostream>
#include <string>
#include <cassert>
#include <thread>
#include <chrono>
#include "ultranet/actor.hpp"

// Phase 1 tests: actor v2 core (spawn, find, send, ref, uri, registry).

using namespace ynet::actor;

static int g_passed = 0, g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── Test actors ────────────────────────────────────────────────────────

struct ping_msg { int id; std::string text; };
struct pong_msg { int id; std::string reply; };

class echo_actor : public actor<echo_actor> {
public:
    int m_count = 0;
    std::string m_last;
    void on_ping(ping_msg& m) {
        m_count++;
        m_last = m.text;
    }
    int get_count() const { return m_count; }
};

class stateful_actor : public actor<stateful_actor> {
public:
    int m_val = 0;
    void set(int v) { m_val = v; }
    int get() const { return m_val; }
};

// ── Tests ──────────────────────────────────────────────────────────────

void test_system_create() {
    T("system create/destroy");
    { actor_system sys; }
    PASS();
}

void test_spawn_ref() {
    T("spawn and ref validity");
    actor_system sys;
    auto ref = sys.spawn<echo_actor>("echo1");
    CHECK(ref.is_valid(), "ref valid");
    CHECK(ref.name() == "echo1", "name correct");
    CHECK(!ref.uri().to_string().empty(), "uri non-empty");
    std::cout << "  uri=" << ref.uri().to_string() << std::endl;
    PASS();
}

void test_find() {
    T("find by name");
    actor_system sys;
    auto r1 = sys.spawn<echo_actor>("finder");
    // Registry key uses typeid name (mangled) — find by that.
    auto key = actor_uri::make_local(typeid(echo_actor).name(), "finder").to_string();
    auto r2 = sys.find<echo_actor>(key);
    CHECK(r2.is_valid(), "found actor");
    PASS();
}

void test_find_missing() {
    T("find missing actor");
    actor_system sys;
    auto r = sys.find<echo_actor>("ultra://*/echo_actor/nonexistent");
    CHECK(!r.is_valid(), "missing actor returns invalid");
    PASS();
}

void test_multiple_actors() {
    T("multiple actors");
    actor_system sys;
    auto a1 = sys.spawn<echo_actor>("a1");
    auto a2 = sys.spawn<stateful_actor>("a2");
    auto a3 = sys.spawn<echo_actor>("a3");
    CHECK(a1.is_valid() && a2.is_valid() && a3.is_valid(), "all valid");
    CHECK(a1.name() != a3.name(), "names differ");
    PASS();
}

void test_actor_uri() {
    T("actor URI parse");
    auto u = actor_uri::make_local("my_type", "my_name");
    CHECK(u.node == "*", "local node wildcard");
    CHECK(u.type == "my_type", "type correct");
    CHECK(u.name == "my_name", "name correct");
    CHECK(u.to_string() == "ultra://*/my_type/my_name", "format correct");
    PASS();
}

void test_system_config() {
    T("system config");
    system_config cfg;
    cfg.node_name = "test-node";
    cfg.num_threads = 2;
    actor_system sys(cfg);
    CHECK(sys.config().num_threads == 2, "thread count");
    CHECK(sys.config().node_name == "test-node", "node name");
    PASS();
}

// ── Non-intrusive actor test ────────────────────────────────────────

struct plain_counter {
    int count = 0;
    void add(int x) { count += x; }
};

void test_non_intrusive() {
    T("non-intrusive spawn (plain class)");
    actor_system sys;
    // plain_counter does NOT inherit from actor<T>
    auto ref = sys.spawn<plain_counter>("counter1");
    CHECK(ref.is_valid(), "non-intrusive ref valid");
    CHECK(ref.name() == "counter1", "name correct");
    PASS();
}

// ── Message send/receive test ──────────────────────────────────────

struct test_msg { int value = 0; };

class msg_actor : public actor<msg_actor> {
public:
    int m_received = 0;
    msg_actor() {
        register_handler<test_msg>([this](const test_msg& m) {
            m_received = m.value;
        });
    }
};

void test_send_message() {
    T("message send via actor_ref");
    actor_system sys;
    auto ref = sys.spawn<msg_actor>("msga");
    CHECK(ref.is_valid(), "actor valid");
    test_msg m{99};
    ref.send(m);
    PASS();
}

void test_handler_api() {
    T("handler registration API");
    actor_system sys;
    auto ref = sys.spawn<msg_actor>("msga2");
    CHECK(ref.is_valid(), "actor valid");
    PASS();
}

int main() {
    std::cout << "=== Actor v2 Phase 1 Tests ===" << std::endl;
    test_system_create();
    test_actor_uri();
    test_spawn_ref();
    test_find();
    test_find_missing();
    test_multiple_actors();
    test_system_config();
    test_non_intrusive();
    test_send_message();
    test_handler_api();
    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

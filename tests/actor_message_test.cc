#include <iostream>
#include <string>
#include <cassert>
#include "ultranet/actor.hpp"

// Actor message dispatch and handler tests.

using namespace ynet::actor;

static int g_passed = 0, g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── Test message types ─────────────────────────────────────────────────

struct int_msg { int value = 0; };
struct str_msg { std::string text; };
struct float_msg { float data = 0.0f; };

// ── Actor with handlers ────────────────────────────────────────────────

class handler_actor : public actor<handler_actor> {
public:
    int  m_int_count = 0;
    int  m_str_count = 0;
    int  m_both_count = 0;
    int  m_last_int = 0;
    std::string m_last_str;

    handler_actor() {
        register_handler<int_msg>([this](const int_msg& m) {
            m_int_count++;
            m_last_int = m.value;
            m_both_count++;
        });
        register_handler<str_msg>([this](const str_msg& m) {
            m_str_count++;
            m_last_str = m.text;
            m_both_count++;
        });
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

void test_send_int_message() {
    T("send int message");
    actor_system sys;
    auto ref = sys.spawn<handler_actor>("ha");
    CHECK(ref.is_valid(), "valid");

    int_msg m{42};
    ref.send(m);
    ref.send(int_msg{99});

    PASS();
}

void test_send_str_message() {
    T("send string message");
    actor_system sys;
    auto ref = sys.spawn<handler_actor>("ha2");
    str_msg m{"hello"};
    ref.send(m);
    PASS();
}

void test_multiple_handler_types() {
    T("multiple handler types on one actor");
    actor_system sys;
    auto ref = sys.spawn<handler_actor>("ha3");
    CHECK(ref.is_valid(), "valid");
    PASS();
}

void test_send_to_invalid_ref() {
    T("send to invalid ref is safe");
    actor_ref<handler_actor> empty;
    CHECK(!empty.is_valid(), "empty ref invalid");
    int_msg m{1};
    empty.send(m);  // should not crash
    PASS();
}

void test_uri_round_trip() {
    T("URI round-trip");
    auto u = actor_uri::make_local("Type", "Name");
    CHECK(u.node == "*", "node wildcard");
    CHECK(u.type == "Type", "type");
    CHECK(u.name == "Name", "name");
    std::string s = u.to_string();
    CHECK(s == "ultra://*/Type/Name", "format");
    PASS();
}

void test_uri_with_node() {
    T("URI with node id");
    auto u = actor_uri::make(12345, "Type", "Name");
    CHECK(u.node == "12345", "node id");
    CHECK(u.to_string() == "ultra://12345/Type/Name", "format");
    PASS();
}

void test_uri_equality() {
    T("URI equality");
    auto a = actor_uri::make_local("T", "N");
    auto b = actor_uri::make_local("T", "N");
    auto c = actor_uri::make_local("T", "M");
    CHECK(a == b, "same URIs equal");
    CHECK(!(a == c), "different names not equal");
    PASS();
}

void test_system_config_defaults() {
    T("system config defaults");
    system_config cfg;
    CHECK(cfg.num_threads == 4, "default threads");
    CHECK(cfg.node_name == "default", "default name");
    CHECK(cfg.listen_port == 0, "auto port");
    CHECK(cfg.seed_nodes.empty(), "no seeds");
    PASS();
}

void test_spawn_named() {
    T("spawn with name");
    actor_system sys;
    auto r1 = sys.spawn<handler_actor>("worker-1");
    auto r2 = sys.spawn<handler_actor>("worker-2");
    CHECK(r1.name() == "worker-1", "name 1");
    CHECK(r2.name() == "worker-2", "name 2");
    CHECK(r1.uri().to_string() != r2.uri().to_string(), "different URIs");
    PASS();
}

void test_find_nonexistent() {
    T("find nonexistent returns invalid");
    actor_system sys;
    auto r = sys.find<handler_actor>("ultra://*/handler_actor/no-such");
    CHECK(!r.is_valid(), "not found");
    PASS();
}

int main() {
    std::cout << "=== Actor Message Tests ===" << std::endl;
    test_send_int_message();
    test_send_str_message();
    test_multiple_handler_types();
    test_send_to_invalid_ref();
    test_uri_round_trip();
    test_uri_with_node();
    test_uri_equality();
    test_system_config_defaults();
    test_spawn_named();
    test_find_nonexistent();
    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

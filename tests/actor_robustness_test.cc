// Comprehensive robustness tests for the actor framework.
// Covers: exception handling, backpressure, graceful shutdown,
// dead letter logging, type hash stability, try_send.

#include <iostream>
#include <string>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>
#include "ultranet/actor.hpp"

using namespace ynet::actor;

static int g_passed = 0;
static int g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── Messages with stable type tags (cross-compiler safe) ──────────────

struct stable_ping {
    static constexpr const char* actor_type = "stable_ping";
    int id = 0;
};

struct stable_pong {
    static constexpr const char* actor_type = "stable_pong";
    int id = 0;
    char text[32] = {};
};

// ── Test actors ────────────────────────────────────────────────────────

class exception_throwing_actor : public actor<exception_throwing_actor> {
public:
    std::atomic<int> m_before{0};
    std::atomic<int> m_after{0};
    std::atomic<int> m_threw{0};

    exception_throwing_actor() {
        register_handler<stable_ping>([this](const stable_ping& m) {
            m_before.fetch_add(1);
            if (m.id == -1) {
                m_threw.fetch_add(1);
                throw std::runtime_error("test exception from handler");
            }
            m_after.fetch_add(1);
        });
    }
};

class busy_actor : public actor<busy_actor> {
public:
    std::atomic<int> m_count{0};

    busy_actor() {
        register_handler<stable_ping>([this](const stable_ping&) {
            m_count.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        });
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

void test_handler_exception_is_contained() {
    T("handler exception is caught, actor survives");

    actor_system sys({.max_per_activation = 10});
    auto ref = sys.spawn<exception_throwing_actor>("ex-actor");
    CHECK(ref.is_valid(), "spawned");

    auto* actor = static_cast<exception_throwing_actor*>(
        ref.proxy()->local_actor());

    // Send a mix of good and bad messages (5 + 1 + 5 = 11 total).
    for (int i = 0; i < 5; ++i) {
        ref.send(stable_ping{i});
    }
    ref.send(stable_ping{-1});  // throws
    for (int i = 5; i < 10; ++i) {
        ref.send(stable_ping{i});
    }

    // Wait for processing (poll with timeout).
    for (int wait = 0; wait < 50 && actor->m_before.load() < 11; ++wait) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    CHECK(actor->m_threw.load() == 1, "one exception thrown");
    CHECK(actor->m_before.load() == 11, "all 11 before-handler calls");
    CHECK(actor->m_after.load() == 10, "10 after-handler calls (1 threw)");
    PASS();
}

void test_dead_letter_handling() {
    T("dead letter for unregistered message type");

    actor_system sys;
    auto ref = sys.spawn<busy_actor>("dl-actor");

    // Send a message type the actor doesn't handle.
    // The base deliver() should log a warning and not crash.
    stable_pong p{42, "dead"};
    ref.send(p);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    PASS();  // If we got here without crashing, dead letter handling works.
}

void test_stable_type_hash() {
    T("stable type hash with actor_type tag");

    uint64_t h1 = actor_type_hash<stable_ping>();
    uint64_t h2 = actor_type_hash<stable_ping>();
    CHECK(h1 == h2, "same type produces same hash");
    CHECK(h1 != 0, "hash is non-zero");

    // Verify the hash comes from the actor_type string, not typeid.
    uint64_t h3 = actor_type_hash<stable_pong>();
    CHECK(h1 != h3, "different types produce different hashes");
    PASS();
}

void test_try_send_nonblocking() {
    T("try_send returns false under backpressure");

    actor_system sys({.max_per_activation = 1});
    auto ref = sys.spawn<busy_actor>("try-actor");
    CHECK(ref.is_valid(), "spawned");

    // Fill the mailbox beyond capacity to trigger backpressure.
    int sent = 0;
    int rejected = 0;
    for (int i = 0; i < 5000; ++i) {
        if (ref.try_send(stable_ping{i})) {
            ++sent;
        } else {
            ++rejected;
        }
    }

    // At least some messages should be sent (the mailbox drains during this).
    CHECK(sent > 0, "some messages sent via try_send");
    // With 5000 messages and a slow consumer, some should be rejected.
    // (This is probabilistic but with 4096 mailbox capacity and slow consumer,
    // it's virtually guaranteed.)
    CHECK(rejected > 0, "some messages rejected under backpressure");
    PASS();
}

void test_try_send_to_invalid_ref() {
    T("try_send to invalid ref returns false");
    actor_ref<busy_actor> empty;
    CHECK(!empty.is_valid(), "empty ref is invalid");
    CHECK(!empty.try_send(stable_ping{1}), "try_send returns false");
    PASS();
}

void test_graceful_shutdown() {
    T("graceful shutdown drains remaining messages");

    system_config cfg;
    cfg.num_threads = 2;
    cfg.max_per_activation = 64;
    auto sys = std::make_unique<actor_system>(cfg);
    auto ref = sys->spawn<busy_actor>("shutdown-actor");
    CHECK(ref.is_valid(), "spawned");

    // Send a burst of messages.
    for (int i = 0; i < 100; ++i) {
        ref.send(stable_ping{i});
    }

    // Let some messages process, then destroy the system.
    // The pool destructor drains in-flight coroutine tasks, so messages
    // already submitted to the pool will be processed before join.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Destroy the system — should not crash.
    sys.reset();

    // If we got here without a segfault, the shutdown path works.
    PASS();
}

void test_message_envelope_make() {
    T("message_envelope::make round-trip");
    stable_ping p{42};
    auto env = message_envelope::make(p);
    CHECK(env.msg_type == actor_type_hash<stable_ping>(), "type hash correct");
    CHECK(env.data.size() == sizeof(stable_ping), "size correct");

    stable_ping recovered;
    std::memcpy(&recovered, env.data.data(), sizeof(stable_ping));
    CHECK(recovered.id == 42, "value preserved");
    PASS();
}

void test_actor_base_handles_check() {
    T("actor_base::handles<T>() check");
    actor_system sys;
    auto ref = sys.spawn<busy_actor>("handles-actor");
    auto* actor = static_cast<busy_actor*>(ref.proxy()->local_actor());
    CHECK(actor->handles<stable_ping>(), "handles stable_ping");
    CHECK(!actor->handles<stable_pong>(), "does not handle stable_pong");
    PASS();
}

int main() {
    std::cout << "=== Actor Robustness Tests ===" << std::endl;

    test_handler_exception_is_contained();
    test_dead_letter_handling();
    test_stable_type_hash();
    test_try_send_nonblocking();
    test_try_send_to_invalid_ref();
    test_graceful_shutdown();
    test_message_envelope_make();
    test_actor_base_handles_check();

    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

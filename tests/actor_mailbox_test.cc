#include <iostream>
#include <string>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>
#include "ultranet/actor.hpp"

// Mailbox + async scheduling tests.
// Verifies: push/pop, overflow, drain, async delivery, self-reactivation, backpressure.

using namespace ynet::actor;

static int g_passed = 0;
static int g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── Test message types ─────────────────────────────────────────────────

struct int_msg { int value = 0; };
struct str_msg { std::string text; };

// ── Test actors ────────────────────────────────────────────────────────

class counting_actor : public actor<counting_actor> {
public:
    std::atomic<int> m_received{0};
    std::atomic<int> m_last_value{0};

    counting_actor() {
        register_handler<int_msg>([this](const int_msg& m) {
            m_received.fetch_add(1, std::memory_order_relaxed);
            m_last_value.store(m.value, std::memory_order_relaxed);
        });
    }
};

class slow_actor : public actor<slow_actor> {
public:
    std::atomic<int> m_count{0};

    slow_actor() {
        register_handler<int_msg>([this](const int_msg&) {
            m_count.fetch_add(1, std::memory_order_relaxed);
            // Simulate work — enough time for more messages to arrive.
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        });
    }
};

// ── Mailbox unit tests ─────────────────────────────────────────────────

void test_mailbox_push_pop() {
    T("mailbox push/pop");
    mailbox mb;
    auto env = message_envelope::make(int_msg{42});
    CHECK(mb.empty(), "empty initially");
    CHECK(mb.try_push(std::move(env)), "push ok");
    CHECK(mb.approximate_size() >= 1, "size >= 1");
    auto popped = mb.try_pop();
    CHECK(popped.has_value(), "pop ok");
    CHECK(popped->msg_type == actor_type_hash<int_msg>(), "type hash preserved");
    int_msg recovered;
    std::memcpy(&recovered, popped->data.data(), sizeof(int_msg));
    CHECK(recovered.value == 42, "value preserved");
    CHECK(mb.empty(), "empty after pop");
    PASS();
}

void test_mailbox_drain() {
    T("mailbox drain");
    mailbox mb;
    for (int i = 0; i < 10; ++i) {
        mb.try_push(message_envelope::make(int_msg{i}));
    }
    size_t total = 0;
    mb.drain([&total](message_envelope) { ++total; }, 10);
    CHECK(total == 10, "all 10 drained");
    CHECK(mb.empty(), "empty after drain");
    PASS();
}

void test_mailbox_drain_limit() {
    T("mailbox drain respects limit");
    mailbox mb;
    for (int i = 0; i < 10; ++i) {
        mb.try_push(message_envelope::make(int_msg{i}));
    }
    size_t total = 0;
    mb.drain([&total](message_envelope) { ++total; }, 3);
    CHECK(total == 3, "only 3 drained");
    CHECK(!mb.empty(), "still has items");
    PASS();
}

void test_mailbox_empty_pop() {
    T("mailbox empty pop returns nullopt");
    mailbox mb;
    auto popped = mb.try_pop();
    CHECK(!popped.has_value(), "empty pop is nullopt");
    PASS();
}

void test_mailbox_backpressure() {
    T("mailbox backpressure detection");
    mailbox mb;
    // Fill past 80%.
    size_t threshold = mailbox::k_default_capacity * 80 / 100;
    for (size_t i = 0; i < threshold + 10; ++i) {
        bool ok = mb.try_push(message_envelope::make(int_msg{0}));
        if (!ok) {
            break;
        }
    }
    CHECK(mb.is_backpressure(), "backpressure detected");
    PASS();
}

// ── Async delivery tests ───────────────────────────────────────────────

void test_async_send_and_verify() {
    T("async send → handler called");
    actor_system sys;
    auto ref = sys.spawn<counting_actor>("counter");
    CHECK(ref.is_valid(), "ref valid");

    // Send and wait for async delivery.
    ref.send(int_msg{99});
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    CHECK(ref.is_valid(), "ref still valid");
    // Access the actor through the proxy to verify delivery.
    auto* proxy = ref.proxy().get();
    auto* local = proxy->local_actor();
    CHECK(local != nullptr, "has local actor");
    auto* actor = static_cast<counting_actor*>(local);
    int received = actor->m_received.load();
    if (received < 1) {
        // Give it a bit more time.
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        received = actor->m_received.load();
    }
    CHECK(received >= 1, "message delivered");
    int last = actor->m_last_value.load();
    CHECK(last == 99, "correct value delivered");
    PASS();
}

void test_multiple_async_sends() {
    T("multiple async sends");
    actor_system sys;
    auto ref = sys.spawn<counting_actor>("counter2");
    CHECK(ref.is_valid(), "ref valid");

    for (int i = 0; i < 10; ++i) {
        ref.send(int_msg{i});
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    auto* proxy = ref.proxy().get();
    auto* local = proxy->local_actor();
    CHECK(local != nullptr, "has local actor");
    auto* actor = static_cast<counting_actor*>(local);
    int received = actor->m_received.load();
    if (received < 10) {
        // Give more time.
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        received = actor->m_received.load();
    }
    CHECK(received == 10, "all 10 messages delivered");
    PASS();
}

void test_bulk_async_sends() {
    T("1000 async sends");
    actor_system sys;
    auto ref = sys.spawn<counting_actor>("counter3");
    CHECK(ref.is_valid(), "ref valid");

    for (int i = 0; i < 1000; ++i) {
        ref.send(int_msg{i});
    }

    // Wait for all to be processed.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto* proxy = ref.proxy().get();
    auto* local = proxy->local_actor();
    CHECK(local != nullptr, "has local actor");
    auto* actor = static_cast<counting_actor*>(local);
    int received = actor->m_received.load();
    if (received < 1000) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        received = actor->m_received.load();
    }
    CHECK(received == 1000, "all 1000 delivered");
    PASS();
}

void test_self_reactivation() {
    T("self-reactivation (messages arrive during processing)");
    actor_system sys({.max_per_activation = 5});
    auto ref = sys.spawn<slow_actor>("slow");
    CHECK(ref.is_valid(), "ref valid");

    // Send more messages than max_per_activation so that during
    // processing new messages arrive, triggering self-reactivation.
    for (int i = 0; i < 20; ++i) {
        ref.send(int_msg{i});
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    auto* proxy = ref.proxy().get();
    auto* local = proxy->local_actor();
    auto* actor = static_cast<slow_actor*>(local);
    int count = actor->m_count.load();
    if (count < 20) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        count = actor->m_count.load();
    }
    CHECK(count == 20, "all 20 delivered via self-reactivation");
    PASS();
}

void test_send_to_invalid_ref() {
    T("send to invalid ref is safe");
    actor_ref<counting_actor> empty;
    CHECK(!empty.is_valid(), "empty ref invalid");
    int_msg m{1};
    empty.send(m);  // should not crash
    PASS();
}

void test_pending_count() {
    T("pending count tracks outstanding messages");
    actor_system sys({.max_per_activation = 1});
    auto ref = sys.spawn<slow_actor>("slow2");
    CHECK(ref.is_valid(), "ref valid");

    auto* proxy = ref.proxy().get();
    auto* local = proxy->local_actor();
    auto* actor = static_cast<slow_actor*>(local);

    // Send a burst.  pending() should be non-zero briefly.
    for (int i = 0; i < 5; ++i) {
        ref.send(int_msg{i});
    }

    // pending should be >0 (messages are queued).
    size_t p = actor->pending();
    // Just check the method works — the exact value is a race.
    (void)p;

    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    int count = actor->m_count.load();
    if (count < 5) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        count = actor->m_count.load();
    }
    CHECK(count == 5, "all 5 delivered");
    CHECK(actor->pending() == 0, "pending is 0 after processing");
    PASS();
}

void test_system_config_forwarding() {
    T("system_config max_per_activation forwarded");
    system_config cfg;
    cfg.max_per_activation = 128;
    actor_system sys(cfg);
    CHECK(sys.config().max_per_activation == 128, "config stored");
    PASS();
}

int main() {
    std::cout << "=== Actor Mailbox + Async Tests ===" << std::endl;

    // Mailbox unit tests.
    test_mailbox_push_pop();
    test_mailbox_drain();
    test_mailbox_drain_limit();
    test_mailbox_empty_pop();
    test_mailbox_backpressure();

    // Async delivery tests.
    test_async_send_and_verify();
    test_multiple_async_sends();
    test_bulk_async_sends();
    test_self_reactivation();
    test_send_to_invalid_ref();
    test_pending_count();

    // Config test.
    test_system_config_forwarding();

    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

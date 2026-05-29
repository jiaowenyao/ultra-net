#include "ultranet/ultranet.h"
#include <iostream>
#include <thread>
#include <set>

using namespace ynet::trace;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

void test_span_basic() {
    T("span basic lifecycle");
    Span s("test-operation");
    CHECK(s.trace_id() != 0, "trace_id should be non-zero");
    CHECK(s.span_id() != 0, "span_id should be non-zero");
    CHECK(s.parent_span_id() == 0, "root span parent should be 0");
    CHECK(s.name() == "test-operation", "name");
    s.stop();
    CHECK(s.latency_us() >= 0, "latency non-negative");
    PASS();
}

void test_span_child() {
    T("span parent-child relationship");
    Span parent("parent-op");
    Span child = parent.child("child-op");

    CHECK(child.trace_id() == parent.trace_id(), "same trace_id");
    CHECK(child.parent_span_id() == parent.span_id(), "parent_span_id matches");
    CHECK(child.span_id() != parent.span_id(), "different span_id");
    CHECK(child.name() == "child-op", "child name");
    PASS();
}

void test_span_deep_hierarchy() {
    T("span deep hierarchy (3 levels)");
    Span root("root");
    Span l1 = root.child("level-1");
    Span l2 = l1.child("level-2");

    CHECK(l2.trace_id() == root.trace_id(), "same trace_id across levels");
    CHECK(l2.parent_span_id() == l1.span_id(), "level-2 parent is level-1");
    CHECK(l1.parent_span_id() == root.span_id(), "level-1 parent is root");
    PASS();
}

void test_span_unique_ids() {
    T("span unique IDs across threads");
    const int N = 100;
    std::vector<Span> spans;
    std::vector<std::thread> threads;

    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < N; ++i) {
                Span s("op-" + std::to_string(t) + "-" + std::to_string(i));
                s.stop();
                spans.push_back(std::move(s));
            }
        });
    }
    for (auto& t : threads) t.join();

    // Verify all span_ids are unique
    std::set<uint64_t> ids;
    for (auto& s : spans) {
        CHECK(ids.count(s.span_id()) == 0, "duplicate span_id");
        ids.insert(s.span_id());
    }
    CHECK(ids.size() == spans.size(), "all ids unique");
    PASS();
}

void test_span_latency_measurement() {
    T("span latency measurement");
    Span s("timed-op");
    // simulate some work
    volatile int x = 0;
    for (int i = 0; i < 1000000; ++i) x += i;
    s.stop();
    CHECK(s.latency_us() > 0, "latency should be > 0");
    PASS();
}

void test_span_context() {
    T("span context propagation");
    Span s("ctx-test");
    auto ctx = s.context();
    CHECK(ctx.trace_id == s.trace_id(), "ctx trace_id");
    CHECK(ctx.span_id == s.span_id(), "ctx span_id");
    CHECK(ctx.parent_span_id == s.parent_span_id(), "ctx parent_span_id");

    // Create new span from context
    Span s2("ctx-child", ctx);
    CHECK(s2.trace_id() == ctx.trace_id, "child same trace");
    CHECK(s2.parent_span_id() == ctx.span_id, "child parent is ctx span");
    PASS();
}

int main() {
    std::cout << "=== Trace Tests ===" << std::endl;
    test_span_basic();
    test_span_child();
    test_span_deep_hierarchy();
    test_span_unique_ids();
    test_span_latency_measurement();
    test_span_context();

    std::cout << "\n=== Trace Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

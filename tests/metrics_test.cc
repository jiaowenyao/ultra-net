#include "ultranet/ultranet.h"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <sstream>

using namespace ynet::metrics;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

void test_counter_basic() {
    T("counter basic operations");
    Counter c("test_total", "Test counter");
    CHECK(c.value() == 0, "initial value");
    c.inc();
    CHECK(c.value() == 1, "inc by 1");
    c.inc(5);
    CHECK(c.value() == 6, "inc by 5");
    c.reset();
    CHECK(c.value() == 0, "reset");
    CHECK(c.name() == "test_total", "name");
    CHECK(c.help() == "Test counter", "help");
    PASS();
}

void test_counter_concurrent() {
    T("counter concurrent increments");
    Counter c("concurrent", "");
    const int N = 10000;
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&] {
            for (int i = 0; i < N; ++i) c.inc();
        });
    }
    for (auto& t : threads) t.join();
    CHECK(c.value() == 4 * N, "concurrent sum");
    PASS();
}

void test_gauge_basic() {
    T("gauge basic operations");
    Gauge g("active_connections", "Active connections");
    CHECK(g.value() == 0, "initial");
    g.set(42);
    CHECK(g.value() == 42, "set");
    g.inc();
    CHECK(g.value() == 43, "inc");
    g.dec(3);
    CHECK(g.value() == 40, "dec");
    PASS();
}

void test_histogram_basic() {
    T("histogram basic operations");
    Histogram h("latency_seconds", "Request latency",
        {0.1, 0.5, 1.0});
    h.observe(0.05);
    h.observe(0.3);
    h.observe(0.7);
    h.observe(2.0);
    CHECK(h.count() == 4, "count");
    CHECK(h.sum() > 3.0, "sum > 3");

    auto text = h.to_prometheus();
    CHECK(text.find("latency_seconds_bucket") != std::string::npos, "prometheus bucket");
    CHECK(text.find("latency_seconds_count") != std::string::npos, "prometheus count");
    PASS();
}

void test_histogram_percentiles() {
    T("histogram percentile calculation");
    Histogram h("p_test", "",
        {10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0});
    for (int i = 0; i < 100; ++i) {
        h.observe(static_cast<double>(i));
    }
    auto p50 = h.p50();
    auto p90 = h.p90();
    auto p99 = h.p99();
    CHECK(p50 >= 45 && p50 <= 55, "p50");
    CHECK(p90 >= 85 && p90 <= 95, "p90");
    CHECK(p99 >= 95 && p99 <= 100, "p99");
    PASS();
}

void test_histogram_empty() {
    T("histogram empty returns 0");
    Histogram h("empty", "");
    CHECK(h.p50() == 0, "empty p50");
    CHECK(h.p99() == 0, "empty p99");
    CHECK(h.count() == 0, "empty count");
    PASS();
}

void test_registry_singleton() {
    T("registry singleton");
    auto& r = MetricRegistry::instance();
    auto* c = r.counter("reg_test_total", "Registry test");
    CHECK(c != nullptr, "counter created");
    CHECK(r.counter("reg_test_total") == c, "same counter");
    r.reset();
    PASS();
}

void test_registry_prometheus_output() {
    T("registry prometheus text output");
    MetricRegistry::instance().reset();
    auto& r = MetricRegistry::instance();
    r.counter("requests_total", "Total requests");
    r.gauge("active", "Active count");
    r.histogram("duration", "Duration", {0.1, 0.5, 1.0});

    auto text = r.to_prometheus_text();
    CHECK(text.find("requests_total") != std::string::npos, "counter in output");
    CHECK(text.find("active") != std::string::npos, "gauge in output");
    CHECK(text.find("duration_bucket") != std::string::npos, "histogram in output");
    CHECK(text.find("# HELP") != std::string::npos, "help in output");
    CHECK(text.find("# TYPE") != std::string::npos, "type in output");
    r.reset();
    PASS();
}

void test_counter_prometheus() {
    T("counter prometheus format");
    Counter c("http_requests", "HTTP requests");
    c.inc(10);
    auto text = c.to_prometheus();
    CHECK(text.find("http_requests 10") != std::string::npos, "format");
    PASS();
}

int main() {
    std::cout << "=== Metrics Tests ===" << std::endl;
    test_counter_basic();
    test_counter_concurrent();
    test_gauge_basic();
    test_histogram_basic();
    test_histogram_percentiles();
    test_histogram_empty();
    test_registry_singleton();
    test_registry_prometheus_output();
    test_counter_prometheus();

    std::cout << "\n=== Metrics Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

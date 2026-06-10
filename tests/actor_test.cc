// Actor framework tests.
#include "ultranet/actor/api.h"
#include <iostream>
#include <atomic>
#include <chrono>
#include <thread>
#include <cassert>

using namespace ynet::actor;

static int g_passed = 0, g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

static actor_system* g_sys = nullptr;

struct counter { int count = 0; void add(int x) { count += x; } };
struct worker  { int val = 0;   void set(int x) { val = x; } };

void test_spawn() {
    T("spawn"); auto r = spawn<counter>(*g_sys); CHECK(r.is_valid(),"valid"); PASS();
}
void test_multi() {
    T("multi actors"); auto a=spawn<worker>(*g_sys), b=spawn<worker>(*g_sys);
    CHECK(a.id()!=b.id(),"unique"); PASS();
}
void test_send() {
    T("send and wait"); auto r = spawn<counter>(*g_sys);
    r.send<&counter::add>(42); std::this_thread::sleep_for(std::chrono::milliseconds(20)); PASS();
}
void test_1k() {
    T("1K messages"); auto r = spawn<counter>(*g_sys);
    for(int i=0;i<1000;++i) r.send<&counter::add>(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100)); PASS();
}
void test_10k() {
    T("10K messages"); auto r = spawn<counter>(*g_sys);
    for(int i=0;i<10000;++i) r.send<&counter::add>(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); PASS();
}
void test_config() {
    T("named actor"); actor_config c; c.name="test";
    auto r=spawn<counter>(*g_sys,std::move(c)); CHECK(r.is_valid(),"valid"); PASS();
}

int main() {
    std::cout << "=== Actor Tests ===" << std::endl;
    actor_system system(4); g_sys = &system;
    test_spawn(); test_multi(); test_send();
    test_1k(); test_10k(); test_config();
    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

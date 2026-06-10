// Actor Framework — Async co_await example.
// Demonstrates call() returning Task<T> for natural co_await usage.
//
// Build: cd build && make actor_async
// Run:   ./bin/actor_async

#include "ultranet/actor/api.h"
#include "ultranet/coroutine/launcher.hpp"

#include <iostream>
#include <string>

using namespace ynet::actor;
using ynet::async::Launcher;
using ynet::async::Task;

// ── Calculator actor ───────────────────────────────────────────────────

struct calculator {
    int add(int a, int b)       { return a + b; }
    int multiply(int a, int b)  { return a * b; }
    std::string greet(std::string name) { return "Hello, " + name + "!"; }
};

// ── Orchestrator: chains multiple actor calls with co_await ───────────

Task<void> orchestrate(actor_ref<calculator> calc) {
    // co_await on actor method calls — reads like synchronous code!
    int sum = co_await calc.call<&calculator::add>(10, 20);
    std::cout << "  10 + 20 = " << sum << std::endl;

    int product = co_await calc.call<&calculator::multiply>(sum, 3);
    std::cout << "  " << sum << " * 3 = " << product << std::endl;

    std::string greeting = co_await calc.call<&calculator::greet>("World");
    std::cout << "  " << greeting << std::endl;

    // Fire-and-forget send (no return value needed).
    calc.send<&calculator::add>(1, 2);
    std::cout << "  (sent async add)" << std::endl;
}

int main() {
    std::cout << "=== Actor co_await Example ===" << std::endl;

    return Launcher().threads(2).run([]() -> Task<void> {
        actor_system system(2);
        auto calc = spawn<calculator>(system);

        co_await orchestrate(calc);
        std::cout << "[main] done" << std::endl;
    });
}

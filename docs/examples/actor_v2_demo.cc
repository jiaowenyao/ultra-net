// Actor v2 Demo — clean API demonstration.
// Shows the actor framework with zero infrastructure exposure:
// no Socket, no manual networking, no serialization code.
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make actor_v2_demo
// Run:
//   ./bin/actor_v2_demo

#include "ultranet/actor.hpp"

#include <iostream>
#include <string>

using namespace ynet::actor;

// Define an actor: inherit from actor<T>.
// The framework handles thread pool, discovery, and transport automatically.

class calculator : public actor<calculator> {
public:
    int add(int a, int b) {
        return a + b;
    }

    int multiply(int a, int b) {
        return a * b;
    }
};

class greeter : public actor<greeter> {
public:
    int m_call_count = 0;

    std::string hello(std::string name) {
        ++m_call_count;
        return "Hello, " + name + "! (call #" + std::to_string(m_call_count) + ")";
    }
};

int main() {
    std::cout << "=== Actor v2 Demo ===" << std::endl;

    // Create the system.  This starts the thread pool and transport.
    // All actor infrastructure is managed internally.
    actor_system system({
        .num_threads = 4,
        .node_name  = "demo"
    });

    // Spawn an actor.  It is globally visible by type and name.
    // Other nodes in the cluster can discover it automatically.
    auto calc  = system.spawn<calculator>("math-svc");
    auto greet = system.spawn<greeter>("hello-svc");

    // Find an actor by its URI.  Same syntax for local and remote actors.
    // The framework handles routing, serialization, and networking.
    auto c = system.find<calculator>("ultra://*/calculator/math-svc");
    auto g = system.find<greeter>("ultra://*/greeter/hello-svc");

    std::cout << "calculator : " << c.uri().to_string() << std::endl;
    std::cout << "greeter    : " << g.uri().to_string() << std::endl;
    std::cout << std::endl;
    std::cout << "[OK] Actor system is running. Press Ctrl+C to stop." << std::endl;

    system.run();
    return 0;
}

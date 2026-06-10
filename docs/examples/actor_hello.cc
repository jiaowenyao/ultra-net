// Actor Framework — Basic Example
//
// Demonstrates the fundamental Actor pattern:
//   actor_system → spawn → send → sleep → exit
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 actor_hello
// Run:
//   ./bin/actor_hello

#include "ultranet/actor/api.h"

#include <iostream>
#include <thread>
#include <chrono>

using namespace ynet::actor;

// A simple counter actor — any C++ class can be an actor.
struct counter {
    int count = 0;

    void add(int x) {
        count += x;
        std::cout << "  [counter] added " << x << ", now " << count << std::endl;
    }

    void reset() {
        count = 0;
        std::cout << "  [counter] reset to 0" << std::endl;
    }

    int get() const { return count; }
};

int main() {
    std::cout << "=== Actor Hello Example ===" << std::endl;

    // 1. Create the actor system with 4 worker threads.
    actor_system system(4);

    // 2. Spawn a counter actor.
    actor_ref<counter> ref = spawn<counter>(system);
    std::cout << "[main] spawned actor id=" << ref.id() << std::endl;

    // 3. Send messages — they are queued and executed serially.
    ref.send<&counter::add>(10);
    ref.send<&counter::add>(20);
    ref.send<&counter::add>(30);
    std::cout << "[main] sent 3 messages (10+20+30)" << std::endl;

    // 4. Wait for the actor to process all messages.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // 5. Reset and send more.
    ref.send<&counter::reset>();
    ref.send<&counter::add>(100);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    std::cout << "[main] done" << std::endl;
    return 0;
}

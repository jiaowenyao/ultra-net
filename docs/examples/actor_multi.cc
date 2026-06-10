// Actor Framework — Multi-Actor Collaboration
//
// Demonstrates multiple actors sending messages to each other,
// with concurrent producers from multiple threads.
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 actor_multi
// Run:
//   ./bin/actor_multi

#include "ultranet/actor/api.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <string>

using namespace ynet::actor;

// A worker actor that processes named jobs.
struct worker {
    std::string name;
    int processed = 0;

    explicit worker(std::string n) : name(std::move(n)) {}

    void process(int job_id) {
        ++processed;
        std::cout << "  [" << name << "] processing job #" << job_id
                  << " (total: " << processed << ")" << std::endl;
    }
};

// A dispatcher that fans out work to multiple workers.
struct dispatcher {
    std::vector<actor_ref<worker>> workers;

    void dispatch(int job_id) {
        // Round-robin: send to the next worker.
        size_t idx = job_id % workers.size();
        workers[idx].send<&worker::process>(job_id);
    }
};

int main() {
    std::cout << "=== Actor Multi Example ===" << std::endl;

    actor_system system(4);

    // Spawn 3 worker actors.
    auto w1 = spawn<worker>(system, actor_config{}, "alpha");
    auto w2 = spawn<worker>(system, actor_config{}, "beta");
    auto w3 = spawn<worker>(system, actor_config{}, "gamma");

    // Create a dispatcher that fans out to all workers.
    actor_ref<dispatcher> disp = spawn<dispatcher>(
        system, actor_config{},
        std::vector<actor_ref<worker>>{w1, w2, w3});

    // Send jobs concurrently from multiple threads.
    std::cout << "[main] sending 15 jobs from 3 threads..." << std::endl;
    std::thread t1([&] { for (int i = 0; i < 5; ++i) disp.send<&dispatcher::dispatch>(i); });
    std::thread t2([&] { for (int i = 5; i < 10; ++i) disp.send<&dispatcher::dispatch>(i); });
    std::thread t3([&] { for (int i = 10; i < 15; ++i) disp.send<&dispatcher::dispatch>(i); });
    t1.join(); t2.join(); t3.join();

    // Wait for all actors to process.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::cout << "[main] done — all 15 jobs dispatched" << std::endl;
    return 0;
}

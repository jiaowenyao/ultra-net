// Distributed Training — Actor-based Parameter Server Demo
//
// Trains a 2-layer neural network using the Actor model:
//   1 PS actor (model weights) + N Worker actors (gradient computation)
//
// The dataset is synthetic: y = 3*x0 + 2*x1 - 1.5*x2 + 0.5*x3 + noise
// Workers compute gradients on disjoint data shards, send to PS.
//
// Build:
//   cd build && make actor_train
// Run:
//   ./bin/actor_train [num_workers=4] [num_epochs=10] [num_samples=10000]

#include "ultranet/actor/api.h"
#include "ultranet/actor/training/trainer.h"

#include <iostream>
#include <iomanip>
#include <chrono>
#include <thread>

using namespace ynet::actor;
using namespace ynet::actor::training;

int main(int argc, char* argv[]) {
    int num_workers = (argc > 1) ? std::atoi(argv[1]) : 4;
    int num_epochs  = (argc > 2) ? std::atoi(argv[2]) : 10;
    int num_samples = (argc > 3) ? std::atoi(argv[3]) : 10000;
    size_t hidden   = 32;

    std::cout << "╔══════════════════════════════════════════════╗" << std::endl;
    std::cout << "║   Distributed Training — Actor Model Demo   ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════╣" << std::endl;
    std::cout << "║  Workers : " << std::setw(4) << num_workers
              << "                              ║" << std::endl;
    std::cout << "║  Epochs  : " << std::setw(4) << num_epochs
              << "                              ║" << std::endl;
    std::cout << "║  Samples : " << std::setw(4) << num_samples
              << " (" << num_samples / 1000 << "K)                       ║" << std::endl;
    std::cout << "║  Model   : " << 10 << "→" << hidden << "→1 ("
              << (10*hidden + hidden + hidden*1 + 1) << " params)        ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════╝" << std::endl;

    // 1. Generate synthetic dataset.
    std::cout << "\n[1] Generating dataset..." << std::flush;
    auto t0 = std::chrono::steady_clock::now();
    auto ds = dataset::generate(num_samples, 10);
    auto t1 = std::chrono::steady_clock::now();
    std::cout << " done ("
              << std::chrono::duration<double, std::milli>(t1 - t0).count()
              << " ms)" << std::endl;

    // 2. Create actor system and training setup.
    std::cout << "[2] Creating actor system (" << (num_workers + 2)
              << " threads)..." << std::endl;
    actor_system system(num_workers + 2);
    auto trainer = make_training_setup(system, ds, num_workers, num_epochs, hidden);

    // 3. Run training.
    std::cout << "[3] Training..." << std::endl;
    auto metrics = trainer.run(ds);

    // 4. Report results.
    std::cout << "\n╔══════════════════════════════════════════════╗" << std::endl;
    std::cout << "║              Training Results                ║" << std::endl;
    std::cout << "╠══════════════════════════════════════════════╣" << std::endl;
    std::cout << "║  Total time    : " << std::setw(8) << std::fixed
              << std::setprecision(0) << metrics.total_time_ms << " ms            ║" << std::endl;
    std::cout << "║  Total steps   : " << std::setw(8)
              << metrics.total_steps << "                ║" << std::endl;
    std::cout << "║  Samples/sec   : " << std::setw(8) << std::fixed
              << std::setprecision(0)
              << (metrics.total_samples / (metrics.total_time_ms / 1000.0))
              << "              ║" << std::endl;
    std::cout << "║  Steps/sec     : " << std::setw(8) << std::fixed
              << std::setprecision(1) << metrics.steps_per_sec
              << "              ║" << std::endl;
    std::cout << "╚══════════════════════════════════════════════╝" << std::endl;

    return 0;
}

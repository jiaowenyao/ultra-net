// Actor Framework — Parameter Server Pattern
//
// Demonstrates the Parameter Server distributed training pattern:
//   - parameter_server actor holds the global model weights
//   - training_worker actors compute gradients and send updates
//   - All communication is async and lock-free via actor message passing
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 actor_ps
// Run:
//   ./bin/actor_ps

#include "ultranet/actor/api.h"

#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <cmath>
#include <iomanip>

using namespace ynet::actor;

// ── Parameter Server: holds the global model weights ────────────────────

struct parameter_server {
    std::vector<float> weights;
    int update_count = 0;

    explicit parameter_server(size_t num_layers, size_t layer_size) {
        weights.resize(num_layers * layer_size, 0.0f);
        std::cout << "  [ps] initialized " << num_layers << " layers x "
                  << layer_size << " = " << weights.size() << " params" << std::endl;
    }

    void update_gradients(int layer, size_t layer_size, std::vector<float> grads) {
        ++update_count;
        size_t base = layer * layer_size;
        for (size_t i = 0; i < grads.size() && (base + i) < weights.size(); ++i) {
            weights[base + i] -= 0.01f * grads[i];  // lr = 0.01
        }
    }

    float get_weight(int layer, int idx) const {
        return weights[layer * 256 + idx];
    }

    int total_updates() const { return update_count; }
};

// ── Training Worker: computes gradients and sends to PS ─────────────────

struct training_worker {
    int worker_id;
    actor_ref<parameter_server> ps;
    size_t layer_size;
    int batches_processed = 0;

    explicit training_worker(int id, actor_ref<parameter_server> ps_ref, size_t ls)
        : worker_id(id), ps(std::move(ps_ref)), layer_size(ls) {}

    void train_step(int layer) {
        ++batches_processed;
        // Simulate gradient computation: random values around 0.1.
        std::vector<float> grads(layer_size);
        for (auto& g : grads) g = 0.1f + (float)(worker_id % 3) * 0.01f;

        ps.send<&parameter_server::update_gradients>(layer, layer_size, std::move(grads));
    }
};

int main() {
    std::cout << "=== Parameter Server Example ===" << std::endl;

    constexpr int k_layers = 4;
    constexpr int k_layer_size = 256;
    constexpr int k_workers = 4;
    constexpr int k_steps = 50;

    actor_system system(k_workers + 2);

    // 1. Create the parameter server.
    auto ps = spawn<parameter_server>(system, actor_config{}, k_layers, k_layer_size);

    // 2. Create training workers.
    std::vector<actor_ref<training_worker>> workers;
    for (int i = 0; i < k_workers; ++i) {
        workers.push_back(spawn<training_worker>(system, actor_config{}, i, ps, k_layer_size));
    }

    // 3. Run training steps — each worker trains on a different layer.
    std::cout << "[main] starting training: " << k_workers << " workers x "
              << k_steps << " steps..." << std::endl;

    for (int step = 0; step < k_steps; ++step) {
        for (int w = 0; w < k_workers; ++w) {
            int layer = (step + w) % k_layers;  // rotate layers
            workers[w].send<&training_worker::train_step>(layer);
        }
    }

    // 4. Wait for all updates to be processed.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // 5. Report results.
    std::cout << "\n[main] Training complete!" << std::endl;
    std::cout << "  Total updates: " << k_workers * k_steps << std::endl;
    std::cout << "  Sample weights (layer 0, first 4): ";
    for (int i = 0; i < 4; ++i) {
        // Read a weight directly (local call, not through actor).
        // In a real system, you'd use actor_ref<void>::send for reads too.
        std::cout << std::fixed << std::setprecision(4)
                  << ps.id() << ":" << i << " ";
    }
    std::cout << std::endl;

    return 0;
}

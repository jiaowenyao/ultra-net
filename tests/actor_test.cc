// Inter-process actor communication test.
//
// Usage:
//   ./bin/actor_test recv [port=9000]    — receive pings (server)
//   ./bin/actor_test send [port=9000]    — send pings (client)
//
// Architecture:
//   recv: actor_system(listen_port) → spawn<ping_recver>("recver-1")
//         → transport accept loop handles inbound messages
//   send: Launcher → tcp_transport::connect() → remote_proxy
//         → deliver() packs envelope → TCP → recv

#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <csignal>
#include <atomic>
#include <cassert>

#include "ultranet/actor.hpp"
#include "ultranet/actor/net/transport.h"
#include "ultranet/actor/dist/remote_proxy.h"
#include "ultranet/actor/dist/serialization.h"
#include "ultranet/ultranet.h"

using namespace ynet::actor;
using namespace ynet::actor::net;
using namespace ynet::actor::dist;
using namespace ynet::async;
using namespace ynet::async::lifecycle;

// IMPORTANT: messages must be trivially copyable — the framework uses
// memcpy which does not work with std::string or std::vector.

struct ping_msg {
    int id;
    char text[32];
};

// ── Receiver actor ────────────────────────────────────────────────────

class ping_recver : public actor<ping_recver> {
public:
    int m_received = 0;

    ping_recver() {
        register_handler<ping_msg>([this](const ping_msg& msg) {
            m_received++;
            std::cout << "[recv] ping #" << msg.id
                      << " text=" << msg.text
                      << " (total=" << m_received << ")" << std::endl;
        });
    }
};

// ── Signal handling ──────────────────────────────────────────────────

static std::atomic<bool> g_running{true};

static void on_signal(int /*sig*/) {
    g_running.store(false, std::memory_order_release);
}

// ── Usage ────────────────────────────────────────────────────────────

static void print_usage() {
    std::cerr << "Usage:\n"
              << "  ./bin/actor_test recv [port=9000]\n"
              << "  ./bin/actor_test send [port=9000]\n";
}

// ── Receiver (server) ────────────────────────────────────────────────

static int run_recver(uint16_t port) {
    system_config cfg;
    cfg.listen_port = port;
    cfg.node_name = "recv-node";
    cfg.num_threads = 4;
    actor_system system(cfg);

    uint16_t actual = system.actual_port();
    if (actual == 0) {
        std::cerr << "[recv] ERROR: failed to bind to port " << port
                  << " (may be in use). Try a different port."
                  << std::endl;
        return 1;
    }

    auto ref = system.spawn<ping_recver>("recver-1");
    std::cout << "[recv] bound to port " << actual
              << ", actor: " << ref.uri().to_string() << std::endl;
    std::cout << "[recv] listening for pings (Ctrl-C to stop)..." << std::endl;

    while (g_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    auto* proxy = ref.proxy().get();
    auto* local = proxy->local_actor();
    auto* actor = static_cast<ping_recver*>(local);
    std::cout << "[recv] shutting down, received " << actor->m_received
              << " pings total" << std::endl;
    return 0;
}

// ── Sender (client) ──────────────────────────────────────────────────
//
// Uses Launcher + tcp_transport to connect to the receiver, creates a
// remote_proxy, and sends ping messages over TCP.

// Helper struct to hold send loop state (avoids GCC 13.2 lambda capture bug).
struct send_state {
    uint16_t port;
    actor_uri target_uri;
    std::atomic<int>& count;
    std::atomic<bool>& running;
};

// Sends messages over a persistent TCP connection.
// On WSL2, io_uring socket reads from the receiver may stall after the
// first message.  We optimise for reliability: connect once, send at a
// moderate rate, accept that sustained throughput is limited by the
// platform.
static Task<void> send_loop(std::shared_ptr<send_state> state) {
    int local_count = 0;
    std::shared_ptr<outbound_conn> conn = nullptr;
    tcp_transport transport(0, [](auto) -> Task<void> { co_return; });

    // Connect once.
    try {
        conn = co_await transport.connect("127.0.0.1", state->port);
    } catch (const std::exception& e) {
        std::cerr << "[send] cannot connect to 127.0.0.1:"
                  << state->port << " — is the receiver running? ("
                  << e.what() << ")" << std::endl;
        co_return;
    }

    if (!conn || !conn->is_valid()) {
        std::cerr << "[send] cannot connect to 127.0.0.1:"
                  << state->port << " — is the receiver running?"
                  << std::endl;
        co_return;
    }

    std::cout << "[send] connected, sending pings every 1s"
              << " (Ctrl-C to stop)..." << std::endl;

    while (state->running.load(std::memory_order_acquire)) {
        ping_msg msg{};
        msg.id = local_count;
        std::snprintf(msg.text, sizeof(msg.text), "hello-from-sender");

        auto envelope = pack_actor_message(
            state->target_uri,
            actor_type_hash<ping_msg>(),
            &msg, sizeof(msg));

        try {
            co_await conn->send(envelope);
            local_count++;
            state->count.store(local_count, std::memory_order_release);
        } catch (const std::exception& e) {
            std::cerr << "[send] send error: " << e.what() << std::endl;
            break;
        }

        co_await sleep_for(std::chrono::seconds(1));
    }
    co_return;
}

static int run_sender(uint16_t port) {
    std::cout << "[send] connecting to 127.0.0.1:" << port << std::endl;

    std::atomic<int> sent_count{0};

    int result = Launcher().threads(1).run(  // 1 thread to reduce WSL2 io_uring pressure
        [port, &sent_count](ShutdownCoordinator& sd) -> Task<void> {
            // Build the target actor URI (must match what recv spawns).
            auto target_uri = actor_uri::make_local(
                typeid(ping_recver).name(), "recver-1");
            std::cout << "[send] target: " << target_uri.to_string()
                      << " on 127.0.0.1:" << port << std::endl;

            auto state = std::make_shared<send_state>(
                send_state{port, target_uri, sent_count, g_running});

            // Submit the send loop as a coroutine.
            auto* sched = ExecutionContext::current();
            if (sched) {
                sched->submit(send_loop(state).release());
            }

            std::cout << "[send] sending pings (Ctrl-C to stop)..."
                      << std::endl;

            // Wait for shutdown signal.
            while (!sd.is_shutdown()
                   && g_running.load(std::memory_order_acquire)) {
                co_await sleep_for(std::chrono::milliseconds(500));
            }

            std::cout << "[send] sent " << sent_count.load()
                      << " pings, shutting down" << std::endl;
        });

    if (result != 0) {
        return result;
    }
    return 0;
}

// ── Main ─────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);
    std::string_view mode(argv[1]);

    uint16_t port = 9000;
    if (argc > 2) {
        port = static_cast<uint16_t>(std::atoi(argv[2]));
    }

    if (mode == "recv") {
        return run_recver(port);
    }

    if (mode == "send") {
        return run_sender(port);
    }

    print_usage();
    return 1;
}

// TCP Echo Server Example
//
// This is a complete, production-style TCP echo server using ultra-net.
// It demonstrates the canonical server pattern:
//   Socket → Bind → Listen → Accept loop → Read/Write per-session → Close
//
// Architecture:
//   - Uses WorkStealingThreadPool with 2 worker threads for io_uring event processing
//   - Accept loop runs as a coroutine on the pool; Accept has a 500ms timeout for
//     periodic shutdown checks
//   - Each accepted client is handled in a separate coroutine submitted to the pool
//   - Uses ShutdownCoordinator for graceful SIGINT/SIGTERM handling
//   - All I/O operations use io_uring under the hood (zero-copy where possible)
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 tcp_echo_server
// Run:
//   ./bin/tcp_echo_server [port]
// Test:
//   echo "hello" | nc localhost 8080

#include "ultranet/ultranet.h"

#include <iostream>
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

// Per-client echo session.
// Reads up to 4096 bytes, echoes them back, then closes.
Task<void> echo_session(int client_fd) {
    char buf[4096];

    // Read with timeout: if the client is idle for 5 seconds, cancel the read.
    // This prevents orphaned connections from leaking coroutine resources.
    Read reader(client_fd, buf, sizeof(buf));
    reader.with_timeout(std::chrono::seconds(5));

    auto n = co_await reader;
    if (!n) {
        // Read failed (timeout, connection reset, etc.)
        // Just close and let the coroutine frame be destroyed.
        co_await Close(client_fd);
        co_return;
    }

    if (*n == 0) {
        // Peer sent FIN — clean close
        co_await Close(client_fd);
        co_return;
    }

    // Echo the received data back.
    // Write uses io_uring_prep_write under the hood.
    auto written = co_await Write(client_fd, buf, *n);
    if (!written) {
        // Write failed
        co_await Close(client_fd);
        co_return;
    }

    // Graceful close via io_uring
    co_await Close(client_fd);
}

// Main server coroutine.
// Sets up the listening socket and enters the accept loop.
// The ShutdownCoordinator enables clean shutdown on Ctrl+C.
Task<void> tcp_echo_server(int port, ShutdownCoordinator& shutdown) {
    // Step 1: Create a non-blocking TCP socket
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket() failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int listen_fd = *sock;

    // Step 2: Set SO_REUSEADDR to avoid "address already in use" on restart
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // Step 3: Bind to port (synchronous — no io_uring opcode for bind)
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));

    // Step 4: Listen with backlog 256
    co_await Listen(listen_fd, 256);

    std::cout << "TCP echo server listening on port " << port << std::endl;

    // Step 5: Accept loop
    // The 500ms timeout on Accept allows periodic shutdown checks.
    // Without timeout, Accept blocks indefinitely, preventing Ctrl+C exit.
    while (!shutdown.is_shutdown()) {
        Accept acceptor(listen_fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));

        auto client = co_await acceptor;
        if (!client) {
            // ETIMEDOUT is expected — just loop back and check shutdown flag again
            if (client.error().value() == ETIMEDOUT) continue;
            // Other errors (e.g., EBADF after shutdown) — exit the loop
            break;
        }

        // Submit the session to the thread pool.
        // .release() detaches the coroutine handle so the pool owns its lifetime.
        // The pool's active_tasks counter keeps track — wait_all() won't return
        // until all sessions complete.
        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->submit(echo_session(*client).release());
        }
    }

    co_await Close(listen_fd);
    std::cout << "Server stopped." << std::endl;
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;

    // Launcher encapsulates the boilerplate:
    //   ShutdownCoordinator (signal handling)
    //   + WorkStealingThreadPool (async I/O workers)
    //   + ExecutionContext::Scope (bind pool to main thread)
    //   + submit + release (launch the server coroutine)
    //   + wait_all (block until graceful shutdown)
    //
    // When the user presses Ctrl+C:
    //   1. Signal handler fires → shutdown.is_shutdown() becomes true
    //   2. Accept loop's next timeout (≤500ms) returns ETIMEDOUT
    //   3. Loop breaks, listen_fd is closed
    //   4. Active sessions drain naturally
    //   5. wait_all returns when active_tasks reaches zero
    return Launcher()
        .threads(2)
        .run([port](lifecycle::ShutdownCoordinator& shutdown) -> Task<void> {
            co_await tcp_echo_server(port, shutdown);
        });
}

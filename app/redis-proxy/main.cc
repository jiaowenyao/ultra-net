// Redis protocol proxy — transparently forwards Redis commands from clients
// to a backend Redis instance.
//
// Usage: redis-proxy [listen_port] [backend_host] [backend_port]
//   Default: listen on :6380, backend 127.0.0.1:6379
//
// Example:
//   ./redis-proxy 6380 127.0.0.1 6379 &
//   redis-cli -p 6380 PING          # → forwarded to :6379, returns +PONG
//   redis-benchmark -p 6380 -t get  # benchmark through proxy

#include "ultranet/ultranet.h"
#include "proxy.hpp"
#include <csignal>
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::lifecycle;
using namespace redis_proxy;

int main(int argc, char* argv[]) {
    int listen_port = (argc > 1) ? std::atoi(argv[1]) : 6380;
    BackendConfig backend;
    backend.host = (argc > 2) ? argv[2] : "127.0.0.1";
    backend.port = (argc > 3) ? static_cast<uint16_t>(std::atoi(argv[3])) : 6379;

    std::signal(SIGPIPE, SIG_IGN);

    std::cout << "Redis Proxy" << std::endl;
    std::cout << "  listen :" << listen_port << std::endl;
    std::cout << "  backend " << backend.host << ":" << backend.port << std::endl;

    return Launcher()
        .threads(std::thread::hardware_concurrency())
        .run([listen_port, backend](ShutdownCoordinator& shutdown) -> Task<void> {
            co_await proxy_server(listen_port, backend, shutdown);
        });
}

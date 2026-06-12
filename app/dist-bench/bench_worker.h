// bench_worker.h — worker coroutine (shared by tcp and worker modes).
#pragma once

#include "ultranet/ultranet.h"
#include "bench_common.h"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

// Worker coroutine: connect to PS, send gradients, wait for acks.
// Defined as a plain function (not lambda) to avoid GCC 13.2 coroutine
// lambda capture corruption.  Parameters are passed by value.
inline Task<void> tcp_worker_run(int worker_id, uint16_t ps_port,
                                  size_t num_params, int steps_per_worker) {
    int fd = -1;
    for (int retry = 0; retry < 3 && fd < 0; ++retry) {
        auto sock = co_await TcpSocket::connect(
            "127.0.0.1", ps_port, std::chrono::seconds(5));
        fd = sock.release();
        if (fd < 0) {
            co_await sleep_for(std::chrono::milliseconds(500));
        }
    }
    if (fd < 0) {
        std::cerr << "[worker " << worker_id
                  << "] failed to connect to PS on port "
                  << ps_port << std::endl;
        co_return;
    }
    tune_tcp(fd);

    auto grads = init_gradients(num_params, worker_id);
    size_t vec_bytes = num_params * sizeof(float);

    for (int step = 0; step < steps_per_worker; ++step) {
        co_await Write(fd, grads.get(), vec_bytes);
        uint8_t ack = 0;
        Read r(fd, &ack, 1);
        r.with_timeout(std::chrono::seconds(5));
        auto rr = co_await r;
        if (!rr || *rr < 1) {
            break;
        }
    }
    co_await Close(fd);
}

// Standalone worker mode (connects to remote PS by host:port).
inline Task<void> worker_mode_run(const bench_config& cfg) {
    auto pos = cfg.worker_addr.find(':');
    if (pos == std::string::npos) {
        std::cerr << "Error: invalid address format '"
                  << cfg.worker_addr << "'. Expected host:port."
                  << std::endl;
        co_return;
    }
    std::string host = cfg.worker_addr.substr(0, pos);
    uint16_t port = static_cast<uint16_t>(
        std::strtoul(cfg.worker_addr.substr(pos + 1).c_str(),
                     nullptr, 10));

    auto sock = co_await TcpSocket::connect(
        host, port, std::chrono::seconds(5));
    if (!sock.is_valid()) {
        std::cerr << "[worker " << cfg.worker_id
                  << "] failed to connect to " << cfg.worker_addr
                  << std::endl;
        co_return;
    }
    int fd = sock.fd();
    tune_tcp(fd);

    auto grads = init_gradients(cfg.num_params, cfg.worker_id);
    size_t vec_bytes = cfg.num_params * sizeof(float);

    for (int step = 0; step < cfg.num_steps; ++step) {
        co_await Write(fd, grads.get(), vec_bytes);
        uint8_t ack = 0;
        Read r(fd, &ack, 1);
        r.with_timeout(std::chrono::seconds(5));
        auto rr = co_await r;
        if (!rr || *rr < 1) {
            break;
        }
    }
    co_await Close(fd);
}

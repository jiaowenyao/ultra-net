// Redis protocol proxy.
//
// Per-command processing loop with two key optimizations:
//   1. Fast boundary scan (detect_request_boundary) for RESP commands avoids
//      argument parsing + vector allocation on the hot path.
//   2. parse_request() is only used for INLINE fallback (rare in benchmarks).
//
// Each client session maintains one persistent backend connection.

#pragma once

#include "ultranet/ultranet.h"
#include "resp.hpp"
#include <vector>
#include <cstring>
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

namespace redis_proxy {

struct BackendConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 6379;
    std::chrono::milliseconds connect_timeout{3000};
    std::chrono::milliseconds read_timeout{5000};
};

Task<void> proxy_session(int client_fd, const BackendConfig& backend_cfg,
                         lifecycle::ShutdownCoordinator& shutdown) {
    auto backend_result = co_await TcpSocket::connect(
        backend_cfg.host, backend_cfg.port, backend_cfg.connect_timeout);
    TcpSocket backend = std::move(backend_result);

    std::vector<uint8_t> client_buf;
    std::vector<uint8_t> backend_buf;
    uint8_t io_buf[65536];

    while (!shutdown.is_shutdown()) {
        // Read from client
        Read client_reader(client_fd, io_buf, sizeof(io_buf));
        client_reader.with_timeout(std::chrono::seconds(30));
        auto client_n = co_await client_reader;
        if (!client_n || *client_n == 0) break;
        client_buf.insert(client_buf.end(), io_buf, io_buf + *client_n);

        // Process all complete commands in buffer
        while (!client_buf.empty()) {
            bool is_resp = (client_buf[0] == '*');

            // Detect command boundary and forward.
            // RESP mode: fast boundary scan, forward raw bytes (zero-alloc).
            // INLINE mode: full parse + re-encode to RESP.
            size_t consumed = 0;
            std::string encoded;

            if (is_resp) {
                consumed = detect_request_boundary(client_buf.data(),
                                                   client_buf.size());
                if (consumed == 0) {
                    // Fast scan failed — try full parse as fallback for
                    // edge cases (unlikely in practice).
                    auto req = parse_request(client_buf.data(), client_buf.size());
                    if (req.consumed == 0) break;
                    consumed = req.consumed;
                }
            } else {
                auto req = parse_request(client_buf.data(), client_buf.size());
                if (req.consumed == 0) break;
                consumed = req.consumed;
                encoded = encode_request(req.args);
            }

            if (consumed == 0) break;

            // Forward to backend
            const uint8_t* fwd = (is_resp && encoded.empty())
                ? client_buf.data()
                : reinterpret_cast<const uint8_t*>(encoded.data());
            size_t fwd_len = (is_resp && encoded.empty())
                ? consumed : encoded.size();

            size_t written = 0;
            while (written < fwd_len) {
                auto w = co_await backend.write(fwd + written,
                                                fwd_len - written);
                if (!w) {
                    std::cerr << "backend write: " << w.error().message() << std::endl;
                    co_await Close(client_fd);
                    co_return;
                }
                written += *w;
            }

            client_buf.erase(client_buf.begin(), client_buf.begin() + consumed);

            // Read response from backend
            size_t resp_consumed = 0;
            while (resp_consumed == 0) {
                resp_consumed = detect_response(backend_buf.data(),
                                                 backend_buf.size());
                if (resp_consumed > 0) break;

                Read backend_reader(backend.fd(), io_buf, sizeof(io_buf));
                backend_reader.with_timeout(backend_cfg.read_timeout);
                auto backend_n = co_await backend_reader;
                if (!backend_n || *backend_n == 0) {
                    std::cerr << "backend disconnected" << std::endl;
                    co_await Close(client_fd);
                    co_return;
                }
                backend_buf.insert(backend_buf.end(), io_buf,
                                   io_buf + *backend_n);
            }

            // Forward response to client
            size_t fwd_resp = 0;
            while (fwd_resp < resp_consumed) {
                Write client_writer(client_fd,
                    backend_buf.data() + fwd_resp,
                    resp_consumed - fwd_resp);
                auto w = co_await client_writer;
                if (!w) {
                    std::cerr << "client write: " << w.error().message() << std::endl;
                    co_await Close(client_fd);
                    co_return;
                }
                fwd_resp += *w;
            }
            backend_buf.erase(backend_buf.begin(),
                              backend_buf.begin() + resp_consumed);
        }
    }

    co_await Close(client_fd);
}

Task<void> proxy_server(int listen_port, const BackendConfig& backend_cfg,
                        lifecycle::ShutdownCoordinator& shutdown) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int fd = *sock;

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(listen_port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    co_await Listen(fd, 512);

    std::cout << "Redis proxy listening on :" << listen_port
              << " -> backend " << backend_cfg.host << ":" << backend_cfg.port
              << std::endl;

    while (!shutdown.is_shutdown()) {
        Accept acceptor(fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ETIMEDOUT) continue;
            break;
        }

        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->submit(proxy_session(*client, backend_cfg, shutdown).release());
        }
    }

    co_await Close(fd);
    std::cout << "Proxy stopped." << std::endl;
}

} // namespace redis_proxy

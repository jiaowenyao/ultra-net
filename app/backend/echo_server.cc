// Minimal HTTP echo backend for proxy benchmarking.
//
// Responds to every request with a JSON payload containing request info.
// Properly supports HTTP/1.1 keep-alive for realistic benchmarks.
//
// Build:
//   cd build && make -j2 backend
// Run:
//   ./bin/backend [port]

#include "ultranet/ultranet.h"

#include <iostream>
#include <sstream>
#include <vector>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <netinet/in.h>
#include <netinet/tcp.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

namespace {

int64_t get_content_length(const std::vector<http::Header>& headers) {
    for (const auto& h : headers) {
        std::string lower = h.name;
        std::transform(lower.begin(), lower.end(), lower.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (lower == "content-length") return std::stoll(h.value);
    }
    return -1;
}

bool request_keepalive(const http::HttpRequest& req) {
    auto conn = req.header("connection");
    if (conn.empty()) return req.http_version == "HTTP/1.1";
    std::string lower(conn);
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (lower.find("close") != std::string::npos) return false;
    return req.http_version == "HTTP/1.1";
}

Task<void> handle_connection(int client_fd) {
    std::vector<uint8_t> buf;
    uint8_t io_buf[16384];
    constexpr size_t kMaxBuf = 1024 * 1024;

    while (true) {
        // Read until headers are complete.
        http::HttpRequest req;
        size_t consumed = 0;

        while (consumed == 0) {
            Read reader(client_fd, io_buf, sizeof(io_buf));
            reader.with_timeout(std::chrono::seconds(30));
            auto n = co_await reader;
            if (!n || *n == 0) {
                co_await Close(client_fd);
                co_return;
            }
            buf.insert(buf.end(), io_buf, io_buf + *n);
            if (buf.size() > kMaxBuf) {
                co_await Close(client_fd);
                co_return;
            }
            consumed = req.parse(reinterpret_cast<const char*>(buf.data()),
                                 buf.size());
        }

        // Wait for body if Content-Length is present.
        int64_t body_len = get_content_length(req.headers);
        size_t total_req = consumed +
            (body_len > 0 ? static_cast<size_t>(body_len) : 0);

        while (buf.size() < total_req) {
            Read reader(client_fd, io_buf, sizeof(io_buf));
            reader.with_timeout(std::chrono::seconds(30));
            auto n = co_await reader;
            if (!n || *n == 0) {
                co_await Close(client_fd);
                co_return;
            }
            buf.insert(buf.end(), io_buf, io_buf + *n);
            if (buf.size() > kMaxBuf) {
                co_await Close(client_fd);
                co_return;
            }
        }

        // Build JSON response.
        std::ostringstream body;
        body << "{\"method\":\"" << http::method_string(req.method)
             << "\",\"path\":\"" << req.path
             << "\",\"server\":\"ultra-net-backend\"}";
        std::string body_str = body.str();

        http::HttpResponse resp;
        resp.http_version = "HTTP/1.1";
        resp.status_code = 200;
        resp.status_message = "OK";
        resp.body = body_str;
        resp.headers.push_back({"Content-Type", "application/json"});
        resp.headers.push_back({"Server", "ultra-net"});
        resp.headers.push_back({"Connection",
            request_keepalive(req) ? "keep-alive" : "close"});

        std::string serialized = resp.serialize();

        size_t written = 0;
        const uint8_t* send_data = reinterpret_cast<const uint8_t*>(serialized.data());
        while (written < serialized.size()) {
            auto w = co_await Write(client_fd, send_data + written,
                                    serialized.size() - written);
            if (!w) {
                co_await Close(client_fd);
                co_return;
            }
            written += *w;
        }

        // Drain consumed data from buffer.
        buf.erase(buf.begin(), buf.begin() + total_req);

        // Check keep-alive.
        if (!request_keepalive(req)) {
            co_await Close(client_fd);
            co_return;
        }
    }
}

Task<void> backend_server(int port, ShutdownCoordinator& shutdown) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }
    int listen_fd = *sock;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    co_await Listen(listen_fd, 512);

    std::cout << "Backend listening on :" << port << " (keep-alive enabled)" << std::endl;

    while (!shutdown.is_shutdown()) {
        Accept acceptor(listen_fd);
        acceptor.with_timeout(std::chrono::milliseconds(500));
        auto client = co_await acceptor;
        if (!client) {
            if (client.error().value() == ETIMEDOUT) continue;
            break;
        }
        auto* sched = ExecutionContext::current();
        if (sched) {
            sched->submit(handle_connection(*client).release());
        }
    }

    co_await Close(listen_fd);
    std::cout << "Backend stopped." << std::endl;
}

} // namespace

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 9000;
    return Launcher()
        .threads(4)
        .run([port](ShutdownCoordinator& shutdown) -> Task<void> {
            co_await backend_server(port, shutdown);
        });
}

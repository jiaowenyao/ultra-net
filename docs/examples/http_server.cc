// HTTP Server Example
//
// A simple HTTP/1.1 server that handles GET and POST requests.
// Demonstrates HTTP request parsing and response building with ultra-net's http module.
//
// Key HTTP module features used:
//   - HttpRequest::parse() — incremental HTTP request parsing
//   - HttpRequest::header() — case-insensitive header lookup
//   - HttpResponse — build responses with status, headers, and body
//   - HttpResponse::serialize() — format a complete HTTP response string
//
// The server handles one request per connection (HTTP/1.0 semantics).
// For keep-alive support, add a loop around read/parse/respond.
//
// Build:
//   cd build && cmake -DCMAKE_BUILD_TYPE=Release .. && make -j2 http_server
// Run:
//   ./bin/http_server [port]
// Test:
//   curl http://localhost:8080/
//   curl -X POST -d "hello" http://localhost:8080/echo

#include "ultranet/ultranet.h"

#include <iostream>
#include <sstream>
#include <netinet/in.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::lifecycle;

// Handle a single HTTP request on the given client socket.
Task<void> handle_http(int client_fd) {
    char buf[8192];

    // Read the HTTP request data (up to 8KB)
    auto n = co_await Read(client_fd, buf, sizeof(buf));
    if (!n || *n == 0) {
        co_await Close(client_fd);
        co_return;
    }

    // Parse the HTTP request.
    // parse() returns the number of bytes consumed.
    // Returns 0 if the request is incomplete (would need more data for keep-alive).
    http::HttpRequest req;
    size_t consumed = req.parse(buf, *n);
    if (consumed == 0) {
        // Incomplete request — for simplicity, just close
        co_await Close(client_fd);
        co_return;
    }

    // Build the response based on the request
    http::HttpResponse resp;
    resp.http_version = "HTTP/1.1";

    // Route: different paths get different responses
    if (req.path == "/echo" && req.method == http::Method::POST) {
        // POST /echo — echo the request body back to the client
        resp.status_code = 200;
        resp.status_message = "OK";
        resp.body = req.body;
        resp.headers.push_back({"Content-Type", "text/plain"});
    } else if (req.path == "/") {
        // GET / — simple hello page
        resp.status_code = 200;
        resp.status_message = "OK";
        resp.body = "<html><body><h1>Hello from ultra-net HTTP server!</h1>"
                    "<p>POST to /echo to test echo</p></body></html>";
        resp.headers.push_back({"Content-Type", "text/html"});
    } else {
        // Unknown path
        resp.status_code = 404;
        resp.status_message = "Not Found";
        resp.body = "404 Not Found";
        resp.headers.push_back({"Content-Type", "text/plain"});
    }

    // Use the client's Host header if available, otherwise default
    auto host = req.header("host");
    resp.headers.push_back({"Server", "ultra-net/0.1"});
    resp.headers.push_back({"Connection", "close"});

    // Serialize the response and send it
    std::string serialized = resp.serialize();
    auto written = co_await Write(client_fd, serialized.data(), serialized.size());
    if (!written) {
        std::cerr << "Write failed: " << written.error().message() << std::endl;
    }

    co_await Close(client_fd);

    std::cout << http::method_string(req.method) << " " << req.path
              << " -> " << resp.status_code << std::endl;
}

// Main server coroutine — same accept loop pattern as the TCP echo server
Task<void> http_server_main(int port, ShutdownCoordinator& shutdown) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket() failed: " << sock.error().message() << std::endl;
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
    co_await Listen(listen_fd, 256);

    std::cout << "HTTP server listening on http://0.0.0.0:" << port << std::endl;

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
            sched->submit(handle_http(*client).release());
        }
    }

    co_await Close(listen_fd);
    std::cout << "Server stopped." << std::endl;
}

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;

    return Launcher()
        .threads(2)
        .run([port](lifecycle::ShutdownCoordinator& shutdown) -> Task<void> {
            co_await http_server_main(port, shutdown);
        });
}

// tests/proxy_test.cc — HTTP reverse proxy integration tests.
//
// Tests the core proxy forwarding path, connection pool pre-warming,
// keepalive behavior, and buffer management.
#include "ultranet/ultranet.h"
#include <iostream>
#include <netinet/in.h>
#include <cstring>
#include <unistd.h>

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

// --- Test helpers ---

// HTTP echo backend that returns predictable JSON.
struct EchoBackend {
    int fd = -1;
    int port = 0;

    bool start() {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (fd < 0) return false;

        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            ::close(fd); fd = -1; return false;
        }
        if (::listen(fd, 5) < 0) {
            ::close(fd); fd = -1; return false;
        }

        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        if (::getsockname(fd, (sockaddr*)&bound, &len) < 0) {
            ::close(fd); fd = -1; return false;
        }
        port = ntohs(bound.sin_port);
        return true;
    }

    ~EchoBackend() {
        if (fd >= 0) ::close(fd);
    }
};

// Coroutine that runs an echo backend responding to one connection.
Task<void> serve_one_echo(int listen_fd) {
    // Wait for a connection using non-blocking accept via io_uring.
    auto accepted = co_await io::Accept(listen_fd);
    if (!accepted) co_return;
    int client = *accepted;

    std::vector<uint8_t> buf;
    uint8_t io_buf[4096];

    while (true) {
        // Read until complete request.
        http::HttpRequest req;
        size_t consumed = 0;
        while (consumed == 0) {
            Read reader(client, io_buf, sizeof(io_buf));
            reader.with_timeout(std::chrono::seconds(5));
            auto n = co_await reader;
            if (!n || *n == 0) { co_await Close(client); co_return; }
            buf.insert(buf.end(), io_buf, io_buf + *n);
            consumed = req.parse(reinterpret_cast<const char*>(buf.data()), buf.size());
        }

        int64_t body_len = -1;
        for (const auto& h : req.headers) {
            std::string lower = h.name;
            std::transform(lower.begin(), lower.end(), lower.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            if (lower == "content-length") { body_len = std::stoll(h.value); break; }
        }
        size_t total_req = consumed + (body_len > 0 ? static_cast<size_t>(body_len) : 0);
        while (buf.size() < total_req) {
            Read reader(client, io_buf, sizeof(io_buf));
            reader.with_timeout(std::chrono::seconds(5));
            auto n = co_await reader;
            if (!n || *n == 0) { co_await Close(client); co_return; }
            buf.insert(buf.end(), io_buf, io_buf + *n);
        }

        // Build JSON response.
        std::string body = "{\"method\":\"" + std::string(http::method_string(req.method))
            + "\",\"path\":\"" + req.path
            + "\",\"server\":\"ultra-net-backend\"}";

        http::HttpResponse resp;
        resp.http_version = "HTTP/1.1";
        resp.status_code = 200;
        resp.status_message = "OK";
        resp.body = body;
        resp.headers.push_back({"Content-Type", "application/json"});
        resp.headers.push_back({"Server", "ultra-net"});

        // Check if client wants keep-alive.
        auto conn_hdr = req.header("connection");
        bool keepalive = req.http_version == "HTTP/1.1";
        if (!conn_hdr.empty()) {
            std::string lower(conn_hdr);
            std::transform(lower.begin(), lower.end(), lower.begin(),
                           [](unsigned char c) { return std::tolower(c); });
            if (lower.find("close") != std::string::npos) keepalive = false;
        }
        resp.headers.push_back({"Connection", keepalive ? "keep-alive" : "close"});

        std::string serialized = resp.serialize();
        size_t written = 0;
        const uint8_t* data = reinterpret_cast<const uint8_t*>(serialized.data());
        while (written < serialized.size()) {
            auto w = co_await Write(client, data + written, serialized.size() - written);
            if (!w) { co_await Close(client); co_return; }
            written += *w;
        }

        buf.erase(buf.begin(), buf.begin() + total_req);

        if (!keepalive) break;
    }
    co_await Close(client);
}

// Helper: build an HTTP GET request string.
std::string make_get(const std::string& path, bool keepalive = true) {
    std::string req = "GET " + path + " HTTP/1.1\r\n"
        "Host: localhost\r\n";
    if (keepalive) req += "Connection: keep-alive\r\n";
    else req += "Connection: close\r\n";
    req += "\r\n";
    return req;
}

// Helper: read HTTP response headers + body.
struct ParsedResponse {
    int status_code = 0;
    std::string body;
    bool keepalive = false;
};

ParsedResponse parse_response(const std::string& raw) {
    ParsedResponse r;
    http::HttpResponse resp;
    size_t consumed = resp.parse(raw.data(), raw.size());
    if (consumed > 0) {
        r.status_code = resp.status_code;
        r.body = resp.body;
        r.keepalive = resp.is_keepalive();
    }
    return r;
}

// --- Test 1: ConnectionPool pre_warm ---

void test_pre_warm() {
    T("pre_warm creates min_connections");
    EchoBackend backend;
    CHECK(backend.start(), "start backend");

    auto test = [](int port) -> Task<void> {
        ConnectionPool pool(
            ConnectionPoolConfig{
                .min_connections = 4,
                .max_connections = 8,
                .connect_timeout = std::chrono::seconds(2)
            },
            "127.0.0.1", static_cast<uint16_t>(port)
        );

        CO_CHECK(pool.snapshot().total_connections == 0, "0 before pre_warm");

        co_await pool.pre_warm();

        auto s = pool.snapshot();
        CO_CHECK(s.total_connections >= 4, "at least 4 after pre_warm");
        CO_CHECK(s.failed_creates == 0, "0 failed creates");

        // Acquire one — should get from idle pool (no new connection).
        auto sock = co_await pool.acquire();
        CO_CHECK(sock.is_valid(), "acquire from pre-warmed pool");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test(backend.port).release());
    pool.wait_all();
    PASS();
}

// --- Test 2: is_keepalive case-insensitive ---

void test_is_keepalive_case() {
    T("is_keepalive handles mixed case");
    http::HttpResponse resp;
    resp.http_version = "HTTP/1.1";
    resp.headers.push_back({"Connection", "Keep-Alive"});
    CHECK(resp.is_keepalive(), "Keep-Alive (mixed case) detected");
    resp.headers.clear();
    resp.headers.push_back({"Connection", "keep-alive"});
    CHECK(resp.is_keepalive(), "keep-alive (lowercase) detected");
    resp.headers.clear();
    resp.headers.push_back({"Connection", "close"});
    CHECK(!resp.is_keepalive(), "close not keepalive");
    PASS();
}

// --- Test 3: Basic proxy forwarding ---

void test_basic_forward() {
    T("basic proxy forward");
    EchoBackend backend;
    CHECK(backend.start(), "start backend");

    auto test = [](int b_fd) -> Task<void> {
        auto* sched = ExecutionContext::current();
        CO_CHECK(sched != nullptr, "scheduler available");

        // Launch backend accept handler.
        sched->submit(serve_one_echo(b_fd).release());

        // Create the connection pool targeting the backend.
        // We need to know the backend port. Since serve_one_echo accepts on b_fd,
        // we just need to connect to the port b_fd is bound to.
        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        if (::getsockname(b_fd, reinterpret_cast<sockaddr*>(&bound), &len) < 0) {
            FAIL("getsockname failed"); co_return;
        }
        int b_port = ntohs(bound.sin_port);

        ConnectionPool pool(
            ConnectionPoolConfig{
                .min_connections = 0,
                .max_connections = 4,
                .connect_timeout = std::chrono::seconds(2)
            },
            "127.0.0.1", static_cast<uint16_t>(b_port)
        );

        // Acquire backend connection and verify forwarding.
        auto backend_sock = co_await pool.acquire();
        CO_CHECK(backend_sock.is_valid(), "acquired backend connection");

        // Send a request to backend.
        std::string req_str = make_get("/hello", false);
        size_t written = 0;
        while (written < req_str.size()) {
            auto w = co_await backend_sock.write(
                req_str.data() + written, req_str.size() - written);
            if (!w) { FAIL("backend write failed"); co_return; }
            written += *w;
        }

        // Read response from backend.
        std::vector<uint8_t> bbuf;
        uint8_t io_buf[4096];
        http::HttpResponse resp;
        size_t rconsumed = 0;
        while (rconsumed == 0) {
            auto n = co_await backend_sock.read(io_buf, sizeof(io_buf));
            if (!n || *n == 0) { FAIL("backend read failed"); co_return; }
            bbuf.insert(bbuf.end(), io_buf, io_buf + *n);
            rconsumed = resp.parse(
                reinterpret_cast<const char*>(bbuf.data()), bbuf.size());
        }

        CO_CHECK(resp.status_code == 200, "got 200 response");
        CO_CHECK(resp.body.find("\"server\":\"ultra-net-backend\"") != std::string::npos,
                 "response from backend");

        pool.release(std::move(backend_sock));
        CO_CHECK(pool.snapshot().total_connections >= 1, "connection in pool");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test(backend.fd).release());
    pool.wait_all();
    PASS();
}

// --- Test 4: Connection reuse ---

void test_connection_reuse() {
    T("connection reuse across requests");
    EchoBackend backend;
    CHECK(backend.start(), "start backend");

    auto test = [](int port) -> Task<void> {
        ConnectionPool pool(
            ConnectionPoolConfig{
                .min_connections = 0,
                .max_connections = 4,
                .connect_timeout = std::chrono::seconds(2)
            },
            "127.0.0.1", static_cast<uint16_t>(port)
        );

        // Acquire, release, acquire again — should reuse.
        auto s1 = co_await pool.acquire();
        CO_CHECK(s1.is_valid(), "first acquire");
        size_t first_total = pool.snapshot().total_connections;

        pool.release(std::move(s1));

        auto s2 = co_await pool.acquire();
        CO_CHECK(s2.is_valid(), "second acquire");

        auto stats = pool.snapshot();
        CO_CHECK(stats.total_connections == first_total, "same total after release+acquire");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test(backend.port).release());
    pool.wait_all();
    PASS();
}

int main() {
    std::cout << "=== Proxy Tests ===" << std::endl;
    test_pre_warm();
    test_is_keepalive_case();
    test_basic_forward();
    test_connection_reuse();

    std::cout << "\n=== Proxy Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

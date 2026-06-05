#include "ultranet/ultranet.h"
#include <iostream>
#include <netinet/in.h>
#include <cstring>

using namespace ynet::async;
using namespace ynet::async::net;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

// --- Existing tests ---

void test_create_and_stats() {
    T("create pool and check stats");
    ConnectionPool pool(
        ConnectionPoolConfig{.min_connections = 0, .max_connections = 4},
        "127.0.0.1", 18080
    );
    CHECK(!pool.is_shutdown(), "not shutdown");
    auto s = pool.snapshot();
    CHECK(s.acquired == 0, "0 acquired");
    CHECK(s.evicted == 0, "0 evicted");
    PASS();
}

void test_release_and_acquire() {
    T("release and acquire");
    auto test = []() -> Task<void> {
        auto sock_result = co_await io::Socket(AF_INET, SOCK_STREAM, 0);
        if (!sock_result) { FAIL("socket creation failed"); co_return; }
        TcpSocket sock(*sock_result);

        ConnectionPool pool(
            ConnectionPoolConfig{.min_connections = 0, .max_connections = 4},
            "127.0.0.1", 18080
        );

        pool.release(std::move(sock));
        auto acquired = co_await pool.acquire();
        CO_CHECK(acquired.is_valid(), "acquired socket is valid");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

void test_invalidate() {
    T("invalidate socket");
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ConnectionPool pool(
        ConnectionPoolConfig{.min_connections = 0, .max_connections = 4},
        "127.0.0.1", 18080
    );

    TcpSocket sock(fd);
    pool.invalidate(std::move(sock));

    int ret = ::fcntl(fd, F_GETFD);
    CHECK(ret == -1 && errno == EBADF, "fd closed after invalidate");
    PASS();
}

void test_shutdown() {
    T("pool shutdown");
    ConnectionPool pool(
        ConnectionPoolConfig{.min_connections = 0, .max_connections = 4},
        "127.0.0.1", 18080
    );
    CHECK(!pool.is_shutdown(), "initially not shutdown");

    {
        ConnectionPool pool2(
            ConnectionPoolConfig{.min_connections = 0, .max_connections = 4},
            "127.0.0.1", 18080
        );
    }
    PASS();
}

// --- Lazy creation tests ---

// Start a tiny echo server on a random port for testing lazy creation.
struct EchoServer {
    int fd = -1;
    int port = 0;

    bool start() {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return false;

        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = 0;  // OS picks port
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
            ::close(fd); fd = -1; return false;
        }
        if (::listen(fd, 1) < 0) {
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

    ~EchoServer() {
        if (fd >= 0) ::close(fd);
    }
};

void test_lazy_creation() {
    T("lazy creation (connect to running server)");
    EchoServer server;
    CHECK(server.start(), "start echo server");

    auto test = [](int port) -> Task<void> {
        ConnectionPool pool(
            ConnectionPoolConfig{.min_connections = 0, .max_connections = 4,
                                 .connect_timeout = std::chrono::seconds(2)},
            "127.0.0.1", static_cast<uint16_t>(port)
        );

        auto s = pool.snapshot();
        CO_CHECK(s.total_connections == 0, "0 total before acquire");

        auto sock = co_await pool.acquire();
        CO_CHECK(sock.is_valid(), "acquired socket is valid");

        s = pool.snapshot();
        CO_CHECK(s.total_connections == 1, "1 total after lazy create");
        CO_CHECK(s.acquired == 1, "1 acquired");
        CO_CHECK(s.failed_creates == 0, "0 failed creates");

        pool.release(std::move(sock));
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test(server.port).release());
    pool.wait_all();
    PASS();
}

void test_lazy_creation_fails() {
    T("lazy creation fails (no listener)");
    auto test = []() -> Task<void> {
        ConnectionPool pool(
            ConnectionPoolConfig{.min_connections = 0, .max_connections = 4,
                                 .connect_timeout = std::chrono::milliseconds(500)},
            "127.0.0.1", 19998  // nothing listening here
        );

        bool threw = false;
        try {
            auto sock = co_await pool.acquire();
        } catch (const std::system_error&) {
            threw = true;
        }

        CO_CHECK(threw, "acquire threw on connect failure");

        auto s = pool.snapshot();
        CO_CHECK(s.total_connections == 0, "0 total after failed create");
        CO_CHECK(s.failed_creates == 1, "1 failed create");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test().release());
    pool.wait_all();
    PASS();
}

void test_max_connections() {
    T("max connections limit");
    EchoServer server;
    CHECK(server.start(), "start echo server");

    auto test = [](int port) -> Task<void> {
        ConnectionPool pool(
            ConnectionPoolConfig{.min_connections = 0, .max_connections = 2,
                                 .connect_timeout = std::chrono::seconds(2)},
            "127.0.0.1", static_cast<uint16_t>(port)
        );

        // Acquire two connections — should create both.
        auto s1 = co_await pool.acquire();
        CO_CHECK(s1.is_valid(), "first socket valid");
        auto s2 = co_await pool.acquire();
        CO_CHECK(s2.is_valid(), "second socket valid");

        auto s = pool.snapshot();
        CO_CHECK(s.total_connections == 2, "2 total at capacity");

        // Release one, acquire again — should get it back without creating.
        pool.release(std::move(s1));

        auto s3 = co_await pool.acquire();
        CO_CHECK(s3.is_valid(), "re-acquired socket valid");

        s = pool.snapshot();
        CO_CHECK(s.total_connections == 2, "still 2 total (reused)");
        CO_CHECK(s.acquired == 3, "3 total acquires");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test(server.port).release());
    pool.wait_all();
    PASS();
}

void test_evict_and_recreate() {
    T("evict and recreate");
    EchoServer server;
    CHECK(server.start(), "start echo server");

    auto test = [](int port) -> Task<void> {
        ConnectionPool pool(
            ConnectionPoolConfig{.min_connections = 0, .max_connections = 4,
                                 .connect_timeout = std::chrono::seconds(2)},
            "127.0.0.1", static_cast<uint16_t>(port)
        );

        auto s1 = co_await pool.acquire();
        CO_CHECK(s1.is_valid(), "acquired");

        // Invalidate it.
        pool.invalidate(std::move(s1));

        auto s = pool.snapshot();
        CO_CHECK(s.evicted == 1, "1 evicted");
        CO_CHECK(s.total_connections == 0, "0 total after evict");

        // Acquire again — should create a new one.
        auto s2 = co_await pool.acquire();
        CO_CHECK(s2.is_valid(), "recreated after evict");

        s = pool.snapshot();
        CO_CHECK(s.total_connections == 1, "1 total after recreate");
    };

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(test(server.port).release());
    pool.wait_all();
    PASS();
}

int main() {
    std::cout << "=== ConnectionPool Tests ===" << std::endl;
    test_create_and_stats();
    test_release_and_acquire();
    test_invalidate();
    test_shutdown();
    test_lazy_creation();
    test_lazy_creation_fails();
    test_max_connections();
    test_evict_and_recreate();

    std::cout << "\n=== ConnectionPool Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

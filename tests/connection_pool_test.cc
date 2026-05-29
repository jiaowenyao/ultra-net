#include "ultranet/ultranet.h"
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::net;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

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
        // Create a socket manually and release to pool
        auto sock_result = co_await io::Socket(AF_INET, SOCK_STREAM, 0);
        if (!sock_result) { FAIL("socket creation failed"); co_return; }
        TcpSocket sock(*sock_result);

        ConnectionPool pool(
            ConnectionPoolConfig{.min_connections = 0, .max_connections = 4},
            "127.0.0.1", 18080
        );

        // Acquire should block since pool is empty and no echo server
        // Instead, release a socket first then acquire
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

    // After invalidate and socket destructor, fd should be closed
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
        // Destructor triggers shutdown
    }
    PASS();
}

int main() {
    std::cout << "=== ConnectionPool Tests ===" << std::endl;
    test_create_and_stats();
    test_release_and_acquire();
    test_invalidate();
    test_shutdown();

    std::cout << "\n=== ConnectionPool Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

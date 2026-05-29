#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <fcntl.h>

using namespace ynet::async;
using namespace ynet::async::net;
using namespace ynet::async::io;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

// Start an echo server, return its port
static Task<void> mini_echo(int port, std::atomic<bool>& ready) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) { ready = true; co_return; }
    int listen_fd = *sock;

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    co_await Bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    co_await Listen(listen_fd, 4);
    ready = true;

    Accept acceptor(listen_fd);
    acceptor.with_timeout(std::chrono::milliseconds(1000));
    auto client = co_await acceptor;
    if (!client) { co_await Close(listen_fd); co_return; }
    int client_fd = *client;

    char buf[256];
    auto data = co_await Read(client_fd, buf, sizeof(buf));
    if (data && *data > 0) {
        co_await Write(client_fd, buf, *data);
    }
    co_await Close(client_fd);
    co_await Close(listen_fd);
}

// Test 1: connect and echo
void test_connect_and_echo() {
    T("connect and echo");
    int port = 18801;
    std::atomic<bool> ready{false};

    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(mini_echo(port, ready).release());

    while (!ready) { usleep(1000); }
    usleep(50000); // give server time to start accepting

    auto task = []() -> Task<void> {
        auto sock = co_await TcpSocket::connect("127.0.0.1", 18801, std::chrono::milliseconds(2000));
        if (!sock.is_valid()) {
            std::cerr << "connect failed" << std::endl;
            co_return;
        }

        const char* msg = "hello";
        auto w = co_await sock.write(msg, 5);
        if (!w) { co_await sock.close(); co_return; }

        char buf[256]{};
        auto r = co_await sock.read(buf, sizeof(buf));
        if (!r) { co_await sock.close(); co_return; }

        CO_CHECK(*r == 5, "read size mismatch");
        CO_CHECK(std::memcmp(buf, "hello", 5) == 0, "echo content mismatch");

        co_await sock.close();
    };
    pool.submit(task().release());
    pool.wait_all();
    PASS();
}

// Test 2: RAII auto-close
void test_raii_close() {
    T("RAII auto-close");
    int fd = -1;
    {
        auto s = ::socket(AF_INET, SOCK_STREAM, 0);
        CHECK(s >= 0, "socket() failed");
        fd = s;
        TcpSocket sock(s);
    }
    // After destruction, fd should be closed
    int ret = ::fcntl(fd, F_GETFD);
    CHECK(ret == -1 && errno == EBADF, "fd not closed by destructor");
    PASS();
}

// Test 3: move semantics
void test_move_semantics() {
    T("move semantics");
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0, "socket() failed");
    TcpSocket a(fd);

    TcpSocket b(std::move(a));
    CHECK(!a.is_valid(), "moved-from should be invalid");
    CHECK(b.is_valid(), "moved-to should be valid");
    CHECK(b.fd() == fd, "fd preserved after move");

    TcpSocket c;
    c = std::move(b);
    CHECK(!b.is_valid(), "move-assigned-from should be invalid");
    CHECK(c.is_valid(), "move-assigned-to should be valid");

    c.close();
    PASS();
}

// Test 4: connection refused
void test_connection_refused() {
    T("connection refused");
    auto task = []() -> Task<void> {
        try {
            co_await TcpSocket::connect("127.0.0.1", 59999, std::chrono::milliseconds(1000));
            std::cerr << "expected exception" << std::endl;
        } catch (const std::system_error&) {
            // expected
        }
    };
    scheduling::WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);
    pool.submit(task().release());
    pool.wait_all();
    PASS();
}

// Test 5: release()
void test_release() {
    T("release() ownership transfer");
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0, "socket() failed");
    TcpSocket sock(fd);

    int extracted = sock.release();
    CHECK(!sock.is_valid(), "should be invalid after release");
    CHECK(extracted == fd, "released fd should match");
    CHECK(::fcntl(extracted, F_GETFD) >= 0, "released fd should be open");

    ::close(extracted);
    PASS();
}

int main() {
    std::cout << "=== TcpSocket Tests ===" << std::endl;
    test_connect_and_echo();
    test_raii_close();
    test_move_semantics();
    test_connection_refused();
    test_release();

    std::cout << "\n=== TcpSocket Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

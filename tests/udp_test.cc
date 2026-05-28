#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>
#include <atomic>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace ynet::async;
using namespace ynet::async::io;

static int passed = 0;
static int failed = 0;

#define TEST(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(msg) do { std::cout << "FAILED: " << msg << std::endl; ++failed; } while(0)
#define CO_CHECK(cond, msg) do { if (!(cond)) { FAIL(msg); co_return; } } while(0)

// ========== Test helpers ==========

Task<void> udp_echo_server(int port, scheduling::WorkStealingThreadPool& pool) {
    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int fd = *sock;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));

    char buf[1024];
    for (int i = 0; i < 5; ++i) {
        RecvFrom recvfrom(fd, buf, sizeof(buf));
        recvfrom.with_timeout(std::chrono::seconds(3));
        auto data = co_await recvfrom;
        if (!data) break;

        size_t n = *data;
        auto src = recvfrom.source_addr();
        auto srclen = recvfrom.source_addr_len();

        SendTo sendto(fd, buf, n, (sockaddr*)&src, srclen);
        co_await sendto;
    }
    co_await Close(fd);
}

// ========== Test 1: Basic UDP sendto/recvfrom ==========

Task<void> test_basic_udp_echo(scheduling::WorkStealingThreadPool& pool) {
    TEST("basic UDP echo (sendto/recvfrom)");

    // Create server socket
    auto server_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    CO_CHECK(server_sock.has_value(), "server socket failed");
    int sfd = *server_sock;

    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = INADDR_ANY;
    auto b = co_await Bind(sfd, (sockaddr*)&saddr, sizeof(saddr));
    CO_CHECK(b.has_value(), "server bind failed");

    // Get actual port
    sockaddr_in bound_addr{};
    socklen_t bound_len = sizeof(bound_addr);
    ::getsockname(sfd, (sockaddr*)&bound_addr, &bound_len);
    uint16_t port = ntohs(bound_addr.sin_port);

    // Create client socket
    auto client_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    CO_CHECK(client_sock.has_value(), "client socket failed");
    int cfd = *client_sock;

    // Set up destination
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(port);
    dest.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // Send
    const char* msg = "hello-udp";
    auto sent = co_await SendTo(cfd, msg, strlen(msg), (sockaddr*)&dest, sizeof(dest));
    CO_CHECK(sent.has_value(), "sendto failed");
    CO_CHECK(*sent == strlen(msg), "sendto size mismatch");

    // Receive on server
    char buf[1024];
    RecvFrom recvfrom(sfd, buf, sizeof(buf));
    recvfrom.with_timeout(std::chrono::seconds(2));
    auto data = co_await recvfrom;
    CO_CHECK(data.has_value(), "recvfrom failed");
    CO_CHECK(*data == strlen(msg), "recvfrom size mismatch");
    CO_CHECK(std::memcmp(buf, msg, *data) == 0, "data mismatch");

    // Verify source address
    auto& src = recvfrom.source_addr();
    auto addrlen = recvfrom.source_addr_len();
    CO_CHECK(addrlen == sizeof(sockaddr_in), "source addrlen mismatch");
    auto* sin = reinterpret_cast<const sockaddr_in*>(&src);
    CO_CHECK(sin->sin_family == AF_INET, "source family mismatch");

    // Echo back
    auto echo = co_await SendTo(sfd, buf, *data, (sockaddr*)&src, addrlen);
    CO_CHECK(echo.has_value(), "echo sendto failed");

    // Receive echo on client
    char ebuf[1024];
    RecvFrom recv2(cfd, ebuf, sizeof(ebuf));
    recv2.with_timeout(std::chrono::seconds(2));
    auto edata = co_await recv2;
    CO_CHECK(edata.has_value(), "echo recvfrom failed");
    CO_CHECK(*edata == strlen(msg), "echo size mismatch");
    CO_CHECK(std::memcmp(ebuf, msg, *edata) == 0, "echo data mismatch");

    co_await Close(sfd);
    co_await Close(cfd);
    PASS();
}

// ========== Test 2: UDP with timeout ==========

Task<void> test_udp_timeout(scheduling::WorkStealingThreadPool& pool) {
    TEST("UDP recvfrom timeout");

    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    CO_CHECK(sock.has_value(), "socket failed");
    int fd = *sock;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(0);
    addr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(fd, (sockaddr*)&addr, sizeof(addr));

    // No one will send to this port — expect timeout
    char buf[64];
    RecvFrom recvfrom(fd, buf, sizeof(buf));
    recvfrom.with_timeout(std::chrono::milliseconds(100));
    auto data = co_await recvfrom;
    CO_CHECK(!data.has_value(), "expected timeout/failure");
    CO_CHECK(data.error().value() == ETIMEDOUT, "expected ETIMEDOUT");

    co_await Close(fd);
    PASS();
}

// ========== Test 3: UDP connected (connect + Write/Read) ==========

Task<void> test_udp_connected(scheduling::WorkStealingThreadPool& pool) {
    TEST("UDP connected socket (Connect + Write/Read)");

    auto server_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int sfd = *server_sock;
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(sfd, (sockaddr*)&saddr, sizeof(saddr));

    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    ::getsockname(sfd, (sockaddr*)&bound, &blen);

    auto client_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int cfd = *client_sock;

    // Connect UDP socket (sets default destination)
    auto conn = co_await Connect(cfd, (sockaddr*)&bound, sizeof(bound));
    CO_CHECK(conn.has_value(), "UDP connect failed");

    // Send via Write (uses connected destination)
    const char* msg = "connected-udp";
    auto wrote = co_await Write(cfd, msg, strlen(msg));
    CO_CHECK(wrote.has_value(), "write failed");
    CO_CHECK(*wrote == strlen(msg), "write size mismatch");

    // Receive on server
    char buf[1024];
    RecvFrom recvfrom(sfd, buf, sizeof(buf));
    recvfrom.with_timeout(std::chrono::seconds(2));
    auto data = co_await recvfrom;
    CO_CHECK(data.has_value(), "recvfrom failed");
    CO_CHECK(*data == strlen(msg), "size mismatch");
    CO_CHECK(std::memcmp(buf, msg, *data) == 0, "data mismatch");

    co_await Close(sfd);
    co_await Close(cfd);
    PASS();
}

// ========== Test 4: Datagram boundaries preserved ==========

Task<void> test_datagram_boundaries(scheduling::WorkStealingThreadPool& pool) {
    TEST("UDP datagram boundaries preserved");

    auto server_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int sfd = *server_sock;
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(sfd, (sockaddr*)&saddr, sizeof(saddr));

    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    ::getsockname(sfd, (sockaddr*)&bound, &blen);

    auto client_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int cfd = *client_sock;

    // Send 3 datagrams of different sizes
    const char* msgs[] = {"a", "bb", "ccc"};
    for (int i = 0; i < 3; ++i) {
        auto sent = co_await SendTo(cfd, msgs[i], strlen(msgs[i]),
            (sockaddr*)&bound, sizeof(bound));
        CO_CHECK(sent.has_value(), "sendto failed");
    }

    // Receive each datagram — boundaries must be preserved
    for (int i = 0; i < 3; ++i) {
        char buf[1024];
        RecvFrom recvfrom(sfd, buf, sizeof(buf));
        recvfrom.with_timeout(std::chrono::seconds(2));
        auto data = co_await recvfrom;
        CO_CHECK(data.has_value(), "recvfrom failed");
        CO_CHECK(*data == strlen(msgs[i]), "datagram boundary broken");
        CO_CHECK(std::memcmp(buf, msgs[i], *data) == 0, "data mismatch");
    }

    co_await Close(sfd);
    co_await Close(cfd);
    PASS();
}

// ========== Test 5: Multiple senders ==========

Task<void> test_multiple_senders(scheduling::WorkStealingThreadPool& pool) {
    TEST("UDP multiple senders to one receiver");

    auto server_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int sfd = *server_sock;
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(sfd, (sockaddr*)&saddr, sizeof(saddr));

    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    ::getsockname(sfd, (sockaddr*)&bound, &blen);

    const int NUM_SENDERS = 5;
    int client_fds[NUM_SENDERS];
    for (int i = 0; i < NUM_SENDERS; ++i) {
        auto cs = co_await Socket(AF_INET, SOCK_DGRAM, 0);
        client_fds[i] = *cs;
    }

    // All senders send a unique message
    for (int i = 0; i < NUM_SENDERS; ++i) {
        char msg[32];
        snprintf(msg, sizeof(msg), "sender-%d", i);
        auto sent = co_await SendTo(client_fds[i], msg, strlen(msg),
            (sockaddr*)&bound, sizeof(bound));
        CO_CHECK(sent.has_value(), "sendto failed");
    }

    // Receive all messages
    bool received[NUM_SENDERS] = {};
    for (int i = 0; i < NUM_SENDERS; ++i) {
        char buf[1024];
        RecvFrom recvfrom(sfd, buf, sizeof(buf));
        recvfrom.with_timeout(std::chrono::seconds(2));
        auto data = co_await recvfrom;
        CO_CHECK(data.has_value(), "recvfrom failed");
        for (int s = 0; s < NUM_SENDERS; ++s) {
            char expected[32];
            snprintf(expected, sizeof(expected), "sender-%d", s);
            if (static_cast<size_t>(*data) == strlen(expected) &&
                std::memcmp(buf, expected, *data) == 0) {
                received[s] = true;
                break;
            }
        }
    }

    for (int i = 0; i < NUM_SENDERS; ++i) {
        CO_CHECK(received[i], "missing message from a sender");
    }

    co_await Close(sfd);
    for (int i = 0; i < NUM_SENDERS; ++i) co_await Close(client_fds[i]);
    PASS();
}

// ========== Test 6: UDP zero-length datagram ==========

Task<void> test_zero_length_datagram(scheduling::WorkStealingThreadPool& pool) {
    TEST("UDP zero-length datagram");

    auto server_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int sfd = *server_sock;
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(sfd, (sockaddr*)&saddr, sizeof(saddr));

    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    ::getsockname(sfd, (sockaddr*)&bound, &blen);

    auto client_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int cfd = *client_sock;

    // Send zero-length datagram
    auto sent = co_await SendTo(cfd, nullptr, 0, (sockaddr*)&bound, sizeof(bound));
    CO_CHECK(sent.has_value(), "sendto failed");
    CO_CHECK(*sent == 0, "expected 0 bytes sent");

    // Receive it
    char buf[64];
    RecvFrom recvfrom(sfd, buf, sizeof(buf));
    recvfrom.with_timeout(std::chrono::seconds(2));
    auto data = co_await recvfrom;
    CO_CHECK(data.has_value(), "recvfrom failed");
    CO_CHECK(*data == 0, "expected 0-byte datagram");

    co_await Close(sfd);
    co_await Close(cfd);
    PASS();
}

// ========== Test 7: RecvFrom flags (MSG_TRUNC detection) ==========

Task<void> test_recvfrom_flags(scheduling::WorkStealingThreadPool& pool) {
    TEST("RecvFrom flags (MSG_TRUNC detection)");

    auto server_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int sfd = *server_sock;
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(sfd, (sockaddr*)&saddr, sizeof(saddr));

    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    ::getsockname(sfd, (sockaddr*)&bound, &blen);

    auto client_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int cfd = *client_sock;

    // Send 100 bytes
    char big_msg[100];
    std::memset(big_msg, 'X', sizeof(big_msg));
    auto sent = co_await SendTo(cfd, big_msg, sizeof(big_msg),
        (sockaddr*)&bound, sizeof(bound));
    CO_CHECK(sent.has_value(), "sendto failed");

    // Receive into a 32-byte buffer — should see MSG_TRUNC
    char small_buf[32];
    RecvFrom recvfrom(sfd, small_buf, sizeof(small_buf));
    recvfrom.with_timeout(std::chrono::seconds(2));
    auto data = co_await recvfrom;
    CO_CHECK(data.has_value(), "recvfrom failed");
    CO_CHECK(*data == sizeof(small_buf), "should receive truncated amount");
    CO_CHECK((recvfrom.flags() & MSG_TRUNC) != 0, "MSG_TRUNC flag should be set");

    co_await Close(sfd);
    co_await Close(cfd);
    PASS();
}

// ========== Test 8: SendTo with invalid address ==========

Task<void> test_sendto_invalid_addr(scheduling::WorkStealingThreadPool& pool) {
    TEST("SendTo to unreachable port (async error)");

    auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int fd = *sock;

    // Send to a port where (likely) nothing is listening
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_port = htons(1);  // Port 1 is typically unused
    dest.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    // UDP sendto to an unreachable port does NOT fail immediately
    // (UDP is connectionless). The send succeeds locally.
    const char* msg = "test";
    auto sent = co_await SendTo(fd, msg, strlen(msg), (sockaddr*)&dest, sizeof(dest));
    CO_CHECK(sent.has_value(), "send should succeed locally for UDP");
    CO_CHECK(*sent == strlen(msg), "size mismatch");

    co_await Close(fd);
    PASS();
}

// ========== Test 9: Concurrent UDP echo server ==========

Task<void> test_concurrent_udp_echo(scheduling::WorkStealingThreadPool& pool) {
    TEST("concurrent UDP echo server (5 clients)");

    auto server_sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
    int sfd = *server_sock;
    sockaddr_in saddr{};
    saddr.sin_family = AF_INET;
    saddr.sin_port = htons(0);
    saddr.sin_addr.s_addr = INADDR_ANY;
    co_await Bind(sfd, (sockaddr*)&saddr, sizeof(saddr));

    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    ::getsockname(sfd, (sockaddr*)&bound, &blen);

    // Spawn server handler as a separate coroutine
    auto server_handler = [](int fd, int expected_msgs) -> Task<void> {
        char buf[256];
        for (int i = 0; i < expected_msgs; ++i) {
            RecvFrom recvfrom(fd, buf, sizeof(buf));
            recvfrom.with_timeout(std::chrono::seconds(3));
            auto data = co_await recvfrom;
            if (!data) continue;
            auto& src = recvfrom.source_addr();
            auto srclen = recvfrom.source_addr_len();
            SendTo sendto(fd, buf, *data, (sockaddr*)&src, srclen);
            co_await sendto;
        }
        co_return;
    };

    pool.submit(server_handler(sfd, 5).task());

    // 5 clients
    for (int c = 0; c < 5; ++c) {
        auto cs = co_await Socket(AF_INET, SOCK_DGRAM, 0);
        int cfd = *cs;

        char msg[64];
        snprintf(msg, sizeof(msg), "ping-%d", c);
        auto sent = co_await SendTo(cfd, msg, strlen(msg),
            (sockaddr*)&bound, sizeof(bound));
        CO_CHECK(sent.has_value(), "sendto failed");

        char response[256] = {};
        RecvFrom recvfrom(cfd, response, sizeof(response));
        recvfrom.with_timeout(std::chrono::seconds(3));
        auto data = co_await recvfrom;
        CO_CHECK(data.has_value(), "recvfrom failed");
        CO_CHECK(*data == strlen(msg), "response size mismatch");
        CO_CHECK(std::memcmp(response, msg, *data) == 0, "response data mismatch");

        co_await Close(cfd);
    }

    co_await Close(sfd);
    PASS();
}

// ========== Main ==========

int main() {
    std::cout << "=== UDP Tests ===" << std::endl;

    try {
        scheduling::WorkStealingThreadPool pool(2);
        {
            ExecutionContext::Scope scope(&pool);

            pool.submit(test_basic_udp_echo(pool).task());
            pool.wait_all();

            pool.submit(test_udp_timeout(pool).task());
            pool.wait_all();

            pool.submit(test_udp_connected(pool).task());
            pool.wait_all();

            pool.submit(test_datagram_boundaries(pool).task());
            pool.wait_all();

            pool.submit(test_multiple_senders(pool).task());
            pool.wait_all();

            pool.submit(test_zero_length_datagram(pool).task());
            pool.wait_all();

            pool.submit(test_recvfrom_flags(pool).task());
            pool.wait_all();

            pool.submit(test_sendto_invalid_addr(pool).task());
            pool.wait_all();

            pool.submit(test_concurrent_udp_echo(pool).task());
            pool.wait_all();
        }
    } catch (const std::exception& e) {
        std::cerr << "Fatal: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "\n=== UDP Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

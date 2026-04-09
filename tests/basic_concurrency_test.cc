// tests/basic_concurrency_test.cc - Minimal concurrency test
#include <thread>
#include <atomic>
#include <iostream>
#include <vector>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>

std::atomic<int> g_counter{0};
std::atomic<int> g_errors{0};

void simple_worker(int id) {
    for (int i = 0; i < 100; ++i) {
        g_counter.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
}

void socket_worker(int id, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        g_errors++;
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (::connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cout << "Worker " << id << " connect failed" << std::endl;
        g_errors++;
        close(fd);
        return;
    }

    char buf[128];
    snprintf(buf, sizeof(buf), "Hello from %d", id);
    if (::write(fd, buf, strlen(buf)) < 0) {
        g_errors++;
        close(fd);
        return;
    }

    ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n > 0) {
        std::cout << "Worker " << id << " received: ";
        std::cout.write(buf, n);
        std::cout << std::endl;
    }

    close(fd);
    g_counter.fetch_add(1, std::memory_order_relaxed);
}

int main() {
    std::cout << "=== Basic Concurrency Test ===" << std::endl;

    // Test 1: Simple atomic counter
    std::cout << "Test 1: Atomic counter..." << std::endl;
    g_counter = 0;
    std::vector<std::thread> workers;
    for (int i = 0; i < 4; ++i) {
        workers.emplace_back(simple_worker, i);
    }
    for (auto& t : workers) {
        t.join();
    }
    std::cout << "Counter value: " << g_counter.load() << " (expected: 400)" << std::endl;
    if (g_counter.load() != 400) {
        std::cout << "FAILED" << std::endl;
        return 1;
    }
    std::cout << "PASSED" << std::endl;

    // Test 2: Socket pair communication
    std::cout << "\nTest 2: Socket pair..." << std::endl;
    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        std::cout << "FAILED (socketpair)" << std::endl;
        return 1;
    }

    g_counter = 0;
    std::thread writer([sv]() {
        const char* msg = "test message";
        write(sv[0], msg, strlen(msg));
    });
    std::thread reader([sv]() {
        char buf[100];
        ssize_t n = read(sv[1], buf, sizeof(buf));
        if (n > 0) {
            g_counter.store(1, std::memory_order_relaxed);
        }
    });

    writer.join();
    reader.join();
    close(sv[0]);
    close(sv[1]);

    if (g_counter.load() == 1) {
        std::cout << "PASSED" << std::endl;
    } else {
        std::cout << "FAILED" << std::endl;
        return 1;
    }

    std::cout << "\n=== All Tests Passed ===" << std::endl;
    return 0;
}
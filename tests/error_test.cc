// tests/error_test.cc - Error path tests
#include "src/task.hpp"
#include "src/io/io_context.hpp"
#include "src/io/buffer.h"
#include "net/op/socket.hpp"
#include "net/op/listen.hpp"
#include "net/op/accept.hpp"
#include "net/op/read.hpp"
#include "net/op/write.hpp"
#include "net/op/connect.hpp"
#include "net/op/close.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <string.h>
#include <iostream>
#include <atomic>
#include <csignal>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

using namespace ynet::async;
using namespace ynet::async::io;

std::atomic<int> g_errors{0};
std::atomic<int> g_passed{0};

void test_connect_refused() {
    std::cout << "Test: Connect to refused port... " << std::flush;

    // 尝试连接一个没有监听进程的端口
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        std::cout << "SKIP (socket failed)" << std::endl;
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(59999);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    int ret = connect(fd, (sockaddr*)&addr, sizeof(addr));
    if (ret < 0 && (errno == EINPROGRESS || errno == ECONNREFUSED)) {
        g_passed++;
        std::cout << "PASSED (expected errno: " << errno << ")" << std::endl;
    } else {
        g_errors++;
        std::cout << "FAILED (unexpected result)" << std::endl;
    }
    close(fd);
}

void test_socketpair() {
    std::cout << "Test: Socketpair basic... " << std::flush;

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        std::cout << "SKIP (socketpair failed)" << std::endl;
        return;
    }

    // 写入然后读取
    const char* msg = "test";
    if (write(sv[0], msg, 4) != 4) {
        std::cout << "FAILED (write)" << std::endl;
        g_errors++;
    } else {
        char buf[4];
        if (read(sv[1], buf, 4) == 4 && memcmp(buf, msg, 4) == 0) {
            g_passed++;
            std::cout << "PASSED" << std::endl;
        } else {
            std::cout << "FAILED (read)" << std::endl;
            g_errors++;
        }
    }

    close(sv[0]);
    close(sv[1]);
}

void test_buffer_group() {
    std::cout << "Test: Buffer group creation... " << std::flush;

    io::IoUringContext::Scope ctx_scope{};
    auto ctx = io::IoUringContext::current();
    if (!ctx) {
        std::cout << "SKIP (no ctx)" << std::endl;
        return;
    }

    auto& bg = ctx->register_buffer_group(100, 4, 512);

    if (bg.entries() == 4 && bg.buf_size() == 512) {
        g_passed++;
        std::cout << "PASSED" << std::endl;
    } else {
        g_errors++;
        std::cout << "FAILED" << std::endl;
    }
}

void test_close_during_idle() {
    std::cout << "Test: Socket close during idle... " << std::flush;

    int sv[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sv) < 0) {
        std::cout << "SKIP" << std::endl;
        return;
    }

    // 设置为非阻塞
    int flags = fcntl(sv[1], F_GETFL, 0);
    fcntl(sv[1], F_SETFL, flags | O_NONBLOCK);

    // 关闭写端
    close(sv[0]);

    // 读应该返回 0 (EOF)
    char buf[10];
    ssize_t n = read(sv[1], buf, sizeof(buf));

    if (n == 0) {
        g_passed++;
        std::cout << "PASSED (EOF detected)" << std::endl;
    } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        g_passed++;
        std::cout << "PASSED (EAGAIN as expected)" << std::endl;
    } else {
        g_errors++;
        std::cout << "FAILED (n=" << n << ", errno=" << errno << ")" << std::endl;
    }

    close(sv[1]);
}

int main() {
    std::cout << "=== Error Path Tests ===" << std::endl;

    try {
        scheduling::WorkStealingThreadPool pool(2);
        ExecutionContext::Scope scope(&pool);

        test_connect_refused();
        test_socketpair();
        test_buffer_group();
        test_close_during_idle();

        pool.wait_all();

        std::cout << "\n=== Error Test Summary ===" << std::endl;
        std::cout << "Passed: " << g_passed << std::endl;
        std::cout << "Errors: " << g_errors << std::endl;

        return g_errors == 0 ? 0 : 1;
    }
    catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
}
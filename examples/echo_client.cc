// examples/echo_client.cc - Echo client using ultranet coroutines + io_uring
#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>
#include <csignal>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>

using namespace ynet::async;
using namespace ynet::async::io;

Task<void> echo_client(const char* host, int port) {
    auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
    if (!sock) {
        std::cerr << "socket failed: " << sock.error().message() << std::endl;
        co_return;
    }

    int fd = *sock;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    auto conn = co_await Connect(fd, (sockaddr*)&addr, sizeof(addr));
    if (!conn) {
        std::cerr << "connect failed: " << conn.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }

    std::cout << "Connected to " << host << ":" << port << std::endl;

    std::string msg = "Hello, ultranet!";
    auto wrote = co_await Write(fd, msg.data(), msg.size());
    if (!wrote) {
        std::cerr << "write failed: " << wrote.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }

    std::cout << "Sent: " << msg << " (" << *wrote << " bytes)" << std::endl;

    char buf[4096];
    auto data = co_await Read(fd, buf, sizeof(buf));
    if (!data) {
        std::cerr << "read failed: " << data.error().message() << std::endl;
        co_await Close(fd);
        co_return;
    }

    std::cout << "Received: ";
    std::cout.write(buf, *data);
    std::cout << " (" << *data << " bytes)" << std::endl;

    co_await Close(fd);
}

int main(int argc, char* argv[]) {
    const char* host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::atoi(argv[2]) : 8080;

    std::signal(SIGPIPE, SIG_IGN);

    try {
        scheduling::WorkStealingThreadPool pool(1);
        ExecutionContext::Scope scope(&pool);

        auto client_task = echo_client(host, port);
        pool.submit(client_task.release());

        pool.wait_all();
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}

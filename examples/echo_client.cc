// examples/echo_client.cc - Simple echo client
#include <iostream>
#include <cstring>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>

int main(int argc, char* argv[]) {
    const char* host = (argc > 1) ? argv[1] : "127.0.0.1";
    int port = (argc > 2) ? std::atoi(argv[2]) : 8080;

    signal(SIGPIPE, SIG_IGN);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "socket failed: " << strerror(errno) << std::endl;
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "connect failed: " << strerror(errno) << std::endl;
        close(fd);
        return 1;
    }

    std::cout << "Connected to " << host << ":" << port << std::endl;

    // Send test message
    std::string msg = "Hello, ultranet!";
    ssize_t sent = write(fd, msg.data(), msg.size());
    std::cout << "Sent: " << msg << " (" << sent << " bytes)" << std::endl;

    // Read echo
    char buf[4096];
    ssize_t received = read(fd, buf, sizeof(buf));
    if (received > 0) {
        std::string echo(buf, received);
        std::cout << "Received: " << echo << " (" << received << " bytes)" << std::endl;
    }

    close(fd);
    return 0;
}

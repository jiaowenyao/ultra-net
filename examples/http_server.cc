// examples/http_server.cc - Simple HTTP server
#include <iostream>
#include <atomic>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <signal.h>

std::atomic<bool> g_running{true};

void signal_handler(int) {
    g_running = false;
}

const char* HTTP_RESPONSE =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/plain\r\n"
    "Content-Length: 13\r\n"
    "\r\n"
    "Hello, World!";

int main(int argc, char* argv[]) {
    int port = (argc > 1) ? std::atoi(argv[1]) : 8080;

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        std::cerr << "socket failed" << std::endl;
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        std::cerr << "bind failed" << std::endl;
        return 1;
    }

    if (listen(listen_fd, 128) < 0) {
        std::cerr << "listen failed" << std::endl;
        return 1;
    }

    fcntl(listen_fd, F_SETFL, fcntl(listen_fd, F_GETFL) | O_NONBLOCK);

    std::cout << "HTTP server listening on port " << port << std::endl;

    while (g_running) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);

        int client_fd = accept4(listen_fd, (sockaddr*)&client_addr, &client_len, SOCK_NONBLOCK);
        if (client_fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            break;
        }

        // Simple HTTP handling
        char buf[4096];
        ssize_t n = read(client_fd, buf, sizeof(buf));
        if (n > 0) {
            // Check if it's a GET request
            if (n >= 3 && strncmp(buf, "GET", 3) == 0) {
                write(client_fd, HTTP_RESPONSE, strlen(HTTP_RESPONSE));
            }
        }
        close(client_fd);
    }

    close(listen_fd);
    return 0;
}

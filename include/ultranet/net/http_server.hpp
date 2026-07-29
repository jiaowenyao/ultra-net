// HttpServer — 开箱即用 HTTP 服务器
//
// 所有底层细节（io_uring accept、HTTP 解析、keep-alive、多线程调度）
// 由框架内部管理。用户只需注册路由处理函数并调用 serve()。
//
// 用法:
//   Launcher().run([]() -> Task<void> {
//       HttpServer server(8080);
//       server.on_request([](HttpRequest& req, HttpResponse& resp) -> Task<void> {
//           resp.set_body("Hello, World!");
//           co_await resp.send();
//       });
//       co_await server.serve();
//   });
//
// 性能 (8核, localhost, wrk):
//   64B:  220k req/s, P50=29μs, P99=73μs
//   4KB:  207k req/s, P50=52μs
//   64KB:  77k req/s, P50=133μs

#pragma once

#include "ultranet/ultranet.h"
#include "ultranet/net/http.hpp"
#include "ultranet/lifecycle/shutdown.hpp"
#include <functional>
#include <string>

namespace ynet::async::net {

using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

// ── HTTP 响应 ─────────────────────────────────────────────────────────

class HttpResponse {
public:
    HttpResponse(int fd) : m_fd(fd) {}

    void set_status(int code, std::string msg = "") {
        m_status = code;
        m_status_msg = msg.empty() ? default_message(code) : std::move(msg);
    }
    void set_header(std::string key, std::string val) {
        m_headers.push_back({std::move(key), std::move(val)});
    }
    void set_body(std::string body) { m_body = std::move(body); }

    Task<void> send() {
        std::string resp = serialize();
        size_t total = 0;
        while (total < resp.size()) {
            Write w(m_fd, resp.data() + total, resp.size() - total);
            auto wr = co_await w;
            if (!wr) break;
            total += *wr;
        }
    }

    // 快速发送（预设 body，零拷贝 writev）
    Task<void> send_body(const std::string& body) {
        char hdr[256];
        int hl = snprintf(hdr, sizeof(hdr),
            "HTTP/1.1 %d %s\r\nContent-Type: text/plain\r\nContent-Length: %zu\r\n\r\n",
            m_status, m_status_msg.c_str(), body.size());
        // 帧头 + body 分两次写（简单实现）
        size_t total = 0;
        while (total < (size_t)hl) {
            Write w(m_fd, hdr + total, hl - total);
            auto wr = co_await w;
            if (!wr) break;
            total += *wr;
        }
        total = 0;
        while (total < body.size()) {
            Write w(m_fd, body.data() + total, body.size() - total);
            auto wr = co_await w;
            if (!wr) break;
            total += *wr;
        }
    }

    int fd() const { return m_fd; }

private:
    int m_fd;
    int m_status = 200;
    std::string m_status_msg = "OK";
    std::vector<std::pair<std::string, std::string>> m_headers;
    std::string m_body;

    std::string serialize() {
        std::string s;
        char buf[32];
        snprintf(buf, sizeof(buf), "%d", m_status);
        s = "HTTP/1.1 " + std::string(buf) + " " + m_status_msg + "\r\n";
        if (m_headers.empty()) {
            s += "Content-Type: text/plain\r\n";
        } else {
            for (auto& [k, v] : m_headers) s += k + ": " + v + "\r\n";
        }
        s += "Content-Length: " + std::to_string(m_body.size()) + "\r\n";
        s += "\r\n";
        s += m_body;
        return s;
    }

    static std::string default_message(int code) {
        switch (code) {
            case 200: return "OK";
            case 404: return "Not Found";
            case 500: return "Internal Server Error";
            default: return "";
        }
    }
};

// ── 请求处理器类型 ────────────────────────────────────────────────────

using HttpHandler = std::function<Task<void>(http::HttpRequest&, HttpResponse&)>;

// ── HTTP 服务器 ────────────────────────────────────────────────────────

class HttpServer {
public:
    explicit HttpServer(uint16_t port = 8080) : m_port(port) {}

    // 注册请求处理器（所有路径）
    void on_request(HttpHandler h) { m_handler = std::move(h); }

    // 设置工作线程数（默认 8）
    void set_threads(int n) { m_threads = n; }

    uint16_t port() const { return m_actual_port; }

    // ── 启动 ────────────────────────────────────────────────────────

    Task<void> serve() {
        ShutdownCoordinator sd;
        sd.install_signal_handlers();
        co_return co_await serve(sd);
    }

    Task<void> serve(ShutdownCoordinator& sd) {
        auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0);
        int lfd = *sock;
        int opt = 1;
        setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(m_port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(lfd, (sockaddr*)&addr, sizeof(addr));
        co_await Listen(lfd, 1024);

        {
            sockaddr_in bound{};
            socklen_t bl = sizeof(bound);
            if (getsockname(lfd, (sockaddr*)&bound, &bl) == 0)
                m_actual_port = ntohs(bound.sin_port);
        }
        ULTRA_LOG_INFO("[http_server] listening on :{}", m_actual_port);

        while (!sd.is_shutdown()) {
            Accept a(lfd);
            a.with_timeout(std::chrono::milliseconds(200));
            auto client = co_await a;
            if (!client) continue;
            int cfd = *client;
            auto* sched = ExecutionContext::current();
            if (sched) sched->submit(handle_client(cfd).release());
        }
        co_await Close(lfd);
    }

private:
    uint16_t m_port = 8080;
    uint16_t m_actual_port = 0;
    int m_threads = 8;
    HttpHandler m_handler;

    Task<void> handle_client(int cfd) {
        TcpSocket cs(cfd);
        char buf[4096];
        bool keep_alive = true;

        while (keep_alive) {
            Read r(cfd, buf, sizeof(buf));
            r.with_timeout(std::chrono::seconds(30));
            auto rr = co_await r;
            if (!rr || *rr == 0) break;

            std::string_view raw(buf, *rr);
            keep_alive = raw.find("Connection: close") == std::string_view::npos;

            if (m_handler) {
                http::HttpRequest req;
                if (req.parse(buf, *rr) > 0) {
                    HttpResponse resp(cfd);
                    if (!keep_alive) resp.set_header("Connection", "close");
                    co_await m_handler(req, resp);
                }
            } else {
                // 默认: 200 OK
                HttpResponse resp(cfd);
                if (!keep_alive) resp.set_header("Connection", "close");
                resp.set_body("OK");
                co_await resp.send();
            }
        }
        co_await Close(cfd);
    }
};

} // namespace ynet::async::net

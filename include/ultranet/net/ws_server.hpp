// WsServer — 开箱即用 WebSocket 服务器。
//
// ShutdownCoordinator 完全隐藏在 serve() 内部，用户无需感知。
//
// 用法（配合 Launcher，最简洁）:
//   Launcher().run([]() -> Task<void> {
//       WsServer server(8080);
//       server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
//           co_await conn.send_text("echo: " + msg);
//       });
//       co_await server.serve();
//   });
#pragma once

#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/lifecycle/shutdown.hpp"
#include <functional>

namespace ynet::async::net {

using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

// ── WebSocket 连接包装 ───────────────────────────────────────────────

class WsConn {
public:
    explicit WsConn(websocket::WebSocket ws) : m_ws(std::move(ws)) {}

    // 发送文本消息
    Task<void> send_text(std::string_view text) {
        co_await m_ws.send_text(std::string(text));
    }

    // 发送二进制消息
    Task<void> send_binary(std::string_view data) {
        co_await m_ws.send_binary(std::string(data));
    }

    // 关闭连接
    Task<void> close() { co_await m_ws.close(); }

    int fd() { return m_ws.socket().fd(); }

private:
    websocket::WebSocket m_ws;
    friend class WsServer;
};

// ── 消息处理器类型 ───────────────────────────────────────────────────

using WsTextHandler  = std::function<Task<void>(WsConn&, std::string)>;
using WsBinaryHandler = std::function<Task<void>(WsConn&, std::vector<uint8_t>)>;

// ── WebSocket 服务器 ──────────────────────────────────────────────────

class WsServer {
public:
    explicit WsServer(uint16_t port) : m_port(port) {}

    void on_text(WsTextHandler h)   { m_on_text = std::move(h); }
    void on_binary(WsBinaryHandler h) { m_on_binary = std::move(h); }
    uint16_t port() const { return m_actual_port; }

    // ── 启动服务器 ──────────────────────────────────────────────────
    // 无参版本：ShutdownCoordinator 完全内部化，用户不可见。
    // 有参版本：高级用户可传入自己的 ShutdownCoordinator。

    Task<void> serve() {
        ShutdownCoordinator sd;
        sd.install_signal_handlers();
        co_return co_await serve(sd);
    }

    Task<void> serve(ShutdownCoordinator& sd) {
        auto sock_fd = co_await Socket(AF_INET, SOCK_STREAM, 0);
        if (!sock_fd) { co_return; }
        int fd = *sock_fd;

        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(m_port);
        addr.sin_addr.s_addr = INADDR_ANY;
        co_await Bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        co_await Listen(fd, 128);

        sockaddr_in bound{};
        socklen_t bound_len = sizeof(bound);
        if (getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0) {
            m_actual_port = ntohs(bound.sin_port);
        }

        ULTRA_LOG_INFO("[ws_server] listening on :{}", m_actual_port);

        while (!sd.is_shutdown()) {
            Accept acceptor(fd);
            acceptor.with_timeout(std::chrono::milliseconds(100));
            auto client_result = co_await acceptor;
            if (!client_result) { continue; }

            auto* sched = ExecutionContext::current();
            if (sched) {
                sched->submit(handle_client(*client_result).release());
            }
        }

        co_await Close(fd);
    }

private:
    uint16_t m_port;
    uint16_t m_actual_port = 0;
    WsTextHandler m_on_text;
    WsBinaryHandler m_on_binary;

    Task<void> handle_client(int client_fd) {
        TcpSocket sock(client_fd);
        websocket::WebSocket ws(std::move(sock));

        // 1. 读取 HTTP upgrade 请求
        char buf[4096];
        Read reader(client_fd, buf, sizeof(buf));
        reader.with_timeout(std::chrono::seconds(5));
        auto r = co_await reader;
        if (!r || *r == 0) {
            co_await Close(client_fd);
            co_return;
        }

        // 2. 解析 HTTP 请求
        http::HttpRequest req;
        size_t consumed = req.parse(buf, static_cast<size_t>(*r));
        if (consumed == 0) {
            co_await Close(client_fd);
            co_return;
        }

        // 3. 完成 WebSocket 握手
        auto err = co_await ws.accept(req);
        if (err) {
            co_await Close(client_fd);
            co_return;
        }

        WsConn conn(std::move(ws));

        // 4. 读帧 → 重组分片 → 回调用户处理器
        std::vector<uint8_t> fragment_buf;
        while (true) {
            auto frame = co_await conn.m_ws.read_frame();
            if (frame.opcode == websocket::OpCode::Close) { break; }
            if (frame.opcode == websocket::OpCode::Ping) {
                co_await conn.m_ws.send_pong({});
                continue;
            }

            fragment_buf.insert(fragment_buf.end(),
                frame.payload.begin(), frame.payload.end());
            if (!frame.fin) { continue; }  // 等待更多分片

            if (frame.opcode == websocket::OpCode::Text && m_on_text) {
                std::string msg(fragment_buf.begin(), fragment_buf.end());
                fragment_buf.clear();
                co_await m_on_text(conn, std::move(msg));
            } else if (frame.opcode == websocket::OpCode::Binary && m_on_binary) {
                fragment_buf.clear();
                co_await m_on_binary(conn, std::move(fragment_buf));
            } else {
                fragment_buf.clear();
            }
        }

        co_await Close(client_fd);
    }
};

} // namespace ynet::async::net

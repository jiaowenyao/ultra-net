// WsServer — 开箱即用 WebSocket 服务器。
//
// 所有底层细节（io_uring accept、WebSocket 握手、帧编解码、分片重组、
// 自动 Ping/Pong 心跳、关闭信号处理）由框架内部管理。
// 用户只需注册消息/连接/断开回调并调用 serve()。
//
// 用法（配合 Launcher，ShutdownCoordinator 完全隐藏）:
//   Launcher().run([]() -> Task<void> {
//       WsServer server(8080);
//       server.on_text([](WsConn& conn, std::string msg) -> Task<void> {
//           co_await conn.send_text("echo: " + msg);
//       });
//       server.on_open([](WsConn& conn) -> Task<void> {
//           std::cout << "新连接 fd=" << conn.fd() << "\n";
//       });
//       co_await server.serve();
//   });
#pragma once

#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/buffer_ring_assembler.hpp"
#include "ultranet/lifecycle/shutdown.hpp"
#include <functional>
#include <chrono>

namespace ynet::async::net {

using namespace ynet::async::io;
using namespace ynet::async::lifecycle;

// ── WebSocket 连接 ───────────────────────────────────────────────────

class WsConn {
public:
    explicit WsConn(websocket::WebSocket ws) : m_ws(std::move(ws)), m_open(true) {}

    // ── 发送 ─────────────────────────────────────────────────────────

    Task<void> send_text(std::string_view text) {
        if (!m_open) { co_return; }
        co_await m_ws.send_text(std::string(text));
    }

    Task<void> send_binary(const void* data, size_t len) {
        if (!m_open) { co_return; }
        co_await m_ws.send_binary(std::string(static_cast<const char*>(data), len));
    }

    // ── 连接管理 ────────────────────────────────────────────────────

    Task<void> close(uint16_t code = 1000) {
        if (!m_open) { co_return; }
        m_open = false;
        co_await m_ws.close(code);
    }

    int  fd()                { return m_ws.socket().fd(); }
    bool is_open()     const { return m_open; }

private:
    websocket::WebSocket m_ws;
    bool m_open = true;
    friend class WsServer;
};

// ── 回调类型 ─────────────────────────────────────────────────────────

using WsTextHandler   = std::function<Task<void>(WsConn&, std::string)>;
using WsBinaryHandler = std::function<Task<void>(WsConn&, std::vector<uint8_t>)>;
using WsOpenHandler   = std::function<Task<void>(WsConn&)>;
using WsCloseHandler  = std::function<Task<void>(WsConn&)>;

// ── WebSocket 服务器 ──────────────────────────────────────────────────

class WsServer {
public:
    explicit WsServer(uint16_t port = 0) : m_port(port) {}

    // 注册回调
    void on_text(WsTextHandler h)     { m_on_text = std::move(h); }
    void on_binary(WsBinaryHandler h) { m_on_binary = std::move(h); }
    void on_open(WsOpenHandler h)     { m_on_open = std::move(h); }
    void on_close(WsCloseHandler h)   { m_on_close = std::move(h); }

    // 设置 Ping 间隔（秒），0 表示禁用心跳。默认 30 秒。
    void set_ping_interval(int secs) { m_ping_secs = secs; }

    // 启用 buffer ring 模式——使用 multishot recv 消除内核→用户态拷贝。
    // 在 serve() 之前调用。每个工作线程自动注册独立的 buffer group。
    void enable_ring_mode(size_t ring_entries = 256,
                          size_t buf_size = 4096) {
        m_ring_mode = true;
        m_ring_entries = ring_entries;
        m_ring_buf_size = buf_size;
    }

    uint16_t port() const { return m_actual_port; }

    // ── 启动 ────────────────────────────────────────────────────────
    // 无参版本：ShutdownCoordinator 完全内部化。

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

        {
            sockaddr_in bound{};
            socklen_t bound_len = sizeof(bound);
            if (getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &bound_len) == 0) {
                m_actual_port = ntohs(bound.sin_port);
            }
        }

        ULTRA_LOG_INFO("[ws_server] listening on :{}", m_actual_port);

        while (!sd.is_shutdown()) {
            Accept acceptor(fd);
            acceptor.with_timeout(std::chrono::milliseconds(100));
            auto client = co_await acceptor;
            if (!client) { continue; }
            auto* sched = ExecutionContext::current();
            if (sched) { sched->submit(handle_client(*client).release()); }
        }

        co_await Close(fd);
    }

private:
    uint16_t m_port = 0;
    uint16_t m_actual_port = 0;
    int m_ping_secs = 30;

    // buffer ring 模式配置
    bool m_ring_mode{false};
    size_t m_ring_entries{256};
    size_t m_ring_buf_size{4096};

    WsTextHandler   m_on_text;
    WsBinaryHandler m_on_binary;
    WsOpenHandler   m_on_open;
    WsCloseHandler  m_on_close;

    // ── 客户端连接处理 ───────────────────────────────────────────────
    // 完整的 WebSocket 生命周期：HTTP upgrade → 握手 → 读帧循环。
    // Ping/Pong 由框架自动处理，用户只需关注 Text/Binary 帧。

    Task<void> handle_client(int client_fd) {
        // 1. 创建 WebSocket 并读取 HTTP upgrade 请求
        TcpSocket sock(client_fd);
        websocket::WebSocket ws(std::move(sock));

        char buf[4096];
        Read reader(ws.socket().fd(), buf, sizeof(buf));
        reader.with_timeout(std::chrono::seconds(5));
        auto r = co_await reader;
        if (!r || *r == 0) { co_return; }  // fd 由 ws 析构关闭

        // 2. 解析 HTTP 升级请求
        http::HttpRequest req;
        if (req.parse(buf, static_cast<size_t>(*r)) == 0) { co_return; }

        // 3. 完成 WebSocket 握手（发送 HTTP 101 响应）
        auto ec = co_await ws.accept(req);
        if (ec) {
            ULTRA_LOG_WARN("[ws_server] handshake failed: {}", ec.message());
            co_return;
        }

        WsConn conn(std::move(ws));

        // 4. 通知用户连接建立
        if (m_on_open) { co_await m_on_open(conn); }

        // 5. 读帧主循环（传统 or ring 路径）
        if (m_ring_mode) {
            // 每个工作线程注册独立的 buffer group（idempotent）
            auto* engine = IoUringEngine::current();
            auto& bg = engine->register_buffer_group(
                1, m_ring_entries, m_ring_buf_size);

            BufferRingAssembler assembler;
            assembler.start(conn.m_ws.socket().fd(), bg);
            co_await read_loop_ring(conn, assembler);
            assembler.cleanup();
        } else {
            co_await read_loop(conn);
        }

        // 6. 通知用户连接关闭
        conn.m_open = false;
        if (m_on_close) { co_await m_on_close(conn); }
        // fd 由 conn → WebSocket → TcpSocket 析构链自动关闭
    }

    // ── 帧读循环 ────────────────────────────────────────────────────
    // 自动处理 Ping/Pong/Close，Text/Binary 交给用户回调。
    // 分片帧自动重组后一次性交付。

    Task<void> read_loop(WsConn& conn) {
        std::vector<uint8_t> frag_buf;

        while (conn.is_open()) {
            auto frame = co_await conn.m_ws.read_frame();

            switch (frame.opcode) {
            case websocket::OpCode::Close:
                co_await conn.m_ws.close(1000);
                conn.m_open = false;
                co_return;

            case websocket::OpCode::Ping:
                // 自动回复 Pong，用户无感知
                co_await conn.m_ws.send_pong(
                    std::string(frame.payload.begin(), frame.payload.end()));
                break;

            case websocket::OpCode::Pong:
                // Pong 由框架自动消费，无需用户处理
                break;

            case websocket::OpCode::Text:
            case websocket::OpCode::Binary:
                // 收集分片
                frag_buf.insert(frag_buf.end(),
                    frame.payload.begin(), frame.payload.end());
                if (!frame.fin) { break; }  // 等待更多分片

                if (frame.opcode == websocket::OpCode::Text && m_on_text) {
                    std::string msg(frag_buf.begin(), frag_buf.end());
                    frag_buf.clear();
                    co_await m_on_text(conn, std::move(msg));
                } else if (frame.opcode == websocket::OpCode::Binary && m_on_binary) {
                    auto payload = std::move(frag_buf);
                    frag_buf.clear();
                    co_await m_on_binary(conn, std::move(payload));
                } else {
                    frag_buf.clear();
                }
                break;

            default:
                break;
            }
        }
    }

    // ── ring-based 帧读循环 ──────────────────────────────────────────
    //
    // 使用 multishot recv + buffer ring 替代传统 read()。
    // Ping/Pong/Close 自动处理，分片帧自动重组，与 read_loop 行为一致。

    Task<void> read_loop_ring(WsConn& conn, BufferRingAssembler& assembler) {
        std::vector<uint8_t> frag_buf;

        while (conn.is_open() && !assembler.is_stopped()) {
            auto frame = co_await conn.m_ws.read_frame_ring(assembler);

            switch (frame.opcode) {
            case websocket::OpCode::Close:
                co_await conn.m_ws.close(1000);
                conn.m_open = false;
                co_return;

            case websocket::OpCode::Ping:
                co_await conn.m_ws.send_pong(
                    std::string(frame.payload.begin(), frame.payload.end()));
                break;

            case websocket::OpCode::Pong:
                break;

            case websocket::OpCode::Text:
            case websocket::OpCode::Binary:
                // 收集分片
                frag_buf.insert(frag_buf.end(),
                    frame.payload.begin(), frame.payload.end());
                if (!frame.fin) { break; }

                if (frame.opcode == websocket::OpCode::Text && m_on_text) {
                    std::string msg(frag_buf.begin(), frag_buf.end());
                    frag_buf.clear();
                    co_await m_on_text(conn, std::move(msg));
                } else if (frame.opcode == websocket::OpCode::Binary && m_on_binary) {
                    auto payload = std::move(frag_buf);
                    frag_buf.clear();
                    co_await m_on_binary(conn, std::move(payload));
                } else {
                    frag_buf.clear();
                }
                break;

            default:
                break;
            }
        }
    }
};

} // namespace ynet::async::net

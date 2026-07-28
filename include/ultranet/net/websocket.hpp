#pragma once

#include "ultranet/net/tcp_socket.hpp"
#include "ultranet/net/http.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/io/io_awaitable.hpp"
#include "ultranet/net/buffer_ring_assembler.hpp"
#include <string>
#include <vector>
#include <cstring>
#include <random>
#include <sstream>

namespace ynet::async::net::websocket {

enum class OpCode : uint8_t {
    Continuation = 0x0,
    Text         = 0x1,
    Binary       = 0x2,
    Close        = 0x8,
    Ping         = 0x9,
    Pong         = 0xA
};

namespace detail {

// Minimal SHA-1 for Sec-WebSocket-Accept
inline std::string sha1(const std::string& input) {
    uint32_t h[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};
    std::vector<uint8_t> buf(input.begin(), input.end());
    uint64_t bit_len = buf.size() * 8;
    buf.push_back(0x80);
    while ((buf.size() + 8) % 64 != 0) buf.push_back(0);
    for (int i = 7; i >= 0; --i) buf.push_back(static_cast<uint8_t>(bit_len >> (i * 8)));

    for (size_t i = 0; i < buf.size(); i += 64) {
        uint32_t w[80];
        for (int j = 0; j < 16; ++j)
            w[j] = (buf[i+j*4]<<24)|(buf[i+j*4+1]<<16)|(buf[i+j*4+2]<<8)|buf[i+j*4+3];
        for (int j = 16; j < 80; ++j) {
            uint32_t t = w[j-3] ^ w[j-8] ^ w[j-14] ^ w[j-16];
            w[j] = (t << 1) | (t >> 31);
        }
        uint32_t a = h[0], b = h[1], c = h[2], d = h[3], e = h[4];
        for (int j = 0; j < 80; ++j) {
            uint32_t f, k;
            if (j < 20) { f = (b & c) | (~b & d); k = 0x5A827999; }
            else if (j < 40) { f = b ^ c ^ d; k = 0x6ED9EBA1; }
            else if (j < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
            else { f = b ^ c ^ d; k = 0xCA62C1D6; }
            uint32_t t = ((a << 5) | (a >> 27)) + f + e + k + w[j];
            e = d; d = c; c = ((b << 30) | (b >> 2)); b = a; a = t;
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e;
    }
    std::string result;
    for (int i = 0; i < 5; ++i)
        for (int j = 3; j >= 0; --j)
            result.push_back(static_cast<char>((h[i] >> (j * 8)) & 0xFF));
    return result;
}

inline std::string base64_encode(const std::string& input) {
    static const char* chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    int val = 0, valb = -6;
    for (unsigned char c : input) {
        val = (val << 8) + c; valb += 8;
        while (valb >= 0) { out.push_back(chars[(val >> valb) & 0x3F]); valb -= 6; }
    }
    if (valb > -6) out.push_back(chars[((val << 8) >> (valb + 8)) & 0x3F]);
    while (out.size() % 4) out.push_back('=');
    return out;
}

inline std::string ws_accept_key(const std::string& key) {
    return base64_encode(sha1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"));
}

inline std::string ws_generate_key() {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 255);
    std::string key(16, 0);
    for (int i = 0; i < 16; ++i) key[i] = static_cast<char>(dist(rng));
    return base64_encode(key);
}

} // namespace detail

// === WebSocketFrame ===

struct WebSocketFrame {
    bool fin = true;
    OpCode opcode = OpCode::Text;
    bool mask = false;
    uint32_t masking_key = 0;
    std::string payload;

    static WebSocketFrame text(std::string data, bool fin = true) {
        WebSocketFrame f;
        f.fin = fin; f.opcode = OpCode::Text; f.payload = std::move(data);
        return f;
    }
    static WebSocketFrame binary(std::string data, bool fin = true) {
        WebSocketFrame f;
        f.fin = fin; f.opcode = OpCode::Binary; f.payload = std::move(data);
        return f;
    }
    static WebSocketFrame close(uint16_t code = 1000, const std::string& reason = "") {
        WebSocketFrame f;
        f.fin = true; f.opcode = OpCode::Close;
        f.payload.resize(2 + reason.size());
        f.payload[0] = static_cast<char>(code >> 8);
        f.payload[1] = static_cast<char>(code & 0xFF);
        if (!reason.empty()) std::memcpy(&f.payload[2], reason.data(), reason.size());
        return f;
    }
    static WebSocketFrame ping(const std::string& data = "") {
        WebSocketFrame f;
        f.fin = true; f.opcode = OpCode::Ping; f.payload = data;
        return f;
    }
    static WebSocketFrame pong(const std::string& data = "") {
        WebSocketFrame f;
        f.fin = true; f.opcode = OpCode::Pong; f.payload = data;
        return f;
    }

    // Total wire size of an encoded frame (header + mask-key + payload).
    static constexpr size_t encoded_size(size_t payload_len, bool apply_mask) {
        size_t n = 2 + payload_len;
        if (payload_len >= 126 && payload_len <= 65535) n += 2;
        else if (payload_len > 65535) n += 8;
        if (apply_mask) n += 4;
        return n;
    }

    std::vector<uint8_t> encode(bool apply_mask = false) const {
        std::vector<uint8_t> result;
        result.resize(encoded_size(payload.size(), apply_mask));
        encode_into(result.data(), result.size(), apply_mask);
        return result;
    }

    // Zero-allocation encode into caller-provided buffer.
    // Returns number of bytes written, or 0 if buffer too small.
    size_t encode_into(uint8_t* buf, size_t cap, bool apply_mask) const {
        size_t needed = encoded_size(payload.size(), apply_mask);
        if (cap < needed) return 0;

        uint8_t* p = buf;
        size_t len = payload.size();

        *p++ = static_cast<uint8_t>((fin ? 0x80 : 0) | static_cast<uint8_t>(opcode));

        uint8_t b1 = apply_mask ? 0x80 : 0;
        if (len < 126) {
            *p++ = b1 | static_cast<uint8_t>(len);
        } else if (len <= 65535) {
            *p++ = b1 | 126;
            *p++ = static_cast<uint8_t>(len >> 8);
            *p++ = static_cast<uint8_t>(len & 0xFF);
        } else {
            *p++ = b1 | 127;
            for (int i = 7; i >= 0; --i)
                *p++ = static_cast<uint8_t>(len >> (i * 8));
        }

        if (apply_mask) {
            uint32_t mk = masking_key;
            if (mk == 0) {
                std::random_device rd;
                mk = static_cast<uint32_t>(rd()) ^ (static_cast<uint32_t>(rd()) << 16);
            }
            *p++ = static_cast<uint8_t>(mk >> 24);
            *p++ = static_cast<uint8_t>(mk >> 16);
            *p++ = static_cast<uint8_t>(mk >> 8);
            *p++ = static_cast<uint8_t>(mk);
            for (size_t i = 0; i < len; ++i)
                *p++ = static_cast<uint8_t>(payload[i]) ^ static_cast<uint8_t>((mk >> (8 * (3 - (i % 4)))) & 0xFF);
        } else {
            std::memcpy(p, payload.data(), len);
            p += len;
        }
        return static_cast<size_t>(p - buf);
    }

    static bool decode(const uint8_t* data, size_t len, size_t& consumed, WebSocketFrame& frame) {
        if (len < 2) { consumed = 0; return false; }
        frame = WebSocketFrame{};
        frame.fin = (data[0] & 0x80) != 0;
        frame.opcode = static_cast<OpCode>(data[0] & 0x0F);
        frame.mask = (data[1] & 0x80) != 0;
        uint64_t plen = data[1] & 0x7F;
        size_t pos = 2;

        if (plen == 126) {
            if (len < 4) { consumed = 0; return false; }
            plen = (static_cast<uint64_t>(data[2]) << 8) | data[3];
            pos = 4;
        } else if (plen == 127) {
            if (len < 10) { consumed = 0; return false; }
            plen = 0;
            for (int i = 0; i < 8; ++i) plen = (plen << 8) | data[2 + i];
            pos = 10;
        }

        if (frame.mask) {
            if (len < pos + 4) { consumed = 0; return false; }
            frame.masking_key = (static_cast<uint32_t>(data[pos]) << 24)
                | (static_cast<uint32_t>(data[pos+1]) << 16)
                | (static_cast<uint32_t>(data[pos+2]) << 8)
                | data[pos+3];
            pos += 4;
        }

        if (len < pos + plen) { consumed = 0; return false; }
        frame.payload.assign(reinterpret_cast<const char*>(data + pos), plen);
        if (frame.mask) {
            for (size_t i = 0; i < plen; ++i)
                frame.payload[i] ^= static_cast<char>((frame.masking_key >> (8 * (3 - (i % 4)))) & 0xFF);
        }
        consumed = pos + plen;
        return true;
    }
};

// ── Inline 帧头解析 ────────────────────────────────────────────────
//
// 从裸缓冲区直接解析帧头，不拷贝 payload。用于 echo 快速路径。
// payload_ptr==nullptr 表示缓冲区数据不足以包含完整帧（需更多数据）。
// 解析逻辑与 WebSocketFrame::decode 一致——修改 decode 时此处也需同步。

struct InlineFrameInfo {
    OpCode opcode = OpCode::Text;
    bool fin = true;
    bool masked = false;
    uint32_t mask_key = 0;
    const uint8_t* payload_ptr = nullptr;  // nullptr = 数据不完整
    size_t payload_len = 0;
    size_t header_len = 0;    // 帧头总字节数（含 mask key）
    size_t total_consumed = 0; // header_len + payload_len
};

inline InlineFrameInfo parse_inline_frame(const uint8_t* data, size_t len)
{
    InlineFrameInfo info{};
    if (len < 2)
    {
        return info;  // payload_ptr 保持 nullptr
    }

    info.fin = (data[0] & 0x80) != 0;
    info.opcode = static_cast<OpCode>(data[0] & 0x0F);
    info.masked = (data[1] & 0x80) != 0;
    uint64_t plen = data[1] & 0x7F;
    size_t pos = 2;

    if (plen == 126)
    {
        if (len < 4) { return info; }
        plen = (static_cast<uint64_t>(data[2]) << 8) | data[3];
        pos = 4;
    }
    else if (plen == 127)
    {
        if (len < 10) { return info; }
        plen = 0;
        for (int i = 0; i < 8; ++i)
        {
            plen = (plen << 8) | data[2 + i];
        }
        pos = 10;
    }

    if (info.masked)
    {
        if (len < pos + 4) { return info; }
        info.mask_key = (static_cast<uint32_t>(data[pos]) << 24)
                      | (static_cast<uint32_t>(data[pos + 1]) << 16)
                      | (static_cast<uint32_t>(data[pos + 2]) << 8)
                      | data[pos + 3];
        pos += 4;
    }

    // 始终设置 total_consumed——即使数据不完整，调用方也能知道需要累积多少字节
    info.header_len = pos;
    info.payload_len = static_cast<size_t>(plen);
    info.total_consumed = pos + static_cast<size_t>(plen);

    if (pos + plen > len)
    {
        return info;  // 帧跨缓冲区边界，数据不完整（payload_ptr 保持 nullptr）
    }

    info.payload_ptr = data + pos;
    return info;
}

// === WebSocket ===

class WebSocket : ynet::utils::Noncopyable {
    // Stack buffer sizes for zero-alloc fast path.
    // WS_READ_BUF covers most frames in a single read (8 KiB).
    // WS_WRITE_BUF covers frames up to ~16 KiB payload without heap alloc.
    // Beyond these, a heap fallback is used transparently.
    static constexpr size_t WS_READ_BUF = 16384;   // 16KB 栈缓冲（覆盖多数帧）
    static constexpr size_t WS_WRITE_BUF = 16384;
    static constexpr size_t WS_READ_CHUNK = 16384;  // 16KB 扩展粒度

public:
    explicit WebSocket(TcpSocket socket) noexcept : m_socket(std::move(socket)), m_masked(true) {}

    WebSocket(WebSocket&& other) noexcept
        : m_socket(std::move(other.m_socket)), m_masked(other.m_masked), m_closed(other.m_closed) {}
    WebSocket& operator=(WebSocket&& other) noexcept {
        if (this != &other) {
            m_socket = std::move(other.m_socket);
            m_masked = other.m_masked;
            m_closed = other.m_closed;
        }
        return *this;
    }

    static Task<WebSocket> connect(
        const std::string& host, uint16_t port,
        const std::string& path = "/",
        std::chrono::milliseconds timeout = std::chrono::milliseconds(5000))
    {
        auto sock = co_await TcpSocket::connect(host, port, timeout);
        std::string key = detail::ws_generate_key();

        // Build upgrade request
        http::HttpRequest req;
        req.method = http::Method::GET;
        req.path = path;
        req.headers.push_back({"Host", host + ":" + std::to_string(port)});
        req.headers.push_back({"Upgrade", "websocket"});
        req.headers.push_back({"Connection", "Upgrade"});
        req.headers.push_back({"Sec-WebSocket-Key", key});
        req.headers.push_back({"Sec-WebSocket-Version", "13"});

        auto data = req.serialize();
        size_t total = 0;
        while (total < data.size()) {
            auto w = co_await sock.write(data.data() + total, data.size() - total);
            if (!w) throw std::system_error(w.error(), "websocket handshake write failed");
            total += *w;
        }

        // Read response
        std::string buf(4096, '\0');
        http::HttpResponse resp;
        size_t consumed = 0;
        while (consumed == 0) {
            auto r = co_await sock.read(buf.data(), buf.size());
            if (!r || *r == 0) throw std::system_error(io::make_io_error(ECONNRESET), "handshake read failed");
            consumed = resp.parse(buf.data(), *r);
        }

        if (resp.status_code != 101)
            throw std::system_error(io::make_io_error(EPROTO), "handshake rejected: " + std::to_string(resp.status_code));
        auto accept_key = detail::ws_accept_key(key);
        auto sv = resp.header("sec-websocket-accept");
        if (sv != accept_key)
            throw std::system_error(io::make_io_error(EPROTO), "Sec-WebSocket-Accept mismatch");

        co_return WebSocket(std::move(sock));
    }

    Task<std::error_code> accept(const http::HttpRequest& req) {
        auto upgrade = req.header("upgrade");
        auto connection = req.header("connection");
        auto ws_key = req.header("sec-websocket-key");
        auto version = req.header("sec-websocket-version");

        // Validate headers (case-insensitive)
        auto lower = [](std::string_view sv) {
            std::string s(sv);
            for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
            return s;
        };
        bool upgrade_ok = lower(upgrade).find("websocket") != std::string::npos;
        bool conn_ok = lower(connection).find("upgrade") != std::string::npos;

        if (!upgrade_ok || !conn_ok || ws_key.empty() || version != "13") {
            co_return io::make_io_error(EPROTO);
        }

        std::string accept_val = detail::ws_accept_key(std::string(ws_key));

        http::HttpResponse resp;
        resp.status_code = 101;
        resp.status_message = "Switching Protocols";
        resp.headers.push_back({"Upgrade", "websocket"});
        resp.headers.push_back({"Connection", "Upgrade"});
        resp.headers.push_back({"Sec-WebSocket-Accept", accept_val});

        auto data = resp.serialize();
        size_t total = 0;
        while (total < data.size()) {
            auto w = co_await m_socket.write(data.data() + total, data.size() - total);
            if (!w) co_return w.error();
            total += *w;
        }
        m_masked = false;
        co_return std::error_code{};
    }

    Task<WebSocketFrame> read_frame() {
        uint8_t stack_buf[WS_READ_BUF];
        std::vector<uint8_t> heap_buf;
        uint8_t* buf = stack_buf;
        size_t cap = WS_READ_BUF;
        size_t total = 0;

        while (true) {
            auto r = co_await m_socket.read(buf + total, cap - total);
            if (!r) throw std::system_error(r.error(), "websocket read failed");
            if (*r == 0) throw std::system_error(io::make_io_error(ECONNRESET), "websocket connection closed");
            total += *r;

            size_t consumed = 0;
            WebSocketFrame frame;
            if (WebSocketFrame::decode(buf, total, consumed, frame)) {
                if (frame.opcode == OpCode::Ping) {
                    auto pong = WebSocketFrame::pong(frame.payload);
                    co_await write_frame(pong);
                    if (consumed < total) {
                        size_t rem = total - consumed;
                        std::memmove(buf, buf + consumed, rem);
                        total = rem;
                    } else {
                        total = 0;
                    }
                    continue;
                }
                if (frame.opcode == OpCode::Close) {
                    auto close_resp = WebSocketFrame::close();
                    co_await write_frame(close_resp);
                    m_closed = true;
                }
                co_return frame;
            }

            // 帧不完整：栈→堆切换
            if (buf == stack_buf) {
                heap_buf.assign(stack_buf, stack_buf + total);
                buf = heap_buf.data();
                cap = heap_buf.size();
            }
            if (total >= cap) {
                heap_buf.resize(cap + WS_READ_CHUNK);
                buf = heap_buf.data();
                cap = heap_buf.size();
            }
        }
    }

    // ── ring-based 帧读取 ──────────────────────────────────────────────
    //
    // 使用 multishot recv + buffer ring 读取帧，消除内核→用户态拷贝。
    // 与 read_frame() 互斥使用——同一连接只能选一种读模式。
    //
    // assembler 需要预先 start()，BufferGroup 由调用方管理（通常所有连接共享）。
    // 内部自动处理 Ping/Pong/Close 控制帧。

    Task<WebSocketFrame> read_frame_ring(BufferRingAssembler& assembler) {
        // 就地解码 lambda：从重组缓冲区尝试提取一个完整帧
        auto try_decode = [&]() -> std::optional<WebSocketFrame> {
            auto& buf = assembler.reasm_buffer();
            if (buf.empty()) {
                return std::nullopt;
            }
            size_t consumed = 0;
            WebSocketFrame frame;
            if (WebSocketFrame::decode(buf.data(), buf.size(), consumed, frame)) {
                assembler.consume_bytes(consumed);
                assembler.return_reasm_buffers();
                return frame;
            }
            return std::nullopt;
        };

        while (true) {
            // 重组缓冲区有残留数据时先尝试解码
            if (assembler.has_remaining()) {
                auto frame = try_decode();
                if (frame) {
                    if (frame->opcode == OpCode::Ping) {
                        co_await write_frame(
                            WebSocketFrame::pong(frame->payload));
                        continue;
                    }
                    if (frame->opcode == OpCode::Close) {
                        co_await write_frame(WebSocketFrame::close());
                        m_closed = true;
                    }
                    co_return *frame;
                }
            }

            // 等待下一个 buffer ring 数据块
            auto chunk = co_await assembler.next_chunk();
            if (chunk.is_eof()) {
                throw std::system_error(
                    io::make_io_error(ECONNRESET),
                    "websocket ring recv: connection closed");
            }
            if (chunk.is_error()) {
                throw std::system_error(
                    io::make_io_error(-chunk.res),
                    "websocket ring recv: error");
            }

            // 追加数据到重组缓冲区
            assembler.append_chunk(chunk);

            // 尝试解码完整帧
            auto frame = try_decode();
            if (frame) {
                // 处理控制帧
                if (frame->opcode == OpCode::Ping) {
                    co_await write_frame(
                        WebSocketFrame::pong(frame->payload));
                    continue;
                }
                if (frame->opcode == OpCode::Close) {
                    co_await write_frame(WebSocketFrame::close());
                    m_closed = true;
                }
                co_return *frame;
            }

            // 帧不完整，继续等待下一个数据块
        }
    }

    Task<void> write_frame(const WebSocketFrame& frame) {
        // 统一 writev 路径：帧头栈编码(≤14B) + payload 原位引用，零拷贝。
        // 消除大帧的 heap 分配和 payload 全拷贝瓶颈。
        size_t payload_len = frame.payload.size();
        size_t total = WebSocketFrame::encoded_size(payload_len, m_masked);
        size_t hdr_len = total - payload_len;

        // 栈编码帧头（最多 14 字节）
        uint8_t hdr[14];
        uint8_t* p = hdr;
        *p++ = static_cast<uint8_t>((frame.fin ? 0x80 : 0x00) |
                                     (static_cast<uint8_t>(frame.opcode) & 0x0F));
        if (payload_len < 126) {
            *p++ = static_cast<uint8_t>(payload_len | (m_masked ? 0x80 : 0x00));
        } else if (payload_len <= 65535) {
            *p++ = static_cast<uint8_t>(126 | (m_masked ? 0x80 : 0x00));
            *p++ = static_cast<uint8_t>(payload_len >> 8);
            *p++ = static_cast<uint8_t>(payload_len);
        } else {
            *p++ = static_cast<uint8_t>(127 | (m_masked ? 0x80 : 0x00));
            for (int i = 7; i >= 0; --i) {
                *p++ = static_cast<uint8_t>(payload_len >> (i * 8));
            }
        }
        if (m_masked) {
            uint32_t mk = frame.masking_key;
            *p++ = static_cast<uint8_t>(mk >> 24);
            *p++ = static_cast<uint8_t>(mk >> 16);
            *p++ = static_cast<uint8_t>(mk >> 8);
            *p++ = static_cast<uint8_t>(mk);
        }

        iovec iov[2];
        iov[0].iov_base = hdr;
        iov[0].iov_len = hdr_len;
        iov[1].iov_base = const_cast<char*>(
            reinterpret_cast<const char*>(frame.payload.data()));
        iov[1].iov_len = payload_len;

        auto w = co_await io::Writev(m_socket.fd(), iov, 2);
        if (!w) {
            throw std::system_error(w.error(), "websocket write failed");
        }
    }

    // ── 零拷贝写帧 ──────────────────────────────────────────────────
    //
    // 帧头栈编码(≤14B) + payload 原位 writev 引用，不经过 WebSocketFrame 对象。
    // payload 指向的内存必须在 writev 完成前保持有效。
    // 服务端 m_masked=false，帧头仅 2-10 字节，无 mask key。

    Task<void> write_frame_raw(OpCode opcode, bool fin,
                               const uint8_t* payload, size_t payload_len) {
        uint8_t hdr[14];
        uint8_t* p = hdr;
        *p++ = static_cast<uint8_t>((fin ? 0x80 : 0x00) |
                                     (static_cast<uint8_t>(opcode) & 0x0F));
        if (payload_len < 126) {
            *p++ = static_cast<uint8_t>(payload_len | (m_masked ? 0x80 : 0x00));
        } else if (payload_len <= 65535) {
            *p++ = static_cast<uint8_t>(126 | (m_masked ? 0x80 : 0x00));
            *p++ = static_cast<uint8_t>(payload_len >> 8);
            *p++ = static_cast<uint8_t>(payload_len);
        } else {
            *p++ = static_cast<uint8_t>(127 | (m_masked ? 0x80 : 0x00));
            for (int i = 7; i >= 0; --i) {
                *p++ = static_cast<uint8_t>(payload_len >> (i * 8));
            }
        }
        if (m_masked) {
            uint32_t mk = 0;
            // 服务端通常 m_masked=false，此分支不执行
            *p++ = static_cast<uint8_t>(mk >> 24);
            *p++ = static_cast<uint8_t>(mk >> 16);
            *p++ = static_cast<uint8_t>(mk >> 8);
            *p++ = static_cast<uint8_t>(mk);
        }
        size_t hdr_len = static_cast<size_t>(p - hdr);

        iovec iov[2];
        iov[0].iov_base = hdr;
        iov[0].iov_len = hdr_len;
        iov[1].iov_base = const_cast<uint8_t*>(payload);
        iov[1].iov_len = payload_len;

        auto w = co_await io::Writev(m_socket.fd(), iov, 2);
        if (!w) {
            throw std::system_error(w.error(), "websocket write failed");
        }
    }

    // ── 多 segment 零拷贝写帧 ──────────────────────────────────────
    //
    // 帧头栈编码 + 多段 payload iovec（用于跨 ring buffer 帧）。
    // 与 write_frame_raw 的区别：payload 分散在多个不连续的内存段中。
    // segments 中的 iov_len 之和必须等于 payload_len。

    Task<void> write_frame_raw_multi(OpCode opcode, bool fin,
                                     const std::vector<iovec>& segments,
                                     size_t payload_len) {
        uint8_t hdr[14];
        uint8_t* p = hdr;
        *p++ = static_cast<uint8_t>((fin ? 0x80 : 0x00) |
                                     (static_cast<uint8_t>(opcode) & 0x0F));
        if (payload_len < 126) {
            *p++ = static_cast<uint8_t>(payload_len | (m_masked ? 0x80 : 0x00));
        } else if (payload_len <= 65535) {
            *p++ = static_cast<uint8_t>(126 | (m_masked ? 0x80 : 0x00));
            *p++ = static_cast<uint8_t>(payload_len >> 8);
            *p++ = static_cast<uint8_t>(payload_len);
        } else {
            *p++ = static_cast<uint8_t>(127 | (m_masked ? 0x80 : 0x00));
            for (int i = 7; i >= 0; --i) {
                *p++ = static_cast<uint8_t>(payload_len >> (i * 8));
            }
        }
        if (m_masked) {
            uint32_t mk = 0;
            *p++ = static_cast<uint8_t>(mk >> 24);
            *p++ = static_cast<uint8_t>(mk >> 16);
            *p++ = static_cast<uint8_t>(mk >> 8);
            *p++ = static_cast<uint8_t>(mk);
        }
        size_t hdr_len = static_cast<size_t>(p - hdr);

        // 构建完整 iovec: [header] + [各 payload 段]
        std::vector<iovec> iov;
        iov.reserve(segments.size() + 1);
        iov.push_back({hdr, hdr_len});
        for (auto& seg : segments) {
            iov.push_back(seg);
        }

        auto w = co_await io::Writev(m_socket.fd(), iov.data(),
                                     static_cast<int>(iov.size()));
        if (!w) {
            throw std::system_error(w.error(), "websocket write failed");
        }
    }

    Task<void> send_text(std::string data) {
        co_return co_await write_frame(WebSocketFrame::text(std::move(data)));
    }
    Task<void> send_binary(std::string data) {
        co_return co_await write_frame(WebSocketFrame::binary(std::move(data)));
    }
    Task<void> send_ping(const std::string& data = "") {
        co_return co_await write_frame(WebSocketFrame::ping(data));
    }
    Task<void> send_pong(const std::string& data = "") {
        co_return co_await write_frame(WebSocketFrame::pong(data));
    }

    // 内联 echo：读帧→写回，payload 不离开协程上下文。
    // 快速路径：inline 帧头解析 + writev 直接引用 buffer 中的 payload（零拷贝）。
    // 慢路径：帧跨读取边界时退到 WebSocketFrame::decode。
    // 返回 false 表示连接关闭。
    Task<bool> echo_inplace() {
        uint8_t stack_buf[WS_READ_BUF];
        std::vector<uint8_t> heap_buf;
        uint8_t* buf = stack_buf;
        size_t cap = WS_READ_BUF;
        size_t total = 0;

        while (true) {
            auto r = co_await m_socket.read(buf + total, cap - total);
            if (!r) { co_return false; }
            if (*r == 0) { co_return false; }
            total += *r;

            // ═══ 快速路径：inline 帧头解析，零拷贝 ═══
            auto info = parse_inline_frame(buf, total);
            if (info.payload_ptr)
            {
                const uint8_t* payload = info.payload_ptr;

                // 原地解掩码（修改用户缓冲区，安全——下一轮 read 会覆盖）
                if (info.masked)
                {
                    auto* mp = const_cast<uint8_t*>(payload);
                    for (size_t i = 0; i < info.payload_len; ++i)
                    {
                        mp[i] ^= static_cast<uint8_t>(
                            (info.mask_key >> (8 * (3 - (i % 4)))) & 0xFF);
                    }
                }

                if (info.opcode == OpCode::Ping)
                {
                    std::string pd(reinterpret_cast<const char*>(payload),
                                   info.payload_len);
                    co_await write_frame(WebSocketFrame::pong(pd));
                }
                else if (info.opcode == OpCode::Close)
                {
                    co_await write_frame(WebSocketFrame::close());
                    m_closed = true;
                    co_return false;
                }
                else
                {
                    // Text/Binary/Continuation：writev 直接引用 buffer 中的 payload
                    co_await write_frame_raw(info.opcode, info.fin,
                                             payload, info.payload_len);
                }

                // 移除已消费数据
                if (info.total_consumed < total)
                {
                    std::memmove(buf, buf + info.total_consumed,
                                 total - info.total_consumed);
                    total -= info.total_consumed;
                }
                else
                {
                    total = 0;
                }
                co_return true;
            }

            // ── 慢路径：帧跨越读取边界，继续累积数据 ──
            if (buf == stack_buf && total >= cap)
            {
                heap_buf.assign(stack_buf, stack_buf + total);
                buf = heap_buf.data();
                cap = heap_buf.capacity();
            }
            if (total >= cap)
            {
                heap_buf.resize(cap + WS_READ_CHUNK);
                buf = heap_buf.data();
                cap = heap_buf.size();
            }
        }
    }

    // ── ring-based 内联 echo ──────────────────────────────────────────
    //
    // 使用 multishot recv + buffer ring 实现零拷贝 echo。
    // 流程：接收 buffer ring 数据块 → 重组 → 解码帧 → 直接写回。
    // 返回 false 表示连接关闭。

    Task<bool> echo_inplace_ring(BufferRingAssembler& assembler) {
        // 慢路径：走 assembler 重组 + WebSocketFrame::decode 流程
        while (true) {

            // 等待下一个 ring buffer 数据块
            auto chunk = co_await assembler.next_chunk();
            if (chunk.is_eof() || chunk.is_error()) {
                co_return false;
            }

            auto* bg = assembler.buffer_group();
            bool need_slow_path = false;

            // ═══ 快速路径：inline 帧头解析，零拷贝 ═══
            if (bg)
            {
                auto* raw = static_cast<const uint8_t*>(
                    bg->get_buffer(chunk.buffer_id()));
                size_t len = static_cast<size_t>(chunk.res);
                auto info = parse_inline_frame(raw, len);

                if (info.payload_ptr)
                {
                    const uint8_t* payload = info.payload_ptr;

                    // 原地解掩码（ring buffer 在 return 前不会被内核触碰）
                    if (info.masked)
                    {
                        auto* mp = const_cast<uint8_t*>(payload);
                        for (size_t i = 0; i < info.payload_len; ++i)
                        {
                            mp[i] ^= static_cast<uint8_t>(
                                (info.mask_key >> (8 * (3 - (i % 4)))) & 0xFF);
                        }
                    }

                    if (info.opcode == OpCode::Ping)
                    {
                        std::string pd(reinterpret_cast<const char*>(payload),
                                       info.payload_len);
                        co_await write_frame(WebSocketFrame::pong(pd));
                    }
                    else if (info.opcode == OpCode::Close)
                    {
                        co_await write_frame(WebSocketFrame::close());
                        m_closed = true;
                        bg->return_buffer(chunk.buffer_id());
                        bg->advance_ring(1);
                        co_return false;
                    }
                    else
                    {
                        co_await write_frame_raw(info.opcode, info.fin,
                                                 payload, info.payload_len);
                    }

                    bg->return_buffer(chunk.buffer_id());
                    bg->advance_ring(1);
                    co_return true;
                }

                // 快速路径失败：帧跨 buffer，走多 segment 零拷贝慢路径
                need_slow_path = true;
            }

            // ═══ 多 segment 零拷贝慢路径 ═══
            if (need_slow_path && bg)
            {
                // 重新解析帧头获取完整帧信息
                auto* raw = static_cast<const uint8_t*>(
                    bg->get_buffer(chunk.buffer_id()));
                auto info = parse_inline_frame(raw, chunk.res);
                // info.total_consumed 已包含完整帧所需字节数
                size_t needed = info.total_consumed;

                // 记录所有 segment: (buffer_id, 指针, 长度)
                std::vector<unsigned> bids;
                std::vector<const uint8_t*> seg_ptrs;
                std::vector<size_t> seg_lens;
                size_t accumulated = chunk.res;

                bids.push_back(chunk.buffer_id());
                seg_ptrs.push_back(raw);
                seg_lens.push_back(chunk.res);

                // 继续接收后续 chunk 直到累积足够
                while (accumulated < needed)
                {
                    auto next_chunk = co_await assembler.next_chunk();
                    if (next_chunk.is_eof() || next_chunk.is_error())
                    {
                        for (unsigned bid : bids) { bg->return_buffer(bid); }
                        if (!bids.empty()) { bg->advance_ring(static_cast<int>(bids.size())); }
                        co_return false;
                    }
                    bids.push_back(next_chunk.buffer_id());
                    seg_ptrs.push_back(static_cast<const uint8_t*>(
                        bg->get_buffer(next_chunk.buffer_id())));
                    seg_lens.push_back(next_chunk.res);
                    accumulated += next_chunk.res;
                }

                // 构建 payload iovec 列表（从 segment 0 的 header_len 偏移开始）
                std::vector<iovec> iovs;
                size_t remaining = info.payload_len;
                size_t seg_idx = 0;
                size_t seg_offset = info.header_len;

                while (remaining > 0 && seg_idx < seg_lens.size())
                {
                    size_t avail = seg_lens[seg_idx] - seg_offset;
                    size_t take = std::min(remaining, avail);
                    iovs.push_back({
                        const_cast<uint8_t*>(seg_ptrs[seg_idx] + seg_offset),
                        take
                    });
                    remaining -= take;
                    seg_idx++;
                    seg_offset = 0;
                }

                // 原地解掩码
                if (info.masked)
                {
                    size_t goff = 0;
                    for (auto& iov_seg : iovs)
                    {
                        auto* mp = static_cast<uint8_t*>(iov_seg.iov_base);
                        for (size_t i = 0; i < iov_seg.iov_len; ++i)
                        {
                            mp[i] ^= static_cast<uint8_t>(
                                (info.mask_key >> (8 * (3 - ((goff + i) % 4)))) & 0xFF);
                        }
                        goff += iov_seg.iov_len;
                    }
                }

                // 发送响应
                if (info.opcode == OpCode::Ping)
                {
                    std::string pd;
                    pd.reserve(info.payload_len);
                    for (auto& s : iovs)
                    {
                        pd.append(static_cast<const char*>(s.iov_base), s.iov_len);
                    }
                    co_await write_frame(WebSocketFrame::pong(pd));
                }
                else if (info.opcode == OpCode::Close)
                {
                    co_await write_frame(WebSocketFrame::close());
                    m_closed = true;
                    for (unsigned bid : bids) { bg->return_buffer(bid); }
                    if (!bids.empty()) { bg->advance_ring(static_cast<int>(bids.size())); }
                    co_return false;
                }
                else
                {
                    co_await write_frame_raw_multi(info.opcode, info.fin,
                                                   iovs, info.payload_len);
                }

                // 归还所有 buffer
                for (unsigned bid : bids) { bg->return_buffer(bid); }
                if (!bids.empty()) { bg->advance_ring(static_cast<int>(bids.size())); }
                co_return true;
            }
        }
    }

    Task<void> close(uint16_t code = 1000, const std::string& reason = "") {
        if (m_closed) co_return;
        co_await write_frame(WebSocketFrame::close(code, reason));
        m_closed = true;
    }

    TcpSocket& socket() noexcept { return m_socket; }
    bool is_open() const noexcept { return !m_closed; }

private:
    TcpSocket m_socket;
    bool m_masked = true;
    bool m_closed = false;
};

} // namespace ynet::async::net::websocket

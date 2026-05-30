#pragma once

#include "ultranet/net/tcp_socket.hpp"
#include "ultranet/net/http.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/io/io_awaitable.hpp"
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

// === WebSocket ===

class WebSocket : ynet::utils::Noncopyable {
    // Stack buffer sizes for zero-alloc fast path.
    // WS_READ_BUF covers most frames in a single read (8 KiB).
    // WS_WRITE_BUF covers frames up to ~16 KiB payload without heap alloc.
    // Beyond these, a heap fallback is used transparently.
    static constexpr size_t WS_READ_BUF = 8192;
    static constexpr size_t WS_WRITE_BUF = 16384;
    static constexpr size_t WS_READ_CHUNK = 4096;

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

            // Frame incomplete: switch to heap if still on stack
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

    Task<void> write_frame(const WebSocketFrame& frame) {
        uint8_t stack_buf[WS_WRITE_BUF];
        size_t n = frame.encode_into(stack_buf, WS_WRITE_BUF, m_masked);
        if (n > 0) {
            auto w = co_await m_socket.write(stack_buf, n);
            if (!w) throw std::system_error(w.error(), "websocket write failed");
            co_return;
        }

        // Large frame (>WS_WRITE_BUF): heap-allocated encode + writev
        auto encoded = frame.encode(m_masked);
        size_t hdr_end = WebSocketFrame::encoded_size(frame.payload.size(), m_masked) - frame.payload.size();
        iovec iov[2];
        iov[0].iov_base = const_cast<uint8_t*>(encoded.data());
        iov[0].iov_len = hdr_end;
        iov[1].iov_base = encoded.data() + hdr_end;
        iov[1].iov_len = encoded.size() - hdr_end;

        auto w = co_await io::Writev(m_socket.fd(), iov, 2);
        if (!w) throw std::system_error(w.error(), "websocket write failed");
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

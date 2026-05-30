#include "ultranet/ultranet.h"
#include <iostream>
#include <cstring>

using namespace ynet::async;
using namespace ynet::async::net::websocket;

static int passed = 0, failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

void test_small_frame_rt() {
    T("small frame encode/decode");
    auto f = WebSocketFrame::text("hello");
    auto enc = f.encode(true);
    size_t consumed = 0;
    WebSocketFrame decoded;
    CHECK(WebSocketFrame::decode(enc.data(), enc.size(), consumed, decoded), "decode ok");
    CHECK(decoded.fin, "FIN");
    CHECK(decoded.opcode == OpCode::Text, "opcode");
    CHECK(decoded.payload == "hello", "payload");
    CHECK(consumed == enc.size(), "consumed all");
    PASS();
}

void test_medium_frame_rt() {
    T("medium frame (126 extended length)");
    std::string data(200, 'x');
    auto f = WebSocketFrame::binary(data);
    auto enc = f.encode(false);
    CHECK(enc.size() > data.size(), "encoded larger than payload");
    size_t consumed = 0;
    WebSocketFrame decoded;
    CHECK(WebSocketFrame::decode(enc.data(), enc.size(), consumed, decoded), "decode ok");
    CHECK(decoded.opcode == OpCode::Binary, "opcode");
    CHECK(decoded.payload.size() == 200, "payload size");
    PASS();
}

void test_large_frame_rt() {
    T("large frame (127 extended length)");
    std::string data(70000, 'y');
    auto f = WebSocketFrame::text(data);
    auto enc = f.encode(false);
    size_t consumed = 0;
    WebSocketFrame decoded;
    CHECK(WebSocketFrame::decode(enc.data(), enc.size(), consumed, decoded), "decode ok");
    CHECK(decoded.payload.size() == 70000, "payload size");
    CHECK(decoded.opcode == OpCode::Text, "opcode");
    PASS();
}

void test_masking_rt() {
    T("masking round-trip");
    auto f = WebSocketFrame::text("secret");
    auto enc = f.encode(true);
    size_t consumed = 0;
    WebSocketFrame decoded;
    CHECK(WebSocketFrame::decode(enc.data(), enc.size(), consumed, decoded), "decode ok");
    CHECK(decoded.payload == "secret", "unmasked correctly");
    PASS();
}

void test_close_frame() {
    T("close frame with code+reason");
    auto f = WebSocketFrame::close(1001, "bye");
    CHECK(f.opcode == OpCode::Close, "opcode");
    CHECK(f.payload.size() >= 2, "has close code");
    PASS();
}

void test_ping_pong() {
    T("ping/pong frames");
    auto ping = WebSocketFrame::ping("data");
    CHECK(ping.opcode == OpCode::Ping, "ping opcode");

    auto pong = WebSocketFrame::pong("data");
    CHECK(pong.opcode == OpCode::Pong, "pong opcode");
    PASS();
}

void test_partial_decode() {
    T("partial decode returns consumed=0");
    uint8_t buf[1] = {0x81};
    size_t consumed = 0;
    WebSocketFrame f;
    CHECK(!WebSocketFrame::decode(buf, 1, consumed, f), "decode fails");
    CHECK(consumed == 0, "consumed == 0");
    PASS();
}

void test_accept_key() {
    T("accept key computation");
    // Test vector from RFC 6455
    auto key = ynet::async::net::websocket::detail::ws_accept_key("dGhlIHNhbXBsZSBub25jZQ==");
    CHECK(key == "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", "accept key matches");
    PASS();
}

int main() {
    std::cout << "=== WebSocket Tests ===" << std::endl;
    test_small_frame_rt();
    test_medium_frame_rt();
    test_large_frame_rt();
    test_masking_rt();
    test_close_frame();
    test_ping_pong();
    test_partial_decode();
    test_accept_key();

    std::cout << "\n=== WebSocket Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

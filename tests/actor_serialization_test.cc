// Serialization + remote proxy tests.
// Verifies: serializer read/write round-trips, pack/unpack envelopes,
// remote proxy construction and basic deliver.
#include "ultranet/actor.hpp"
#include <iostream>
#include <string>
#include <cstring>
#include <cassert>

using namespace ynet::actor;
using namespace ynet::actor::dist;

static int g_passed = 0;
static int g_failed = 0;
#define T(n) do { std::cout << "  " << n << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++g_passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++g_failed; } while(0)
#define CHECK(c,m) do { if(!(c)){FAIL(m);return;} } while(0)

// ── Serializer unit tests ──────────────────────────────────────────────

void test_serializer_u8() {
    T("serializer u8 round-trip");
    serializer s;
    s.write_u8(0x42);
    s.write_u8(0xFF);
    s.write_u8(0x00);

    serializer r(s.consume());
    CHECK(r.read_u8() == 0x42, "u8 value 1");
    CHECK(r.read_u8() == 0xFF, "u8 value 2");
    CHECK(r.read_u8() == 0x00, "u8 value 3");
    PASS();
}

void test_serializer_u16() {
    T("serializer u16 round-trip");
    serializer s;
    s.write_u16(0x1234);
    s.write_u16(0x0000);
    s.write_u16(0xFFFF);

    serializer r(s.consume());
    CHECK(r.read_u16() == 0x1234, "u16 value 1");
    CHECK(r.read_u16() == 0x0000, "u16 value 2");
    CHECK(r.read_u16() == 0xFFFF, "u16 value 3");
    PASS();
}

void test_serializer_u32() {
    T("serializer u32 round-trip");
    serializer s;
    s.write_u32(0xDEADBEEF);
    s.write_u32(0);
    s.write_u32(0xFFFFFFFF);

    serializer r(s.consume());
    CHECK(r.read_u32() == 0xDEADBEEF, "u32 value 1");
    CHECK(r.read_u32() == 0, "u32 value 2");
    CHECK(r.read_u32() == 0xFFFFFFFF, "u32 value 3");
    PASS();
}

void test_serializer_u64() {
    T("serializer u64 round-trip");
    serializer s;
    s.write_u64(0x0123456789ABCDEFULL);
    s.write_u64(0);

    serializer r(s.consume());
    CHECK(r.read_u64() == 0x0123456789ABCDEFULL, "u64 value 1");
    CHECK(r.read_u64() == 0, "u64 value 2");
    PASS();
}

void test_serializer_float() {
    T("serializer float round-trip");
    serializer s;
    s.write_float(3.14159f);
    s.write_float(-1.0f);
    s.write_float(0.0f);

    auto raw = s.consume();
    serializer r(std::move(raw));
    float v1 = r.read_float();
    float v2 = r.read_float();
    float v3 = r.read_float();
    CHECK(v1 > 3.14158f && v1 < 3.14160f, "float value 1");
    CHECK(v2 < -0.95f && v2 > -1.05f, "float value 2");
    CHECK(v3 > -0.001f && v3 < 0.001f, "float value 3");
    PASS();
}

void test_serializer_double() {
    T("serializer double round-trip");
    serializer s;
    s.write_double(3.141592653589793);
    s.write_double(0.0);

    serializer r(s.consume());
    CHECK(r.read_double() > 3.141592653 && r.read_double() < 3.141592654, "double value 1");
    CHECK(r.read_double() == 0.0, "double value 2");
    PASS();
}

void test_serializer_string() {
    T("serializer string round-trip");
    serializer s;
    s.write_string("hello world");
    s.write_string("");

    serializer r(s.consume());
    CHECK(r.read_string() == "hello world", "string value 1");
    CHECK(r.read_string() == "", "empty string");
    PASS();
}

void test_serializer_bytes() {
    T("serializer bytes round-trip");
    serializer s;
    uint8_t src[] = {0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB};
    s.write_bytes(src, sizeof(src));

    serializer r(s.consume());
    uint8_t dst[sizeof(src)] = {};
    CHECK(r.remaining() == sizeof(src), "correct byte count");
    r.read_bytes(dst, sizeof(dst));
    CHECK(std::memcmp(src, dst, sizeof(src)) == 0, "bytes match");
    PASS();
}

void test_serializer_empty_buffer() {
    T("serializer read from empty buffer");
    serializer r;  // empty
    CHECK(r.read_u8() == 0, "read_u8 from empty returns 0");
    CHECK(r.read_u16() == 0, "read_u16 from empty returns 0");
    CHECK(r.read_u32() == 0, "read_u32 from empty returns 0");
    CHECK(r.read_u64() == 0, "read_u64 from empty returns 0");
    CHECK(r.read_string() == "", "read_string from empty returns empty");
    PASS();
}

void test_serializer_type_byte() {
    T("serializer type byte");
    serializer s;
    s.write_type_byte(message_type::actor_message);
    s.write_type_byte(message_type::gossip);
    s.write_type_byte(message_type::actor_location);

    serializer r(s.consume());
    CHECK(r.read_type_byte() == message_type::actor_message, "actor_message type");
    CHECK(r.read_type_byte() == message_type::gossip, "gossip type");
    CHECK(r.read_type_byte() == message_type::actor_location, "actor_location type");
    PASS();
}

// ── Wire envelope tests ────────────────────────────────────────────────

void test_pack_actor_message() {
    T("pack actor message envelope");
    auto uri = actor_uri::make_local("my_type", "my_actor");
    uint8_t payload[] = {0x01, 0x02, 0x03};
    auto envelope = pack_actor_message(uri, 0xABCD, payload, sizeof(payload));
    CHECK(envelope.size() > sizeof(payload), "envelope larger than payload");
    // First byte must be actor_message type.
    CHECK(envelope[0] == 0x02, "type byte is actor_message");
    PASS();
}

void test_unpack_actor_message() {
    T("unpack actor message envelope");
    auto uri = actor_uri::make_local("my_type", "my_actor");
    uint8_t payload[] = {0xAA, 0xBB, 0xCC, 0xDD};
    auto envelope = pack_actor_message(uri, 0x12345678, payload, sizeof(payload));

    actor_uri out_uri;
    uint64_t out_hash = 0;
    std::vector<uint8_t> out_payload;
    bool ok = unpack_actor_message(envelope.data(), envelope.size(),
                                    out_uri, out_hash, out_payload);
    CHECK(ok, "unpack succeeded");
    CHECK(out_hash == 0x12345678, "type hash preserved");
    CHECK(out_payload.size() == sizeof(payload), "payload size correct");
    CHECK(std::memcmp(out_payload.data(), payload, sizeof(payload)) == 0,
          "payload data preserved");
    CHECK(out_uri.type == "my_type", "uri type preserved");
    CHECK(out_uri.name == "my_actor", "uri name preserved");
    PASS();
}

void test_pack_unpack_empty_payload() {
    T("pack/unpack zero-length payload rejected");
    auto uri = actor_uri::make_local("T", "N");
    auto envelope = pack_actor_message(uri, 0x1, nullptr, 0);
    actor_uri out_uri;
    uint64_t out_hash = 0;
    std::vector<uint8_t> out_payload;
    // unpack should fail for zero-length payload
    bool ok = unpack_actor_message(envelope.data(), envelope.size(),
                                    out_uri, out_hash, out_payload);
    CHECK(!ok, "zero-length payload rejected");
    PASS();
}

void test_pack_unpack_truncated() {
    T("unpack truncated data returns false");
    std::vector<uint8_t> truncated = {0x02, 0x00};  // too short
    actor_uri out_uri;
    uint64_t out_hash = 0;
    std::vector<uint8_t> out_payload;
    bool ok = unpack_actor_message(truncated.data(), truncated.size(),
                                    out_uri, out_hash, out_payload);
    CHECK(!ok, "truncated data rejected");
    PASS();
}

void test_pack_unpack_wrong_type() {
    T("unpack with wrong type byte");
    auto uri = actor_uri::make_local("T", "N");
    auto envelope = pack_actor_message(uri, 0x1, "data", 4);
    // Corrupt the type byte.
    envelope[0] = 0x01;  // gossip type
    actor_uri out_uri;
    uint64_t out_hash = 0;
    std::vector<uint8_t> out_payload;
    bool ok = unpack_actor_message(envelope.data(), envelope.size(),
                                    out_uri, out_hash, out_payload);
    CHECK(!ok, "wrong type rejected");
    PASS();
}

void test_actor_location_pack_unpack() {
    T("actor location announce pack/unpack");
    auto uri = actor_uri::make(42, "my_type", "my_actor");
    auto envelope = pack_actor_location(uri, 5);

    actor_uri out_uri;
    uint32_t out_ttl = 0;
    bool ok = unpack_actor_location(envelope.data(), envelope.size(),
                                     out_uri, out_ttl);
    CHECK(ok, "unpack location succeeded");
    CHECK(out_ttl == 5, "ttl preserved");
    CHECK(out_uri.node == "42", "node id preserved");
    CHECK(out_uri.type == "my_type", "type preserved");
    CHECK(out_uri.name == "my_actor", "name preserved");
    PASS();
}

// ── Message type discriminator tests ───────────────────────────────────

void test_message_type_values() {
    T("message type discriminator values");
    CHECK(static_cast<uint8_t>(message_type::gossip) == 0x01, "gossip = 0x01");
    CHECK(static_cast<uint8_t>(message_type::actor_message) == 0x02, "actor_message = 0x02");
    CHECK(static_cast<uint8_t>(message_type::actor_location) == 0x03, "actor_location = 0x03");
    PASS();
}

void test_serializer_combined() {
    T("serializer combined types in sequence");
    serializer s;
    s.write_u8(0xAA);
    s.write_u32(0xDEADBEEF);
    s.write_string("test");
    s.write_u16(0xBEEF);
    s.write_double(1.5);

    serializer r(s.consume());
    CHECK(r.read_u8() == 0xAA, "u8");
    CHECK(r.read_u32() == 0xDEADBEEF, "u32");
    CHECK(r.read_string() == "test", "string");
    CHECK(r.read_u16() == 0xBEEF, "u16");
    double d = r.read_double();
    CHECK(d > 1.49 && d < 1.51, "double");
    CHECK(r.eof(), "consumed all data");
    PASS();
}

int main() {
    std::cout << "=== Actor Serialization + Remote Proxy Tests ===" << std::endl;

    // Serializer tests.
    test_serializer_u8();
    test_serializer_u16();
    test_serializer_u32();
    test_serializer_u64();
    test_serializer_float();
    test_serializer_double();
    test_serializer_string();
    test_serializer_bytes();
    test_serializer_empty_buffer();
    test_serializer_type_byte();

    // Envelope tests.
    test_pack_actor_message();
    test_unpack_actor_message();
    test_pack_unpack_empty_payload();
    test_pack_unpack_truncated();
    test_pack_unpack_wrong_type();
    test_actor_location_pack_unpack();

    // Misc.
    test_message_type_values();
    test_serializer_combined();

    std::cout << "\n" << g_passed << " passed, " << g_failed << " failed" << std::endl;
    return g_failed > 0 ? 1 : 0;
}

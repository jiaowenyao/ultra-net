// Binary serialization for actor messages over the network.
// Provides a simple network-byte-order serializer plus wire-envelope
// helpers for packing/unpacking actor messages for TCP transport.
#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <string>
#include <arpa/inet.h>

#include "ultranet/actor/core/actor_uri.h"
#include "ultranet/actor/core/type_hash.h"

namespace ynet::actor::dist {

// ── Message type discriminator (first byte of every transport payload) ──

enum class message_type : uint8_t {
    gossip          = 0x01,
    actor_message   = 0x02,
    actor_location  = 0x03,
};

// ── Binary serializer (network byte order) ─────────────────────────────

class serializer {
public:
    serializer() = default;
    explicit serializer(std::vector<uint8_t> buf)
        : m_data(std::move(buf)) {}

    // ── Write primitives ──────────────────────────────────────────────

    void write_u8(uint8_t v) {
        m_data.push_back(v);
    }

    void write_u16(uint16_t v) {
        uint16_t n = htons(v);
        const auto* p = reinterpret_cast<const uint8_t*>(&n);
        m_data.insert(m_data.end(), p, p + sizeof(n));
    }

    void write_u32(uint32_t v) {
        uint32_t n = htonl(v);
        const auto* p = reinterpret_cast<const uint8_t*>(&n);
        m_data.insert(m_data.end(), p, p + sizeof(n));
    }

    void write_u64(uint64_t v) {
        // No standard htonll — write big-endian manually.
        m_data.push_back(static_cast<uint8_t>((v >> 56) & 0xFF));
        m_data.push_back(static_cast<uint8_t>((v >> 48) & 0xFF));
        m_data.push_back(static_cast<uint8_t>((v >> 40) & 0xFF));
        m_data.push_back(static_cast<uint8_t>((v >> 32) & 0xFF));
        m_data.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
        m_data.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
        m_data.push_back(static_cast<uint8_t>((v >>  8) & 0xFF));
        m_data.push_back(static_cast<uint8_t>( v        & 0xFF));
    }

    void write_float(float v) {
        uint32_t raw;
        std::memcpy(&raw, &v, sizeof(raw));
        write_u32(raw);
    }

    void write_double(double v) {
        uint64_t raw;
        std::memcpy(&raw, &v, sizeof(raw));
        write_u64(raw);
    }

    void write_string(const std::string& s) {
        write_u32(static_cast<uint32_t>(s.size()));
        m_data.insert(m_data.end(), s.begin(), s.end());
    }

    void write_bytes(const void* data, size_t len) {
        m_data.insert(m_data.end(),
                      static_cast<const uint8_t*>(data),
                      static_cast<const uint8_t*>(data) + len);
    }

    void write_type_byte(message_type t) {
        write_u8(static_cast<uint8_t>(t));
    }

    // ── Read primitives ──────────────────────────────────────────────

    uint8_t read_u8() {
        if (m_offset >= m_data.size()) {
            return 0;
        }
        return m_data[m_offset++];
    }

    uint16_t read_u16() {
        if (m_offset + sizeof(uint16_t) > m_data.size()) {
            return 0;
        }
        uint16_t n;
        std::memcpy(&n, m_data.data() + m_offset, sizeof(n));
        m_offset += sizeof(n);
        return ntohs(n);
    }

    uint32_t read_u32() {
        if (m_offset + sizeof(uint32_t) > m_data.size()) {
            return 0;
        }
        uint32_t n;
        std::memcpy(&n, m_data.data() + m_offset, sizeof(n));
        m_offset += sizeof(n);
        return ntohl(n);
    }

    uint64_t read_u64() {
        if (m_offset + sizeof(uint64_t) > m_data.size()) {
            return 0;
        }
        // Big-endian manual decode.
        const uint8_t* p = m_data.data() + m_offset;
        uint64_t v = (static_cast<uint64_t>(p[0]) << 56)
                   | (static_cast<uint64_t>(p[1]) << 48)
                   | (static_cast<uint64_t>(p[2]) << 40)
                   | (static_cast<uint64_t>(p[3]) << 32)
                   | (static_cast<uint64_t>(p[4]) << 24)
                   | (static_cast<uint64_t>(p[5]) << 16)
                   | (static_cast<uint64_t>(p[6]) <<  8)
                   | (static_cast<uint64_t>(p[7])      );
        m_offset += sizeof(v);
        return v;
    }

    float read_float() {
        uint32_t raw = read_u32();
        float v;
        std::memcpy(&v, &raw, sizeof(v));
        return v;
    }

    double read_double() {
        uint64_t raw = read_u64();
        double v;
        std::memcpy(&v, &raw, sizeof(v));
        return v;
    }

    std::string read_string() {
        uint32_t len = read_u32();
        if (len == 0 || m_offset + len > m_data.size()) {
            if (len == 0) {
                return {};
            }
            return {};
        }
        std::string s(reinterpret_cast<const char*>(m_data.data() + m_offset), len);
        m_offset += len;
        return s;
    }

    void read_bytes(void* dest, size_t len) {
        if (m_offset + len > m_data.size()) {
            return;
        }
        std::memcpy(dest, m_data.data() + m_offset, len);
        m_offset += len;
    }

    message_type read_type_byte() {
        return static_cast<message_type>(read_u8());
    }

    // ── State ────────────────────────────────────────────────────────

    const std::vector<uint8_t>& data() const { return m_data; }
    std::vector<uint8_t> consume() { return std::move(m_data); }
    size_t offset() const { return m_offset; }
    size_t remaining() const { return m_data.size() - m_offset; }
    bool eof() const { return m_offset >= m_data.size(); }

private:
    std::vector<uint8_t> m_data;
    size_t m_offset = 0;
};

// ── Wire envelope helpers ───────────────────────────────────────────────
//
// Actor message envelope (type = 0x02):
//   type:1      — always 0x02
//   uri_len:4   — length of target URI string (net order)
//   uri:var     — UTF-8 actor URI
//   msg_hash:8  — FNV-1a type hash (net order)
//   payload_len:4 — length of serialized message (net order)
//   payload:var — serialized message body
//
// Actor location announce (type = 0x03):
//   type:1      — always 0x03
//   uri_len:4   — length of URI string (net order)
//   uri:var     — UTF-8 actor URI
//   ttl:4       — remaining hop count (net order)

inline std::vector<uint8_t> pack_actor_message(const actor_uri& target_uri,
                                                uint64_t msg_type_hash,
                                                const void* payload,
                                                size_t payload_len) {
    serializer s;
    s.write_type_byte(message_type::actor_message);
    s.write_string(target_uri.to_string());
    s.write_u64(msg_type_hash);
    s.write_u32(static_cast<uint32_t>(payload_len));
    s.write_bytes(payload, payload_len);
    return s.consume();
}

inline bool unpack_actor_message(const uint8_t* data, size_t len,
                                  actor_uri& out_uri,
                                  uint64_t& out_msg_type,
                                  std::vector<uint8_t>& out_payload) {
    serializer s(std::vector<uint8_t>(data, data + len));

    message_type t = s.read_type_byte();
    if (t != message_type::actor_message) {
        return false;
    }

    std::string uri_str = s.read_string();
    // Simple parse: "ultra://node/type/name"
    // For full parsing we rely on actor_uri::parse() — for now,
    // store as a local URI.
    auto first_slash = uri_str.find('/', 8);  // skip "ultra://"
    auto second_slash = uri_str.find('/', first_slash + 1);
    if (first_slash != std::string::npos && second_slash != std::string::npos) {
        out_uri.node = uri_str.substr(8, first_slash - 8);
        out_uri.type = uri_str.substr(first_slash + 1, second_slash - first_slash - 1);
        out_uri.name = uri_str.substr(second_slash + 1);
    } else {
        out_uri.type = uri_str;
        out_uri.node = "*";
        out_uri.name = "";
    }

    out_msg_type = s.read_u64();
    uint32_t payload_len = s.read_u32();
    if (payload_len == 0) {
        return false;
    }
    if (s.offset() + payload_len > len) {
        return false;
    }

    out_payload.assign(data + s.offset(), data + s.offset() + payload_len);
    return true;
}

// ── Actor location announce ─────────────────────────────────────────────

inline std::vector<uint8_t> pack_actor_location(const actor_uri& uri,
                                                 uint32_t ttl = 10) {
    serializer s;
    s.write_type_byte(message_type::actor_location);
    s.write_string(uri.to_string());
    s.write_u32(ttl);
    return s.consume();
}

inline bool unpack_actor_location(const uint8_t* data, size_t len,
                                   actor_uri& out_uri,
                                   uint32_t& out_ttl) {
    serializer s(std::vector<uint8_t>(data, data + len));

    message_type t = s.read_type_byte();
    if (t != message_type::actor_location) {
        return false;
    }

    std::string uri_str = s.read_string();
    auto first_slash = uri_str.find('/', 8);
    auto second_slash = uri_str.find('/', first_slash + 1);
    if (first_slash != std::string::npos && second_slash != std::string::npos) {
        out_uri.node = uri_str.substr(8, first_slash - 8);
        out_uri.type = uri_str.substr(first_slash + 1, second_slash - first_slash - 1);
        out_uri.name = uri_str.substr(second_slash + 1);
    } else {
        out_uri.type = uri_str;
        out_uri.node = "*";
        out_uri.name = "";
    }
    out_ttl = s.read_u32();
    return true;
}

} // namespace ynet::actor::dist

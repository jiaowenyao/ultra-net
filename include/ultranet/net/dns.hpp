#pragma once

#include "ultranet/io/io_awaitable.hpp"
#include "ultranet/coroutine/task.hpp"
#include <vector>
#include <string>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <fstream>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace ynet::async::io {

namespace detail {

struct DnsHeader {
    uint16_t id;
    uint16_t flags;
    uint16_t qdcount;
    uint16_t ancount;
    uint16_t nscount;
    uint16_t arcount;

    void encode(uint8_t* buf) const {
        buf[0] = static_cast<uint8_t>(id >> 8);
        buf[1] = static_cast<uint8_t>(id & 0xff);
        buf[2] = static_cast<uint8_t>(flags >> 8);
        buf[3] = static_cast<uint8_t>(flags & 0xff);
        buf[4] = static_cast<uint8_t>((qdcount >> 8) & 0xff);
        buf[5] = static_cast<uint8_t>(qdcount & 0xff);
        buf[6] = static_cast<uint8_t>((ancount >> 8) & 0xff);
        buf[7] = static_cast<uint8_t>(ancount & 0xff);
        buf[8] = static_cast<uint8_t>((nscount >> 8) & 0xff);
        buf[9] = static_cast<uint8_t>(nscount & 0xff);
        buf[10] = static_cast<uint8_t>((arcount >> 8) & 0xff);
        buf[11] = static_cast<uint8_t>(arcount & 0xff);
    }

    void decode(const uint8_t* buf) {
        id      = (buf[0] << 8) | buf[1];
        flags   = (buf[2] << 8) | buf[3];
        qdcount = (buf[4] << 8) | buf[5];
        ancount = (buf[6] << 8) | buf[7];
        nscount = (buf[8] << 8) | buf[9];
        arcount = (buf[10] << 8) | buf[11];
    }
};

inline size_t encode_name(uint8_t* dst, const std::string& name) {
    size_t pos = 0;
    size_t start = 0;
    while (start < name.size()) {
        size_t dot = name.find('.', start);
        if (dot == std::string::npos) dot = name.size();
        size_t len = dot - start;
        if (len > 63) len = 63;
        dst[pos++] = static_cast<uint8_t>(len);
        std::memcpy(dst + pos, name.data() + start, len);
        pos += len;
        start = dot + 1;
    }
    dst[pos++] = 0;
    return pos;
}

inline std::string decode_name(const uint8_t* buf, size_t buflen, size_t& offset) {
    std::string name;
    bool jumped = false;
    size_t orig_offset = offset;

    for (;;) {
        if (offset >= buflen) break;
        uint8_t len = buf[offset];
        if (len == 0) {
            if (!jumped) ++offset;
            break;
        }
        if ((len & 0xc0) == 0xc0) {
            if (offset + 1 >= buflen) break;
            uint16_t ptr = ((len & 0x3f) << 8) | buf[offset + 1];
            if (!jumped) {
                orig_offset = offset + 2;
                jumped = true;
            }
            offset = ptr;
            continue;
        }
        if (offset + 1 + len > buflen) break;
        if (!name.empty()) name += '.';
        name.append(reinterpret_cast<const char*>(buf + offset + 1), len);
        offset += 1 + len;
    }

    if (jumped) offset = orig_offset;
    return name;
}

inline std::vector<std::string> read_nameservers() {
    std::vector<std::string> servers;
    std::ifstream f("/etc/resolv.conf");
    if (f.is_open()) {
        std::string line;
        while (std::getline(f, line)) {
            if (line.rfind("nameserver", 0) == 0) {
                size_t pos = line.find_first_not_of(" \t", 10);
                if (pos != std::string::npos) {
                    servers.push_back(line.substr(pos));
                }
            }
        }
    }
    if (servers.empty()) {
        servers.push_back("8.8.8.8");
        servers.push_back("1.1.1.1");
    }
    // Always prefer public DNS as fallback
    servers.push_back("8.8.8.8");
    return servers;
}

} // namespace detail

inline ynet::async::Task<std::vector<std::string>> resolve_host(
    const std::string& hostname,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {

    std::vector<std::string> results;
    auto nameservers = detail::read_nameservers();
    auto per_server_timeout = timeout / std::max(size_t(1), nameservers.size());

    for (const auto& ns_ip : nameservers) {
        auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
        if (!sock) continue;
        int fd = *sock;

        sockaddr_in ns_addr{};
        ns_addr.sin_family = AF_INET;
        ns_addr.sin_port = htons(53);
        inet_pton(AF_INET, ns_ip.c_str(), &ns_addr.sin_addr);

        auto conn = co_await Connect(fd, (sockaddr*)&ns_addr, sizeof(ns_addr));
        if (!conn) { co_await Close(fd); continue; }

        uint8_t query[512] = {};
        detail::DnsHeader hdr{};
        hdr.id = 0x1a2b;
        hdr.flags = 0x0100;
        hdr.qdcount = 1;
        hdr.encode(query);

        size_t pos = 12;
        pos += detail::encode_name(query + pos, hostname);
        query[pos++] = 0; query[pos++] = 1;
        query[pos++] = 0; query[pos++] = 1;

        auto wrote = co_await Write(fd, query, pos);
        if (!wrote || *wrote != pos) { co_await Close(fd); continue; }

        uint8_t response[512] = {};
        Read reader(fd, response, sizeof(response));
        reader.with_timeout(per_server_timeout);
        auto data = co_await reader;

        co_await Close(fd);

        if (!data || *data < 12) continue;

        size_t rlen = *data;
        detail::DnsHeader rhdr;
        rhdr.decode(response);
        uint16_t ancount = rhdr.ancount;
        if (ancount == 0) continue;

        size_t off = 12;
        detail::decode_name(response, rlen, off);
        off += 4;

        for (uint16_t i = 0; i < ancount && off + 12 <= rlen; ++i) {
            detail::decode_name(response, rlen, off);
            if (off + 10 > rlen) break;
            uint16_t rtype = (response[off] << 8) | response[off + 1];
            off += 2;
            off += 2;
            off += 4;
            uint16_t rdlen = (response[off] << 8) | response[off + 1];
            off += 2;
            if (off + rdlen > rlen) break;

            if (rtype == 1 && rdlen == 4) {
                char ip[INET_ADDRSTRLEN];
                inet_ntop(AF_INET, response + off, ip, sizeof(ip));
                results.push_back(ip);
            }
            off += rdlen;
        }

        if (!results.empty()) break;
    }

    co_return results;
}

struct SrvRecord {
    uint16_t priority;
    uint16_t weight;
    uint16_t port;
    std::string target;
};

inline ynet::async::Task<std::vector<SrvRecord>> resolve_srv(
    const std::string& service,
    const std::string& protocol,
    const std::string& domain,
    std::chrono::milliseconds timeout = std::chrono::milliseconds(5000))
{
    std::vector<SrvRecord> results;
    auto nameservers = detail::read_nameservers();
    auto per_server_timeout = timeout / std::max(size_t(1), nameservers.size());

    std::string qname = (service[0] == '_' ? service : "_" + service) + "."
        + (protocol[0] == '_' ? protocol : "_" + protocol) + "."
        + domain;

    for (const auto& ns_ip : nameservers) {
        auto sock = co_await Socket(AF_INET, SOCK_DGRAM, 0);
        if (!sock) continue;
        int fd = *sock;

        sockaddr_in ns_addr{};
        ns_addr.sin_family = AF_INET;
        ns_addr.sin_port = htons(53);
        inet_pton(AF_INET, ns_ip.c_str(), &ns_addr.sin_addr);

        auto conn = co_await Connect(fd, (sockaddr*)&ns_addr, sizeof(ns_addr));
        if (!conn) { co_await Close(fd); continue; }

        uint8_t query[512] = {};
        detail::DnsHeader hdr{};
        hdr.id = 0x1a2b;
        hdr.flags = 0x0100;
        hdr.qdcount = 1;
        hdr.encode(query);

        size_t pos = 12;
        pos += detail::encode_name(query + pos, qname);
        query[pos++] = 0; query[pos++] = 33;  // QTYPE=SRV
        query[pos++] = 0; query[pos++] = 1;   // QCLASS=IN

        auto wrote = co_await Write(fd, query, pos);
        if (!wrote || *wrote != pos) { co_await Close(fd); continue; }

        uint8_t response[512] = {};
        Read reader(fd, response, sizeof(response));
        reader.with_timeout(per_server_timeout);
        auto data = co_await reader;
        co_await Close(fd);

        if (!data || *data < 12) continue;

        size_t rlen = *data;
        detail::DnsHeader rhdr;
        rhdr.decode(response);
        uint16_t ancount = rhdr.ancount;
        if (ancount == 0) continue;

        size_t off = 12;
        detail::decode_name(response, rlen, off);
        off += 4; // skip QTYPE + QCLASS

        for (uint16_t i = 0; i < ancount && off + 10 <= rlen; ++i) {
            detail::decode_name(response, rlen, off);
            if (off + 10 > rlen) break;
            uint16_t rtype = static_cast<uint16_t>(response[off]) << 8 | response[off + 1];
            off += 2; // type
            off += 2; // class
            off += 4; // ttl
            uint16_t rdlen = static_cast<uint16_t>(response[off]) << 8 | response[off + 1];
            off += 2;
            if (off + rdlen > rlen) break;

            if (rtype == 33 && rdlen >= 6) { // SRV
                SrvRecord rec;
                rec.priority = static_cast<uint16_t>(response[off]) << 8 | response[off + 1];
                rec.weight   = static_cast<uint16_t>(response[off + 2]) << 8 | response[off + 3];
                rec.port     = static_cast<uint16_t>(response[off + 4]) << 8 | response[off + 5];
                size_t name_start = off + 6;
                size_t name_off = name_start;
                rec.target = detail::decode_name(response, rlen, name_off);
                results.push_back(std::move(rec));
            }
            off += rdlen;
        }

        if (!results.empty()) break;
    }

    co_return results;
}

} // namespace ynet::async::io

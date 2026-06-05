// RESP (Redis Serialization Protocol) parser.
//
// Handles both request parsing (client→proxy) and response boundary detection
// (backend→proxy).  RESP is a simple text-based protocol:
//
//   Request:  *<argc>\r\n$<len>\r\n<arg>\r\n...
//   Response: +<str>\r\n  |  -<err>\r\n  |  :<int>\r\n
//             $<len>\r\n<data>\r\n  |  *<count>\r\n...
//
// Design: zero-copy where possible — parsed arguments are string_views into
// the original buffer.  The caller owns the buffer; views are valid as long
// as the buffer is not overwritten.

#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <cstdint>
#include <cstring>
#include <algorithm>

namespace redis_proxy {

struct RespRequest {
    std::vector<std::string_view> args; // parsed arguments
    size_t consumed = 0;                // bytes consumed from buffer
};

// Parse a client request from raw buffer data.
// Returns a RespRequest with args and consumed byte count.
// consumed=0 means incomplete (need more data).
// Supports both RESP format (*<n>\r\n...) and INLINE format (CMD arg...\r\n).
inline RespRequest parse_request(const uint8_t* data, size_t len) {
    RespRequest req;
    if (len == 0) return req;

    const char* p = reinterpret_cast<const char*>(data);
    const char* end = p + len;

    if (*p == '*') {
        // RESP mode: *<argc>\r\n$<len>\r\n<arg>\r\n...
        p++;
        if (p >= end) { req.consumed = 0; return req; }

        int64_t argc = 0;
        while (p < end && *p >= '0' && *p <= '9') {
            argc = argc * 10 + (*p - '0');
            p++;
        }
        if (p >= end || *p != '\r') { req.consumed = 0; return req; }
        p++;
        if (p >= end || *p != '\n') { req.consumed = 0; return req; }
        p++;

        for (int64_t i = 0; i < argc; i++) {
            if (p >= end || *p != '$') { req.consumed = 0; return req; }
            p++;
            if (p >= end) { req.consumed = 0; return req; }

            int64_t slen = 0;
            while (p < end && *p >= '0' && *p <= '9') {
                slen = slen * 10 + (*p - '0');
                p++;
            }
            if (p >= end || *p != '\r') { req.consumed = 0; return req; }
            p++;
            if (p >= end || *p != '\n') { req.consumed = 0; return req; }
            p++;

            if (p + slen + 2 > end) { req.consumed = 0; return req; }
            req.args.push_back(std::string_view(p, slen));
            p += slen;

            if (*p != '\r') { req.consumed = 0; return req; }
            p++;
            if (p >= end || *p != '\n') { req.consumed = 0; return req; }
            p++;
        }
        req.consumed = static_cast<size_t>(p - reinterpret_cast<const char*>(data));
        return req;
    }

    // INLINE mode: <CMD> [arg1 arg2 ...]\r\n
    // Find the \r\n terminator
    const char* cr = static_cast<const char*>(std::memchr(p, '\r', end - p));
    if (!cr || cr + 1 >= end || *(cr + 1) != '\n') {
        req.consumed = 0;
        return req;
    }

    // Parse space-separated arguments
    const char* line_end = cr;
    while (p < line_end) {
        // Skip leading spaces
        while (p < line_end && *p == ' ') p++;
        if (p >= line_end) break;

        const char* arg_start = p;
        // Find end of argument (space or end of line)
        while (p < line_end && *p != ' ') p++;
        req.args.push_back(std::string_view(arg_start, p - arg_start));
    }

    req.consumed = static_cast<size_t>(cr + 2 - reinterpret_cast<const char*>(data));
    return req;
}

// Detect if the buffer contains a complete RESP response from a backend.
// Returns the number of bytes consumed (the complete response), or 0 if
// more data is needed.  This handles all 5 RESP response types and nested
// arrays recursively.
inline size_t detect_response(const uint8_t* data, size_t len) {
    if (len == 0) return 0;
    const char* p = reinterpret_cast<const char*>(data);
    const char* end = p + len;

    char type = *p;
    p++; // skip type byte

    switch (type) {
    case '+': // Simple String
    case '-': // Error
    case ':': // Integer
        // Read until \r\n
        while (p < end) {
            if (*p == '\r' && p + 1 < end && *(p + 1) == '\n') {
                return static_cast<size_t>(p + 2 - reinterpret_cast<const char*>(data));
            }
            p++;
        }
        return 0; // incomplete

    case '$': { // Bulk String
        // Parse length
        if (p >= end) return 0;
        int64_t slen = 0;
        bool negative = false;
        if (*p == '-') { negative = true; p++; }
        while (p < end && *p >= '0' && *p <= '9') {
            slen = slen * 10 + (*p - '0');
            p++;
        }
        if (p + 1 >= end || *p != '\r' || *(p + 1) != '\n') return 0;
        p += 2;
        if (negative) return static_cast<size_t>(p - reinterpret_cast<const char*>(data)); // $-1\r\n
        // Check if data + trailing \r\n is available
        if (p + slen + 2 > end) return 0;
        return static_cast<size_t>(p + slen + 2 - reinterpret_cast<const char*>(data));
    }

    case '*': { // Array
        // Parse element count
        if (p >= end) return 0;
        int64_t count = 0;
        bool negative = false;
        if (*p == '-') { negative = true; p++; }
        while (p < end && *p >= '0' && *p <= '9') {
            count = count * 10 + (*p - '0');
            p++;
        }
        if (p + 1 >= end || *p != '\r' || *(p + 1) != '\n') return 0;
        p += 2;
        if (negative) return static_cast<size_t>(p - reinterpret_cast<const char*>(data)); // *-1\r\n

        // Recursively detect each array element
        size_t offset = static_cast<size_t>(p - reinterpret_cast<const char*>(data));
        for (int64_t i = 0; i < count; i++) {
            size_t consumed = detect_response(data + offset, len - offset);
            if (consumed == 0) return 0;
            offset += consumed;
        }
        return offset;
    }

    default:
        return 0; // unknown type
    }
}

// Encode a list of arguments into a RESP request string.
// Used when forwarding INLINE commands to the backend in RESP format.
inline std::string encode_request(const std::vector<std::string_view>& args) {
    std::string out;
    out.reserve(64);
    out += '*';
    out += std::to_string(args.size());
    out += "\r\n";
    for (const auto& arg : args) {
        out += '$';
        out += std::to_string(arg.size());
        out += "\r\n";
        out += arg;
        out += "\r\n";
    }
    return out;
}

// Fast boundary detection for RESP requests — returns the byte length of
// one complete RESP command without parsing arguments or allocating memory.
// Used for the hot path where we forward raw bytes without inspection.
// Returns 0 if the buffer contains an incomplete command.
inline size_t detect_request_boundary(const uint8_t* data, size_t len) {
    if (len < 4) return 0; // minimum: "*1\r\n..."
    const char* p = reinterpret_cast<const char*>(data);
    const char* end = p + len;

    if (*p != '*') {
        // INLINE mode: scan for \r\n
        const char* cr = static_cast<const char*>(std::memchr(p, '\r', end - p));
        if (!cr || cr + 1 >= end || *(cr + 1) != '\n') return 0;
        return static_cast<size_t>(cr + 2 - p);
    }

    p++; // skip '*'
    // Parse argument count
    int64_t argc = 0;
    while (p < end && *p >= '0' && *p <= '9') { argc = argc * 10 + (*p - '0'); p++; }
    if (p + 1 >= end || *p != '\r' || *(p + 1) != '\n') return 0;
    p += 2;

    // Skip each bulk string: $<len>\r\n<data>\r\n
    for (int64_t i = 0; i < argc; i++) {
        if (p >= end || *p != '$') return 0;
        p++;
        int64_t slen = 0;
        while (p < end && *p >= '0' && *p <= '9') { slen = slen * 10 + (*p - '0'); p++; }
        if (p + 1 >= end || *p != '\r' || *(p + 1) != '\n') return 0;
        p += 2;
        if (p + slen + 2 > end) return 0;
        p += slen + 2; // data + \r\n
    }
    return static_cast<size_t>(p - reinterpret_cast<const char*>(data));
}

} // namespace redis_proxy

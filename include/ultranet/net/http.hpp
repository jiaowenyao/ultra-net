#pragma once

#include "ultranet/net/tcp_socket.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include <string>
#include <vector>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <optional>
#include <cerrno>
#include <climits>
#include <strings.h>

namespace ynet::async::net::http {

// Exception-free string-to-integer conversion.
// std::stoll throws on malformed input — uncaught in coroutine context
// this terminates the process.  safe_stoll wraps strtoll instead.
inline std::optional<int64_t> safe_stoll(std::string_view sv) {
    if (sv.empty()) return std::nullopt;
    // strtoll requires null-termination; string_view is not guaranteed.
    // Copy to a small stack buffer for the common case.
    char buf[32];
    size_t len = sv.size() < sizeof(buf) - 1 ? sv.size() : sizeof(buf) - 1;
    std::memcpy(buf, sv.data(), len);
    buf[len] = '\0';
    char* end = nullptr;
    errno = 0;
    int64_t val = strtoll(buf, &end, 10);
    if (errno == ERANGE || end == buf || *end != '\0') return std::nullopt;
    return val;
}

enum class Method : uint8_t {
    GET = 0, POST, PUT, DELETE_,
    HEAD, PATCH, OPTIONS, CONNECT, TRACE
};

inline std::string_view method_string(Method m) {
    switch (m) {
        case Method::GET:     return "GET";
        case Method::POST:    return "POST";
        case Method::PUT:     return "PUT";
        case Method::DELETE_: return "DELETE";
        case Method::HEAD:    return "HEAD";
        case Method::PATCH:   return "PATCH";
        case Method::OPTIONS: return "OPTIONS";
        case Method::CONNECT: return "CONNECT";
        case Method::TRACE:   return "TRACE";
    }
    return "GET";
}

struct Header {
    std::string name;
    std::string value;
};

struct HttpRequest {
    Method method = Method::GET;
    std::string path = "/";
    std::string http_version = "HTTP/1.1";
    std::vector<Header> headers;
    std::string body;

    std::string_view header(std::string_view name) const {
        for (const auto& h : headers) {
            if (h.name.size() == name.size()) {
                bool match = true;
                for (size_t i = 0; i < name.size(); ++i) {
                    if (std::tolower(static_cast<unsigned char>(h.name[i]))
                        != std::tolower(static_cast<unsigned char>(name[i]))) {
                        match = false;
                        break;
                    }
                }
                if (match) return h.value;
            }
        }
        return {};
    }

    std::string serialize() const {
        std::string result;
        result += method_string(method);
        result += " ";
        result += path;
        result += " ";
        result += http_version;
        result += "\r\n";

        for (const auto& h : headers) {
            result += h.name;
            result += ": ";
            result += h.value;
            result += "\r\n";
        }
        if (!body.empty()) {
            result += "Content-Length: ";
            result += std::to_string(body.size());
            result += "\r\n";
        }
        result += "\r\n";
        result += body;
        return result;
    }

    size_t parse(const char* data, size_t len, std::string* error = nullptr) {
        const char* end = data + len;
        const char* p = data;

        // Parse request line: METHOD PATH VERSION\r\n
        const char* line_end = static_cast<const char*>(std::memchr(p, '\r', end - p));
        if (!line_end || line_end + 1 >= end || *(line_end + 1) != '\n') return 0;
        std::string_view line(p, line_end - p);
        p = line_end + 2;

        size_t sp1 = line.find(' ');
        if (sp1 == std::string_view::npos) return 0;
        size_t sp2 = line.find(' ', sp1 + 1);
        if (sp2 == std::string_view::npos) return 0;

        std::string_view method_sv = line.substr(0, sp1);
        if (method_sv == "GET") method = Method::GET;
        else if (method_sv == "POST") method = Method::POST;
        else if (method_sv == "PUT") method = Method::PUT;
        else if (method_sv == "DELETE") method = Method::DELETE_;  // Note: DELETE_ not DELETE
        else if (method_sv == "HEAD") method = Method::HEAD;
        else method = Method::GET;

        path = line.substr(sp1 + 1, sp2 - sp1 - 1);
        http_version = line.substr(sp2 + 1);

        // Parse headers
        headers.clear();
        while (p < end) {
            if (p + 1 < end && *p == '\r' && *(p + 1) == '\n') {
                p += 2;
                break;
            }
            line_end = static_cast<const char*>(std::memchr(p, '\r', end - p));
            if (!line_end || line_end + 1 >= end || *(line_end + 1) != '\n') return 0;
            std::string_view hl(p, line_end - p);
            p = line_end + 2;

            size_t col = hl.find(':');
            if (col == std::string_view::npos) continue;
            std::string name(hl.substr(0, col));
            size_t vs = col + 1;
            while (vs < hl.size() && hl[vs] == ' ') ++vs;
            std::string value(hl.substr(vs));
            headers.push_back({std::move(name), std::move(value)});
        }

        return static_cast<size_t>(p - data);
    }
};

struct HttpResponse {
    int status_code = 200;
    std::string status_message = "OK";
    std::string http_version = "HTTP/1.1";
    std::vector<Header> headers;
    std::string body;

    std::string serialize() const {
        std::string result;
        result += http_version;
        result += " ";
        result += std::to_string(status_code);
        result += " ";
        result += status_message;
        result += "\r\n";

        for (const auto& h : headers) {
            result += h.name;
            result += ": ";
            result += h.value;
            result += "\r\n";
        }
        if (!body.empty()) {
            result += "Content-Length: ";
            result += std::to_string(body.size());
            result += "\r\n";
        }
        result += "\r\n";
        result += body;
        return result;
    }

    size_t parse(const char* data, size_t len, std::string* error = nullptr) {
        const char* end = data + len;
        const char* p = data;

        // Parse status line: HTTP/VERSION CODE MESSAGE\r\n
        const char* line_end = static_cast<const char*>(std::memchr(p, '\r', end - p));
        if (!line_end || line_end + 1 >= end || *(line_end + 1) != '\n') return 0;
        std::string_view line(p, line_end - p);
        p = line_end + 2;

        size_t sp1 = line.find(' ');
        if (sp1 == std::string_view::npos) return 0;
        size_t sp2 = line.find(' ', sp1 + 1);
        http_version = line.substr(0, sp1);
        {
            auto code_sv = line.substr(sp1 + 1, sp2 - sp1 - 1);
            auto sc = safe_stoll(code_sv);
            status_code = sc.has_value() ? static_cast<int>(*sc) : 0;
        }
        if (sp2 != std::string_view::npos) {
            status_message = line.substr(sp2 + 1);
        }

        // Parse headers
        headers.clear();
        while (p < end) {
            if (p + 1 < end && *p == '\r' && *(p + 1) == '\n') {
                p += 2;
                break;
            }
            line_end = static_cast<const char*>(std::memchr(p, '\r', end - p));
            if (!line_end || line_end + 1 >= end || *(line_end + 1) != '\n') return 0;
            std::string_view hl(p, line_end - p);
            p = line_end + 2;

            size_t col = hl.find(':');
            if (col == std::string_view::npos) continue;
            std::string name(hl.substr(0, col));
            size_t vs = col + 1;
            while (vs < hl.size() && hl[vs] == ' ') ++vs;
            std::string value(hl.substr(vs));
            headers.push_back({std::move(name), std::move(value)});
        }

        // Parse body based on Content-Length
        int64_t content_length = -1;
        for (const auto& h : headers) {
            std::string hl = h.name;
            std::transform(hl.begin(), hl.end(), hl.begin(),
                [](unsigned char c) { return std::tolower(c); });
            if (hl == "content-length") {
                auto cl = safe_stoll(h.value);
                content_length = cl.has_value() ? *cl : -1;
                break;
            }
        }

        if (content_length > 0) {
            if (static_cast<size_t>(end - p) < static_cast<size_t>(content_length)) return 0;
            body.assign(p, p + content_length);
            p += content_length;
        } else {
            body.clear();
        }

        return static_cast<size_t>(p - data);
    }

    std::string_view header(std::string_view name) const {
        for (const auto& h : headers) {
            if (h.name.size() == name.size()) {
                bool match = true;
                for (size_t i = 0; i < name.size(); ++i) {
                    if (std::tolower(static_cast<unsigned char>(h.name[i]))
                        != std::tolower(static_cast<unsigned char>(name[i]))) {
                        match = false;
                        break;
                    }
                }
                if (match) return h.value;
            }
        }
        return {};
    }

    bool is_keepalive() const {
        auto v = header("connection");
        // Zero-allocation case-insensitive substring search for "keep-alive".
        for (size_t i = 0; i + 10 <= v.size(); ++i) {
            if (strncasecmp(v.data() + i, "keep-alive", 10) == 0) return true;
        }
        return false;
    }
};

class HttpClient : ynet::utils::Noncopyable {
public:
    explicit HttpClient(TcpSocket socket, std::string host_header = "")
        : m_socket(std::move(socket)), m_host_header(std::move(host_header)) {}

    Task<HttpResponse> send(HttpRequest request) {
        if (!m_host_header.empty() && request.header("host").empty()) {
            request.headers.push_back({"Host", m_host_header});
        }
        if (request.header("connection").empty()) {
            request.headers.push_back({"Connection", "close"});
        }

        auto data = request.serialize();
        size_t total = 0;
        while (total < data.size()) {
            auto w = co_await m_socket.write(data.data() + total, data.size() - total);
            if (!w) {
                throw std::system_error(w.error(), "HTTP write failed");
            }
            total += *w;
        }

        // Read response
        std::string buf;
        buf.resize(4096);
        HttpResponse resp;

        for (;;) {
            auto r = co_await m_socket.read(buf.data() + (buf.size() - 4096), 4096);
            if (!r) {
                throw std::system_error(r.error(), "HTTP read failed");
            }
            if (*r == 0) break;

            size_t consumed = resp.parse(buf.data(), buf.size() - 4096 + *r);
            if (consumed > 0) break;
            buf.resize(buf.size() + 4096);
        }

        co_return resp;
    }

    Task<HttpResponse> get(std::string_view path) {
        HttpRequest req;
        req.method = Method::GET;
        req.path = path;
        co_return co_await send(std::move(req));
    }

    Task<HttpResponse> post(std::string_view path,
                            std::string body = {},
                            std::string_view content_type = "text/plain") {
        HttpRequest req;
        req.method = Method::POST;
        req.path = path;
        req.body = std::move(body);
        req.headers.push_back({"Content-Type", std::string(content_type)});
        co_return co_await send(std::move(req));
    }

    TcpSocket& socket() noexcept { return m_socket; }

private:
    TcpSocket m_socket;
    std::string m_host_header;
};

} // namespace ynet::async::net::http

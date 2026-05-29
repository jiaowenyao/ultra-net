#include "ultranet/ultranet.h"
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::net::http;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

void test_request_serialize() {
    T("request serialize");
    HttpRequest req;
    req.method = Method::GET;
    req.path = "/index.html";
    auto s = req.serialize();
    CHECK(s.find("GET /index.html HTTP/1.1\r\n") != std::string::npos, "request line");
    PASS();
}

void test_response_parse() {
    T("response parse");
    std::string raw =
        "HTTP/1.1 200 OK\r\n"
        "Content-Length: 13\r\n"
        "Content-Type: text/plain\r\n"
        "\r\n"
        "Hello, World!";

    HttpResponse resp;
    size_t consumed = resp.parse(raw.data(), raw.size());
    CHECK(consumed == raw.size(), "fully consumed");
    CHECK(resp.status_code == 200, "status 200");
    CHECK(resp.body == "Hello, World!", "body matches");
    CHECK(resp.header("content-type") == "text/plain", "header lookup");
    PASS();
}

void test_header_case_insensitive() {
    T("header lookup case insensitive");
    std::string raw =
        "HTTP/1.1 200 OK\r\n"
        "X-Custom-Header: value123\r\n"
        "\r\n";

    HttpResponse resp;
    resp.parse(raw.data(), raw.size());
    CHECK(resp.header("x-custom-header") == "value123", "lowercase lookup");
    CHECK(resp.header("X-CUSTOM-HEADER") == "value123", "uppercase lookup");
    PASS();
}

void test_incomplete_returns_zero() {
    T("incomplete buffer returns 0");
    std::string raw = "HTTP/1.1 200 OK\r\nConte"; // incomplete
    HttpResponse resp;
    size_t consumed = resp.parse(raw.data(), raw.size());
    CHECK(consumed == 0, "returns 0 for incomplete");
    PASS();
}

void test_response_serialize() {
    T("response serialize");
    HttpResponse resp;
    resp.status_code = 404;
    resp.status_message = "Not Found";
    resp.body = "not found";
    auto s = resp.serialize();
    CHECK(s.find("HTTP/1.1 404 Not Found") != std::string::npos, "status line");
    CHECK(s.find("not found") != std::string::npos, "body present");
    PASS();
}

void test_method_string() {
    T("method string conversion");
    CHECK(method_string(Method::GET) == "GET", "GET");
    CHECK(method_string(Method::POST) == "POST", "POST");
    CHECK(method_string(Method::PUT) == "PUT", "PUT");
    PASS();
}

int main() {
    std::cout << "=== HTTP Tests ===" << std::endl;
    test_request_serialize();
    test_response_parse();
    test_header_case_insensitive();
    test_incomplete_returns_zero();
    test_response_serialize();
    test_method_string();

    std::cout << "\n=== HTTP Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

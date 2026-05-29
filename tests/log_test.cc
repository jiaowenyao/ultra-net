#include "ultranet/ultranet.h"
#include <iostream>
#include <sstream>
#include <vector>
#include <mutex>

using namespace ynet::log;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)

class MockLogger : public ILogger {
public:
    void log(Level level, const std::string& msg) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_records.push_back({level, msg});
    }
    void flush() override {}
    void set_level(Level lvl) override { m_level = lvl; }
    Level level() const override { return m_level; }

    struct Record { Level level; std::string msg; };
    std::vector<Record> records() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_records;
    }
    void clear() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_records.clear();
    }

private:
    Level m_level{Level::Info};
    mutable std::mutex m_mutex;
    std::vector<Record> m_records;
};

void test_mock_logger_interface() {
    T("mock logger implements ILogger");
    auto mock = std::make_shared<MockLogger>();
    mock->set_level(Level::Debug);
    CHECK(mock->level() == Level::Debug, "level not set");
    PASS();
}

void test_mock_basic_logging() {
    T("mock logger basic logging");
    auto mock = std::make_shared<MockLogger>();
    auto old = get_logger();
    set_logger(mock);

    ULTRA_LOG_INFO("test message {}", 42);
    auto records = mock->records();
    CHECK(records.size() == 1, "should have 1 record");
    CHECK(records[0].level == Level::Info, "level should be Info");
    CHECK(records[0].msg.find("test message 42") != std::string::npos, "message content");

    set_logger(old);
    PASS();
}

void test_mock_all_levels() {
    T("mock logger all log levels");
    auto mock = std::make_shared<MockLogger>();
    mock->set_level(Level::Trace);
    auto old = get_logger();
    set_logger(mock);

    ULTRA_LOG_TRACE("trace");
    ULTRA_LOG_DEBUG("debug");
    ULTRA_LOG_INFO("info");
    ULTRA_LOG_WARN("warn");
    ULTRA_LOG_ERROR("error");
    ULTRA_LOG_CRITICAL("critical");

    auto records = mock->records();
    CHECK(records.size() == 6, "should have 6 records");
    CHECK(records[0].level == Level::Trace, "trace");
    CHECK(records[2].level == Level::Info, "info");
    CHECK(records[5].level == Level::Critical, "critical");

    set_logger(old);
    PASS();
}

void test_logger_switching() {
    T("logger switching (pluggable)");
    auto mock1 = std::make_shared<MockLogger>();
    auto mock2 = std::make_shared<MockLogger>();
    auto old = get_logger();

    set_logger(mock1);
    ULTRA_LOG_INFO("to mock1");
    CHECK(mock1->records().size() == 1, "mock1 received");
    CHECK(mock2->records().empty(), "mock2 empty");

    set_logger(mock2);
    ULTRA_LOG_INFO("to mock2");
    CHECK(mock1->records().size() == 1, "mock1 unchanged");
    CHECK(mock2->records().size() == 1, "mock2 received");

    set_logger(old);
    PASS();
}

#ifdef ULTRANET_HAS_SPDLOG
void test_spdlog_logger_creation() {
    T("spdlog logger creation");
    try {
        SpdlogLogger logger("test-spdlog");
        logger.set_level(Level::Info);
        CHECK(logger.level() == Level::Info, "spdlog level");
        logger.info("test");
        logger.flush();
        PASS();
    } catch (const std::exception& e) {
        FAIL(std::string("exception: ") + e.what());
    }
}
#endif

int main() {
    std::cout << "=== Log Tests ===" << std::endl;
    test_mock_logger_interface();
    test_mock_basic_logging();
    test_mock_all_levels();
    test_logger_switching();
#ifdef ULTRANET_HAS_SPDLOG
    test_spdlog_logger_creation();
#endif

    std::cout << "\n=== Log Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

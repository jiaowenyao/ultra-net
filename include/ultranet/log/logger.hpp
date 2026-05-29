#pragma once

#include <string>
#include <memory>
#include <mutex>
#include <format>

#ifdef ULTRANET_HAS_SPDLOG
#include <spdlog/spdlog.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/async.h>
#endif

namespace ynet::log {

enum class Level { Trace, Debug, Info, Warn, Error, Critical };

// 可插拔的日志接口 — 切换日志库只需实现此接口
class ILogger {
public:
    virtual ~ILogger() = default;
    virtual void log(Level level, const std::string& msg) = 0;
    virtual void flush() = 0;
    virtual void set_level(Level level) = 0;
    virtual Level level() const = 0;

    void trace(const std::string& msg)    { log(Level::Trace, msg); }
    void debug(const std::string& msg)    { log(Level::Debug, msg); }
    void info(const std::string& msg)     { log(Level::Info, msg); }
    void warn(const std::string& msg)     { log(Level::Warn, msg); }
    void error(const std::string& msg)    { log(Level::Error, msg); }
    void critical(const std::string& msg) { log(Level::Critical, msg); }
};

// 内置的 stdout 日志实现 — 无外部依赖，始终可用
class StdoutLogger : public ILogger {
public:
    void log(Level level, const std::string& msg) override {
        const char* prefix = "";
        switch (level) {
            case Level::Trace:    prefix = "[TRACE] "; break;
            case Level::Debug:    prefix = "[DEBUG] "; break;
            case Level::Info:     prefix = "[INFO]  "; break;
            case Level::Warn:     prefix = "[WARN]  "; break;
            case Level::Error:    prefix = "[ERROR] "; break;
            case Level::Critical: prefix = "[FATAL] "; break;
        }
        fprintf(stderr, "%s%s\n", prefix, msg.c_str());
    }
    void flush() override { fflush(stderr); }
    void set_level(Level lvl) override { m_level = lvl; }
    Level level() const override { return m_level; }

private:
    Level m_level{Level::Info};
};

#ifdef ULTRANET_HAS_SPDLOG

inline spdlog::level::level_enum to_spdlog(Level level) {
    switch (level) {
        case Level::Trace:    return spdlog::level::trace;
        case Level::Debug:    return spdlog::level::debug;
        case Level::Info:     return spdlog::level::info;
        case Level::Warn:     return spdlog::level::warn;
        case Level::Error:    return spdlog::level::err;
        case Level::Critical: return spdlog::level::critical;
    }
    return spdlog::level::info;
}

// spdlog 异步日志实现
class SpdlogLogger : public ILogger {
public:
    explicit SpdlogLogger(const std::string& name = "ultranet") {
        static std::once_flag s_init_flag;
        std::call_once(s_init_flag, [] {
            spdlog::init_thread_pool(8192, 2);
        });

        auto sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        m_logger = std::make_shared<spdlog::async_logger>(
            name, std::move(sink), spdlog::thread_pool(),
            spdlog::async_overflow_policy::block);
        m_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] [%t] %v");
        spdlog::register_logger(m_logger);
    }

    void log(Level level, const std::string& msg) override {
        m_logger->log(to_spdlog(level), msg);
    }

    void flush() override { m_logger->flush(); }
    void set_level(Level lvl) override { m_logger->set_level(to_spdlog(lvl)); }
    Level level() const override {
        switch (m_logger->level()) {
            case spdlog::level::trace:    return Level::Trace;
            case spdlog::level::debug:    return Level::Debug;
            case spdlog::level::info:     return Level::Info;
            case spdlog::level::warn:     return Level::Warn;
            case spdlog::level::err:      return Level::Error;
            case spdlog::level::critical: return Level::Critical;
            default: return Level::Info;
        }
    }

private:
    std::shared_ptr<spdlog::logger> m_logger;
};

#endif // ULTRANET_HAS_SPDLOG

// 全局日志实例的延迟初始化访问器
inline std::shared_ptr<ILogger>& get_logger() {
#ifdef ULTRANET_HAS_SPDLOG
    static std::shared_ptr<ILogger> s_logger = std::make_shared<SpdlogLogger>();
#else
    static std::shared_ptr<ILogger> s_logger = std::make_shared<StdoutLogger>();
#endif
    return s_logger;
}

inline void set_logger(std::shared_ptr<ILogger> logger) {
    if (logger) get_logger() = std::move(logger);
}

} // namespace ynet::log

// 便捷宏
#define ULTRA_LOG_TRACE(fmt, ...) \
    ynet::log::get_logger()->trace(std::format(fmt __VA_OPT__(,) __VA_ARGS__))
#define ULTRA_LOG_DEBUG(fmt, ...) \
    ynet::log::get_logger()->debug(std::format(fmt __VA_OPT__(,) __VA_ARGS__))
#define ULTRA_LOG_INFO(fmt, ...) \
    ynet::log::get_logger()->info(std::format(fmt __VA_OPT__(,) __VA_ARGS__))
#define ULTRA_LOG_WARN(fmt, ...) \
    ynet::log::get_logger()->warn(std::format(fmt __VA_OPT__(,) __VA_ARGS__))
#define ULTRA_LOG_ERROR(fmt, ...) \
    ynet::log::get_logger()->error(std::format(fmt __VA_OPT__(,) __VA_ARGS__))
#define ULTRA_LOG_CRITICAL(fmt, ...) \
    ynet::log::get_logger()->critical(std::format(fmt __VA_OPT__(,) __VA_ARGS__))

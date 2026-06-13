#pragma once

#include <cgx/reactor/clock.hpp>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <type_traits>

namespace cgx::reactor {

// Forward declaration (defined in config.hpp)
// Needed for default template arguments in log::info / debug / warn / error.
struct default_config;

// ---------------------------------------------------------------------------
// Log level enum (ordered — used in >= comparisons for level filtering)
// ---------------------------------------------------------------------------

enum class log_level : int {
    debug = 0,
    info  = 1,
    warn  = 2,
    error = 3
};

// ---------------------------------------------------------------------------
// Logger policy types
//
// Default: no_logger — zero-cost disabled (snprintf eliminated via
// double `if constexpr`).
//
// To enable logging, define a logger struct with:
//   static void print(const char* msg);
// and pass it as the Logger template argument to engine/make_engine, e.g.:
//   make_engine<Config, Clock, MyLogger>(...)
// ---------------------------------------------------------------------------

struct no_logger {
    static void print(const char*) noexcept {}
};

// ---------------------------------------------------------------------------
// Internal: double-if-constexpr log implementation
// ---------------------------------------------------------------------------

namespace log::detail {

// Silence -Wformat-security inside log_impl — the format string is always
// a caller-provided literal, so the warning is noise.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-security"

template <typename Config, log_level Level, typename Logger, typename Clock,
          typename... Args>
void log_impl(const char* level_str, const char* tag, const char* fmt,
              Args... args) {
    // Gate 1: level filter
    if constexpr (Level >= Config::min_level) {
        // Gate 2: logger type filter — zero-cost when Logger == no_logger
        if constexpr (!std::is_same_v<Logger, no_logger>) {
            char msg_buf[256];

            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          Clock::now().time_since_epoch())
                          .count();
            int offset = std::snprintf(
                msg_buf, sizeof(msg_buf),
                "%lld [%s] <%s> ",
                static_cast<long long>(ms), level_str, tag);
            if (offset > 0 &&
                offset < static_cast<int>(sizeof(msg_buf))) {
                std::snprintf(msg_buf + offset,
                              sizeof(msg_buf) - offset, fmt, args...);
            }
            Logger::print(msg_buf);
        }
    }
}

#pragma clang diagnostic pop

} // namespace log::detail

// ---------------------------------------------------------------------------
// User-facing log API
//
// Each function is a thin wrapper over log_impl with the appropriate level.
// Template parameters (Logger, Config, Clock) all have defaults for
// zero-cost no-logger usage when called directly.
//
// The engine itself does NOT use these — it calls log::detail::log_impl
// directly with its own Config, ClockType, and Logger template parameters.
// ---------------------------------------------------------------------------

namespace log {

template <typename Logger = no_logger, typename Config = default_config,
          typename Clock = steady_clock, typename... Args>
void info(const char* fmt, Args... args) {
    detail::log_impl<Config, log_level::info, Logger, Clock>("INF", "", fmt,
                                                             args...);
}

template <typename Logger = no_logger, typename Config = default_config,
          typename Clock = steady_clock, typename... Args>
void debug(const char* fmt, Args... args) {
    detail::log_impl<Config, log_level::debug, Logger, Clock>("DBG", "", fmt,
                                                              args...);
}

template <typename Logger = no_logger, typename Config = default_config,
          typename Clock = steady_clock, typename... Args>
void warn(const char* fmt, Args... args) {
    detail::log_impl<Config, log_level::warn, Logger, Clock>("WRN", "", fmt,
                                                             args...);
}

template <typename Logger = no_logger, typename Config = default_config,
          typename Clock = steady_clock, typename... Args>
void error(const char* fmt, Args... args) {
    detail::log_impl<Config, log_level::error, Logger, Clock>("ERR", "", fmt,
                                                              args...);
}

} // namespace log

} // namespace cgx::reactor

#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <cctype>
#include <string>
#include <vector>

namespace {

using namespace cgx::reactor;
using namespace std::chrono_literals;

// =========================================================================
// Helpers
// =========================================================================

/// Logger that captures all messages into a static vector.
struct capture_logger {
    static std::vector<std::string> messages;
    static void print(const char* msg) { messages.emplace_back(msg); }
    static void clear() { messages.clear(); }
};
std::vector<std::string> capture_logger::messages;

/// Config with min_level = debug (to allow DEBUG-level logs through).
struct debug_config {
    static constexpr std::size_t max_timers = 16;
    static constexpr std::size_t max_signal_listeners = 8;
    static constexpr std::size_t reserved_pool_size = 8192;
    static constexpr std::size_t scratchpad_pool_size = 2048;
    static constexpr log_level min_level = log_level::debug;
};

/// Config with min_level = warn (to test level filtering).
struct warn_config {
    static constexpr std::size_t max_timers = 16;
    static constexpr std::size_t max_signal_listeners = 8;
    static constexpr std::size_t reserved_pool_size = 8192;
    static constexpr std::size_t scratchpad_pool_size = 2048;
    static constexpr log_level min_level = log_level::warn;
};

/// Simple task used in engine integration tests.
task delayed_increment(int& val) {
    co_await delay_ms<test::mock_clock>(100ms);
    ++val;
    co_return;
}

// =========================================================================
// Direct log_impl tests
// =========================================================================

TEST(LoggerTest, CustomLoggerCapture) {
    capture_logger::clear();

    log::detail::log_impl<default_config, log_level::info, capture_logger,
                          steady_clock>("INF", "TEST", "hello %s %d", "world",
                                        42);

    ASSERT_EQ(capture_logger::messages.size(), 1);
    const auto& msg = capture_logger::messages[0];
    EXPECT_NE(msg.find("[INF]"), std::string::npos);
    EXPECT_NE(msg.find("<TEST>"), std::string::npos);
    EXPECT_NE(msg.find("hello world 42"), std::string::npos);
}

TEST(LoggerTest, TimestampInOutput) {
    capture_logger::clear();

    log::detail::log_impl<default_config, log_level::info, capture_logger,
                          steady_clock>("INF", "TEST", "ts check");

    ASSERT_FALSE(capture_logger::messages.empty());
    // First character should be a digit (epoch timestamp in ms)
    EXPECT_TRUE(
        std::isdigit(static_cast<unsigned char>(capture_logger::messages[0][0])));
}

TEST(LoggerTest, LevelFilteringExcludesDebugWhenMinInfo) {
    capture_logger::clear();

    // default_config has min_level = info — debug should be suppressed
    log::detail::log_impl<default_config, log_level::debug, capture_logger,
                          steady_clock>("DBG", "TEST", "should not appear");
    EXPECT_TRUE(capture_logger::messages.empty());

    // info should pass through
    log::detail::log_impl<default_config, log_level::info, capture_logger,
                          steady_clock>("INF", "TEST", "info ok");
    ASSERT_EQ(capture_logger::messages.size(), 1);
    EXPECT_NE(capture_logger::messages[0].find("info ok"),
              std::string::npos);
}

TEST(LoggerTest, LevelFilteringCustomMinLevel) {
    capture_logger::clear();

    // warn_config has min_level = warn — info and debug suppressed
    log::detail::log_impl<warn_config, log_level::info, capture_logger,
                          steady_clock>("INF", "TEST", "no info");
    EXPECT_TRUE(capture_logger::messages.empty());

    log::detail::log_impl<warn_config, log_level::warn, capture_logger,
                          steady_clock>("WRN", "TEST", "warn ok");
    ASSERT_EQ(capture_logger::messages.size(), 1);
    EXPECT_NE(capture_logger::messages[0].find("warn ok"),
              std::string::npos);

    log::detail::log_impl<warn_config, log_level::error, capture_logger,
                          steady_clock>("ERR", "TEST", "error ok");
    ASSERT_EQ(capture_logger::messages.size(), 2);
    EXPECT_NE(capture_logger::messages[1].find("error ok"),
              std::string::npos);
}

TEST(LoggerTest, NoLoggerSuppressesEverything) {
    capture_logger::clear();

    // Even with capture_logger defined, passing no_logger as the Logger
    // type means the inner if-constexpr eliminates the formatting.
    log::detail::log_impl<default_config, log_level::info, no_logger,
                          steady_clock>("INF", "TEST", "should not appear");
    EXPECT_TRUE(capture_logger::messages.empty());
}

// =========================================================================
// User-facing log::info / debug / warn / error tests
// =========================================================================

TEST(LoggerTest, LogInfoApiWithCustomLogger) {
    capture_logger::clear();

    log::info<capture_logger>("hello %d", 99);

    ASSERT_EQ(capture_logger::messages.size(), 1);
    EXPECT_NE(capture_logger::messages[0].find("hello 99"),
              std::string::npos);
}

TEST(LoggerTest, LogWarnApiWithCustomLogger) {
    capture_logger::clear();

    log::warn<capture_logger>("warning: %s", "low fuel");

    ASSERT_EQ(capture_logger::messages.size(), 1);
    EXPECT_NE(capture_logger::messages[0].find("warning: low fuel"),
              std::string::npos);
}

TEST(LoggerTest, LogDebugApiFilteredByDefault) {
    capture_logger::clear();

    // Default config has min_level = info, so debug is suppressed
    // even with explicit logger.
    log::debug<capture_logger, default_config>("should be filtered");
    EXPECT_TRUE(capture_logger::messages.empty());

    // With debug_config it passes through
    log::debug<capture_logger, debug_config>("debug ok");
    ASSERT_EQ(capture_logger::messages.size(), 1);
    EXPECT_NE(capture_logger::messages[0].find("debug ok"),
              std::string::npos);
}

// =========================================================================
// Engine integration — log output verification
// =========================================================================

class LoggerEngineTest : public ::testing::Test {
protected:
    void SetUp() override {
        capture_logger::clear();
        test::mock_clock::set({}); // reset to epoch
    }
};

TEST_F(LoggerEngineTest, TriggerLogsTagAndTriggered) {
    int val = 0;
    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_task<"FLSH"_tag, &delayed_increment>());

    eng.template trigger<&delayed_increment>(val);

    // "triggered" at INFO level
    bool found_triggered = false;
    bool found_tag = false;
    for (const auto& msg : capture_logger::messages) {
        if (msg.find("triggered") != std::string::npos)
            found_triggered = true;
        // Check tag format: <reactor::task::FLSH>
        if (msg.find("<reactor::task::FLSH>") != std::string::npos)
            found_tag = true;
    }
    EXPECT_TRUE(found_triggered) << "Should contain 'triggered'";
    EXPECT_TRUE(found_tag)
        << "Should contain '<reactor::task::FLSH>' tag";
}

TEST_F(LoggerEngineTest, AlreadyRunningLogsWarning) {
    int val = 0;
    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_task<"RUN"_tag, &delayed_increment>());

    // First trigger starts the task (suspends on delay_ms).
    eng.template trigger<&delayed_increment>(val);
    capture_logger::clear();

    // Second trigger while already running.
    auto ec = eng.template trigger<&delayed_increment>(val);
    EXPECT_EQ(ec, error::task_already_running);

    ASSERT_FALSE(capture_logger::messages.empty());
    bool found_warning = false;
    for (const auto& msg : capture_logger::messages) {
        if (msg.find("already running") != std::string::npos) {
            found_warning = true;
            EXPECT_NE(msg.find("[WRN]"), std::string::npos);
            EXPECT_NE(msg.find("<reactor::task::RUN>"),
                      std::string::npos);
        }
    }
    EXPECT_TRUE(found_warning);
}

TEST_F(LoggerEngineTest, TimerFlowLogsDelayExpiredCompleted) {
    int val = 0;
    // Use debug_config so DEBUG-level "delay … registered" shows up
    auto eng = make_engine<debug_config, test::mock_clock, capture_logger>(
        register_task<"TIMR"_tag, &delayed_increment>());

    eng.template trigger<&delayed_increment>(val);

    // Should see "triggered" + "delay … registered"
    bool triggered = false, delay = false;
    for (const auto& msg : capture_logger::messages) {
        if (msg.find("triggered") != std::string::npos) triggered = true;
        if (msg.find("delay") != std::string::npos &&
            msg.find("registered") != std::string::npos)
            delay = true;
    }
    EXPECT_TRUE(triggered);
    EXPECT_TRUE(delay);

    capture_logger::clear();

    // Advance time past the 100ms delay and tick.
    test::mock_clock::advance(100ms);
    eng.tick();

    bool expired = false, completed = false;
    for (const auto& msg : capture_logger::messages) {
        if (msg.find("timer expired") != std::string::npos) expired = true;
        if (msg.find("completed") != std::string::npos) completed = true;
    }
    EXPECT_TRUE(expired);
    EXPECT_TRUE(completed);

    // Task should have executed.
    EXPECT_EQ(val, 1);
}

// A task with a zero-delay (immediately expiring timer) to exercise the
// "completed" log path in tick().
task immediate_complete_task(int& v) {
    co_await delay_ms<test::mock_clock>(0ms);
    ++v;
    co_return;
}

TEST_F(LoggerEngineTest, CompletedLogInDirectResume) {
    int val = 0;
    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_task<"SYNC"_tag, &immediate_complete_task>());

    capture_logger::clear();
    eng.template trigger<&immediate_complete_task>(val);

    // Trigger will have logged "triggered"; the task suspends on delay_ms(0),
    // which registers an immediate timer.  Advance 1ms so it expires, then
    // tick to resume.
    test::mock_clock::advance(1ms);
    eng.tick();

    bool completed = false;
    for (const auto& msg : capture_logger::messages) {
        if (msg.find("completed") != std::string::npos) completed = true;
    }
    EXPECT_TRUE(completed);
    EXPECT_EQ(val, 1);
}

TEST_F(LoggerEngineTest, CapacityExceededLogsError) {
    int val = 0;
    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_task<"CAP"_tag, &delayed_increment>());

    // Fill the timer queue directly.
    auto far = test::mock_clock::now() + 1h;
    for (std::size_t i = 0; i < default_config::max_timers; ++i) {
        eng.add_timer(far, std::coroutine_handle<>{});
    }
    capture_logger::clear();

    // Next add_timer should log capacity exceeded.
    auto ec = eng.add_timer(far, std::coroutine_handle<>{});
    EXPECT_EQ(ec, error::capacity_exceeded);

    ASSERT_FALSE(capture_logger::messages.empty());
    bool found = false;
    for (const auto& msg : capture_logger::messages) {
        if (msg.find("capacity exceeded") != std::string::npos) {
            found = true;
            EXPECT_NE(msg.find("[ERR]"), std::string::npos);
        }
    }
    EXPECT_TRUE(found);
}

} // anonymous namespace

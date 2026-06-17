#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <coroutine>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace cgx::reactor;
using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// A minimal driver class with two member-function tasks
// -----------------------------------------------------------------------

class test_driver {
    int counter_ = 0;
    int loop_count_ = 0;
    int recorded_ = 0;

public:
    task init() {
        ++counter_;
        co_return;
    }

    task loop() {
        while (true) {
            co_await std::suspend_always{};
            ++counter_;
            ++loop_count_;
        }
    }

    task fire_once(int val) {
        recorded_ = val;
        co_return;
    }

    int counter() const { return counter_; }
    int loop_count() const { return loop_count_; }
    int recorded() const { return recorded_; }

    using reactor_tasks = task_list<&test_driver::init, &test_driver::loop,
                                     &test_driver::fire_once>;
};

// -----------------------------------------------------------------------
// A class with a single task for simple tests
// -----------------------------------------------------------------------

class simple_driver {
    int val_ = 0;

public:
    task set_val(int v) {
        val_ = v;
        co_return;
    }

    int val() const { return val_; }

    using reactor_tasks = task_list<&simple_driver::set_val>;
};

// -----------------------------------------------------------------------
// A free-function task
// -----------------------------------------------------------------------

int g_free_counter = 0;

task free_inc(int& c) {
    ++c;
    co_return;
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

TEST(InstanceTriggerTest, SingleInstanceSingleMethod) {
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    auto h = eng.trigger(drv, &test_driver::init);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.counter(), 1);
}

TEST(InstanceTriggerTest, SingleInstanceMultipleMethods) {
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // Trigger init — fire and return
    auto h = eng.trigger(drv, &test_driver::init);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.counter(), 1);

    // Trigger loop — suspend on first co_await
    h = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.counter(), 1);  // not incremented yet (suspended at entry)

    // Tick — loop resumes, counter increments
    eng.tick();
    EXPECT_EQ(drv.counter(), 2);

    // Fire once
    h = eng.trigger(drv, &test_driver::fire_once, 42);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.recorded(), 42);
}

TEST(InstanceTriggerTest, TwoInstancesSameClass) {
    test_driver drv_a;
    test_driver drv_b;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv_a),
        register_instance(drv_b));

    // Trigger init on drv_a
    auto h = eng.trigger(drv_a, &test_driver::init);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv_a.counter(), 1);
    EXPECT_EQ(drv_b.counter(), 0);

    // Trigger init on drv_b
    h = eng.trigger(drv_b, &test_driver::init);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv_a.counter(), 1);
    EXPECT_EQ(drv_b.counter(), 1);

    // Trigger loop on drv_a
    h = eng.trigger(drv_a, &test_driver::loop);
    ASSERT_EQ(h.error(), error::ok);

    // Tick — only drv_a's loop advances
    eng.tick();
    EXPECT_EQ(drv_a.counter(), 2);  // init + one loop iteration
    EXPECT_EQ(drv_b.counter(), 1);  // just init

    // Trigger loop on drv_b
    h = eng.trigger(drv_b, &test_driver::loop);
    ASSERT_EQ(h.error(), error::ok);

    // Tick — both advance
    eng.tick();
    EXPECT_EQ(drv_a.counter(), 3);
    EXPECT_EQ(drv_b.counter(), 2);
}

TEST(InstanceTriggerTest, InstanceNotRegistered) {
    test_driver drv_registered;
    test_driver drv_unregistered;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv_registered));

    // Trigger on an unregistered instance
    auto h = eng.trigger(drv_unregistered, &test_driver::init);
    ASSERT_EQ(h.error(), error::task_not_registered);

    // Registered instance still works
    h = eng.trigger(drv_registered, &test_driver::init);
    ASSERT_EQ(h.error(), error::ok);
}

TEST(InstanceTriggerTest, AlreadyRunning) {
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // Trigger the loop task
    auto h = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(h.error(), error::ok);

    // Second trigger while running
    h = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(h.error(), error::task_already_running);

    // Tick — loop advances
    eng.tick();
    EXPECT_EQ(drv.counter(), 1);

    // Still running
    h = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(h.error(), error::task_already_running);
}

TEST(InstanceTriggerTest, FreeFunctionStillWorks) {
    int cnt = 0;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&free_inc>());

    auto h = eng.template trigger<&free_inc>(cnt);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(cnt, 1);
}

TEST(InstanceTriggerTest, FreeFunctionAndInstanceTogether) {
    test_driver drv;
    int cnt = 0;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&free_inc>(),
        register_instance(drv));

    // Free function
    auto h = eng.template trigger<&free_inc>(cnt);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(cnt, 1);

    // Instance-based
    h = eng.trigger(drv, &test_driver::init);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.counter(), 1);

    // Legacy NTTP trigger on instance
    h = eng.template trigger<&test_driver::init>();
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.counter(), 2);
}

// -----------------------------------------------------------------------
// Dump tests
// -----------------------------------------------------------------------

struct capture_logger {
    static std::vector<std::string> lines;

    static void print(const char* msg) noexcept {
        lines.push_back(msg);
    }
};

std::vector<std::string> capture_logger::lines{};

TEST(InstanceTriggerTest, DumpWithLoggerContainsTaskInfo) {
    capture_logger::lines.clear();
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_instance(drv));

    eng.dump();

    // Should contain truncated task info (16-byte tag limit)
    // test_driver::init (17 chars) → ~t_driver::init
    // test_driver::loop (17 chars) → ~t_driver::loop
    // test_driver::fire_once (21 chars) → ~ver::fire_once
    ASSERT_GE(capture_logger::lines.size(), 1u);
    bool found_init = false;
    bool found_loop = false;
    bool found_fire_once = false;
    for (const auto& line : capture_logger::lines) {
        if (line.find("~t_driver::init") != std::string::npos) {
            found_init = true;
        }
        if (line.find("~t_driver::loop") != std::string::npos) {
            found_loop = true;
        }
        if (line.find("~ver::fire_once") != std::string::npos) {
            found_fire_once = true;
        }
    }
    EXPECT_TRUE(found_init) << "dump should mention ~t_driver::init (truncated)";
    EXPECT_TRUE(found_loop) << "dump should mention ~t_driver::loop (truncated)";
    EXPECT_TRUE(found_fire_once) << "dump should mention ~ver::fire_once (truncated)";
}

TEST(InstanceTriggerTest, DumpReturnsCorrectStats) {
    capture_logger::lines.clear();
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_instance(drv));

    auto r = eng.dump();
    EXPECT_EQ(r.task_count, 3u);
    EXPECT_EQ(r.reserved_count, 3u);
    EXPECT_EQ(r.scratchpad_count, 0u);
    EXPECT_EQ(r.scratchpad_size, 2048u);
}

TEST(InstanceTriggerTest, DumpWithCustomSink) {
    std::vector<std::string> sink_lines;

    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    auto r = eng.dump([&sink_lines](std::string_view line) {
        sink_lines.push_back(std::string(line));
    });

    // Reserved pool summary + scratchpad pool summary + 3 per-task lines
    ASSERT_GE(sink_lines.size(), 5u);
    EXPECT_EQ(r.task_count, 3u);

    // First line is the reserved pool summary
    EXPECT_NE(sink_lines[0].find("Reserved pool:"), std::string::npos);
    // Second line is the scratchpad pool summary
    EXPECT_NE(sink_lines[1].find("Scratchpad pool:"), std::string::npos);

    // Per-task lines should start with [0], [1], [2]
    EXPECT_TRUE(sink_lines[2].starts_with("[0]"));
    EXPECT_TRUE(sink_lines[3].starts_with("[1]"));
    EXPECT_TRUE(sink_lines[4].starts_with("[2]"));

    // Should contain "offset=" and "size="
    EXPECT_NE(sink_lines[2].find("offset="), std::string::npos);
    EXPECT_NE(sink_lines[2].find("size="), std::string::npos);
}

TEST(InstanceTriggerTest, DumpWithNoLoggerReturnsStats) {
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // dump() with no_logger should still return the report
    auto r = eng.dump();
    EXPECT_EQ(r.task_count, 3u);
    EXPECT_EQ(r.reserved_count, 3u);
    EXPECT_EQ(r.scratchpad_count, 0u);
}

TEST(InstanceTriggerTest, AutoTagShowsInDump) {
    capture_logger::lines.clear();
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_instance(drv));

    eng.dump();

    // Output should contain truncated function name (~t_driver::init)
    ASSERT_GE(capture_logger::lines.size(), 3u);
    bool found_init = false;
    for (const auto& line : capture_logger::lines) {
        if (line.find("~t_driver::init") != std::string::npos) {
            found_init = true;
            break;
        }
    }
    EXPECT_TRUE(found_init) << "dump should contain ~t_driver::init (truncated)";
}

// -----------------------------------------------------------------------
// Const member function test
// -----------------------------------------------------------------------

class const_driver {
    int val_ = 0;
public:
    task set_val(int v) { val_ = v; co_return; }
    task get_val() const { co_return; }
    int val() const { return val_; }
    using reactor_tasks = task_list<&const_driver::set_val, &const_driver::get_val>;
};

TEST(InstanceTriggerTest, ConstMemberFunction) {
    const_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // Non-const method
    auto h = eng.trigger(drv, &const_driver::set_val, 42);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.val(), 42);

    // Const method
    h = eng.trigger(drv, &const_driver::get_val);
    ASSERT_EQ(h.error(), error::ok);
}

// -----------------------------------------------------------------------
// Frame size probe tests (requires capture_logger for dump output)
// -----------------------------------------------------------------------

TEST(InstanceTriggerTest, DumpShowsProbedFrameSize) {
    capture_logger::lines.clear();
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_instance(drv));

    eng.dump();

    // Each per-task dump line should contain "size=" followed by a number.
    // The frame size should be non-zero (probed at construction).
    // Skip the two summary lines (reserved pool, scratchpad pool),
    // check the remaining per-task lines.
    ASSERT_GE(capture_logger::lines.size(), 5u);  // 2 summaries + 3 tasks
    for (std::size_t i = 2; i < capture_logger::lines.size(); ++i) {
        const auto& line = capture_logger::lines[i];
        auto pos = line.find("size=");
        ASSERT_NE(pos, std::string::npos) << "line missing size=: " << line;
        pos += 5;  // past "size="
        auto end = line.find('B', pos);
        ASSERT_NE(end, std::string::npos);
        auto size_str = line.substr(pos, end - pos);
        auto size = std::stoul(size_str);
        EXPECT_GT(size, 0u) << "frame size should be non-zero: " << line;
        EXPECT_LE(size, 1024u) << "frame size should not exceed default: " << line;
    }
}

// -----------------------------------------------------------------------
// Reserved pool tests
// -----------------------------------------------------------------------

TEST(InstanceTriggerTest, PoolSizingShowsCorrectUsage) {
    std::vector<std::string> sink_lines;
    simple_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));  // one task: set_val(int)

    eng.dump([&sink_lines](std::string_view line) {
        sink_lines.push_back(std::string(line));
    });

    // 2 summary lines + 1 per-task line
    ASSERT_GE(sink_lines.size(), 3u);
    EXPECT_NE(sink_lines[0].find("Reserved pool:"), std::string::npos);
    EXPECT_NE(sink_lines[0].find("/ 8192B used"), std::string::npos);
    EXPECT_NE(sink_lines[1].find("Scratchpad pool:"), std::string::npos);

    // Per-task line (index 2) should have correct size (set_val has params, gets fallback)
    EXPECT_NE(sink_lines[2].find("size=1024B"), std::string::npos);
}

TEST(InstanceTriggerTest, FrameProbingCapturesNoArgTasks) {
    // test_driver::init and test_driver::loop are no-arg and should be probed.
    // test_driver::fire_once has a parameter and gets the fallback size.
    std::vector<std::string> sink_lines;
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    eng.dump([&sink_lines](std::string_view line) {
        sink_lines.push_back(std::string(line));
    });

    // 2 pool summaries + 3 per-task lines
    ASSERT_GE(sink_lines.size(), 5u);

    // Per-task lines start at index 2 (after reserved + scratchpad summaries)
    // init (index 2): no-arg member, should be probed (small, <= 64B)
    // Truncated: test_driver::init → ~t_driver::init
    EXPECT_NE(sink_lines[2].find("~t_driver::init"), std::string::npos);
    EXPECT_TRUE(sink_lines[2].find("size=") != std::string::npos)
        << "init missing size=: " << sink_lines[2];
    {
        // Extract size value and verify it's small (probed, not fallback)
        auto pos = sink_lines[2].find("size=") + 5;
        auto end = sink_lines[2].find('B', pos);
        auto sz = std::stoul(sink_lines[2].substr(pos, end - pos));
        EXPECT_LT(sz, 256u) << "init probed size should be small: " << sink_lines[2];
        EXPECT_GT(sz, 0u) << "init probed size should be non-zero: " << sink_lines[2];
    }

    // loop (index 3): no-arg member, should be probed (small, <= 64B)
    // Truncated: test_driver::loop → ~t_driver::loop
    EXPECT_NE(sink_lines[3].find("~t_driver::loop"), std::string::npos);
    {
        auto pos = sink_lines[3].find("size=") + 5;
        auto end = sink_lines[3].find('B', pos);
        auto sz = std::stoul(sink_lines[3].substr(pos, end - pos));
        EXPECT_LT(sz, 256u) << "loop probed size should be small: " << sink_lines[3];
        EXPECT_GT(sz, 0u) << "loop probed size should be non-zero: " << sink_lines[3];
    }

    // fire_once (index 4): takes int param, NOT probed, gets fallback 1024B
    // Truncated: test_driver::fire_once → ~ver::fire_once
    EXPECT_NE(sink_lines[4].find("~ver::fire_once"), std::string::npos);
    EXPECT_NE(sink_lines[4].find("size=1024B"), std::string::npos)
        << "fire_once should get fallback: " << sink_lines[4];
}

TEST(InstanceTriggerTest, PoolAlignment) {
    std::vector<std::string> sink_lines;
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    eng.dump([&sink_lines](std::string_view line) {
        sink_lines.push_back(std::string(line));
    });

    // Per-task lines (skip the two summary lines at indices 0 and 1)
    ASSERT_GE(sink_lines.size(), 5u);
    for (std::size_t i = 2; i < sink_lines.size(); ++i) {
        const auto& line = sink_lines[i];
        auto pos = line.find("offset=");
        ASSERT_NE(pos, std::string::npos) << "line missing offset=: " << line;
        pos += 7;  // past "offset="
        auto end = line.find("  ", pos);
        ASSERT_NE(end, std::string::npos);
        auto offset_str = line.substr(pos, end - pos);
        auto offset = std::stoul(offset_str);
        EXPECT_EQ(offset % alignof(std::max_align_t), 0u)
            << "offset should be aligned to max_align_t: " << line;
    }
}

// Config with a tiny pool to test overflow handling
struct tiny_pool : default_config {
    static constexpr std::size_t reserved_pool_size = 32;
};

TEST(InstanceTriggerTest, PoolOversizeReturnsError) {
    test_driver drv;
    auto eng = make_engine<tiny_pool, test::mock_clock>(
        register_instance(drv));

    // Pool is too small to hold even one task's frame
    EXPECT_TRUE(eng.pool_exhausted());

    // All triggers should return capacity_exceeded
    auto h = eng.template trigger<&test_driver::init>();
    ASSERT_EQ(h.error(), error::capacity_exceeded);

    h = eng.template trigger<&test_driver::loop>();
    ASSERT_EQ(h.error(), error::capacity_exceeded);

    // Instance-based trigger should also fail
    h = eng.trigger(drv, &test_driver::init);
    ASSERT_EQ(h.error(), error::capacity_exceeded);
}

}  // anonymous namespace

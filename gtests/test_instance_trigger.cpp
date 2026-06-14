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
        register_instance(drv));  // no tag — auto-generate

    auto ec = eng.trigger(drv, &test_driver::init);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv.counter(), 1);
}

TEST(InstanceTriggerTest, SingleInstanceMultipleMethods) {
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // Trigger init — fire and return
    auto ec = eng.trigger(drv, &test_driver::init);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv.counter(), 1);

    // Trigger loop — suspend on first co_await
    ec = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv.counter(), 1);  // not incremented yet (suspended at entry)

    // Tick — loop resumes, counter increments
    eng.tick();
    EXPECT_EQ(drv.counter(), 2);

    // Fire once
    ec = eng.trigger(drv, &test_driver::fire_once, 42);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv.recorded(), 42);
}

TEST(InstanceTriggerTest, TwoInstancesSameClass) {
    test_driver drv_a;
    test_driver drv_b;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance<"A"_tag>(drv_a),
        register_instance<"B"_tag>(drv_b));

    // Trigger init on drv_a
    auto ec = eng.trigger(drv_a, &test_driver::init);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv_a.counter(), 1);
    EXPECT_EQ(drv_b.counter(), 0);

    // Trigger init on drv_b
    ec = eng.trigger(drv_b, &test_driver::init);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv_a.counter(), 1);
    EXPECT_EQ(drv_b.counter(), 1);

    // Trigger loop on drv_a
    ec = eng.trigger(drv_a, &test_driver::loop);
    ASSERT_EQ(ec, error::ok);

    // Tick — only drv_a's loop advances
    eng.tick();
    EXPECT_EQ(drv_a.counter(), 2);  // init + one loop iteration
    EXPECT_EQ(drv_b.counter(), 1);  // just init

    // Trigger loop on drv_b
    ec = eng.trigger(drv_b, &test_driver::loop);
    ASSERT_EQ(ec, error::ok);

    // Tick — both advance
    eng.tick();
    EXPECT_EQ(drv_a.counter(), 3);
    EXPECT_EQ(drv_b.counter(), 2);
}

TEST(InstanceTriggerTest, InstanceNotRegistered) {
    test_driver drv_registered;
    test_driver drv_unregistered;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance<"R"_tag>(drv_registered));

    // Trigger on an unregistered instance
    auto ec = eng.trigger(drv_unregistered, &test_driver::init);
    ASSERT_EQ(ec, error::task_not_registered);

    // Registered instance still works
    ec = eng.trigger(drv_registered, &test_driver::init);
    ASSERT_EQ(ec, error::ok);
}

TEST(InstanceTriggerTest, AlreadyRunning) {
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // Trigger the loop task
    auto ec = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(ec, error::ok);

    // Second trigger while running
    ec = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(ec, error::task_already_running);

    // Tick — loop advances
    eng.tick();
    EXPECT_EQ(drv.counter(), 1);

    // Still running
    ec = eng.trigger(drv, &test_driver::loop);
    ASSERT_EQ(ec, error::task_already_running);
}

TEST(InstanceTriggerTest, FreeFunctionStillWorks) {
    int cnt = 0;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&free_inc>());

    auto ec = eng.template trigger<&free_inc>(cnt);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(cnt, 1);
}

TEST(InstanceTriggerTest, FreeFunctionAndInstanceTogether) {
    test_driver drv;
    int cnt = 0;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<"FREE"_tag, &free_inc>(),
        register_instance<"DRV"_tag>(drv));

    // Free function
    auto ec = eng.template trigger<&free_inc>(cnt);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(cnt, 1);

    // Instance-based
    ec = eng.trigger(drv, &test_driver::init);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv.counter(), 1);

    // Legacy NTTP trigger on instance
    ec = eng.template trigger<&test_driver::init>();
    ASSERT_EQ(ec, error::ok);
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
        register_instance<"DRV"_tag>(drv));

    eng.dump();

    // Should contain task info
    ASSERT_GE(capture_logger::lines.size(), 1u);
    bool found_init = false;
    bool found_loop = false;
    bool found_fire_once = false;
    for (const auto& line : capture_logger::lines) {
        if (line.find("test_driver::init") != std::string::npos) {
            found_init = true;
        }
        if (line.find("test_driver::loop") != std::string::npos) {
            found_loop = true;
        }
        if (line.find("test_driver::fire_once") != std::string::npos) {
            found_fire_once = true;
        }
    }
    EXPECT_TRUE(found_init) << "dump should mention test_driver::init";
    EXPECT_TRUE(found_loop) << "dump should mention test_driver::loop";
    EXPECT_TRUE(found_fire_once) << "dump should mention test_driver::fire_once";
}

TEST(InstanceTriggerTest, DumpReturnsCorrectStats) {
    capture_logger::lines.clear();
    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_instance<"DRV"_tag>(drv));

    auto r = eng.dump();
    EXPECT_EQ(r.task_count, 3u);
    EXPECT_EQ(r.reserved_count, 3u);
    EXPECT_EQ(r.scratchpad_count, 0u);
    EXPECT_EQ(r.scratchpad_size, 0u);
}

TEST(InstanceTriggerTest, DumpWithCustomSink) {
    std::vector<std::string> sink_lines;

    test_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));  // auto-tag

    auto r = eng.dump([&sink_lines](std::string_view line) {
        sink_lines.push_back(std::string(line));
    });

    ASSERT_GE(sink_lines.size(), 3u);
    EXPECT_EQ(r.task_count, 3u);

    // Each line should start with [0], [1], [2]
    EXPECT_TRUE(sink_lines[0].starts_with("[0]"));
    EXPECT_TRUE(sink_lines[1].starts_with("[1]"));
    EXPECT_TRUE(sink_lines[2].starts_with("[2]"));

    // Should contain "reserved" and "frame="
    EXPECT_NE(sink_lines[0].find("reserved"), std::string::npos);
    EXPECT_NE(sink_lines[0].find("frame=~"), std::string::npos);
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

    // No tag — auto-generate TSK0, TSK1, TSK2
    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_instance(drv));

    eng.dump();

    // Output should contain the auto-generated tag suffix (TSK0 etc.)
    ASSERT_GE(capture_logger::lines.size(), 3u);
    // The dump output strips the "reactor::task::" prefix, so we should see
    // "TSK0" not "reactor::task::TSK0"
    bool found_tsk0 = false;
    for (const auto& line : capture_logger::lines) {
        if (line.find("TSK0") != std::string::npos) {
            found_tsk0 = true;
            break;
        }
    }
    EXPECT_TRUE(found_tsk0) << "dump should contain auto-generated TSK0 tag";
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
    auto ec = eng.trigger(drv, &const_driver::set_val, 42);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv.val(), 42);

    // Const method
    ec = eng.trigger(drv, &const_driver::get_val);
    ASSERT_EQ(ec, error::ok);
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

    // Each dump line should contain "frame=~" followed by a number.
    // The frame size should be non-zero (probed at construction).
    ASSERT_GE(capture_logger::lines.size(), 3u);
    for (const auto& line : capture_logger::lines) {
        auto pos = line.find("frame=~");
        ASSERT_NE(pos, std::string::npos) << "line missing frame=~: " << line;
        pos += 7;  // past "frame=~"
        auto end = line.find('B', pos);
        ASSERT_NE(end, std::string::npos);
        auto size_str = line.substr(pos, end - pos);
        auto size = std::stoul(size_str);
        EXPECT_GT(size, 0u) << "frame size should be non-zero: " << line;
        EXPECT_LE(size, 1024u) << "frame size should not exceed slot size: " << line;
    }
}

}  // anonymous namespace

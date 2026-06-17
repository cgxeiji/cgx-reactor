#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <coroutine>
#include <string>
#include <string_view>
#include <vector>

namespace {

namespace cr = cgx::reactor;

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// A driver with both reserved and scratchpad tasks
// -----------------------------------------------------------------------

class mixed_driver {
    int val_ = 0;
    int count_ = 0;

public:
    cr::task reserved_loop() {
        while (true) {
            co_await std::suspend_always{};
            ++count_;
        }
    }

    cr::task scratch_once() {
        val_ = 42;
        co_return;
    }

    cr::task scratch_loop() {
        while (true) {
            co_await std::suspend_always{};
            ++count_;
        }
    }

    int val() const { return val_; }
    int count() const { return count_; }

    using reactor_tasks = cr::task_list<
        &mixed_driver::reserved_loop,              // reserved
        cr::scratch_v<&mixed_driver::scratch_once>, // scratchpad
        cr::scratch_v<&mixed_driver::scratch_loop>  // scratchpad
    >;
};

// -----------------------------------------------------------------------
// A driver with only scratchpad tasks
// -----------------------------------------------------------------------

class scratch_driver {
    int val_ = 0;

public:
    cr::task fire() {
        val_ = 99;
        co_return;
    }

    int val() const { return val_; }

    using reactor_tasks = cr::task_list<
        cr::scratch_v<&scratch_driver::fire>
    >;
};

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

TEST(ScratchpadTest, ScratchpadAllocationAndRun) {
    scratch_driver drv;
    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_instance(drv));

    // Use try_trigger for scratchpad in non-coroutine context
    auto ec = eng.template try_trigger<&scratch_driver::fire>();
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(drv.val(), 99);
}

TEST(ScratchpadTest, MixedReservedAndScratchpad) {
    mixed_driver drv;
    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_instance(drv));

    auto ec = eng.template trigger<&mixed_driver::reserved_loop>();
    ASSERT_EQ(ec, cr::error::ok);

    ec = eng.template try_trigger<&mixed_driver::scratch_once>();
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(drv.val(), 42);

    ec = eng.template try_trigger<&mixed_driver::scratch_loop>();
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(drv.count(), 0);

    eng.tick();
    EXPECT_EQ(drv.count(), 2);
}

TEST(ScratchpadTest, ScratchpadCompletionFreesPool) {
    scratch_driver drv;
    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_instance(drv));

    auto ec = eng.template try_trigger<&scratch_driver::fire>();
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(drv.val(), 99);

    drv = scratch_driver{};
    ec = eng.template try_trigger<&scratch_driver::fire>();
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(drv.val(), 99);
}

TEST(ScratchpadTest, TryTriggerReturnsOccupied) {
    mixed_driver drv;
    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_instance(drv));

    auto ec = eng.template try_trigger<&mixed_driver::scratch_loop>();
    ASSERT_EQ(ec, cr::error::ok);

    ec = eng.template try_trigger<&mixed_driver::scratch_loop>();
    ASSERT_EQ(ec, cr::error::task_already_running);

    eng.tick();
    EXPECT_EQ(drv.count(), 1);
}

struct tiny_scratch_pool : cr::default_config {
    static constexpr std::size_t scratchpad_pool_size = 16;
};

TEST(ScratchpadTest, TryTriggerReturnsPoolFull) {
    // Use a task with a large buffer so its frame exceeds the 16B pool.
    struct big_driver {
        cr::task big() { std::array<char, 100> b{}; b.fill(0); co_await std::suspend_always{}; (void)b[0]; co_return; }
        using reactor_tasks = cr::task_list<cr::scratch_v<&big_driver::big>>;
    };
    big_driver drv;
    auto eng = cr::make_engine<tiny_scratch_pool, cr::test::mock_clock>(
        cr::register_instance(drv));

    // Frame is larger than 16B pool → capacity_exceeded
    auto ec = eng.template try_trigger<&big_driver::big>();
    ASSERT_EQ(ec, cr::error::capacity_exceeded);
}

TEST(ScratchpadTest, InstanceBasedTrigger) {
    mixed_driver drv;
    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_instance(drv));

    // Trigger via instance + method pointer
    auto ec = eng.trigger(drv, &mixed_driver::scratch_once);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(drv.val(), 42);
}

TEST(ScratchpadTest, DumpShowsScratchpadInfo) {
    std::vector<std::string> sink_lines;
    mixed_driver drv;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_instance(drv));

    eng.dump([&sink_lines](std::string_view line) {
        sink_lines.push_back(std::string(line));
    });

    // Should have: reserved pool, scratchpad pool, and 3 per-task lines
    ASSERT_GE(sink_lines.size(), 5u);
    EXPECT_NE(sink_lines[0].find("Reserved pool:"), std::string::npos);
    EXPECT_NE(sink_lines[1].find("Scratchpad pool:"), std::string::npos);

    // Second per-task line should show scratchpad
    // Index 2: reserved_loop (reserved, offset=N, size=N)
    EXPECT_NE(sink_lines[2].find("reserved_loop"), std::string::npos);
    EXPECT_NE(sink_lines[2].find("reserved"), std::string::npos);

    // Index 3: scratch_once (scratchpad)
    EXPECT_NE(sink_lines[3].find("scratch_once"), std::string::npos);
    EXPECT_NE(sink_lines[3].find("scratchpad"), std::string::npos);

    // Index 4: scratch_loop (scratchpad)
    EXPECT_NE(sink_lines[4].find("scratch_loop"), std::string::npos);
    EXPECT_NE(sink_lines[4].find("scratchpad"), std::string::npos);
}

TEST(ScratchpadTest, ReportShowsScratchpadCount) {
    mixed_driver drv;
    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_instance(drv));

    auto r = eng.report();
    EXPECT_EQ(r.task_count, 3u);
    EXPECT_EQ(r.reserved_count, 1u);    // reserved_loop
    EXPECT_EQ(r.scratchpad_count, 2u);  // scratch_once + scratch_loop
    EXPECT_EQ(r.scratchpad_size, 2048u);
}

TEST(ScratchpadTest, DestructorAfterCompletion) {
    // Regression test: after a scratchpad task completes, meta.handle
    // must be cleared.  Otherwise the engine destructor calls
    // handle.destroy() on freed memory → SEGV.
    scratch_driver drv;
    {
        auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
            cr::register_instance(drv));

        auto ec = eng.template try_trigger<&scratch_driver::fire>();
        ASSERT_EQ(ec, cr::error::ok);
        EXPECT_EQ(drv.val(), 99);

        // Engine destroyed here — must not crash
    }
}

TEST(ScratchpadTest, DestructorAfterMultipleCompletions) {
    mixed_driver drv;
    {
        auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
            cr::register_instance(drv));

        auto ec = eng.template try_trigger<&mixed_driver::scratch_once>();
        ASSERT_EQ(ec, cr::error::ok);

        ec = eng.template try_trigger<&mixed_driver::scratch_loop>();
        ASSERT_EQ(ec, cr::error::ok);

        // Tick completes scratch_loop
        eng.tick();

        // Engine destroyed here — must not crash
    }
}

}  // anonymous namespace

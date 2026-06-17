#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <coroutine>

namespace {

using namespace cgx::reactor;
using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Task coroutines used in tests
// -----------------------------------------------------------------------

// A simple fire-and-return task that increments a counter.
task increment_counter(int& counter) {
    ++counter;
    co_return;
}

// A task that suspends once, then increments a counter and returns.
task increment_after_suspend(int& counter) {
    co_await std::suspend_always{};
    ++counter;
    co_return;
}

// A task that loops forever, suspending each iteration.
task infinite_loop(int& counter) {
    while (true) {
        co_await std::suspend_always{};
        ++counter;
    }
}

// -----------------------------------------------------------------------
// Test cases
// -----------------------------------------------------------------------

TEST(TaskTest, FireAndReturn) {
    int counter = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&increment_counter>());

    auto h = eng.template trigger<&increment_counter>(counter);
    ASSERT_EQ(h.error(), error::ok);
    // Task ran to completion immediately (no suspension points).
    EXPECT_EQ(counter, 1);
}

TEST(TaskTest, TriggerTickAndReTrigger) {
    int counter = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&increment_after_suspend>());

    // First trigger -- suspends at first co_await.
    auto h = eng.template trigger<&increment_after_suspend>(counter);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(counter, 0);  // Not incremented yet

    // Tick -- resumes, increments, completes.
    eng.tick();
    EXPECT_EQ(counter, 1);  // Now incremented

    // Re-trigger -- should work since task completed.
    h = eng.template trigger<&increment_after_suspend>(counter);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(counter, 1);  // Not incremented yet (suspended)

    eng.tick();
    EXPECT_EQ(counter, 2);  // Incremented again
}

TEST(TaskTest, AlreadyRunning) {
    int counter = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&infinite_loop>());

    // First trigger -- suspends at first co_await.
    auto h = eng.template trigger<&infinite_loop>(counter);
    ASSERT_EQ(h.error(), error::ok);

    // Trying to trigger again while running should fail.
    h = eng.template trigger<&infinite_loop>(counter);
    ASSERT_EQ(h.error(), error::task_already_running);

    // Tick -- advances one iteration.
    eng.tick();
    EXPECT_EQ(counter, 1);

    // Task is still alive (looping), still occupied.
    h = eng.template trigger<&infinite_loop>(counter);
    ASSERT_EQ(h.error(), error::task_already_running);
}

TEST(TaskTest, EngineSizeIsValidConstant) {
    // Verify that the engine type is complete and has a known size.
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&increment_counter>());
    static_assert(
        sizeof(eng) > 0,
        "engine must be a complete type");
}

}  // anonymous namespace

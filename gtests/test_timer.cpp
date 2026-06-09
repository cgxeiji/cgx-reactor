#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <coroutine>

namespace {

using namespace cgx::reactor;
using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Test 1: task co_awaits delay_ms(100), advance clock 50ms → tick →
//         task still suspended
// -----------------------------------------------------------------------

task delayed_increment(int& counter) {
    co_await delay_ms<test::mock_clock>(100ms);
    ++counter;
    co_return;
}

TEST(TimerTest, SuspendUntilExpiry) {
    int counter = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<"DELY"_tag, &delayed_increment>());

    // Trigger the task (suspends at delay_ms).
    auto ec = eng.template trigger<&delayed_increment>(counter);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(counter, 0);  // Not incremented yet.

    // Advance clock by 50ms — not enough.
    test::mock_clock::advance(50ms);
    eng.tick();
    EXPECT_EQ(counter, 0);  // Still suspended.

    // Advance another 50ms (total 100ms) — timer expires.
    test::mock_clock::advance(50ms);
    eng.tick();
    EXPECT_EQ(counter, 1);  // Incremented.
}

// -----------------------------------------------------------------------
// Test 2: two tasks with different delays, verify correct resume order
// -----------------------------------------------------------------------

task first_task(int& order, int& out) {
    co_await delay_ms<test::mock_clock>(100ms);
    out = ++order;
    co_return;
}

task second_task(int& order, int& out) {
    co_await delay_ms<test::mock_clock>(200ms);
    out = ++order;
    co_return;
}

TEST(TimerTest, TwoTasksDifferentDelays) {
    int order = 0;
    int first_done = 0;
    int second_done = 0;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<"FIRST"_tag, &first_task>(),
        register_task<"SCND"_tag, &second_task>());

    auto ec1 = eng.template trigger<&first_task>(order, first_done);
    ASSERT_EQ(ec1, error::ok);

    auto ec2 = eng.template trigger<&second_task>(order, second_done);
    ASSERT_EQ(ec2, error::ok);

    // Advance 100ms → first task should fire.
    test::mock_clock::advance(100ms);
    eng.tick();
    EXPECT_EQ(first_done, 1);
    EXPECT_EQ(second_done, 0);  // Second still pending.

    // Advance another 100ms → second task fires.
    test::mock_clock::advance(100ms);
    eng.tick();
    EXPECT_EQ(second_done, 2);
}

// -----------------------------------------------------------------------
// Test 3: fill timer queue, verify error::queue_full on next delay_ms
// -----------------------------------------------------------------------

TEST(TimerTest, QueueFull) {
    auto eng = make_engine<default_config, test::mock_clock>();

    // Fill the timer queue via the engine's add_timer internal API.
    auto some_time = test::mock_clock::now() + 1h;
    for (std::size_t i = 0; i < default_config::max_timers; ++i) {
        auto ec = eng.add_timer(some_time, std::coroutine_handle<>{});
        ASSERT_EQ(ec, error::ok);
    }

    // Next add should fail.
    auto ec = eng.add_timer(some_time, std::coroutine_handle<>{});
    ASSERT_EQ(ec, error::queue_full);
}

// -----------------------------------------------------------------------
// Test 4: periodic task runs 5 iterations by advancing clock and ticking
// -----------------------------------------------------------------------

task periodic_counter(int& count) {
    for (int i = 0; i < 5; ++i) {
        co_await delay_ms<test::mock_clock>(50ms);
        ++count;
    }
    co_return;
}

TEST(TimerTest, PeriodicTaskFiveIterations) {
    int count = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<"PERI"_tag, &periodic_counter>());

    auto ec = eng.template trigger<&periodic_counter>(count);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(count, 0);

    // Run 5 iterations: each advances by 50ms then ticks.
    for (int i = 1; i <= 5; ++i) {
        test::mock_clock::advance(50ms);
        eng.tick();
        EXPECT_EQ(count, i) << "iteration " << i;
    }

    // Task should be done now.
    EXPECT_EQ(count, 5);
}

}  // anonymous namespace

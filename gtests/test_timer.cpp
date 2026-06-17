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
        register_task<&delayed_increment>());

    // Trigger the task (suspends at delay_ms).
    auto h = eng.template trigger<&delayed_increment>(counter);
    ASSERT_EQ(h.error(), error::ok);
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
        register_task<&first_task>(),
        register_task<&second_task>());

    auto h1 = eng.template trigger<&first_task>(order, first_done);
    ASSERT_EQ(h1.error(), error::ok);

    auto h2 = eng.template trigger<&second_task>(order, second_done);
    ASSERT_EQ(h2.error(), error::ok);

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
// Test 3: fill timer queue, verify error::capacity_exceeded on next delay_ms
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
    ASSERT_EQ(ec, error::capacity_exceeded);
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
        register_task<&periodic_counter>());

    auto h = eng.template trigger<&periodic_counter>(count);
    ASSERT_EQ(h.error(), error::ok);
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

// -----------------------------------------------------------------------
// Test 5: delay_until suspends until exact time point
// -----------------------------------------------------------------------

template <typename Clock>
task wake_at_exact_time(int& counter, cgx::reactor::test::mock_clock::time_point target) {
    co_await delay_until<Clock>(target);
    ++counter;
    co_return;
}

TEST(DelayUntilTest, WakeAtExactTime) {
    int counter = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&wake_at_exact_time<test::mock_clock>>());

    auto target = test::mock_clock::now() + 100ms;

    auto h = eng.template trigger<&wake_at_exact_time<test::mock_clock>>(counter, target);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(counter, 0);

    // Advance clock to 99ms — not woken yet.
    test::mock_clock::advance(99ms);
    eng.tick();
    EXPECT_EQ(counter, 0);

    // Advance to exactly target.
    test::mock_clock::advance(1ms);
    eng.tick();
    EXPECT_EQ(counter, 1);
}

// -----------------------------------------------------------------------
// Test 6: delay_until with past time point wakes immediately
// -----------------------------------------------------------------------

TEST(DelayUntilTest, PastTimePoint) {
    int counter = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&wake_at_exact_time<test::mock_clock>>());

    // Target is 100ms in the past.
    auto target = test::mock_clock::now() - 100ms;

    auto h = eng.template trigger<&wake_at_exact_time<test::mock_clock>>(counter, target);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(counter, 0);

    // Even with a past target, the coroutine is suspended; tick should expire it.
    eng.tick();
    EXPECT_EQ(counter, 1);
}

// -----------------------------------------------------------------------
// Test 7: delay_until when queue is full
// -----------------------------------------------------------------------

task delay_until_task(error& out_err, test::mock_clock::time_point target) {
    out_err = co_await delay_until<test::mock_clock>(target);
    co_return;
}

TEST(DelayUntilTest, QueueFull) {
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&delay_until_task>());

    // Fill the timer queue.
    auto far = test::mock_clock::now() + 1h;
    for (std::size_t i = 0; i < default_config::max_timers; ++i) {
        auto ec = eng.add_timer(far, std::coroutine_handle<>{});
        ASSERT_EQ(ec, error::ok);
    }

    // delay_until should fail.
    error err = error::ok;
    auto h = eng.template trigger<&delay_until_task>(err, test::mock_clock::now() + 10ms);
    ASSERT_EQ(h.error(), error::ok);
    eng.tick();
    EXPECT_EQ(err, error::capacity_exceeded);
}

// -----------------------------------------------------------------------
// Test 8: delay_quantized snaps to next grid tick
// -----------------------------------------------------------------------

template <typename Clock>
task quantized_wake(int& counter,
                    typename Clock::duration interval,
                    typename Clock::time_point& out_wake) {
    out_wake = Clock::now();
    co_await delay_quantized<Clock>(interval);
    out_wake = Clock::now();
    ++counter;
    co_return;
}

TEST(DelayQuantizedTest, GridAlignment) {
    int counter = 0;
    auto wake_at = test::mock_clock::time_point{};
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&quantized_wake<test::mock_clock>>());

    // Start at t=50ms.
    test::mock_clock::set(test::mock_clock::time_point{50ms});

    auto h = eng.template trigger<&quantized_wake<test::mock_clock>>(counter, 100ms, wake_at);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(counter, 0);

    // Advance to 100ms — should fire.
    test::mock_clock::advance(50ms);
    eng.tick();
    EXPECT_EQ(counter, 1);
    EXPECT_EQ(wake_at.time_since_epoch(), std::chrono::milliseconds{100});
}

// -----------------------------------------------------------------------
// Test 9: delay_quantized drift-free periodic scheduling
// -----------------------------------------------------------------------

task quantized_periodic(int& count, test::mock_clock::duration interval) {
    for (int i = 0; i < 3; ++i) {
        co_await delay_quantized<test::mock_clock>(interval);
        ++count;
    }
    co_return;
}

TEST(DelayQuantizedTest, DriftFreePeriodic) {
    int count = 0;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&quantized_periodic>());

    // Set epoch to t=0.
    test::mock_clock::set(test::mock_clock::time_point{});

    auto h = eng.template trigger<&quantized_periodic>(count, 100ms);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(count, 0);

    // Advance to t=50ms, tick — task still suspended (next tick at 100ms).
    test::mock_clock::advance(50ms);
    eng.tick();
    EXPECT_EQ(count, 0);

    // Advance to t=100ms — should wake (tick 1).
    test::mock_clock::advance(50ms);
    eng.tick();
    EXPECT_EQ(count, 1);

    // Advance to t=150ms, tick — still before next tick at 200ms.
    test::mock_clock::advance(50ms);
    eng.tick();
    EXPECT_EQ(count, 1);

    // Advance to t=200ms — should wake (tick 2).
    test::mock_clock::advance(50ms);
    eng.tick();
    EXPECT_EQ(count, 2);

    // Advance to t=300ms — overshoot but should snap to 300ms (tick 3).
    test::mock_clock::advance(100ms);
    eng.tick();
    EXPECT_EQ(count, 3);
}

// -----------------------------------------------------------------------
// Test 10: delay_quantized on exact tick boundary advances to next tick
// -----------------------------------------------------------------------

TEST(DelayQuantizedTest, ExactTickBoundary) {
    int counter = 0;
    auto wake_at = test::mock_clock::time_point{};
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&quantized_wake<test::mock_clock>>());

    // Start on a tick boundary (t=100ms).
    test::mock_clock::set(test::mock_clock::time_point{100ms});

    auto h = eng.template trigger<&quantized_wake<test::mock_clock>>(counter, 100ms, wake_at);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(counter, 0);

    // Tick at exactly 100ms — should NOT fire yet (next tick is 200ms).
    eng.tick();
    EXPECT_EQ(counter, 0);

    // Advance to 200ms — should fire.
    test::mock_clock::advance(100ms);
    eng.tick();
    EXPECT_EQ(counter, 1);
    EXPECT_EQ(wake_at.time_since_epoch(), std::chrono::milliseconds{200});
}

// -----------------------------------------------------------------------
// Test 11: delay_quantized when queue is full
// -----------------------------------------------------------------------

task quantized_error_task(error& out_err, test::mock_clock::duration interval) {
    out_err = co_await delay_quantized<test::mock_clock>(interval);
    co_return;
}

TEST(DelayQuantizedTest, QueueFull) {
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&quantized_error_task>());

    // Fill the timer queue.
    auto far = test::mock_clock::now() + 1h;
    for (std::size_t i = 0; i < default_config::max_timers; ++i) {
        auto ec = eng.add_timer(far, std::coroutine_handle<>{});
        ASSERT_EQ(ec, error::ok);
    }

    // Trigger the quantized task — delay_quantized in await_suspend will fail.
    error err = error::ok;
    auto h = eng.template trigger<&quantized_error_task>(err, 100ms);
    ASSERT_EQ(h.error(), error::ok);

    // Task completed synchronously (await_suspend returned false), err is set.
    EXPECT_EQ(err, error::capacity_exceeded);
}

}  // anonymous namespace

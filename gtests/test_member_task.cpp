#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <coroutine>

namespace {

using namespace cgx::reactor;
using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// A driver class with two member-function tasks
// -----------------------------------------------------------------------

class fake_driver {
    int counter_ = 0;
    int loop_count_ = 0;
    int recorded_ = 0;

public:
    // An infinite-loop task that increments a counter each iteration.
    task run_loop() {
        while (true) {
            co_await std::suspend_always{};
            ++counter_;
            ++loop_count_;
        }
    }

    // A fire-and-return task that records a value.
    task fire_once(int val) {
        recorded_ = val;
        co_return;
    }

    int counter() const { return counter_; }
    int loop_count() const { return loop_count_; }
    int recorded() const { return recorded_; }

    // Required: the reactor_tasks alias listing member-function tasks.
    using reactor_tasks = task_list<&fake_driver::run_loop, &fake_driver::fire_once>;
};

// -----------------------------------------------------------------------
// A free-function task used in the combined test
// -----------------------------------------------------------------------

int g_free_counter = 0;

task free_inc(int& c) {
    ++c;
    co_return;
}

// -----------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------

TEST(MemberTaskTest, TriggerMemberFunctionFireAndReturn) {
    fake_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance<"TST1"_tag>(drv));

    // Trigger the fire-and-return member function.
    auto ec = eng.template trigger<&fake_driver::fire_once>(42);
    ASSERT_EQ(ec, error::ok);

    // The task should have run to completion immediately.
    EXPECT_EQ(drv.recorded(), 42);
    EXPECT_EQ(drv.counter(), 0);  // run_loop was not triggered
}

TEST(MemberTaskTest, TriggerMemberFunctionLoopAndIncrement) {
    fake_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance<"TST1"_tag>(drv));

    // Trigger the infinite-loop member function.
    auto ec = eng.template trigger<&fake_driver::run_loop>();
    ASSERT_EQ(ec, error::ok);

    // Suspended at first co_await — counter not incremented yet.
    EXPECT_EQ(drv.counter(), 0);

    // Tick — resumes one iteration, increments.
    eng.tick();
    EXPECT_EQ(drv.counter(), 1);

    // Tick again — second iteration.
    eng.tick();
    EXPECT_EQ(drv.counter(), 2);
}

TEST(MemberTaskTest, AlreadyRunningForMemberFn) {
    fake_driver drv;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance<"TST1"_tag>(drv));

    // Trigger the loop — it suspends immediately.
    auto ec = eng.template trigger<&fake_driver::run_loop>();
    ASSERT_EQ(ec, error::ok);

    // Second trigger while running should fail.
    ec = eng.template trigger<&fake_driver::run_loop>();
    ASSERT_EQ(ec, error::task_already_running);

    // Tick advances one iteration.
    eng.tick();
    EXPECT_EQ(drv.counter(), 1);

    // Still running, so trigger still fails.
    ec = eng.template trigger<&fake_driver::run_loop>();
    ASSERT_EQ(ec, error::task_already_running);
}

TEST(MemberTaskTest, TwoInstancesWithDifferentTags) {
    fake_driver drv_a;
    fake_driver drv_b;

    // Register two instances with different tags.
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance<"DRVA"_tag>(drv_a),
        register_instance<"DRVB"_tag>(drv_b));

    // Trigger run_loop on drv_a (first match wins with slot_index).
    auto ec = eng.template trigger<&fake_driver::run_loop>();
    ASSERT_EQ(ec, error::ok);

    // Tick once — only drv_a's loop should advance.
    eng.tick();
    EXPECT_EQ(drv_a.counter(), 1);
    EXPECT_EQ(drv_b.counter(), 0);

    // Can't trigger the same function pointer again — it's already running.
    ec = eng.template trigger<&fake_driver::run_loop>();
    ASSERT_EQ(ec, error::task_already_running);
}

TEST(MemberTaskTest, FreeFunctionAndMemberTaskTogether) {
    fake_driver drv;
    int free_cnt = 0;

    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<"FREE"_tag, &free_inc>(),
        register_instance<"TST1"_tag>(drv));

    // Trigger the free function task.
    auto ec = eng.template trigger<&free_inc>(free_cnt);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(free_cnt, 1);

    // Trigger the member-function task.
    ec = eng.template trigger<&fake_driver::fire_once>(99);
    ASSERT_EQ(ec, error::ok);
    EXPECT_EQ(drv.recorded(), 99);
}

}  // anonymous namespace

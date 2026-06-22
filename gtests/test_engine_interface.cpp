#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace cgx::reactor;
using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Free function that takes the abstract `engine&`.
//
// This is the core use case: a helper or library function accepts a
// non-templated engine reference and uses the virtual interface only.
// -----------------------------------------------------------------------

static void drive(cgx::reactor::engine& eng) {
    // The interface allows `tick()`, `report()`, `pool_exhausted()`.
    eng.tick();
    auto r = eng.report();
    (void)r.task_count;
    (void)r.reserved_count;
    (void)r.scratchpad_count;
    (void)eng.pool_exhausted();
}

// -----------------------------------------------------------------------
// A driver with a few registered tasks
// -----------------------------------------------------------------------

class iface_driver {
    int val_ = 0;
    int count_ = 0;

public:
    task fire() { val_ = 7; co_return; }
    task spin() {
        while (true) {
            co_await std::suspend_always{};
            ++count_;
        }
    }
    int val() const { return val_; }
    int count() const { return count_; }
    using reactor_tasks = task_list<&iface_driver::fire, &iface_driver::spin>;
};

// -----------------------------------------------------------------------
// A logger that captures all log lines
// -----------------------------------------------------------------------

struct capture_logger {
    static std::vector<std::string> lines;
    static void print(const char* msg) noexcept {
        lines.push_back(msg);
    }
};
std::vector<std::string> capture_logger::lines;

// -----------------------------------------------------------------------
// A tiny pool config to test pool_exhausted() through the base
// -----------------------------------------------------------------------

struct tiny_pool : default_config {
    static constexpr std::size_t reserved_pool_size = 32;
};

// -----------------------------------------------------------------------
// EngineInterface: pass an engine through the abstract `engine&`
// -----------------------------------------------------------------------

TEST(EngineInterface, PassEngineByReference) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // Upcast to abstract base — must compile.
    cgx::reactor::engine& base_ref = eng;

    // Call the free helper — proves virtual dispatch works through the
    // abstract base.
    drive(base_ref);

    // Sanity: the engine is still alive and has the expected tasks.
    auto r = base_ref.report();
    EXPECT_EQ(r.task_count, 2u);
    EXPECT_EQ(r.reserved_count, 2u);
    EXPECT_EQ(r.scratchpad_count, 0u);
    EXPECT_FALSE(base_ref.pool_exhausted());
}

TEST(EngineInterface, VirtualTickRunsTasks) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    cgx::reactor::engine& base_ref = eng;

    // `fire` is a fire-and-return task.  `spin` suspends on first co_await.
    // The interface has no `trigger(uid)` (that's M3), so use the
    // concrete engine's existing templated `trigger` to fire the tasks.
    auto h = eng.template trigger<&iface_driver::fire>();
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.val(), 7);

    h = eng.template trigger<&iface_driver::spin>();
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.count(), 0);

    // tick() through the abstract base — virtual dispatch.
    base_ref.tick();
    EXPECT_EQ(drv.count(), 1);

    base_ref.tick();
    EXPECT_EQ(drv.count(), 2);
}

TEST(EngineInterface, PoolExhaustedThroughBase) {
    iface_driver drv;
    auto eng = make_engine<tiny_pool, test::mock_clock>(
        register_instance(drv));

    cgx::reactor::engine& base_ref = eng;

    // Pool is too small to probe even one frame → pool_exhausted() == true.
    EXPECT_TRUE(base_ref.pool_exhausted());
}

// -----------------------------------------------------------------------
// EngineInterface: dump(sink) through the abstract base
// -----------------------------------------------------------------------

TEST(EngineInterface, DumpCapturelessSinkThroughBase) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    cgx::reactor::engine& base_ref = eng;

    // Captureless lambda → function-pointer convertible → base's
    // templated `dump<Sink>` wrapper type-erases it and calls the
    // virtual `dump_erased`.
    std::vector<std::string> lines;
    auto r = base_ref.dump([&lines](std::string_view line) {
        lines.push_back(std::string(line));
    });

    EXPECT_EQ(r.task_count, 2u);

    // Two summary lines + 2 per-task lines = 4 lines minimum.
    ASSERT_GE(lines.size(), 4u);
    EXPECT_NE(lines[0].find("Reserved pool:"), std::string::npos);
    EXPECT_NE(lines[1].find("Scratchpad pool:"), std::string::npos);
    EXPECT_TRUE(lines[2].starts_with("[0]"));
    EXPECT_TRUE(lines[3].starts_with("[1]"));
}

TEST(EngineInterface, DumpWithLoggerThroughBase) {
    capture_logger::lines.clear();
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock, capture_logger>(
        register_instance(drv));

    cgx::reactor::engine& base_ref = eng;

    // dump() (no args) routes through the virtual `dump()` → logger version.
    auto r = base_ref.dump();
    EXPECT_EQ(r.task_count, 2u);
    ASSERT_GE(capture_logger::lines.size(), 4u);
}

// -----------------------------------------------------------------------
// EngineInterface: task_handle holds engine* (abstract), not basic_engine*
// -----------------------------------------------------------------------

TEST(EngineInterface, TaskHandleHoldsAbstractEnginePtr) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    auto h = eng.template trigger<&iface_driver::fire>();
    ASSERT_EQ(h.error(), error::ok);

    // task_handle is the non-templated, namespace-scope type.
    static_assert(std::is_same_v<decltype(h), task_handle>,
                  "trigger must return the non-templated task_handle");

    // task_handle::self is `engine*` (abstract base).
    static_assert(std::is_same_v<decltype(h.self), engine*>,
                  "task_handle::self must be `engine*`");
}

// -----------------------------------------------------------------------
// EngineInterface: done() awaiter uses virtuals through the base
// -----------------------------------------------------------------------

TEST(EngineInterface, DoneAwaiterUsesVirtuals) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    auto h = eng.template trigger<&iface_driver::fire>();
    ASSERT_EQ(h.error(), error::ok);

    // Fire-and-return task → done() is immediately ready.
    auto awaiter = h.done();
    // await_ready() calls engine::task_is_done() virtually.
    EXPECT_TRUE(awaiter.await_ready());
}

TEST(EngineInterface, DoneAwaiterSuspendsAndResumes) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    auto h = eng.template trigger<&iface_driver::spin>();
    ASSERT_EQ(h.error(), error::ok);

    // spin is suspended at the first co_await.
    auto awaiter = h.done();
    EXPECT_FALSE(awaiter.await_ready());

    // After a tick, spin runs once but doesn't complete (loop).
    eng.tick();
    // spin is still occupied (the loop repeats).  task_is_done returns
    // true only when !occupied OR handle.done().  spin is occupied
    // (loops forever) and handle is not done → false.
    auto awaiter2 = h.done();
    EXPECT_FALSE(awaiter2.await_ready());
}

// -----------------------------------------------------------------------
// EngineInterface: destruction via `engine*` invokes the virtual dtor
// and cleans up task handles (no leaks, no double-free).
// -----------------------------------------------------------------------

TEST(EngineInterface, DestructorViaBasePtr) {
    static int instance_count = 0;
    struct counter {
        task count() { ++instance_count; co_return; }
        using reactor_tasks = task_list<&counter::count>;
    };

    counter c;
    // Heap-allocate the engine; own it via `engine*` (abstract base) so
    // `delete` invokes the virtual destructor → `~basic_engine()`.
    using engine_t = decltype(make_engine<default_config, test::mock_clock>(
        register_instance(c)));
    auto* base_ptr = new engine_t(
        make_engine<default_config, test::mock_clock>(register_instance(c)));

    // Trigger the task through the concrete engine (no interface
    // trigger yet — that's M3).
    auto hh = base_ptr->template trigger<&counter::count>();
    ASSERT_EQ(hh.error(), error::ok);
    EXPECT_EQ(instance_count, 1);

    // Destroy via `engine*` — virtual destructor cleans up handles.
    // (ASan/LeakSanitizer would flag any leaked coroutine frames.)
    delete base_ptr;
    SUCCEED();
}

// -----------------------------------------------------------------------
// EngineInterface: concrete templated trigger still returns the
// non-templated task_handle
// -----------------------------------------------------------------------

TEST(EngineInterface, ConcreteTriggerReturnsNonTemplatedHandle) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // NTTP-based trigger (concrete class)
    auto h1 = eng.template trigger<&iface_driver::fire>();
    static_assert(std::is_same_v<decltype(h1), task_handle>);

    // Instance-based trigger (concrete class)
    auto h2 = eng.trigger(drv, &iface_driver::fire);
    static_assert(std::is_same_v<decltype(h2), task_handle>);

    // try_trigger (concrete class)
    auto h3 = eng.template try_trigger<&iface_driver::fire>();
    static_assert(std::is_same_v<decltype(h3), task_handle>);

    EXPECT_EQ(h1.error(), error::ok);
    EXPECT_EQ(h2.error(), error::ok);
    EXPECT_EQ(h3.error(), error::ok);
}

// -----------------------------------------------------------------------
// EngineInterface: free function drive() with zero-task engine
// -----------------------------------------------------------------------

TEST(EngineInterface, DriveZeroTaskEngine) {
    auto eng = make_engine<default_config, test::mock_clock>();
    cgx::reactor::engine& base_ref = eng;

    // Must compile and run without crashing.
    drive(base_ref);

    auto r = base_ref.report();
    EXPECT_EQ(r.task_count, 0u);
    EXPECT_EQ(r.reserved_count, 0u);
    EXPECT_EQ(r.scratchpad_count, 0u);
    EXPECT_FALSE(base_ref.pool_exhausted());
}

// -----------------------------------------------------------------------
// EngineInterface: static assertions for the abstract base
// -----------------------------------------------------------------------

static_assert(std::has_virtual_destructor_v<engine>,
              "engine must have a virtual destructor");
static_assert(std::is_polymorphic_v<engine>,
              "engine must be polymorphic");
static_assert(std::is_abstract_v<engine>,
              "engine must be abstract (has pure virtuals)");

// =======================================================================
// M2 — UID mechanism + compile-time distinctness assert
// =======================================================================

// Free functions used for UID tests.  Distinct names so the FNV-1a
// hashes are different.
task uid_free_alpha() { co_return; }
task uid_free_beta()  { co_return; }
task uid_free_gamma() { co_return; }

// A driver with several distinct methods, used for member-function UID
// tests and the multi-task / multi-class compile check.
class uid_driver {
public:
    task m_one()   { co_return; }
    task m_two()   { co_return; }
    task m_three() { co_return; }
    using reactor_tasks = task_list<&uid_driver::m_one,
                                     &uid_driver::m_two,
                                     &uid_driver::m_three>;
};

// A different driver for the multi-class compile test.
class uid_driver_b {
public:
    task task_x() { co_return; }
    task task_y() { co_return; }
    using reactor_tasks = task_list<&uid_driver_b::task_x,
                                     &uid_driver_b::task_y>;
};

// -----------------------------------------------------------------------
// M2: task_uid_v — compile-time UID derivation
// -----------------------------------------------------------------------

// Compile-time: task_uid_v must be usable in a constant expression.
static_assert(task_uid_v<&uid_free_alpha>.value != 0,
              "task_uid_v must be non-zero for a real function");
static_assert(task_uid{1} == task_uid{1},
              "task_uid must support == (defaulted comparison)");

// Compile-time: same fn → same UID.
static_assert(task_uid_v<&uid_free_alpha>.value ==
              task_uid_v<&uid_free_alpha>.value,
              "task_uid_v must be deterministic for the same fn");
static_assert(task_uid_v<&uid_driver::m_one>.value ==
              task_uid_v<&uid_driver::m_one>.value,
              "task_uid_v must be deterministic for the same member fn");

// Compile-time: distinct fns → distinct UIDs (with overwhelming
// probability under FNV-1a for short distinct strings).
static_assert(task_uid_v<&uid_free_alpha>.value !=
              task_uid_v<&uid_free_beta>.value,
              "different free fns must hash to different UIDs");
static_assert(task_uid_v<&uid_free_alpha>.value !=
              task_uid_v<&uid_free_gamma>.value,
              "different free fns must hash to different UIDs");
static_assert(task_uid_v<&uid_driver::m_one>.value !=
              task_uid_v<&uid_driver::m_two>.value,
              "different member fns must hash to different UIDs");
static_assert(task_uid_v<&uid_driver::m_one>.value !=
              task_uid_v<&uid_driver::m_three>.value,
              "different member fns must hash to different UIDs");
static_assert(task_uid_v<&uid_free_alpha>.value !=
              task_uid_v<&uid_driver::m_one>.value,
              "free vs member fn with same simple name must still differ");

TEST(TaskUidVTest, DistinctFnsHaveDistinctUids) {
    // Mirror the static_asserts in a runtime test (some toolchains
    // surface static_asserts less loudly than runtime failures).
    EXPECT_NE(task_uid_v<&uid_free_alpha>.value,
              task_uid_v<&uid_free_beta>.value);
    EXPECT_NE(task_uid_v<&uid_free_alpha>.value,
              task_uid_v<&uid_free_gamma>.value);
    EXPECT_NE(task_uid_v<&uid_driver::m_one>.value,
              task_uid_v<&uid_driver::m_two>.value);
    EXPECT_NE(task_uid_v<&uid_driver::m_one>.value,
              task_uid_v<&uid_driver::m_three>.value);
    EXPECT_NE(task_uid_v<&uid_free_alpha>.value,
              task_uid_v<&uid_driver::m_one>.value);
}

TEST(TaskUidVTest, SameFnHasSameUid) {
    EXPECT_EQ(task_uid_v<&uid_free_alpha>.value,
              task_uid_v<&uid_free_alpha>.value);
    EXPECT_EQ(task_uid_v<&uid_driver::m_one>.value,
              task_uid_v<&uid_driver::m_one>.value);
}

// -----------------------------------------------------------------------
// M2: detail::uid_pair_ok — the collision-detection predicate
// -----------------------------------------------------------------------

TEST(UidPairOkTest, EqualUidSameFnIsOk) {
    // Multi-instance registration: same fn, same UID — must be allowed.
    EXPECT_TRUE((detail::uid_pair_ok(42u, 42u, true)));
    EXPECT_TRUE((detail::uid_pair_ok(0u, 0u, true)));
    EXPECT_TRUE((detail::uid_pair_ok(0xFFFFFFFFu, 0xFFFFFFFFu, true)));
}

TEST(UidPairOkTest, EqualUidDifferentFnIsCollision) {
    // Two distinct functions with the same UID — hash collision, must fail.
    EXPECT_FALSE((detail::uid_pair_ok(42u, 42u, false)));
    EXPECT_FALSE((detail::uid_pair_ok(0u, 0u, false)));
    EXPECT_FALSE((detail::uid_pair_ok(0xDEADBEEFu, 0xDEADBEEFu, false)));
}

TEST(UidPairOkTest, DifferentUidIsOkRegardlessOfSameFn) {
    // No collision possible when the UIDs differ.
    EXPECT_TRUE((detail::uid_pair_ok(0u, 1u, false)));
    EXPECT_TRUE((detail::uid_pair_ok(1u, 0u, false)));
    EXPECT_TRUE((detail::uid_pair_ok(0u, 1u, true)));
    EXPECT_TRUE((detail::uid_pair_ok(0xFFFFFFFFu, 0u, false)));
    EXPECT_TRUE((detail::uid_pair_ok(100u, 200u, true)));
}

// -----------------------------------------------------------------------
// M2: static_assert — multi-instance does NOT trip the assert
// (same fn referenced twice → same UID + same_fn=true → allowed)
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// M6: static_report — compile-time engine metadata
// -----------------------------------------------------------------------

namespace {

// Custom Config for static_report test (non-default pool sizes + max_timers
// so the values are distinguishable from default_config).
struct sr_config : cgx::reactor::default_config {
    static constexpr std::size_t reserved_pool_size   = 4096;
    static constexpr std::size_t scratchpad_pool_size = 2048;
    static constexpr std::size_t max_timers            = 8;
};

// 2 reserved + 1 scratchpad, so the counts are unambiguous.
class sr_driver {
public:
    task r1() { co_return; }
    task r2() { co_return; }
    task s1() {
        co_await cgx::reactor::delay_ms<test::mock_clock>(100ms);
        co_return;
    }
    using reactor_tasks = cgx::reactor::task_list<
        &sr_driver::r1,
        &sr_driver::r2,
        cgx::reactor::scratch_v<&sr_driver::s1>
    >;
};

}  // namespace

// Compile-time: all fields are constexpr.  A runtime test exercises the
// same values (some toolchains surface static_asserts less loudly than
// runtime failures).

TEST(StaticReportTest, ValuesMatchConfig) {
    sr_driver drv;
    auto eng = make_engine<sr_config, test::mock_clock>(register_instance(drv));

    // Mirrors the static_asserts in a runtime test (some toolchains
    // surface static_asserts less loudly than runtime failures).
    constexpr auto r = decltype(eng)::static_report();
    EXPECT_EQ(r.task_count, 3u);
    EXPECT_EQ(r.reserved_count, 2u);
    EXPECT_EQ(r.scratchpad_count, 1u);
    EXPECT_EQ(r.reserved_pool_size, 4096u);
    EXPECT_EQ(r.scratchpad_pool_size, 2048u);
    EXPECT_EQ(r.max_timers, 8u);
}

TEST(StaticReportTest, ZeroTaskEngine) {
    auto eng = make_engine<default_config, test::mock_clock>();
    constexpr auto r = decltype(eng)::static_report();
    EXPECT_EQ(r.task_count, 0u);
    EXPECT_EQ(r.reserved_count, 0u);
    EXPECT_EQ(r.scratchpad_count, 0u);
    // Pool sizes / max_timers come from default_config.
    EXPECT_EQ(r.reserved_pool_size, default_config::reserved_pool_size);
    EXPECT_EQ(r.scratchpad_pool_size, default_config::scratchpad_pool_size);
    EXPECT_EQ(r.max_timers, default_config::max_timers);
}

// -----------------------------------------------------------------------

TEST(StaticAssertTest, MultiInstanceSameClassCompiles) {
    uid_driver a, b;
    // If the static_assert trips, this won't compile.
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(a),
        register_instance(b));
    // Both instances' m_one, m_two, m_three produce the same UIDs
    // (same fn), and the static_assert allows equal UIDs when same_fn.
    auto r = eng.report();
    EXPECT_EQ(r.task_count, 6u);
    SUCCEED();
}

TEST(StaticAssertTest, MultiTaskMultiClassCompiles) {
    uid_driver a;
    uid_driver_b b;
    // 3 + 2 = 5 tasks, all distinct functions, all distinct UIDs.
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(a),
        register_instance(b));
    auto r = eng.report();
    EXPECT_EQ(r.task_count, 5u);
    SUCCEED();
}

TEST(StaticAssertTest, SingleTaskCompiles) {
    class solo {
    public:
        task only() { co_return; }
        using reactor_tasks = task_list<&solo::only>;
    };
    solo s;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(s));
    auto r = eng.report();
    EXPECT_EQ(r.task_count, 1u);
    SUCCEED();
}

TEST(StaticAssertTest, ZeroTasksCompiles) {
    // Same as test_timer.cpp:93 — zero-task engine.  Exercises the
    // num_tasks == 0 branch of the pair-enumeration folds.
    auto eng = make_engine<default_config, test::mock_clock>();
    auto r = eng.report();
    EXPECT_EQ(r.task_count, 0u);
    SUCCEED();
}

TEST(StaticAssertTest, FreeFnAndMemberFnTogether) {
    uid_driver a;
    // Mix free fns and member tasks in one engine.
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&uid_free_alpha>(),
        register_instance(a),
        register_task<&uid_free_beta>());
    auto r = eng.report();
    EXPECT_EQ(r.task_count, 5u);  // 1 free + 3 member + 1 free
    SUCCEED();
}

// =======================================================================
// M3 — Interface UID trigger + hashmap + instance disambiguation
// =======================================================================

// Reset mock_clock to epoch for tests that use it.
struct mock_clock_reset {
    mock_clock_reset() { test::mock_clock::set({}); }
    ~mock_clock_reset() { test::mock_clock::set({}); }
};

// -----------------------------------------------------------------------
// M3: reserved task via interface trigger
// -----------------------------------------------------------------------

TEST(EngineInterface, ReservedViaInterface) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    // Fire-and-return task via interface trigger.
    auto h = base_ref.trigger(task_uid_v<&iface_driver::fire>);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.val(), 7);

    // done() is immediately ready (task already completed).
    EXPECT_TRUE(h.done().await_ready());
}

// -----------------------------------------------------------------------
// M3: scratchpad task via interface trigger + co_await h.done()
// -----------------------------------------------------------------------

class delayed_scratch_driver {
    int counter_ = 0;
public:
    task delayed_scratch() {
        co_await cgx::reactor::delay_ms<test::mock_clock>(100ms);
        ++counter_;
    }
    int counter() const { return counter_; }
    using reactor_tasks = cgx::reactor::task_list<
        cgx::reactor::scratch_v<&delayed_scratch_driver::delayed_scratch>
    >;
};

// Scheduler coroutine: triggers a task by UID via the interface, then
// awaits its completion.  Takes the engine by `engine&` to prove the
// interface path works through the abstract base.
static cgx::reactor::task scheduler_via_uid(cgx::reactor::engine& eng,
                                            cgx::reactor::task_uid uid,
                                            int& sched_counter) {
    auto h = eng.trigger(uid);
    if (h.error() != cgx::reactor::error::ok) co_return;
    co_await h.done();
    ++sched_counter;
}

TEST(EngineInterface, ScratchpadViaInterfaceAndDone) {
    mock_clock_reset _clock_reset;
    delayed_scratch_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    int sched_counter = 0;
    auto s = scheduler_via_uid(base_ref,
                               task_uid_v<&delayed_scratch_driver::delayed_scratch>,
                               sched_counter);
    s.handle().resume();

    // Scheduler triggered the task, now suspended on h.done().
    EXPECT_EQ(sched_counter, 0);
    EXPECT_EQ(drv.counter(), 0);

    // Tick — the scratchpad task starts, registers a timer, suspends.
    eng.tick();
    EXPECT_EQ(drv.counter(), 0);

    // Advance time past the delay and tick.
    test::mock_clock::advance(100ms);
    eng.tick();
    EXPECT_EQ(drv.counter(), 1);
    EXPECT_EQ(sched_counter, 1);  // scheduler resumed after done()
}

// -----------------------------------------------------------------------
// M3: scratchpad pool full → non-blocking capacity_exceeded
// -----------------------------------------------------------------------

// Config with a scratchpad pool that fits one small scratchpad task but
// not two.  48B = 3 blocks of 16B; a no-arg loop task's frame is ~32B
// (2 blocks), so the first fits and the second does not (needs 2
// consecutive free blocks, only 1 remains).
struct small_scratch_config : default_config {
    static constexpr std::size_t scratchpad_pool_size = 48;
};

class scratch_full_driver {
public:
    task loop1() { while (true) co_await std::suspend_always{}; }
    task loop2() { while (true) co_await std::suspend_always{}; }
    using reactor_tasks = cgx::reactor::task_list<
        cgx::reactor::scratch_v<&scratch_full_driver::loop1>,
        cgx::reactor::scratch_v<&scratch_full_driver::loop2>
    >;
};

TEST(EngineInterface, ScratchpadPoolFullViaInterface) {
    scratch_full_driver drv;
    auto eng = make_engine<small_scratch_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    // First trigger: loop1 allocated in the pool (suspended on co_await).
    auto h1 = base_ref.trigger(task_uid_v<&scratch_full_driver::loop1>);
    ASSERT_EQ(h1.error(), error::ok);

    // Second trigger: loop2 cannot fit → capacity_exceeded (non-blocking).
    auto h2 = base_ref.trigger(task_uid_v<&scratch_full_driver::loop2>);
    EXPECT_EQ(h2.error(), error::capacity_exceeded);

    // The call returned immediately (didn't suspend).  If it had
    // suspended, this test would hang and ctest would time out.
    SUCCEED();
}

// -----------------------------------------------------------------------
// M3: unknown uid → task_not_registered; done() is OOB-safe
// -----------------------------------------------------------------------

TEST(EngineInterface, UnknownUidReturnsNotRegistered) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    // A UID value that almost certainly doesn't collide with any real fn.
    auto h = base_ref.trigger(task_uid{0xDEADBEEFu});
    EXPECT_EQ(h.error(), error::task_not_registered);

    // OOB guard: done() is immediately ready, no OOB read of tasks_.
    EXPECT_TRUE(h.done().await_ready());
}

// -----------------------------------------------------------------------
// M3: multi-instance — trigger(uid) hits first; trigger(obj, uid) disambiguates
// -----------------------------------------------------------------------

class multi_driver {
    int counter_ = 0;
public:
    task side_effect() { ++counter_; co_return; }
    int counter() const { return counter_; }
    using reactor_tasks = task_list<&multi_driver::side_effect>;
};

TEST(EngineInterface, MultiInstanceUidDispatch) {
    multi_driver a, b;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(a),
        register_instance(b));
    cgx::reactor::engine& base_ref = eng;

    // trigger(uid) (no obj) hits the FIRST registered instance.
    auto h = base_ref.trigger(task_uid_v<&multi_driver::side_effect>);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(a.counter(), 1);
    EXPECT_EQ(b.counter(), 0);

    // trigger(a, uid) hits instance a specifically.
    h = base_ref.trigger(a, task_uid_v<&multi_driver::side_effect>);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(a.counter(), 2);
    EXPECT_EQ(b.counter(), 0);

    // trigger(b, uid) hits instance b specifically.
    h = base_ref.trigger(b, task_uid_v<&multi_driver::side_effect>);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(a.counter(), 2);
    EXPECT_EQ(b.counter(), 1);
}

// -----------------------------------------------------------------------
// M3: already-running reserved task via interface
// -----------------------------------------------------------------------

TEST(EngineInterface, AlreadyRunningViaInterface) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    // spin is a loop task; first trigger suspends at co_await.
    auto h = base_ref.trigger(task_uid_v<&iface_driver::spin>);
    ASSERT_EQ(h.error(), error::ok);

    // Second trigger while running → task_already_running.
    h = base_ref.trigger(task_uid_v<&iface_driver::spin>);
    EXPECT_EQ(h.error(), error::task_already_running);
}

// -----------------------------------------------------------------------
// M3: free function task via interface trigger
// -----------------------------------------------------------------------

task uid_iface_free() { co_return; }

TEST(EngineInterface, FreeFunctionViaInterface) {
    auto eng = make_engine<default_config, test::mock_clock>(
        register_task<&uid_iface_free>());
    cgx::reactor::engine& base_ref = eng;

    auto h = base_ref.trigger(task_uid_v<&uid_iface_free>);
    EXPECT_EQ(h.error(), error::ok);
    EXPECT_TRUE(h.done().await_ready());
}

// -----------------------------------------------------------------------
// M3: core use case — free fn with `engine&` and task_uid args
// -----------------------------------------------------------------------

static void run_pipeline(cgx::reactor::engine& eng,
                          cgx::reactor::task_uid a,
                          cgx::reactor::task_uid b) {
    // No concrete engine type in scope.  Compiles and runs.
    auto ha = eng.trigger(a);
    auto hb = eng.trigger(b);
    (void)ha;
    (void)hb;
}

TEST(EngineInterface, FreeFunctionWithEngineRef) {
    iface_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    run_pipeline(base_ref,
                 task_uid_v<&iface_driver::fire>,
                 task_uid_v<&iface_driver::spin>);

    // fire ran (val_ = 7), spin was triggered (suspended on co_await).
    EXPECT_EQ(drv.val(), 7);
}

// -----------------------------------------------------------------------
// M3: trigger(obj, uid) with unregistered object → task_not_registered
// -----------------------------------------------------------------------

TEST(EngineInterface, TriggerObjUnregisteredObject) {
    multi_driver a;  // registered
    multi_driver rogue;  // NOT registered
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(a));
    cgx::reactor::engine& base_ref = eng;

    // rogue has the same `self` type as a but isn't in the engine.
    auto h = base_ref.trigger(rogue, task_uid_v<&multi_driver::side_effect>);
    EXPECT_EQ(h.error(), error::task_not_registered);

    // a still works.
    h = base_ref.trigger(a, task_uid_v<&multi_driver::side_effect>);
    EXPECT_EQ(h.error(), error::ok);
    EXPECT_EQ(a.counter(), 1);
}

// -----------------------------------------------------------------------
// M3: trigger(uid) for an arg-taking task → task_not_registered
// (the interface can't forward typed args; only the templated
// trigger<Fn>(args...) can.)
// -----------------------------------------------------------------------

class arg_taking_driver {
    int val_ = 0;
public:
    task set_val(int v) { val_ = v; co_return; }
    int val() const { return val_; }
    using reactor_tasks = task_list<&arg_taking_driver::set_val>;
};

TEST(EngineInterface, ArgTakingTaskViaInterfaceReturnsNotRegistered) {
    arg_taking_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    // Interface trigger with no args → can't forward the int arg.
    auto h = base_ref.trigger(task_uid_v<&arg_taking_driver::set_val>);
    EXPECT_EQ(h.error(), error::task_not_registered);
    EXPECT_EQ(drv.val(), 0);

    // But the templated trigger<Fn>(args...) still works.
    h = eng.template trigger<&arg_taking_driver::set_val>(42);
    EXPECT_EQ(h.error(), error::ok);
    EXPECT_EQ(drv.val(), 42);
}

// -----------------------------------------------------------------------
// M3: hashmap sanity — triggering each by UID hits the right slot
// (covers probe chains: 5 tasks with different UIDs hash into a table
// of size next_pow2(10) = 16; the linear-probe walk must reach each one.)
// -----------------------------------------------------------------------

class hash_driver {
    int counters_[5] = {0, 0, 0, 0, 0};
public:
    task f0() { ++counters_[0]; co_return; }
    task f1() { ++counters_[1]; co_return; }
    task f2() { ++counters_[2]; co_return; }
    task f3() { ++counters_[3]; co_return; }
    task f4() { ++counters_[4]; co_return; }
    int counter(int i) const { return counters_[i]; }
    using reactor_tasks = task_list<
        &hash_driver::f0, &hash_driver::f1, &hash_driver::f2,
        &hash_driver::f3, &hash_driver::f4
    >;
};

TEST(EngineInterface, HashmapSanity) {
    hash_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));
    cgx::reactor::engine& base_ref = eng;

    constexpr cgx::reactor::task_uid uids[5] = {
        cgx::reactor::task_uid_v<&hash_driver::f0>,
        cgx::reactor::task_uid_v<&hash_driver::f1>,
        cgx::reactor::task_uid_v<&hash_driver::f2>,
        cgx::reactor::task_uid_v<&hash_driver::f3>,
        cgx::reactor::task_uid_v<&hash_driver::f4>
    };

    for (int i = 0; i < 5; ++i) {
        auto h = base_ref.trigger(uids[i]);
        ASSERT_EQ(h.error(), error::ok) << "trigger " << i << " failed";
        EXPECT_EQ(drv.counter(i), 1) << "counter " << i << " wrong";
    }
}

// -----------------------------------------------------------------------
// M3: overload coexistence — trigger(obj, uid) vs trigger(obj, &Class::method)
// -----------------------------------------------------------------------

TEST(EngineInterface, OverloadCoexistenceObjUidVsMemberPtr) {
    multi_driver a, b;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(a),
        register_instance(b));
    cgx::reactor::engine& base_ref = eng;

    // Member-pointer overload (concrete class only): triggers a's
    // side_effect.  The interface `trigger(Class&, task_uid)` does NOT
    // accept a member fn pointer, so this must go through the concrete
    // engine type.
    auto h = eng.trigger(a, &multi_driver::side_effect);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(a.counter(), 1);
    EXPECT_EQ(b.counter(), 0);

    // task_uid overload (works through either base or concrete):
    // triggers a's side_effect (uid disambiguates the instance).
    h = base_ref.trigger(a, task_uid_v<&multi_driver::side_effect>);
    ASSERT_EQ(h.error(), error::ok);
    EXPECT_EQ(a.counter(), 2);
    EXPECT_EQ(b.counter(), 0);
}

// -----------------------------------------------------------------------
// M4 follow-up: engine-managed reserved task awaiting a scratchpad
// sibling's completion (mirrors the rp2040_pico bootstrap).
// The bug being reproduced: `co_await h.done()` inside an engine-
// managed reserved task triggered via the concrete instance trigger
// does not actually suspend — the reserved task runs straight through.
// (The standalone-scheduler test `ScratchpadViaInterfaceAndDone` does
// not exercise this path.)
// -----------------------------------------------------------------------

class m4_bootstrap_driver {
    int val_ = 0;
    int boot_ = 0;
public:
    // Scratchpad init with a delay (same shape as init_led / init_temp).
    cgx::reactor::task init_slow() {
        co_await cgx::reactor::delay_ms<test::mock_clock>(100ms);
        val_ = 42;
    }
    // Reserved bootstrap — triggers the scratchpad via the abstract
    // `engine&` interface and awaits its completion, mirroring the
    // rp2040_pico `coordinator::bootstrap`.
    cgx::reactor::task bootstrap(cgx::reactor::engine& eng) {
        auto h = eng.trigger(
            cgx::reactor::task_uid_v<&m4_bootstrap_driver::init_slow>);
        co_await h.done();
        ++boot_;
    }
    int val() const { return val_; }
    int boot() const { return boot_; }
    using reactor_tasks = cgx::reactor::task_list<
        cgx::reactor::scratch_v<&m4_bootstrap_driver::init_slow>,
        &m4_bootstrap_driver::bootstrap
    >;
};

TEST(EngineInterface, EngineManagedBootstrapWaitsForScratchpad) {
    mock_clock_reset _;
    m4_bootstrap_driver drv;
    auto eng = make_engine<default_config, test::mock_clock>(
        register_instance(drv));

    // Trigger the reserved bootstrap via the concrete instance trigger
    // with arg forwarding.  This is the same path the rp2040_pico
    // example uses: `eng.trigger(coord, &coordinator::bootstrap, eng)`.
    eng.trigger(drv, &m4_bootstrap_driver::bootstrap, eng);

    // After the trigger returns: init_slow is allocated in the
    // scratchpad pool and suspended on its first delay_ms; bootstrap
    // is suspended on `co_await h.done()`.  Both should remain so.
    EXPECT_EQ(drv.boot(), 0) << "bootstrap completed without waiting";
    EXPECT_EQ(drv.val(), 0) << "init_slow completed without waiting";

    // Tick — nothing should change (delay hasn't elapsed).
    eng.tick();
    EXPECT_EQ(drv.boot(), 0) << "bootstrap completed on first tick";
    EXPECT_EQ(drv.val(), 0) << "init_slow completed on first tick";

    // Advance time past the delay and tick.
    test::mock_clock::advance(100ms);
    eng.tick();

    // Now init_slow should be done, bootstrap should have resumed and
    // completed.
    EXPECT_EQ(drv.val(), 42) << "init_slow did not run to completion";
    EXPECT_EQ(drv.boot(), 1) << "bootstrap did not resume after init_slow";
}

}  // anonymous namespace

#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <coroutine>

namespace {

// NOTE: We do NOT use `using namespace cgx::reactor` here because the
// POSIX `<sys/signal.h>` (pulled in indirectly via <coroutine> or other
// standard headers) declares `signal()` in the global namespace, which
// clashes with `cgx::reactor::signal`.  Instead we use a short alias.
namespace cr = cgx::reactor;

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Helper task coroutines used across tests
// -----------------------------------------------------------------------

// Fire-and-return listener: reads one value from the signal.
cr::task one_shot_listener(cr::signal<int>& sig, int& out) {
    out = co_await sig.listen();
    co_return;
}

// Three-way listeners — with NTTPs, same signature is fine.
cr::task listener_a(cr::signal<int>& sig, int& out) {
    out = co_await sig.listen();
    co_return;
}

cr::task listener_b(cr::signal<int>& sig, int& out) {
    out = co_await sig.listen();
    co_return;
}

cr::task listener_c(cr::signal<int>& sig, int& out) {
    out = co_await sig.listen();
    co_return;
}

// Listener that goes through two listen/fire cycles.
cr::task two_cycle_listener(cr::signal<int>& sig, int& out1, int& out2) {
    out1 = co_await sig.listen();
    out2 = co_await sig.listen();
    co_return;
}

// Listener that fills one slot — used for the overflow test.
cr::task filler(cr::signal<int, 1>& sig) {
    (void)co_await sig.listen();
    co_return;
}

// Listener that tries to listen on a full signal and records whether an
// overflow was detected.
cr::task overflow_detector(cr::signal<int, 1>& sig, bool& detected) {
    auto aw = sig.listen();
    (void)co_await aw;
    detected = (aw.ec_ == cr::error::listener_limit_exceeded);
    co_return;
}

// -----------------------------------------------------------------------
// Test 1 — fire with no listeners must not crash
// -----------------------------------------------------------------------

TEST(SignalTest, FireWithNoListeners) {
    cr::signal<int> sig;
    sig.fire(42);  // no crash
}

// -----------------------------------------------------------------------
// Test 2 — single listener receives the fired value
// -----------------------------------------------------------------------

TEST(SignalTest, SingleListener) {
    cr::signal<int> sig;
    int result = 0;

    cr::engine<cr::default_config, cr::test::mock_clock,
               &one_shot_listener> eng;

    auto ec = eng.trigger<&one_shot_listener>(sig, result);
    ASSERT_EQ(ec, cr::error::ok);

    // Task should be suspended at listen().
    EXPECT_EQ(result, 0);

    sig.fire(42);

    // The listener was resumed directly inside fire() and ran to
    // completion, setting result = 42.
    EXPECT_EQ(result, 42);
}

// -----------------------------------------------------------------------
// Test 3 — three listeners all receive the same value
// -----------------------------------------------------------------------

TEST(SignalTest, ThreeListenersAllReceiveValue) {
    cr::signal<int> sig;
    int r1 = 0, r2 = 0, r3 = 0;

    cr::engine<cr::default_config, cr::test::mock_clock,
               &listener_a, &listener_b, &listener_c> eng;

    ASSERT_EQ(eng.trigger<&listener_a>(sig, r1), cr::error::ok);
    ASSERT_EQ(eng.trigger<&listener_b>(sig, r2), cr::error::ok);
    ASSERT_EQ(eng.trigger<&listener_c>(sig, r3), cr::error::ok);

    // All three are now suspended at listen().
    EXPECT_EQ(r1, 0);
    EXPECT_EQ(r2, 0);
    EXPECT_EQ(r3, 0);

    sig.fire(99);

    EXPECT_EQ(r1, 99);
    EXPECT_EQ(r2, 99);
    EXPECT_EQ(r3, 99);
}

// -----------------------------------------------------------------------
// Test 4 — multiple listen/fire cycles
// -----------------------------------------------------------------------

TEST(SignalTest, MultipleCycles) {
    cr::signal<int> sig;
    int out1 = 0, out2 = 0;

    cr::engine<cr::default_config, cr::test::mock_clock,
               &two_cycle_listener> eng;

    ASSERT_EQ(eng.trigger<&two_cycle_listener>(sig, out1, out2),
              cr::error::ok);

    // Cycle 1
    sig.fire(10);
    EXPECT_EQ(out1, 10);
    EXPECT_EQ(out2, 0);  // second listen() not yet satisfied

    // Cycle 2
    sig.fire(20);
    EXPECT_EQ(out1, 10);
    EXPECT_EQ(out2, 20);
}

// -----------------------------------------------------------------------
// Test 5 — listener limit exceeded
// -----------------------------------------------------------------------

TEST(SignalTest, ListenerLimitExceeded) {
    cr::signal<int, 1> sig;
    bool overflow = false;

    cr::engine<cr::default_config, cr::test::mock_clock,
               &filler, &overflow_detector> eng;

    // Fill the single listener slot.
    ASSERT_EQ(eng.trigger<&filler>(sig), cr::error::ok);

    // The next listen() should detect overflow.  Because the overflow
    // checker never suspends (await_suspend returns false), it runs to
    // completion during trigger().  The slot is free again immediately.
    ASSERT_EQ(eng.trigger<&overflow_detector>(sig, overflow),
              cr::error::ok);
    EXPECT_TRUE(overflow);
}

// -----------------------------------------------------------------------
// Test 6 — engine integration: a periodic task fires a signal and a
//          listener task receives the value through tick()
//
// Both tasks use timers so tick() step (a) skips them (has_timer == true).
// The consumer is triggered *first* so its timer sits at index 0 and is
// processed before the producer in step (b).  This ensures the consumer
// is already listening when the producer fires the signal.
// -----------------------------------------------------------------------

// Consumer: waits a minimal amount, then listens for the signal.
// The tiny delay gives this task a timer entry, preventing tick()
// step (a) from treating the signal-waiting task as runnable.
cr::task consumer(cr::signal<int>& sig, int& out) {
    co_await cr::delay_ms<cr::test::mock_clock>(0ms);
    out = co_await sig.listen();
    co_return;
}

// Producer: waits a while, then fires the current counter on the signal.
cr::task producer(cr::signal<int>& sig, int& counter) {
    co_await cr::delay_ms<cr::test::mock_clock>(50ms);
    sig.fire(counter);
    co_return;
}

TEST(SignalTest, EngineIntegration) {
    cr::signal<int> sig;
    int counter = 42;
    int received = 0;

    cr::engine<cr::default_config, cr::test::mock_clock,
               &consumer, &producer> eng;

    // Trigger consumer FIRST so its timer sits at index 0 and will be
    // processed before the producer's timer in tick() step (b).
    ASSERT_EQ(eng.trigger<&consumer>(sig, received),
              cr::error::ok);
    ASSERT_EQ(eng.trigger<&producer>(sig, counter), cr::error::ok);

    EXPECT_EQ(received, 0);

    // Advance clock by 50ms — both timers now expire.
    cr::test::mock_clock::advance(50ms);

    // tick() step (a): both tasks have pending timers → skipped.
    // tick() step (b): consumer timer (0ms) processed first → consumer
    //   resumes from delay_ms, calls listen(), suspends on signal.  Then
    //   producer timer (50ms) processed → producer fires the signal.
    eng.tick();

    EXPECT_EQ(received, 42);
}

}  // anonymous namespace

#include <cgx/reactor/clock.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

// ---------------------------------------------------------------------------
// Concept conformance
// ---------------------------------------------------------------------------

static_assert(cgx::reactor::clock<cgx::reactor::steady_clock>,
              "steady_clock must satisfy the clock concept");

static_assert(cgx::reactor::clock<cgx::reactor::test::mock_clock>,
              "mock_clock must satisfy the clock concept");

// ---------------------------------------------------------------------------
// mock_clock unit tests
// ---------------------------------------------------------------------------

TEST(MockClock, now_returns_set_time) {
    using namespace std::chrono_literals;
    auto const tp = cgx::reactor::steady_clock::now();
    cgx::reactor::test::mock_clock::set(tp);
    EXPECT_EQ(tp, cgx::reactor::test::mock_clock::now());
}

TEST(MockClock, advance_moves_time_forward) {
    using namespace std::chrono_literals;
    auto const start = cgx::reactor::steady_clock::now();
    cgx::reactor::test::mock_clock::set(start);
    cgx::reactor::test::mock_clock::advance(100ms);
    EXPECT_EQ(start + 100ms, cgx::reactor::test::mock_clock::now());
}

TEST(MockClock, advance_is_accumulative) {
    using namespace std::chrono_literals;
    auto const start = cgx::reactor::steady_clock::now();
    cgx::reactor::test::mock_clock::set(start);
    cgx::reactor::test::mock_clock::advance(50ms);
    cgx::reactor::test::mock_clock::advance(150ms);
    EXPECT_EQ(start + 200ms, cgx::reactor::test::mock_clock::now());
}

#pragma once

#include <chrono>
#include <concepts>

namespace cgx::reactor {

template <typename C>
concept clock = requires {
    typename C::time_point;
    typename C::duration;
    { C::now() } -> std::same_as<typename C::time_point>;
};

// steady_clock is the default clock type for the engine.  It provides
// monotonic time points and durations based on std::chrono::steady_clock.
struct steady_clock {
    using time_point = std::chrono::steady_clock::time_point;
    using duration   = std::chrono::steady_clock::duration;

    static time_point now() noexcept {
        return std::chrono::steady_clock::now();
    }
};

static_assert(clock<steady_clock>, "steady_clock must satisfy the clock concept");

} // namespace cgx::reactor

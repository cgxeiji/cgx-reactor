#pragma once

#include <chrono>
#include <concepts>

namespace cgx::reactor {

template <typename C>
concept Clock = requires {
    typename C::time_point;
    typename C::duration;
    { C::now() } -> std::same_as<typename C::time_point>;
};

struct steady_clock {
    using time_point = std::chrono::steady_clock::time_point;
    using duration   = std::chrono::steady_clock::duration;

    static time_point now() noexcept {
        return std::chrono::steady_clock::now();
    }
};

static_assert(Clock<steady_clock>, "steady_clock must satisfy the Clock concept");

} // namespace cgx::reactor

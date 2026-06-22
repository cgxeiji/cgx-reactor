#pragma once

#include <chrono>
#include <cstdint>

// Forward-declared by Pico SDK; included via pico/stdlib.h in main.cpp
extern "C" uint64_t time_us_64(void);

/// Thin clock wrapper around the Pico hardware timer.
///
/// Satisfies cgx::reactor::clock. Uses time_us_64() for microsecond
/// precision since boot, avoiding the std::chrono::steady_clock indirection.
struct pico_clock {
    using duration   = std::chrono::steady_clock::duration;
    using time_point = std::chrono::steady_clock::time_point;

    static time_point now() noexcept {
        return time_point{std::chrono::microseconds{time_us_64()}};
    }
};

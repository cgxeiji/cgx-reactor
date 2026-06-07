#pragma once

#include <chrono>

namespace cgx::reactor::test {

struct mock_clock {
    using time_point = std::chrono::steady_clock::time_point;
    using duration   = std::chrono::steady_clock::duration;

    static time_point now_;

    static time_point now() noexcept { return now_; }

    static void advance(std::chrono::milliseconds ms) noexcept { now_ += ms; }

    static void set(time_point tp) noexcept { now_ = tp; }
};

inline mock_clock::time_point mock_clock::now_{};

} // namespace cgx::reactor::test

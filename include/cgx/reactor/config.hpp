#pragma once

#include <cstddef>

namespace cgx::reactor {

struct default_config {
    static constexpr std::size_t max_timers = 16;
    static constexpr std::size_t max_signal_listeners = 8;
};

} // namespace cgx::reactor

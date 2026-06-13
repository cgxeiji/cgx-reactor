#pragma once

#include <cstddef>

#include <cgx/reactor/logger.hpp>

namespace cgx::reactor {

struct default_config {
    static constexpr std::size_t max_timers = 16;
    static constexpr std::size_t max_signal_listeners = 8;
    static constexpr log_level min_level = log_level::info;
};

} // namespace cgx::reactor

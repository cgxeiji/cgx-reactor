#pragma once

#include <cstddef>

#include <cgx/reactor/logger.hpp>

namespace cgx::reactor {

struct default_config {
    static constexpr std::size_t max_timers = 16;
    static constexpr std::size_t max_signal_listeners = 8;
    static constexpr std::size_t reserved_pool_size = 8192;
    static constexpr std::size_t scratchpad_pool_size = 2048;
    static constexpr log_level min_level = log_level::info;
};

} // namespace cgx::reactor

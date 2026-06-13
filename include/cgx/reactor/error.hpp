#pragma once

#include <string_view>

namespace cgx::reactor {

enum class error {
    ok,
    task_already_running,
    capacity_exceeded,
    listener_limit_exceeded,
    closed
};

constexpr std::string_view to_string(error e) noexcept {
    using namespace std::string_view_literals;
    switch (e) {
        case error::ok: return "ok"sv;
        case error::task_already_running: return "task_already_running"sv;
        case error::capacity_exceeded: return "capacity_exceeded"sv;
        case error::listener_limit_exceeded: return "listener_limit_exceeded"sv;
        case error::closed: return "closed"sv;
    }
    return "unknown"sv;
}

} // namespace cgx::reactor

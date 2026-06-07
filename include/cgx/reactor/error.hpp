#pragma once

#include <string_view>

namespace cgx::reactor {

enum class error {
    ok,
    task_already_running,
    queue_full,
    invalid_task,
    listener_limit_exceeded
};

constexpr std::string_view to_string(error e) noexcept {
    using namespace std::string_view_literals;
    switch (e) {
        case error::ok: return "ok"sv;
        case error::task_already_running: return "task_already_running"sv;
        case error::queue_full: return "queue_full"sv;
        case error::invalid_task: return "invalid_task"sv;
        case error::listener_limit_exceeded: return "listener_limit_exceeded"sv;
    }
    return "unknown"sv;
}

} // namespace cgx::reactor

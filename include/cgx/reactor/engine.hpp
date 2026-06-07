#pragma once

#include <cgx/reactor/task.hpp>
#include <cgx/reactor/config.hpp>
#include <cgx/reactor/error.hpp>
#include <cgx/reactor/timer.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <new>
#include <thread>
#include <type_traits>
#include <utility>

namespace cgx::reactor {

namespace detail {

// -------------------------------------------------------------------------
// Compile-time index lookup: find position of Target in a pack of NTTPs.
// Works with heterogeneous function pointer types.
// -------------------------------------------------------------------------

template <auto Target, auto First, auto... Rest>
consteval std::size_t index_of() {
    // Only compare if types match; otherwise skip
    if constexpr (std::is_same_v<decltype(First), decltype(Target)>) {
        if constexpr (First == Target) {
            return 0;
        } else if constexpr (sizeof...(Rest) == 0) {
            return static_cast<std::size_t>(-1);  // not found
        } else {
            return 1 + index_of<Target, Rest...>();
        }
    } else {
        // Types don't match, skip this one
        if constexpr (sizeof...(Rest) == 0) {
            return static_cast<std::size_t>(-1);  // not found
        } else {
            return 1 + index_of<Target, Rest...>();
        }
    }
}

// -------------------------------------------------------------------------
// Slot: owns coroutine-frame storage and tracks occupancy.
// -------------------------------------------------------------------------

template <std::size_t SlotSize>
struct slot {
    alignas(std::max_align_t) std::byte storage[SlotSize];
    task current_task;
    bool occupied = false;
    bool suspended = true;  // true if task is suspended (waiting for timer/signal/etc)
};

}  // namespace detail

/// Compile-time task-registering coroutine engine.
///
/// \tparam Config     Policy type (e.g. default_config).
/// \tparam ClockType  Clock satisfying the Clock concept.
/// \tparam TaskPtrs   Function pointers to registered task coroutines
///                    (e.g. `&hello_task`, `&producer_task`).
///
/// Each task is uniquely identified by its function pointer (NTTP).
/// No signature collisions are possible — two tasks with the same
/// signature are still distinct slots.
template <typename Config, typename ClockType, auto... TaskPtrs>
class engine {
    static constexpr std::size_t num_tasks = sizeof...(TaskPtrs);

    // Per-task slot storage size.
    // Override by defining Config::task_frame_size, otherwise 1024.
    static constexpr std::size_t slot_storage_size() noexcept {
        if constexpr (requires { Config::task_frame_size; }) {
            return Config::task_frame_size;
        } else {
            return 1024;
        }
    }

    using slot_t = detail::slot<slot_storage_size()>;
    std::array<slot_t, num_tasks> slots_{};

    template <auto Fn>
    static constexpr std::size_t slot_index() {
        constexpr auto idx = detail::index_of<Fn, TaskPtrs...>();
        static_assert(idx < num_tasks,
                      "Task not registered with this engine. "
                      "Pass the function pointer as a template argument to engine<...>.");
        return idx;
    }

    template <typename F>
    void for_each_slot(F&& f) {
        for (auto& s : slots_) f(s);
    }

    /// Static callback for the thread-local timer registrar.
    static error timer_registrar_add(void* ctx,
                                      std::chrono::steady_clock::time_point wake,
                                      std::coroutine_handle<> h) noexcept {
        return static_cast<engine*>(ctx)->add_timer(wake, h);
    }

    /// Static callback to mark a task as suspended on an external event.
    static void mark_suspended(void* ctx, std::coroutine_handle<> h) noexcept {
        auto* self = static_cast<engine*>(ctx);
        for (auto& s : self->slots_) {
            if (s.current_task.handle() && s.current_task.handle().address() == h.address()) {
                s.suspended = true;
                break;
            }
        }
    }

    std::array<timer_entry, Config::max_timers> timers_{};
    std::size_t timer_count_ = 0;

public:
    engine() = default;

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

    ~engine() {
        for_each_slot([](slot_t& s) {
            if (s.current_task.handle()) {
                s.current_task.handle().destroy();
            }
        });
    }

    /// Register a timer entry (internal API for delay_ms).
    error add_timer(std::chrono::steady_clock::time_point wake,
                    std::coroutine_handle<> h) noexcept {
        if (timer_count_ >= Config::max_timers) {
            return error::queue_full;
        }
        timers_[timer_count_++] = {wake, h};
        return error::ok;
    }

    /// Trigger a registered task coroutine.
    ///
    /// \tparam Fn  The function pointer of the task (must be one of TaskPtrs).
    /// \param args Arguments forwarded to the coroutine function.
    /// \return error::ok on success, error::task_already_running if active.
    ///
    /// Usage:
    ///   eng.trigger<&my_task>(arg1, arg2);
    template <auto Fn, typename... Args>
    error trigger(Args&&... args) {
        constexpr auto idx = slot_index<Fn>();
        auto& s = slots_[idx];

        if (s.occupied) {
            return error::task_already_running;
        }

        // Destroy any previous coroutine frame in the slot buffer.
        if (s.current_task.handle()) {
            s.current_task.handle().destroy();
        }
        s.current_task = task{};

        // Point the thread-local allocator at the slot buffer and invoke the
        // coroutine. The promise_type::operator new will return this buffer.
        // With initial_suspend = suspend_always, the coroutine suspends immediately,
        // allowing us to store the handle before any user code runs.
        detail::current_task_allocator = {s.storage, sizeof(s.storage)};
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};
        s.suspended = false;  // Task is running
        s.current_task = task{Fn(std::forward<Args>(args)...)};
        
        // Now the handle is stored in the slot. Resume to start execution.
        s.current_task.handle().resume();
        
        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};

        // If the coroutine ran to completion immediately, the slot is free.
        if (s.current_task.handle().done()) {
            s.occupied = false;
        } else {
            s.occupied = true;
        }

        return error::ok;
    }

    /// Resume expired timers and their associated coroutines.
    void tick() {
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};

        // Clean up completed tasks (e.g., tasks that completed via signal resume)
        for_each_slot([this](slot_t& s) {
            if (s.occupied && s.current_task.handle() && s.current_task.handle().done()) {
                s.occupied = false;
            }
        });

        // (a) Resume directly-runnable tasks — occupied, not done,
        //     not suspended on external event, and NOT waiting on a pending timer.
        for_each_slot([this](slot_t& s) {
            if (s.occupied && s.current_task.handle() &&
                !s.current_task.handle().done() && !s.suspended) {
                bool has_timer = false;
                for (std::size_t j = 0; j < timer_count_; ++j) {
                    if (timers_[j].handle == s.current_task.handle()) {
                        has_timer = true;
                        break;
                    }
                }
                if (!has_timer) {
                    s.current_task.handle().resume();
                    if (s.current_task.handle().done()) {
                        s.occupied = false;
                    }
                }
            }
        });

        // (b) Collect all expired timers first (maintains FIFO order)
        std::array<std::coroutine_handle<>, Config::max_timers> expired;
        std::size_t expired_count = 0;
        
        auto now = ClockType::now();
        for (std::size_t i = 0; i < timer_count_;) {
            if (timers_[i].wake_time <= now) {
                expired[expired_count++] = timers_[i].handle;
                // Shift remaining elements to maintain order
                for (std::size_t j = i; j < timer_count_ - 1; ++j) {
                    timers_[j] = timers_[j + 1];
                }
                --timer_count_;
                // Don't increment i, check the new element at index i
            } else {
                ++i;
            }
        }
        
        // (c) Resume all expired timers
        for (std::size_t i = 0; i < expired_count; ++i) {
            auto h = expired[i];
            for_each_slot([&](slot_t& s) {
                if (s.current_task.handle() == h) {
                    s.suspended = false;
                }
            });
            h.resume();
            if (h.done()) {
                for_each_slot([&](slot_t& s) {
                    if (s.current_task.handle() == h) {
                        s.occupied = false;
                    }
                });
            }
        }

        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};
    }

    /// Run the event loop (blocking).
    void run() {
        for (;;) {
            tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
};

}  // namespace cgx::reactor

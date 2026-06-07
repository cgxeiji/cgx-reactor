#pragma once

#include <coroutine>
#include <cstddef>
#include <new>
#include <utility>

namespace cgx::reactor {

namespace detail {

// Thread-local allocator used by the engine to pass the slot buffer
// to the coroutine's promise_type::operator new.
inline thread_local struct {
    void* buffer = nullptr;
    std::size_t size = 0;
} current_task_allocator;

} // namespace detail

/// A non-owning coroutine task type.
///
/// The caller (engine) manages the coroutine frame lifetime via dedicated
/// per-task slots.  The task object itself does NOT destroy the handle --
/// that is the engine's responsibility.
struct task {
    struct promise_type {
        void* operator new(std::size_t sz) {
            if (detail::current_task_allocator.buffer) {
                if (sz > detail::current_task_allocator.size) {
                    std::terminate();  // frame too large for slot
                }
                auto* buf = detail::current_task_allocator.buffer;
                detail::current_task_allocator = {};  // consumed
                return buf;
            }
            // Standalone use (no engine) – heap allocate.
            return ::operator new(sz);
        }

        void operator delete(void*, std::size_t) noexcept {
            // No-op: engine owns slot-allocated frames.
        }

        task get_return_object() noexcept {
            return task{
                std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() noexcept { return {}; }

        std::suspend_always final_suspend() noexcept { return {}; }

        void return_void() noexcept {}

        void unhandled_exception() noexcept { std::terminate(); }
    };

    std::coroutine_handle<promise_type> handle_;

    task() noexcept : handle_(nullptr) {}

    explicit task(std::coroutine_handle<promise_type> h) noexcept
        : handle_(h) {}

    task(task&& other) noexcept
        : handle_(std::exchange(other.handle_, {})) {}

    task& operator=(task&& other) noexcept {
        if (this != &other) {
            handle_ = std::exchange(other.handle_, {});
        }
        return *this;
    }

    // Non-owning: engine manages frame lifetime.
    ~task() = default;

    std::coroutine_handle<promise_type> handle() const noexcept {
        return handle_;
    }

    explicit operator bool() const noexcept { return handle_ != nullptr; }
};

} // namespace cgx::reactor

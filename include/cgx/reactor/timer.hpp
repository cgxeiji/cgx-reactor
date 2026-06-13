#pragma once

#include <cgx/reactor/error.hpp>
#include <cgx/reactor/logger.hpp>

#include <chrono>
#include <coroutine>
#include <cstddef>

namespace cgx::reactor {

// ---------------------------------------------------------------------------
// Timer entry
// ---------------------------------------------------------------------------

/// A single entry in the engine's timer queue.
struct timer_entry {
    /// Time point type used by all clocks in this barebones reactor.
    using time_point = std::chrono::steady_clock::time_point;

    time_point wake_time;
    std::coroutine_handle<> handle;
};

// ---------------------------------------------------------------------------
// Thread-local timer registrar
//
// The engine installs itself here before invoking/resuming any coroutine
// so that awaitables (e.g. delay_ms) can register timers without needing
// an explicit engine reference in the task signature.
// ---------------------------------------------------------------------------

namespace detail {

/// Function-pointer type for the type-erased timer-registration callback.
using timer_registrar_fn =
    error (*)(void* ctx, timer_entry::time_point wake,
              std::coroutine_handle<> handle);

/// Context handed to delay_ms by the currently-active engine.
struct timer_registrar {
    timer_registrar_fn fn = nullptr;
    void* ctx = nullptr;
};

/// Set by engine::trigger() / engine::tick() before any coroutine resume.
inline thread_local timer_registrar current_timer_registrar{};

/// Function-pointer type for marking a task as suspended on an external event.
using external_suspension_fn = void (*)(void* ctx, std::coroutine_handle<> handle);

/// Context for marking external suspensions.
struct external_suspension_registrar {
    external_suspension_fn fn = nullptr;
    void* ctx = nullptr;
};

/// Set by engine before any coroutine resume, used by awaitables to mark external suspensions.
inline thread_local external_suspension_registrar current_external_suspension_registrar{};

}  // namespace detail

// ---------------------------------------------------------------------------
// delay_ms awaitable
// ---------------------------------------------------------------------------

/// Awaitable that suspends the current coroutine for at least `ms`
/// milliseconds.
///
/// The engine reference is obtained from thread-local storage set by
/// the engine before each resume.  This avoids passing the engine through
/// every task signature.
///
/// \tparam Clock  A type satisfying the Clock concept (e.g. steady_clock
///                or test::mock_clock).
///
/// Usage:
///   co_await delay_ms<steady_clock>(100);
///   co_await delay_ms<mock_clock>(50ms);
///
/// Drift note: delay_ms(N) guarantees the coroutine is resumed at least N ms
/// after suspension, but not exactly at N ms.  Periodic tasks using
/// delay_ms will accumulate drift.  Use delay_until or delay_quantized
/// for precise periodic scheduling.
template <typename Clock>
struct delay_ms {
    using duration = typename Clock::duration;

    duration ms_;
    error result_{error::ok};

    /// Construct with an integer number of milliseconds.
    explicit delay_ms(int ms) noexcept
        : ms_(std::chrono::milliseconds{ms}) {}

    /// Construct with any duration convertible to Clock::duration.
    explicit delay_ms(duration ms) noexcept : ms_(ms) {}

    /// Always suspend — the timer determines when we wake up.
    bool await_ready() const noexcept { return false; }

    /// Register a timer with the currently-active engine.
    bool await_suspend(std::coroutine_handle<> h) noexcept {
        auto& reg = detail::current_timer_registrar;
        if (!reg.fn || !reg.ctx) {
            std::terminate();  // No engine context (should not happen)
        }
        auto wake = Clock::now() + ms_;
        result_ = reg.fn(reg.ctx, wake, h);
        return result_ == error::ok;
    }

    /// Returns error::ok on success, or error::queue_full if the timer
    /// queue was exhausted.
    error await_resume() const noexcept { return result_; }
};

// ---------------------------------------------------------------------------
// delay_until awaitable
// ---------------------------------------------------------------------------

/// Awaitable that suspends the current coroutine until a specific time point.
///
/// Unlike delay_ms, this does not accumulate drift when used in a periodic loop:
///   auto next = Clock::now() + period;
///   while (running) {
///       co_await delay_until<Clock>(next);
///       next += period;
///       // do work
///   }
///
/// \tparam Clock  A type satisfying the Clock concept.
template <typename Clock>
struct delay_until {
    using time_point = typename Clock::time_point;

    time_point wake_time_;
    error result_{error::ok};

    explicit delay_until(time_point tp) noexcept : wake_time_(tp) {}

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> h) noexcept {
        auto& reg = detail::current_timer_registrar;
        if (!reg.fn || !reg.ctx) {
            std::terminate();
        }
        result_ = reg.fn(reg.ctx, wake_time_, h);
        return result_ == error::ok;
    }

    error await_resume() const noexcept { return result_; }
};

// ---------------------------------------------------------------------------
// delay_quantized awaitable
// ---------------------------------------------------------------------------

/// Awaitable that suspends until the next grid-aligned tick.
///
/// Always snaps to the next boundary aligned to the clock epoch:
///   co_await delay_quantized<steady_clock>(100ms);  // next 100ms tick
///   co_await delay_quantized<steady_clock>(100ms);  // next 100ms tick
///
/// If called exactly on a tick boundary, the next tick is used
/// (not the current one).  This guarantees zero drift without
/// needing to track an epoch anchor.
///
/// \tparam Clock  A type satisfying the Clock concept.
template <typename Clock>
struct delay_quantized {
    using duration   = typename Clock::duration;
    using time_point = typename Clock::time_point;

    duration interval_;
    error result_{error::ok};

    explicit delay_quantized(duration interval) noexcept : interval_(interval) {}

    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> h) noexcept {
        auto& reg = detail::current_timer_registrar;
        if (!reg.fn || !reg.ctx) {
            std::terminate();
        }

        auto now = Clock::now();

        // Compute the next grid-aligned tick from epoch.
        // wake = now + (interval - (elapsed % interval))
        auto elapsed = now.time_since_epoch();
        auto remainder = elapsed % interval_;
        auto wake = now + (interval_ - remainder);

        result_ = reg.fn(reg.ctx, wake, h);
        return result_ == error::ok;
    }

    error await_resume() const noexcept { return result_; }
};

}  // namespace cgx::reactor

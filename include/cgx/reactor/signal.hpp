#pragma once

#include <cgx/reactor/config.hpp>
#include <cgx/reactor/error.hpp>
#include <cgx/reactor/logger.hpp>

#include <array>
#include <coroutine>
#include <cstddef>

namespace cgx::reactor {

// ---------------------------------------------------------------------------
// signal<T, MaxListeners> — standalone broadcast primitive
//
// Tasks co_await signal.listen() to suspend until another task (or ISR)
// calls signal.fire(value).  All suspended listeners are resumed directly
// — not via the engine's tick() — with the fired value.
//
// Fire-but-no-listeners is a no-op.
//
// ── Re-entrancy warning ───────────────────────────────────────────────────
// If a listener's resumption immediately calls fire() (or listen()) on the
// same signal, the behaviour is undefined.  fire() iterates the internal
// listener array while resuming handles; a nested fire() or listen()
// modifies that array concurrently.  Do not fire a signal from within a
// listener of the same signal.
// ---------------------------------------------------------------------------

/// Broadcast signal for inter-coroutine communication.
///
/// \tparam T            Value type carried by the signal.
/// \tparam MaxListeners Maximum number of concurrently suspended listeners.
///                      Defaults to the config-provided value.
///
/// \note  Lifetime: when used as a class member, the signal must outlive all
///        coroutines suspended on it.  Destroying the owning object while a
///        listener is suspended produces a dangling reference.
template <typename T, std::size_t MaxListeners = default_config::max_signal_listeners,
          typename Logger = no_logger>
class signal {
    /// Per-listener bookkeeping entry.
    struct listener_entry {
        T* value_ptr;                 // Points into the awaiter's local storage
        std::coroutine_handle<> handle;
    };

    mutable std::array<listener_entry, MaxListeners> listeners_;
    mutable std::size_t count_ = 0;

public:
    // -----------------------------------------------------------------------
    // listen_awaiter
    // -----------------------------------------------------------------------

    /// Awaitable returned by listen().
    ///
    /// Stores the received value locally (in the coroutine frame) so that
    /// no heap allocation is required.  The signal's fire() writes through
    /// a pointer into this storage before resuming the handle.
    struct listen_awaiter {
        T value_{};
        error ec_{error::ok};
        const signal* self_;

        /// Never ready — always suspend (unless the listener array is full).
        bool await_ready() const noexcept { return false; }

        /// Register this listener with the signal, or return early if full.
        ///
        /// \return true  → suspend the coroutine (listener added).
        /// \return false → don't suspend; await_resume is called immediately
        ///                 with a default-constructed value and ec_ = full.
        bool await_suspend(std::coroutine_handle<> h) noexcept {
            if (self_->count_ >= self_->listeners_.size()) {
                ec_ = error::capacity_exceeded;
                log::detail::log_impl<default_config, log_level::error, Logger, steady_clock>(
                    "ERR", "reactor::signal", "listener capacity exceeded (%zu/%zu)",
                    self_->count_, self_->listeners_.size());
                return false;  // resume immediately with error
            }
            self_->listeners_[self_->count_++] = {&value_, h};

            log::detail::log_impl<default_config, log_level::debug, Logger, steady_clock>(
                "DBG", "reactor::signal", "listener registered (%zu/%zu capacity)",
                self_->count_, self_->listeners_.size());

            // Mark this task as suspended on an external event (signal)
            auto& reg = detail::current_external_suspension_registrar;
            if (reg.fn && reg.ctx) {
                reg.fn(reg.ctx, h);
            }

            return true;  // suspend
        }

        /// Return the value that was written by fire().
        T await_resume() noexcept { return value_; }
    };

    /// Return an awaitable that suspends the calling coroutine until
    /// fire(value) is called.
    ///
    /// If the listener array is full, the awaitable does NOT suspend and
    /// await_resume returns a default-constructed T.  Check the awaiter's
    /// `ec` member for error::listener_limit_exceeded.
    listen_awaiter listen() const { return listen_awaiter{{}, {}, this}; }

    // -----------------------------------------------------------------------
    // fire
    // -----------------------------------------------------------------------

    /// Broadcast `value` to every currently suspended listener.
    ///
    /// Each listener is resumed synchronously during this call.  The
    /// internal listener array is cleared after all listeners have been
    /// notified.
    ///
    /// \note  Non-const by design.  Paired with const listen(), this prevents
    ///        consumers from calling fire() through a const getter (the
    ///        recommended encapsulation pattern).
    ///
    /// \warning Re-entrant calls to fire() or listen() from within a
    ///          listener's resumption produce undefined behaviour.
    void fire(T value) {
        auto n = count_;
        // Clear the listener table *before* resuming so that any
        // listen() call from within a resumed coroutine registers
        // fresh entries rather than modifying the current batch.
        count_ = 0;

        log::detail::log_impl<default_config, log_level::info, Logger, steady_clock>(
            "INF", "reactor::signal", "fired to %zu listener(s)", n);

        for (std::size_t i = 0; i < n; ++i) {
            *listeners_[i].value_ptr = value;
            listeners_[i].handle.resume();
        }
    }
};

}  // namespace cgx::reactor

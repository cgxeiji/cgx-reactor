#pragma once

#include <cgx/reactor/error.hpp>
#include <cgx/reactor/logger.hpp>
#include <cgx/reactor/timer.hpp>

#include <array>
#include <coroutine>
#include <cstddef>
#include <optional>
#include <utility>

namespace cgx::reactor {

// ---------------------------------------------------------------------------
// channel<T, Capacity> — point-to-point producer/consumer queue
//
// Tasks co_await channel.push(value) to send data, or co_await channel.pop()
// to receive data.  If the buffer is full, push() suspends the producer.
// If the buffer is empty, pop() suspends the consumer.
//
// Direct-resume semantics (like signals): when a push() satisfies a waiting
// consumer (or vice versa), the waiter is resumed immediately — not via the
// engine's tick().
//
// ISR safety: try_push() never blocks.  If a consumer is suspended on pop(),
// try_push() will resume it immediately (including from ISR context).  Keep
// consumer logic short or use a local buffer pattern.
//
// ── Re-entrancy warning ───────────────────────────────────────────────────
// If a consumer's resumption immediately calls pop() or try_push() on the
// same channel (or a producer's resumption calls push() or try_push()), the
// behaviour is undefined.  Same constraint as signal<T>.
// ---------------------------------------------------------------------------

/// Bounded multi-producer multi-consumer channel.
///
/// After close(), buffered data is still readable — consumers drain
/// pending values before getting nullopt.  New push() / try_push()
/// calls fail with error::closed.
///
/// \tparam T        Value type.
/// \tparam Capacity Maximum buffered elements (also bounds the number of
///                  suspended producers and consumers).  Must be > 0.
template <typename T, std::size_t Capacity, typename Logger = no_logger>
class channel {
    static_assert(Capacity > 0, "Channel capacity must be > 0");
    // Ring buffer for queued data.
    // buffer_, head_, count_, and the consumer-wait-queue are mutable
    // so that pop() can be const (mirroring signal::listen()).
    mutable std::array<T, Capacity> buffer_{};
    mutable std::size_t head_ = 0;
    mutable std::size_t tail_ = 0;
    mutable std::size_t count_ = 0;

    // Producer wait queue — producers suspended because buffer was full.
    struct producer_entry {
        T* value_ptr;
        std::coroutine_handle<> handle;
    };
    mutable std::array<producer_entry, Capacity> prod_waiters_{};
    mutable std::size_t prod_count_ = 0;

    // Consumer wait queue — consumers suspended because buffer was empty.
    struct consumer_entry {
        std::optional<T>* dest_ptr;
        std::coroutine_handle<> handle;
    };
    mutable std::array<consumer_entry, Capacity> cons_waiters_{};
    mutable std::size_t cons_count_ = 0;

    bool closed_ = false;

public:
    // -----------------------------------------------------------------------
    // push_awaiter
    // -----------------------------------------------------------------------

    /// Awaitable returned by push().
    struct push_awaiter {
        T value_;
        error ec_{error::ok};
        channel* self_;

        bool await_ready() const noexcept { return false; }

        bool await_suspend(std::coroutine_handle<> h) noexcept {
            // 1) Closed — fail immediately.  Must be first so push() on a
            //    closed channel always fails, consistent with try_push().
            if (self_->closed_) {
                ec_ = error::closed;
                log::detail::log_impl<default_config, log_level::error, Logger, steady_clock>(
                    "ERR", "reactor::channel", "push rejected, channel closed");
                return false;
            }

            // 2) Hand off to a waiting consumer.
            if (self_->cons_count_ > 0) {
                auto& entry = self_->cons_waiters_[0];
                *entry.dest_ptr = std::move(value_);
                auto h_cons = entry.handle;
                // Shift consumer queue.
                for (std::size_t i = 0; i < self_->cons_count_ - 1; ++i)
                    self_->cons_waiters_[i] = self_->cons_waiters_[i + 1];
                --self_->cons_count_;
                h_cons.resume();
                ec_ = error::ok;
                log::detail::log_impl<default_config, log_level::debug, Logger, steady_clock>(
                    "DBG", "reactor::channel", "pushed, handed off to consumer");
                return false;
            }

            // 3) Buffer has space.
            if (self_->count_ < Capacity) {
                self_->buffer_[self_->tail_] = std::move(value_);
                self_->tail_ = (self_->tail_ + 1) % Capacity;
                ++self_->count_;
                ec_ = error::ok;
                log::detail::log_impl<default_config, log_level::debug, Logger, steady_clock>(
                    "DBG", "reactor::channel", "pushed, buffered (%zu/%zu capacity)",
                    self_->count_, Capacity);
                return false;
            }

            // 4) Buffer full and not closed — register as producer waiter.
            self_->prod_waiters_[self_->prod_count_++] = {&value_, h};

            log::detail::log_impl<default_config, log_level::warn, Logger, steady_clock>(
                "WRN", "reactor::channel", "push suspended, buffer full (%zu/%zu)",
                self_->count_, Capacity);

            // Notify the engine that this task is suspended on an external
            // event, so tick() doesn't treat it as directly-runnable.
            auto& reg = detail::current_external_suspension_registrar;
            if (reg.fn && reg.ctx) {
                reg.fn(reg.ctx, h);
            }

            return true;
        }

        error await_resume() noexcept {
            return self_->closed_ ? error::closed : ec_;
        }
    };

    // -----------------------------------------------------------------------
    // pop_awaiter
    // -----------------------------------------------------------------------

    /// Awaitable returned by pop().
    struct pop_awaiter {
        std::optional<T> result_;
        const channel* self_;

        bool await_ready() const noexcept { return false; }

        bool await_suspend(std::coroutine_handle<> h) noexcept {
            // 1) Buffer has data — satisfy immediately and wake a producer.
            if (self_->count_ > 0) {
                result_ = std::move(self_->buffer_[self_->head_]);
                self_->head_ = (self_->head_ + 1) % Capacity;
                --self_->count_;

                // If a producer is waiting, move its value into the buffer
                // and resume it.
                if (self_->prod_count_ > 0) {
                    self_->buffer_[self_->tail_] =
                        std::move(*self_->prod_waiters_[0].value_ptr);
                    self_->tail_ = (self_->tail_ + 1) % Capacity;
                    ++self_->count_;
                    auto h_prod = self_->prod_waiters_[0].handle;
                    for (std::size_t i = 0; i < self_->prod_count_ - 1; ++i)
                        self_->prod_waiters_[i] = self_->prod_waiters_[i + 1];
                    --self_->prod_count_;
                    h_prod.resume();

                    log::detail::log_impl<default_config, log_level::debug, Logger, steady_clock>(
                        "DBG", "reactor::channel", "popped, handed off from producer");
                } else {
                    log::detail::log_impl<default_config, log_level::debug, Logger, steady_clock>(
                        "DBG", "reactor::channel", "popped, buffer had data");
                }

                return false;
            }

            // 2) Closed — return empty.
            if (self_->closed_) {
                result_ = std::nullopt;
                log::detail::log_impl<default_config, log_level::info, Logger, steady_clock>(
                    "INF", "reactor::channel", "pop returning nullopt, channel closed");
                return false;
            }

            // 3) Empty and not closed — register as consumer waiter.
            self_->cons_waiters_[self_->cons_count_++] = {&result_, h};

            log::detail::log_impl<default_config, log_level::warn, Logger, steady_clock>(
                "WRN", "reactor::channel", "pop suspended, buffer empty");

            // Notify the engine that this task is suspended on an external
            // event, so tick() doesn't treat it as directly-runnable.
            auto& reg = detail::current_external_suspension_registrar;
            if (reg.fn && reg.ctx) {
                reg.fn(reg.ctx, h);
            }

            return true;
        }

        std::optional<T> await_resume() noexcept {
            // result_ is set either from the buffer (data available) or by
            // close() (nullopt).  Just return it — don't re-check closed_
            // here because data buffered before close() is still valid.
            return std::move(result_);
        }
    };

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Blocking push — suspends if the buffer is full and no consumer is
    /// immediately available.
    ///
    /// Returns error::ok on success, error::closed if the channel was closed
    /// while (or before) the producer was waiting.
    push_awaiter push(T value) {
        return push_awaiter{std::move(value), error::ok, this};
    }

    /// Blocking pop — suspends if the buffer is empty and the channel is
    /// not closed.
    ///
    /// Returns the value on success, or std::nullopt if the channel was
    /// closed while (or before) the consumer was waiting.
    ///
    /// Const-qualified so a const channel& can receive (mirrors Go's
    /// receive-only <-chan T pattern).  Internal mutation is via mutable
    /// members, analogous to signal::listen().
    pop_awaiter pop() const {
        return pop_awaiter{std::nullopt, this};
    }

    /// Non-blocking push.  Returns immediately.
    ///
    /// - error::ok: value was buffered or handed off to a waiting consumer.
    /// - error::capacity_exceeded: buffer full, no consumer waiting.
    /// - error::closed: channel is closed.
    ///
    /// \note  If a consumer is suspended on pop(), try_push() will resume it
    ///        immediately (including from ISR context).
    error try_push(T value) {
        if (closed_) {
            log::detail::log_impl<default_config, log_level::error, Logger, steady_clock>(
                "ERR", "reactor::channel", "try_push rejected, channel closed");
            return error::closed;
        }

        // Hand off to a waiting consumer.
        if (cons_count_ > 0) {
            auto& entry = cons_waiters_[0];
            *entry.dest_ptr = std::move(value);
            auto h = entry.handle;
            for (std::size_t i = 0; i < cons_count_ - 1; ++i)
                cons_waiters_[i] = cons_waiters_[i + 1];
            --cons_count_;
            h.resume();
            log::detail::log_impl<default_config, log_level::debug, Logger, steady_clock>(
                "DBG", "reactor::channel", "try_push handed off to consumer");
            return error::ok;
        }

        // Buffer has space.
        if (count_ < Capacity) {
            buffer_[tail_] = std::move(value);
            tail_ = (tail_ + 1) % Capacity;
            ++count_;
            log::detail::log_impl<default_config, log_level::debug, Logger, steady_clock>(
                "DBG", "reactor::channel", "try_push buffered (%zu/%zu capacity)",
                count_, Capacity);
            return error::ok;
        }

        log::detail::log_impl<default_config, log_level::warn, Logger, steady_clock>(
            "WRN", "reactor::channel", "try_push rejected, buffer full");
        return error::capacity_exceeded;
    }

    /// Close the channel.  Wakes all waiting producers (their push() returns
    /// error::closed) and all waiting consumers (their pop() returns
    /// std::nullopt).
    void close() {
        closed_ = true;

        // Wake all waiting producers.
        for (std::size_t i = 0; i < prod_count_; ++i)
            prod_waiters_[i].handle.resume();

        // Wake all waiting consumers with empty optional.
        for (std::size_t i = 0; i < cons_count_; ++i) {
            *cons_waiters_[i].dest_ptr = std::nullopt;
            cons_waiters_[i].handle.resume();
        }

        log::detail::log_impl<default_config, log_level::info, Logger, steady_clock>(
            "INF", "reactor::channel", "closed, woke %zu producer(s) and %zu consumer(s)",
            prod_count_, cons_count_);

        prod_count_ = 0;
        cons_count_ = 0;
    }

    /// Check whether the channel has been closed.
    bool is_closed() const noexcept { return closed_; }

    /// Compile-time buffer capacity.
    static constexpr std::size_t capacity() noexcept { return Capacity; }

    /// Current number of buffered elements.
    std::size_t size() const noexcept { return count_; }
};

}  // namespace cgx::reactor

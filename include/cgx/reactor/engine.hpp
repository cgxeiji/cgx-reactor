#pragma once

#include <cgx/reactor/task.hpp>
#include <cgx/reactor/config.hpp>
#include <cgx/reactor/error.hpp>
#include <cgx/reactor/logger.hpp>
#include <cgx/reactor/timer.hpp>
#include <cgx/reactor/task_list.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <new>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>

namespace cgx::reactor {

// Forward declaration (needed by engine_from_desc_list)
template <typename Config, typename Clock, typename Logger = no_logger, typename... Entries>
class engine;

namespace detail {

// Build engine type from a type_list of descriptors
template <typename Config, typename Clock, typename Logger, typename List>
struct engine_from_desc_list;

template <typename Config, typename Clock, typename Logger, typename... Ds>
struct engine_from_desc_list<Config, Clock, Logger, detail::type_list<Ds...>> {
    using type = engine<Config, Clock, Logger, Ds...>;
};

// -------------------------------------------------------------------------
// compiled_str<N> — constexpr string type for compile-time function names
// -------------------------------------------------------------------------

template <std::size_t N>
struct compiled_str {
    char data[N]{};  // N includes null terminator
    static constexpr std::size_t size = N;  // includes null
    
    constexpr operator const char*() const { return data; }
    constexpr std::string_view view() const { return {data, N - 1}; }
};

// -------------------------------------------------------------------------
// Function-name extraction via __PRETTY_FUNCTION__ (clang/GCC C++20)
//
// Returns a compiled_str<N> at compile time.  Zero heap allocation.
// -------------------------------------------------------------------------

template <auto Fn>
constexpr auto extract_fn_name() {
    constexpr std::string_view pretty = __PRETTY_FUNCTION__;
    constexpr auto prefix = pretty.find("Fn = ");
    static_assert(prefix != std::string_view::npos, "Cannot find Fn = in __PRETTY_FUNCTION__");
    constexpr auto start = prefix + 5;
    constexpr auto end = pretty.find(']', start);
    static_assert(end != std::string_view::npos, "Cannot find ] after Fn = ");
    constexpr auto name = pretty.substr(start, end - start);
    
    // Strip "&" or "&(" / ")"
    if constexpr (name.starts_with("&(") && name.ends_with(")")) {
        constexpr auto stripped = name.substr(2, name.size() - 3);
        compiled_str<stripped.size() + 1> result{};
        for (std::size_t i = 0; i < stripped.size(); ++i)
            result.data[i] = stripped[i];
        return result;
    } else if constexpr (name.starts_with("&")) {
        constexpr auto stripped = name.substr(1);
        compiled_str<stripped.size() + 1> result{};
        for (std::size_t i = 0; i < stripped.size(); ++i)
            result.data[i] = stripped[i];
        return result;
    } else {
        compiled_str<name.size() + 1> result{};
        for (std::size_t i = 0; i < name.size(); ++i)
            result.data[i] = name[i];
        return result;
    }
}

// -------------------------------------------------------------------------
// format_tag<Fn>() — formats tag with truncation, returns const char*
//
// 16 bytes total (15 content + 1 null)
// Short names (≤15 chars): stored as-is
// Long names (>15 chars): ~ + last 14 chars
// -------------------------------------------------------------------------

template <auto Fn>
const char* format_tag() {
    static constexpr auto name = extract_fn_name<Fn>();
    static char tag[16]; // 15 bytes content + 1 byte null
    
    if constexpr (name.size <= 16) {
        // Fits: store as-is (name.size includes null)
        for (std::size_t i = 0; i < name.size; ++i)
            tag[i] = name.data[i];
    } else {
        // Truncate: ~ + last 14 chars
        // name.size includes null, so content length = name.size - 1
        // We want last 14 chars of content: start at (name.size - 1) - 14 = name.size - 15
        tag[0] = '~';
        for (std::size_t i = 0; i < 14; ++i)
            tag[i + 1] = name.data[name.size - 15 + i];
        tag[15] = '\0';
    }
    return tag;
}

/// Fallback frame size for tasks that could not be probed (e.g., tasks
/// with parameters).  The probe only works for no-arg coroutines.
static constexpr std::size_t default_frame_size = 1024;

}  // namespace detail

/// Compile-time task-registering coroutine engine.
template <typename Config, typename ClockType, typename Logger, typename... Entries>
class engine {
    static constexpr std::size_t num_tasks = sizeof...(Entries);

    // Function name storage (pointers to static constexpr strings)
    std::array<const char*, num_tasks> fn_names_{};

    // Reserved pool.
    alignas(std::max_align_t) std::array<std::byte, Config::reserved_pool_size> pool_{};

    // Scratchpad pool.
    alignas(std::max_align_t) std::array<std::byte, Config::scratchpad_pool_size> scratchpad_pool_{};

    // Per-task metadata.
    std::array<detail::task_meta, num_tasks> tasks_{};

    // Sentinel for scratch_offset meaning "not allocated" (0 is a valid offset).
    static constexpr std::size_t scratch_unused = Config::scratchpad_pool_size;

    bool pool_overflow_ = false;

    // -----------------------------------------------------------------------
    // Scratchpad bitmap allocator (16-byte blocks, first-fit)
    // -----------------------------------------------------------------------

    static constexpr std::size_t scratchpad_block_size = 16;
    static constexpr std::size_t num_scratchpad_blocks = Config::scratchpad_pool_size / scratchpad_block_size;
    static constexpr std::size_t scratchpad_bitmap_words = (num_scratchpad_blocks + 63) / 64;
    std::array<std::uint64_t, scratchpad_bitmap_words> scratchpad_bitmap_{};

    // -----------------------------------------------------------------------
    // Scratchpad waiter list — coroutines waiting for pool space.
    // When a scratchpad task completes, the first waiter is resumed.
    // -----------------------------------------------------------------------

    struct scratchpad_waiter {
        std::size_t task_index;
        std::coroutine_handle<> handle;
    };
    static constexpr std::size_t max_scratchpad_waiters = 8;
    std::array<scratchpad_waiter, max_scratchpad_waiters> scratchpad_waiters_{};
    std::size_t scratchpad_waiter_count_ = 0;

    // -----------------------------------------------------------------------
    // Bitmap helpers
    // -----------------------------------------------------------------------

    bool is_allocated(std::size_t block) const noexcept {
        return (scratchpad_bitmap_[block / 64] >> (block % 64)) & 1ULL;
    }
    void set_allocated(std::size_t block) noexcept {
        scratchpad_bitmap_[block / 64] |= (1ULL << (block % 64));
    }
    void clear_allocated(std::size_t block) noexcept {
        scratchpad_bitmap_[block / 64] &= ~(1ULL << (block % 64));
    }

    // First-fit allocation in the scratchpad pool.
    std::size_t scratchpad_allocate(std::size_t size) noexcept {
        if (size == 0) size = scratchpad_block_size;
        std::size_t needed = (size + scratchpad_block_size - 1) / scratchpad_block_size;
        for (std::size_t start = 0; start + needed <= num_scratchpad_blocks; ++start) {
            bool free = true;
            for (std::size_t b = 0; b < needed; ++b) {
                if (is_allocated(start + b)) { free = false; break; }
            }
            if (free) {
                for (std::size_t b = 0; b < needed; ++b) set_allocated(start + b);
                return start * scratchpad_block_size;
            }
        }
        return Config::scratchpad_pool_size; // allocation failed
    }

    void scratchpad_free(std::size_t offset, std::size_t size) noexcept {
        std::size_t start = offset / scratchpad_block_size;
        std::size_t count = (size + scratchpad_block_size - 1) / scratchpad_block_size;
        for (std::size_t b = 0; b < count; ++b) clear_allocated(start + b);
    }

    // -----------------------------------------------------------------------
    // Waiter list management
    // -----------------------------------------------------------------------

public:
    /// Awaiter returned by trigger() for scratchpad tasks.
    struct scratchpad_trigger_awaiter {
        engine* self;
        std::size_t task_idx;
        std::coroutine_handle<> handle;
        bool allocated = false;

        bool await_ready() const noexcept { return false; }

        bool await_suspend(std::coroutine_handle<> h) noexcept {
            handle = h;
            auto& meta = self->tasks_[task_idx];

            // FIFO ordering: if waiters exist, new triggers join the
            // back of the list instead of jumping ahead by allocating
            // immediately, even if space is available.
            if (meta.occupied || self->scratchpad_waiter_count_ > 0) {
                return self->try_add_waiter(task_idx, h);
            }

            std::size_t a = self->scratchpad_allocate(meta.size);
            if (a != self->scratch_unused) {
                self->execute_scratchpad(task_idx, a);
                allocated = true;
                return false;
            }
            return self->try_add_waiter(task_idx, h);
        }

        error await_resume() const noexcept {
            // Check the actual task state — the waiter may have been
            // resumed by try_resume_waiter() which sets occupied=true
            // and scratch_offset, but doesn't update the allocated flag.
            auto& meta = self->tasks_[task_idx];
            if (meta.occupied && meta.scratch_offset != self->scratch_unused)
                return error::ok;
            return allocated ? error::ok : error::capacity_exceeded;
        }
    };

private:
    bool try_add_waiter(std::size_t task_idx, std::coroutine_handle<> h) noexcept {
        if (scratchpad_waiter_count_ < max_scratchpad_waiters) {
            scratchpad_waiters_[scratchpad_waiter_count_++] = {task_idx, h};
            return true;
        }
        return false;
    }

    // Try to allocate for waiters and resume them.
    // Called after a scratchpad task completes (pool space freed).
    void try_resume_waiter() noexcept {
        if (scratchpad_waiter_count_ == 0) return;
        std::size_t i = 0;
        while (i < scratchpad_waiter_count_) {
            auto& w = scratchpad_waiters_[i];
            auto& meta = tasks_[w.task_index];
            if (meta.occupied) { ++i; continue; }
            std::size_t alloc = scratchpad_allocate(meta.size);
            if (alloc != Config::scratchpad_pool_size) {
                execute_scratchpad(w.task_index, alloc);
                w.handle.resume();
                for (std::size_t j = i; j + 1 < scratchpad_waiter_count_; ++j)
                    scratchpad_waiters_[j] = scratchpad_waiters_[j + 1];
                --scratchpad_waiter_count_;
            } else {
                break;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    template <std::size_t I>
    static constexpr bool is_scratchpad_entry() {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        return entry_t::is_scratchpad;
    }

    template <std::size_t... Is>
    static constexpr std::size_t count_scratchpad_impl(std::index_sequence<Is...>) {
        return ((is_scratchpad_entry<Is>() ? std::size_t{1} : std::size_t{0}) + ...);
    }
    static constexpr std::size_t num_scratchpad = count_scratchpad_impl(std::make_index_sequence<num_tasks>{});
    static constexpr std::size_t num_reserved = num_tasks - num_scratchpad;

    void init_fn_names() {
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((fn_names_[Is] = detail::format_tag<Entries::fn>()), ...);
        }(std::make_index_sequence<num_tasks>{});
    }

    std::size_t find_slot_for_handle(std::coroutine_handle<> h) const noexcept {
        for (std::size_t i = 0; i < num_tasks; ++i) {
            if (tasks_[i].handle == h) return i;
        }
        return num_tasks;
    }

    template <auto Fn>
    static constexpr std::size_t slot_index() {
        constexpr auto idx = detail::index_of_fn<Fn, Entries::fn...>();
        static_assert(idx < num_tasks,
                      "Task not registered with this engine.");
        return idx;
    }

    static error timer_registrar_add(void* ctx,
                                      std::chrono::steady_clock::time_point wake,
                                      std::coroutine_handle<> h) noexcept {
        return static_cast<engine*>(ctx)->add_timer(wake, h);
    }

    static void mark_suspended(void* ctx, std::coroutine_handle<> h) noexcept {
        auto* self = static_cast<engine*>(ctx);
        for (auto& t : self->tasks_) {
            if (t.handle && t.handle.address() == h.address()) {
                t.suspended = true;
                break;
            }
        }
    }

    std::size_t total_pool_used() const noexcept {
        if (num_tasks == 0) return 0;
        std::size_t max_end = 0;
        for (std::size_t i = 0; i < num_tasks; ++i) {
            if (!tasks_[i].is_scratchpad) {
                std::size_t end = tasks_[i].offset + tasks_[i].size;
                if (end > max_end) max_end = end;
            }
        }
        return max_end;
    }

    // -----------------------------------------------------------------------
    // Scratchpad coroutine creation (compile-time dispatch for runtime index)
    // -----------------------------------------------------------------------

    template <std::size_t I>
    void execute_scratchpad_at(std::size_t alloc) {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        if constexpr (entry_t::is_scratchpad) {
            auto& meta = tasks_[I];
            meta.scratch_offset = alloc;
            if (meta.handle) { meta.handle.destroy(); meta.handle = {}; }

            detail::current_task_allocator = {&scratchpad_pool_[alloc], meta.size, nullptr};
            detail::current_timer_registrar = {&timer_registrar_add, this};
            detail::current_external_suspension_registrar = {&mark_suspended, this};
            meta.suspended = false;
            meta.occupied = true;

            using fn_type = decltype(entry_t::fn);
            if constexpr (std::is_member_function_pointer_v<fn_type>) {
                using Class = typename entry_t::class_type;
                auto* obj = static_cast<Class*>(meta.self);
                meta.handle = (obj->*entry_t::fn)().handle();
            } else {
                meta.handle = entry_t::fn().handle();
            }
            meta.handle.resume();
            detail::current_external_suspension_registrar = {};
            detail::current_timer_registrar = {};

            if (meta.handle.done()) {
                scratchpad_free(alloc, meta.size);
                meta.scratch_offset = scratch_unused;
                meta.handle = {};
                meta.occupied = false;
                try_resume_waiter();
            }

            log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                "INF", fn_names_[I], "scratchpad triggered");
        }
    }

    void execute_scratchpad(std::size_t idx, std::size_t alloc) {
        [this, idx, alloc]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((idx == Is ? (execute_scratchpad_at<Is>(alloc), 0) : 0), ...);
        }(std::make_index_sequence<num_tasks>{});
    }

    std::array<timer_entry, Config::max_timers> timers_{};
    std::size_t timer_count_ = 0;

    // -----------------------------------------------------------------------
    // Internal: trigger a reserved task at a given index
    // -----------------------------------------------------------------------

    template <std::size_t I, typename... Args>
    error trigger_reserved(Args&&... args) {
        auto& meta = tasks_[I];

        if (pool_overflow_) {
            log::detail::log_impl<Config, log_level::error, Logger, ClockType>(
                "ERR", fn_names_[I], "reserved pool exhausted");
            return error::capacity_exceeded;
        }

        if (meta.occupied) {
            log::detail::log_impl<Config, log_level::warn, Logger, ClockType>(
                "WRN", fn_names_[I], "already running");
            return error::task_already_running;
        }

        if (meta.handle) { meta.handle.destroy(); meta.handle = {}; }

        detail::current_task_allocator = {&pool_[meta.offset], meta.size, nullptr};
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};
        meta.suspended = false;

        using entry_t = typename detail::type_at<I, Entries...>::type;
        using fn_type = decltype(entry_t::fn);
        if constexpr (std::is_member_function_pointer_v<fn_type>) {
            using Class = typename entry_t::class_type;
            auto* obj = static_cast<Class*>(meta.self);
            meta.handle = (obj->*entry_t::fn)(std::forward<Args>(args)...).handle();
        } else {
            meta.handle = entry_t::fn(std::forward<Args>(args)...).handle();
        }

        meta.handle.resume();
        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};

        if (meta.handle.done()) meta.occupied = false;
        else meta.occupied = true;

        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", fn_names_[I], "triggered");
        return error::ok;
    }

    // -----------------------------------------------------------------------
    // Internal: try to trigger a task by instance + function pointer match
    // -----------------------------------------------------------------------

    template <std::size_t I, typename Class, typename Fn, typename... Args>
    bool try_trigger_instance(Class& obj, Fn fn, error& result, Args&&... args) {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        using registered_type = std::remove_cv_t<decltype(entry_t::fn)>;

        if constexpr (std::is_same_v<Fn, registered_type>) {
            if (tasks_[I].self == &obj && entry_t::fn == fn) {
                if constexpr (entry_t::is_scratchpad) {
                    // For scratchpad, use try_trigger since we're in a non-coroutine
                    // context (instance dispatch).
                    result = try_scratchpad(I);
                } else {
                    if (tasks_[I].occupied) {
                        result = error::task_already_running;
                    } else {
                        // Forward args via compile-time dispatch
                        result = trigger_reserved<I>(std::forward<Args>(args)...);
                    }
                }
                return true;
            }
        }
        return false;
    }

    error try_scratchpad(std::size_t idx) {
        auto& meta = tasks_[idx];
        if (meta.occupied) return error::task_already_running;

        // FIFO ordering: if waiters exist, can't jump ahead.
        if (scratchpad_waiter_count_ > 0) return error::capacity_exceeded;

        std::size_t alloc = scratchpad_allocate(meta.size);
        if (alloc == Config::scratchpad_pool_size) return error::capacity_exceeded;

        execute_scratchpad(idx, alloc);
        return error::ok;
    }

public:
    engine() = delete;

    engine(std::array<void*, num_tasks> self_ptrs) : tasks_{} {
        for (std::size_t i = 0; i < num_tasks; ++i) {
            tasks_[i].self = self_ptrs[i];
            tasks_[i].scratch_offset = scratch_unused;
        }
        init_fn_names();
        probe_frame_sizes();
    }

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

    ~engine() {
        for (auto& meta : tasks_) {
            if (meta.handle) meta.handle.destroy();
        }
    }

    error add_timer(std::chrono::steady_clock::time_point wake,
                    std::coroutine_handle<> h) noexcept {
        if (timer_count_ >= Config::max_timers) {
            std::size_t slot_idx = find_slot_for_handle(h);
            const char* tag = (slot_idx < num_tasks) ? fn_names_[slot_idx] : "unknown";
            log::detail::log_impl<Config, log_level::error, Logger, ClockType>(
                "ERR", tag, "timer capacity exceeded (%zu/%zu)",
                static_cast<std::size_t>(timer_count_),
                static_cast<std::size_t>(Config::max_timers));
            return error::capacity_exceeded;
        }
        timers_[timer_count_++] = {wake, h};

        std::size_t slot_idx = find_slot_for_handle(h);
        const char* tag = (slot_idx < num_tasks) ? fn_names_[slot_idx] : "unknown";
        auto delay_ms_val = std::chrono::duration_cast<
            std::chrono::milliseconds>(wake - ClockType::now()).count();
        log::detail::log_impl<Config, log_level::debug, Logger, ClockType>(
            "DBG", tag, "delay %lldms registered",
            static_cast<long long>(delay_ms_val));
        return error::ok;
    }

    // -------------------------------------------------------------------
    // trigger() — blocking (suspends caller if scratchpad pool full)
    // -------------------------------------------------------------------

    /// Trigger a registered task coroutine by function pointer.
    ///
    /// For RESERVED tasks: returns error immediately.
    /// For SCRATCHPAD tasks: returns an awaiter.  In a coroutine context
    /// (co_await), the caller suspends if the pool is full and resumes
    /// when space opens.  In a non-coroutine context, use try_trigger().
    template <auto Fn, typename... Args>
    auto trigger(Args&&... args) {
        constexpr auto idx = slot_index<Fn>();
        using entry_t = typename detail::type_at<idx, Entries...>::type;

        if constexpr (entry_t::is_scratchpad) {
            // Return an awaiter that blocks/suspends when pool is full.
            return scratchpad_trigger_awaiter{this, idx, {}, {}};
        } else {
            return trigger_reserved<idx>(std::forward<Args>(args)...);
        }
    }

    // Non-const member function (instance-based)
    // Instance-based dispatch is always non-blocking (uses try_trigger semantics
    // for scratchpad tasks).  For blocking behavior, use NTTP-based trigger<&fn>().
    template <typename Class, typename Ret, typename... FnArgs, typename... Args>
    error trigger(Class& obj, Ret (Class::*fn)(FnArgs...), Args&&... args) {
        error result = error::task_not_registered;
        [this, &obj, fn, &result, &args...]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((result == error::task_not_registered
                  ? (try_trigger_instance<Is>(obj, fn, result, std::forward<Args>(args)...), 0)
                  : 0),
             ...);
        }(std::make_index_sequence<num_tasks>{});
        return result;
    }

    // Const member function
    template <typename Class, typename Ret, typename... FnArgs, typename... Args>
    error trigger(Class& obj, Ret (Class::*fn)(FnArgs...) const, Args&&... args) {
        error result = error::task_not_registered;
        [this, &obj, fn, &result, &args...]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((result == error::task_not_registered
                  ? (try_trigger_instance<Is>(obj, fn, result, std::forward<Args>(args)...), 0)
                  : 0),
             ...);
        }(std::make_index_sequence<num_tasks>{});
        return result;
    }

    // -------------------------------------------------------------------
    // try_trigger() — non-blocking, returns error immediately
    // -------------------------------------------------------------------

    /// Non-blocking variant: returns error::capacity_exceeded if the
    /// scratchpad pool is full, rather than suspending the caller.
    template <auto Fn, typename... Args>
    error try_trigger(Args&&... args) {
        constexpr auto idx = slot_index<Fn>();
        using entry_t = typename detail::type_at<idx, Entries...>::type;

        if constexpr (entry_t::is_scratchpad) {
            return try_scratchpad(idx);
        } else {
            return trigger_reserved<idx>(std::forward<Args>(args)...);
        }
    }

    engine_report report() const noexcept {
        engine_report r{};
        r.task_count = num_tasks;
        r.reserved_count = num_reserved;
        r.scratchpad_count = num_scratchpad;
        r.scratchpad_size = Config::scratchpad_pool_size;
        return r;
    }

    /// Dump via Logger::print().
    engine_report dump() const {
        auto r = report();
        dump_pool_summary_log();
        dump_scratchpad_summary_log();
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((dump_one_line_log<Is>()), ...);
        }(std::make_index_sequence<num_tasks>{});
        return r;
    }

    /// Dump via custom sink.
    template <typename Sink>
    engine_report dump(Sink&& sink) const {
        auto r = report();
        dump_pool_summary_sink(sink);
        dump_scratchpad_summary_sink(sink);
        [this, &sink]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((dump_one_line_sink<Is>(sink)), ...);
        }(std::make_index_sequence<num_tasks>{});
        return r;
    }

private:
    // -----------------------------------------------------------------------
    // Frame-size probing
    // -----------------------------------------------------------------------

    void probe_frame_sizes() {
        std::size_t current_offset = 0;
        [this, &current_offset]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((probe_frame_size<Is>(current_offset)), ...);
        }(std::make_index_sequence<num_tasks>{});
    }

    template <std::size_t I>
    void probe_frame_size(std::size_t& current_offset) {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        using fn_type = std::remove_cv_t<decltype(entry_t::fn)>;
        tasks_[I].is_scratchpad = entry_t::is_scratchpad;

        if constexpr (entry_t::is_scratchpad) {
            tasks_[I].offset = 0;
            if (pool_overflow_) { tasks_[I].size = 0; return; }
            std::size_t remaining = Config::reserved_pool_size - current_offset;
            constexpr std::size_t min_probe_space = 64;
            bool probed = false;
            if (remaining >= min_probe_space) {
                if constexpr (std::is_member_function_pointer_v<fn_type>) {
                    using Class = typename entry_t::class_type;
                    auto* obj = static_cast<Class*>(tasks_[I].self);
                    if constexpr (std::is_invocable_v<fn_type, Class*>) {
                        if (obj) {
                            detail::current_task_allocator = {&pool_[current_offset], remaining, &tasks_[I].size};
                            { task t = (obj->*entry_t::fn)(); t.handle().destroy(); }
                            detail::current_task_allocator = {};
                            probed = true;
                        }
                    }
                } else {
                    if constexpr (std::is_invocable_v<fn_type>) {
                        detail::current_task_allocator = {&pool_[current_offset], remaining, &tasks_[I].size};
                        { task t = entry_t::fn(); t.handle().destroy(); }
                        detail::current_task_allocator = {};
                        probed = true;
                    }
                }
            }
            if (!probed) tasks_[I].size = detail::default_frame_size;
            return;
        }

        // Reserved task
        if (pool_overflow_) { tasks_[I].offset = 0; tasks_[I].size = 0; return; }
        current_offset = (current_offset + alignof(std::max_align_t) - 1) & ~(alignof(std::max_align_t) - 1);
        tasks_[I].offset = current_offset;
        std::size_t remaining = Config::reserved_pool_size - current_offset;
        constexpr std::size_t min_probe_space = 64;
        bool can_probe = (remaining >= min_probe_space);
        bool probed = false;
        if (can_probe) {
            if constexpr (std::is_member_function_pointer_v<fn_type>) {
                using Class = typename entry_t::class_type;
                auto* obj = static_cast<Class*>(tasks_[I].self);
                if constexpr (std::is_invocable_v<fn_type, Class*>) {
                    if (obj) {
                        detail::current_task_allocator = {&pool_[current_offset], remaining, &tasks_[I].size};
                        { task t = (obj->*entry_t::fn)(); t.handle().destroy(); }
                        detail::current_task_allocator = {};
                        probed = true;
                    }
                }
            } else {
                if constexpr (std::is_invocable_v<fn_type>) {
                    detail::current_task_allocator = {&pool_[current_offset], remaining, &tasks_[I].size};
                    { task t = entry_t::fn(); t.handle().destroy(); }
                    detail::current_task_allocator = {};
                    probed = true;
                }
            }
        }
        if (!probed) tasks_[I].size = detail::default_frame_size;
        current_offset += tasks_[I].size;
        if (current_offset > Config::reserved_pool_size) {
            pool_overflow_ = true;
            tasks_[I].offset = 0;
            tasks_[I].size = 0;
        }
    }

    // -----------------------------------------------------------------------
    // Dump helpers
    // -----------------------------------------------------------------------

    void dump_pool_summary_log() const {
        std::size_t used = total_pool_used();
        double pct = Config::reserved_pool_size > 0
            ? (static_cast<double>(used) / Config::reserved_pool_size) * 100.0 : 0.0;
        char line[256];
        std::snprintf(line, sizeof(line), "Reserved pool: %zuB / %zuB used (%.1f%%)",
                      used, static_cast<std::size_t>(Config::reserved_pool_size), pct);
        log::detail::log_impl<Config, log_level::info, Logger, ClockType>("INF", "", "%s", line);
    }

    template <typename Sink>
    void dump_pool_summary_sink(Sink& sink) const {
        std::size_t used = total_pool_used();
        double pct = Config::reserved_pool_size > 0
            ? (static_cast<double>(used) / Config::reserved_pool_size) * 100.0 : 0.0;
        char line[256];
        int n = std::snprintf(line, sizeof(line), "Reserved pool: %zuB / %zuB used (%.1f%%)",
                              used, static_cast<std::size_t>(Config::reserved_pool_size), pct);
        if (n > 0) sink(std::string_view(line, static_cast<std::size_t>(n)));
    }

    void dump_scratchpad_summary_log() const {
        std::size_t used_blocks = 0;
        for (auto word : scratchpad_bitmap_)
            used_blocks += static_cast<std::size_t>(__builtin_popcountll(word));
        std::size_t used_bytes = used_blocks * scratchpad_block_size;
        double pct = Config::scratchpad_pool_size > 0
            ? (static_cast<double>(used_bytes) / Config::scratchpad_pool_size) * 100.0 : 0.0;
        char line[256];
        std::snprintf(line, sizeof(line),
                      "Scratchpad pool: %zuB / %zuB used (%.1f%%), waiters: %zu",
                      used_bytes, static_cast<std::size_t>(Config::scratchpad_pool_size),
                      pct, scratchpad_waiter_count_);
        log::detail::log_impl<Config, log_level::info, Logger, ClockType>("INF", "", "%s", line);
    }

    template <typename Sink>
    void dump_scratchpad_summary_sink(Sink& sink) const {
        std::size_t used_blocks = 0;
        for (auto word : scratchpad_bitmap_)
            used_blocks += static_cast<std::size_t>(__builtin_popcountll(word));
        std::size_t used_bytes = used_blocks * scratchpad_block_size;
        double pct = Config::scratchpad_pool_size > 0
            ? (static_cast<double>(used_bytes) / Config::scratchpad_pool_size) * 100.0 : 0.0;
        char line[256];
        int n = std::snprintf(line, sizeof(line),
                              "Scratchpad pool: %zuB / %zuB used (%.1f%%), waiters: %zu",
                              used_bytes, static_cast<std::size_t>(Config::scratchpad_pool_size),
                              pct, scratchpad_waiter_count_);
        if (n > 0) sink(std::string_view(line, static_cast<std::size_t>(n)));
    }

    template <std::size_t I>
    void dump_one_line_log() const {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        const auto& meta = tasks_[I];
        if constexpr (entry_t::is_scratchpad) {
            char line[256];
            std::snprintf(line, sizeof(line), "[%zu] <%s>  scratchpad  size=%zuB",
                          I, fn_names_[I], meta.size);
            log::detail::log_impl<Config, log_level::info, Logger, ClockType>("INF", fn_names_[I], "%s", line);
        } else {
            char line[256];
            std::snprintf(line, sizeof(line), "[%zu] <%s>  reserved  offset=%zu  size=%zuB",
                          I, fn_names_[I], meta.offset, meta.size);
            log::detail::log_impl<Config, log_level::info, Logger, ClockType>("INF", fn_names_[I], "%s", line);
        }
    }

    template <std::size_t I, typename Sink>
    void dump_one_line_sink(Sink& sink) const {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        const auto& meta = tasks_[I];
        char line[256];
        int n;
        if constexpr (entry_t::is_scratchpad) {
            n = std::snprintf(line, sizeof(line), "[%zu] <%s>  scratchpad  size=%zuB",
                              I, fn_names_[I], meta.size);
        } else {
            n = std::snprintf(line, sizeof(line), "[%zu] <%s>  reserved  offset=%zu  size=%zuB",
                              I, fn_names_[I], meta.offset, meta.size);
        }
        if (n > 0) sink(std::string_view(line, static_cast<std::size_t>(n)));
    }

public:

    /// Resume expired timers and their associated coroutines.
    void tick() {
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};

        // Clean up completed scratchpad tasks (reserved tasks complete inline).
        for (std::size_t i = 0; i < num_tasks; ++i) {
            auto& meta = tasks_[i];
            if (meta.occupied && meta.handle && meta.handle.done()) {
                if (meta.is_scratchpad && meta.scratch_offset != scratch_unused) {
                    scratchpad_free(meta.scratch_offset, meta.size);
                    meta.scratch_offset = scratch_unused;
                    meta.handle = {};
                }
                log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                    "INF", fn_names_[i], "completed");
                meta.occupied = false;

                // A scratchpad task freed pool space — try to resume a waiter.
                if (meta.is_scratchpad) try_resume_waiter();
            }
        }

        // (a) Resume directly-runnable tasks.
        for (std::size_t i = 0; i < num_tasks; ++i) {
            auto& meta = tasks_[i];
            if (meta.occupied && meta.handle && !meta.handle.done() && !meta.suspended) {
                bool has_timer = false;
                for (std::size_t j = 0; j < timer_count_; ++j) {
                    if (timers_[j].handle == meta.handle) { has_timer = true; break; }
                }
                if (!has_timer) {
                    meta.handle.resume();
                    if (meta.handle.done()) {
                        if (meta.is_scratchpad && meta.scratch_offset != scratch_unused) {
                            scratchpad_free(meta.scratch_offset, meta.size);
                            meta.scratch_offset = scratch_unused;
                            meta.handle = {};
                        }
                        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                            "INF", fn_names_[i], "completed");
                        meta.occupied = false;
                        if (meta.is_scratchpad) try_resume_waiter();
                    }
                }
            }
        }

        // (b) Collect expired timers.
        std::array<std::coroutine_handle<>, Config::max_timers> expired;
        std::size_t expired_count = 0;
        auto now = ClockType::now();
        for (std::size_t i = 0; i < timer_count_;) {
            if (timers_[i].wake_time <= now) {
                expired[expired_count++] = timers_[i].handle;
                for (std::size_t j = i; j < timer_count_ - 1; ++j) timers_[j] = timers_[j + 1];
                --timer_count_;
            } else { ++i; }
        }

        // (c) Resume expired timers.
        for (std::size_t i = 0; i < expired_count; ++i) {
            auto h = expired[i];
            std::size_t task_idx = find_slot_for_handle(h);
            if (task_idx < num_tasks) {
                log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                    "INF", fn_names_[task_idx], "timer expired, resuming");
                tasks_[task_idx].suspended = false;
            }
            h.resume();
            if (h.done()) {
                if (task_idx < num_tasks) {
                    auto& meta = tasks_[task_idx];
                    if (meta.is_scratchpad && meta.scratch_offset != scratch_unused) {
                        scratchpad_free(meta.scratch_offset, meta.size);
                        meta.scratch_offset = scratch_unused;
                        meta.handle = {};
                    }
                    log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                        "INF", fn_names_[task_idx], "completed");
                    tasks_[task_idx].occupied = false;
                    if (meta.is_scratchpad) try_resume_waiter();
                }
            }
        }

        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};
    }

    void run() {
        for (;;) { tick(); std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
    }

    bool pool_exhausted() const noexcept { return pool_overflow_; }
};

// ---------------------------------------------------------------------------
// make_engine
// ---------------------------------------------------------------------------

template <typename Config, typename Clock, typename Logger = no_logger, typename... Specs>
auto make_engine(Specs&&... specs) {
    using desc_list = typename detail::spec_unfolder<std::decay_t<Specs>...>::type;
    using eng_t = typename detail::engine_from_desc_list<Config, Clock, Logger, desc_list>::type;
    constexpr std::size_t N = detail::count_slots<std::decay_t<Specs>...>::value;

    std::array<void*, N> self_ptrs{};
    std::size_t idx = 0;
    (
        [&](auto& spec) {
            using SpecT = std::decay_t<decltype(spec)>;
            if constexpr (requires { spec.self; }) {
                for (std::size_t i = 0; i < SpecT::num_fns; ++i) self_ptrs[idx++] = static_cast<void*>(spec.self);
            } else {
                self_ptrs[idx++] = nullptr;
            }
        }(specs),
        ...
    );
    return eng_t{self_ptrs};
}

}  // namespace cgx::reactor

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

// Compile-time concatenation of "reactor::task::" with a task tag.
// Produces strings like "reactor::task::FLSH" for use in log points.
template <typename TagType>
struct prefixed_tag {
    static constexpr const char prefix[] = "reactor::task::";
    static constexpr std::size_t prefix_len = sizeof(prefix) - 1;
    static constexpr auto& suffix = TagType::value;
    static constexpr std::size_t suffix_len = sizeof(suffix);
    static constexpr std::size_t total = prefix_len + suffix_len;

    static constexpr auto make() {
        std::array<char, total> arr{};
        for (std::size_t i = 0; i < prefix_len; ++i)
            arr[i] = prefix[i];
        for (std::size_t i = 0; i < suffix_len; ++i)
            arr[prefix_len + i] = suffix[i];
        return arr;
    }

    static constexpr auto arr = make();
    static constexpr const char* value = arr.data();
};

// Length of the "reactor::task::" prefix (used by engine::dump to strip it)
static constexpr std::size_t log_tag_prefix_len = sizeof("reactor::task::") - 1;

// -------------------------------------------------------------------------
// Function-name extraction via __PRETTY_FUNCTION__ (clang/GCC)
//
// Returns a pointer to a program-lifetime static buffer.  This is safe for
// diagnostic dump output — all calls with the same Fn NTTP share the same
// static, which means two instances of the same class emitting the same
// method in dump output will get the same pointer (not a problem for dump).
// -------------------------------------------------------------------------

template <auto Fn>
static const char* fn_name() {
    std::string_view pretty = __PRETTY_FUNCTION__;
    auto prefix = pretty.find("Fn = ");
    if (prefix == std::string_view::npos) return "unknown";
    auto start = prefix + 5;
    auto end = pretty.find(']', start);
    if (end == std::string_view::npos) return "unknown";
    auto name = pretty.substr(start, end - start);
    // Strip leading "&" or "&(" / trailing ")"
    if (name.starts_with("&(") && name.ends_with(")")) {
        name = name.substr(2, name.size() - 3);
    } else if (name.starts_with("&")) {
        name = name.substr(1);
    }
    static std::string result(name);
    return result.c_str();
}

/// Fallback frame size for tasks that could not be probed (e.g., tasks
/// with parameters).  The probe only works for no-arg coroutines.
static constexpr std::size_t default_frame_size = 1024;

}  // namespace detail

/// Compile-time task-registering coroutine engine.
///
/// \tparam Config     Policy type (e.g. default_config).
/// \tparam ClockType  Clock satisfying the Clock concept.
/// \tparam Entries    Typed slot entries (task_descriptor<Fn, Tag, Class>)
///                    produced by make_engine or directly via unfold_specs.
///
/// Each task is uniquely identified by its function pointer (NTTP).
template <typename Config, typename ClockType, typename Logger, typename... Entries>
class engine {
    static constexpr std::size_t num_tasks = sizeof...(Entries);

    // Tag string buffer for auto-generated tags (e.g. "reactor::task::TSK0")
    static constexpr std::size_t tag_buf_size = 64;

    // Runtime tag strings: user-provided tags (compile-time const char*) or
    // auto-generated tags stored in auto_tag_bufs_.
    // Initialized in constructor.
    std::array<const char*, num_tasks> tag_strings_{};
    std::array<char[tag_buf_size], num_tasks> auto_tag_bufs_{};

    // Reserved pool — compile-time-sized byte array for coroutine frames.
    alignas(std::max_align_t) std::array<std::byte, Config::reserved_pool_size> pool_{};

    // Per-task metadata.
    std::array<detail::task_meta, num_tasks> tasks_{};

    // Set to true if the reserved pool is too small for all tasks.
    bool pool_overflow_ = false;

    // Initialize tag strings: use user tag if non-empty, else auto-generate "TSK<n>"
    void init_tag_strings() {
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((init_one_tag<Is>()), ...);
        }(std::make_index_sequence<num_tasks>{});
    }

    template <std::size_t I>
    void init_one_tag() {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        using tag_t = typename entry_t::tag_type;
        if constexpr (tag_t::value[0] != '\0') {
            // User-provided tag — use compile-time prefixed string
            tag_strings_[I] = detail::prefixed_tag<tag_t>::value;
        } else {
            // Auto-generated tag: "reactor::task::TSK<n>"
            std::snprintf(auto_tag_bufs_[I], tag_buf_size,
                          "reactor::task::TSK%zu", I);
            tag_strings_[I] = auto_tag_bufs_[I];
        }
    }

    // Find the task index for a given coroutine handle (runtime lookup)
    std::size_t find_slot_for_handle(std::coroutine_handle<> h) const noexcept {
        for (std::size_t i = 0; i < num_tasks; ++i) {
            if (tasks_[i].handle == h) {
                return i;
            }
        }
        return num_tasks; // not found
    }

    template <auto Fn>
    static constexpr std::size_t slot_index() {
        constexpr auto idx = detail::index_of_fn<Fn, Entries::fn...>();
        static_assert(idx < num_tasks,
                      "Task not registered with this engine. "
                      "Make sure the function pointer is included in a spec "
                      "passed to make_engine(...).");
        return idx;
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
        for (auto& t : self->tasks_) {
            if (t.handle && t.handle.address() == h.address()) {
                t.suspended = true;
                break;
            }
        }
    }

    std::size_t total_pool_used() const noexcept {
        if (num_tasks == 0) return 0;
        auto& last = tasks_[num_tasks - 1];
        return last.offset + last.size;
    }

    std::array<timer_entry, Config::max_timers> timers_{};
    std::size_t timer_count_ = 0;

    // -----------------------------------------------------------------------
    // Internal: trigger the coroutine at a given task index
    // -----------------------------------------------------------------------

    template <std::size_t I, typename... Args>
    error trigger_slot(Args&&... args) {
        if (pool_overflow_) {
            log::detail::log_impl<Config, log_level::error, Logger, ClockType>(
                "ERR", tag_strings_[I],
                "reserved pool exhausted, trigger rejected");
            return error::capacity_exceeded;
        }

        auto& meta = tasks_[I];

        if (meta.occupied) {
            log::detail::log_impl<Config, log_level::warn, Logger, ClockType>(
                "WRN", tag_strings_[I],
                "already running, trigger rejected");
            return error::task_already_running;
        }

        // Destroy any previous coroutine frame in the pool region.
        if (meta.handle) {
            meta.handle.destroy();
        }
        meta.handle = {};

        // Point the thread-local allocator at the pool region and invoke the
        // coroutine. The promise_type::operator new will return this buffer.
        detail::current_task_allocator = {&pool_[meta.offset], meta.size, nullptr};
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};
        meta.suspended = false;  // Task is running

        // Dispatch: member function vs free function
        using desc = typename detail::type_at<I, Entries...>::type;
        using fn_type = decltype(desc::fn);
        if constexpr (std::is_member_function_pointer_v<fn_type>) {
            using Class = typename desc::class_type;
            auto* obj = static_cast<Class*>(meta.self);
            meta.handle = (obj->*desc::fn)(std::forward<Args>(args)...).handle();
        } else {
            meta.handle = desc::fn(std::forward<Args>(args)...).handle();
        }

        // Now the handle is stored in the metadata. Resume to start execution.
        meta.handle.resume();

        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};

        // If the coroutine ran to completion immediately, the slot is free.
        if (meta.handle.done()) {
            meta.occupied = false;
        } else {
            meta.occupied = true;
        }

        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", tag_strings_[I], "triggered");

        return error::ok;
    }

    // -----------------------------------------------------------------------
    // Internal: try to trigger a task by instance + function pointer match
    // -----------------------------------------------------------------------

    // Accept any member function pointer type (const or non-const).
    // Fn is deduced from the runtime argument and compared against the
    // registered function pointer type (with remove_cv_t to strip the
    // top-level const that constexpr auto adds).
    template <std::size_t I, typename Class, typename Fn, typename... Args>
    bool try_trigger_instance(Class& obj, Fn fn, error& result, Args&&... args) {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        using registered_type = std::remove_cv_t<decltype(entry_t::fn)>;

        if constexpr (std::is_same_v<Fn, registered_type>) {
            if (tasks_[I].self == &obj && entry_t::fn == fn) {
                if (tasks_[I].occupied) {
                    result = error::task_already_running;
                } else {
                    result = trigger_slot<I>(std::forward<Args>(args)...);
                }
                return true;
            }
        }
        return false;
    }

public:
    engine() = delete;

    /// Construct an engine with self pointers for member-function tasks.
    /// Called by make_engine().  For engines with zero tasks the array
    /// is empty and all self pointers are null-initialised.
    engine(std::array<void*, num_tasks> self_ptrs) : tasks_{} {
        for (std::size_t i = 0; i < num_tasks; ++i) {
            tasks_[i].self = self_ptrs[i];
        }
        init_tag_strings();
        // Probe each task directly in the reserved pool to capture the
        // actual coroutine frame size and assign pool regions inline.
        probe_frame_sizes();
    }

    engine(const engine&) = delete;
    engine& operator=(const engine&) = delete;

    ~engine() {
        for (auto& meta : tasks_) {
            if (meta.handle) {
                meta.handle.destroy();
            }
        }
    }

    /// Register a timer entry (internal API for delay_ms).
    error add_timer(std::chrono::steady_clock::time_point wake,
                    std::coroutine_handle<> h) noexcept {
        if (timer_count_ >= Config::max_timers) {
            // Log: capacity exceeded
            std::size_t slot_idx = find_slot_for_handle(h);
            const char* tag = (slot_idx < num_tasks) ? tag_strings_[slot_idx]
                                                     : "unknown";
            log::detail::log_impl<Config, log_level::error, Logger, ClockType>(
                "ERR", tag,
                "timer capacity exceeded (%zu/%zu)",
                static_cast<std::size_t>(timer_count_),
                static_cast<std::size_t>(Config::max_timers));
            return error::capacity_exceeded;
        }
        timers_[timer_count_++] = {wake, h};

        // Log: delay registered
        std::size_t slot_idx = find_slot_for_handle(h);
        const char* tag = (slot_idx < num_tasks) ? tag_strings_[slot_idx]
                                                 : "unknown";
        auto delay_ms_val = std::chrono::duration_cast<
            std::chrono::milliseconds>(wake - ClockType::now()).count();
        log::detail::log_impl<Config, log_level::debug, Logger, ClockType>(
            "DBG", tag, "delay %lldms registered",
            static_cast<long long>(delay_ms_val));

        return error::ok;
    }

    /// Trigger a registered task coroutine by function pointer (compile-time lookup).
    ///
    /// \tparam Fn  The function pointer of the task.
    /// \param args Arguments forwarded to the coroutine function.
    ///             For member-function tasks, the instance pointer was
    ///             captured at registration — do NOT pass it here.
    /// \return error::ok on success, error::task_already_running if active.
    ///
    /// Usage:
    ///   eng.trigger<&my_task>(arg1, arg2);
    ///   eng.trigger<&MyClass::method>(arg1, arg2);  // triggers FIRST instance (legacy)
    template <auto Fn, typename... Args>
    error trigger(Args&&... args) {
        constexpr auto idx = slot_index<Fn>();
        return trigger_slot<idx>(std::forward<Args>(args)...);
    }

    /// Trigger a registered task coroutine by instance + method pointer (runtime lookup).
    ///
    /// Searches all tasks for one where the self pointer matches \p obj AND
    /// the registered function pointer matches \p fn.  If found and idle,
    /// the task is triggered.  If found and running, returns
    /// error::task_already_running.  If not found, returns
    /// error::task_not_registered.
    ///
    /// Usage:
    ///   eng.trigger(obj, &MyClass::method, arg1, arg2);
    ///   eng.trigger(obj, &MyClass::const_method, arg1, arg2);

    // Non-const member function
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

    /// Return diagnostics about the engine layout.
    engine_report report() const noexcept {
        engine_report r{};
        r.task_count = num_tasks;
        r.reserved_count = num_tasks;
        r.scratchpad_count = 0;
        r.scratchpad_size = 0;
        return r;
    }

    /// Dump engine diagnostics via Logger::print().
    /// Returns the engine_report struct with summary stats.
    engine_report dump() const {
        auto r = report();
        dump_pool_summary_log();
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((dump_one_line_log<Is>()), ...);
        }(std::make_index_sequence<num_tasks>{});
        return r;
    }

    /// Dump engine diagnostics via a custom sink.
    /// The sink is called with each formatted line.
    template <typename Sink>
    engine_report dump(Sink&& sink) const {
        auto r = report();
        dump_pool_summary_sink(sink);
        [this, &sink]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((dump_one_line_sink<Is>(sink)), ...);
        }(std::make_index_sequence<num_tasks>{});
        return r;
    }

private:
    // -----------------------------------------------------------------------
    // Frame-size probing: creates each coroutine at construction directly in
    // the reserved pool to capture the actual frame size, assigns the pool
    // region inline, then destroys the probe coroutine.
    //
    // No-arg coroutines are created at the current pool offset; operator new
    // captures the frame size via size_out and the frame is placed in the
    // pool.  After probing, the region is free (destroyed) and its offset+size
    // are committed for future real coroutine creation.
    //
    // Tasks with parameters cannot be probed (the compiler cannot deduce
    // argument values at construction), so they receive a fallback size
    // (detail::default_frame_size = 1024B).  Their region is reserved but
    // unused until trigger.
    //
    // The pool is empty at construction, so probing directly in the pool
    // avoids any heap allocation.  No heap is used during normal operation
    // either — frames always live in the reserved pool.
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

        // Once overflow is detected, skip all remaining tasks.
        if (pool_overflow_) {
            tasks_[I].offset = 0;
            tasks_[I].size = 0;
            return;
        }

        // Align offset before assigning this task's region.
        current_offset = (current_offset + alignof(std::max_align_t) - 1)
                       & ~(alignof(std::max_align_t) - 1);
        tasks_[I].offset = current_offset;

        std::size_t remaining = Config::reserved_pool_size - current_offset;

        // Minimum space needed to attempt a probe.  64 bytes is a safe lower
        // bound for any no-arg coroutine frame (promise_type + implicit this).
        // If less is available, skip probing and flag overflow below.
        constexpr std::size_t min_probe_space = 64;
        bool can_probe = (remaining >= min_probe_space);
        bool probed = false;

        if (can_probe) {
            if constexpr (std::is_member_function_pointer_v<fn_type>) {
                using Class = typename entry_t::class_type;
                auto* obj = static_cast<Class*>(tasks_[I].self);

                if constexpr (std::is_invocable_v<fn_type, Class*>) {
                    if (obj) {
                        // Create coroutine directly in the pool at current_offset.
                        // The size_out pointer captures the actual frame size.
                        detail::current_task_allocator = {&pool_[current_offset], remaining, &tasks_[I].size};
                        {
                            task t = (obj->*entry_t::fn)();
                            t.handle().destroy();
                        }
                        detail::current_task_allocator = {};
                        probed = true;
                    }
                }
            } else {
                if constexpr (std::is_invocable_v<fn_type>) {
                    detail::current_task_allocator = {&pool_[current_offset], remaining, &tasks_[I].size};
                    {
                        task t = entry_t::fn();
                        t.handle().destroy();
                    }
                    detail::current_task_allocator = {};
                    probed = true;
                }
            }
        }

        if (!probed) {
            tasks_[I].size = detail::default_frame_size;
        }

        // Advance past this task's region.
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
        double pct = (static_cast<double>(used) / Config::reserved_pool_size) * 100.0;
        char line[256];
        std::snprintf(line, sizeof(line),
                      "Reserved pool: %zuB / %zuB used (%.1f%%)",
                      used, static_cast<std::size_t>(Config::reserved_pool_size), pct);
        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", "", "%s", line);
    }

    template <typename Sink>
    void dump_pool_summary_sink(Sink& sink) const {
        std::size_t used = total_pool_used();
        double pct = (static_cast<double>(used) / Config::reserved_pool_size) * 100.0;
        char line[256];
        int n = std::snprintf(line, sizeof(line),
                              "Reserved pool: %zuB / %zuB used (%.1f%%)",
                              used, static_cast<std::size_t>(Config::reserved_pool_size), pct);
        if (n > 0) {
            sink(std::string_view(line, static_cast<std::size_t>(n)));
        }
    }

    template <std::size_t I>
    void dump_one_line_log() const {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        constexpr auto entry_fn = entry_t::fn;
        const char* fn_name = detail::fn_name<entry_fn>();
        char line[256];
        std::snprintf(line, sizeof(line),
                      "[%zu] %s  %s  offset=%zu  size=%zuB",
                      I, tag_strings_[I] + detail::log_tag_prefix_len,
                      fn_name, tasks_[I].offset, tasks_[I].size);
        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", tag_strings_[I], "%s", line);
    }

    template <std::size_t I, typename Sink>
    void dump_one_line_sink(Sink& sink) const {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        constexpr auto entry_fn = entry_t::fn;
        const char* fn_name = detail::fn_name<entry_fn>();
        char line[256];
        int n = std::snprintf(line, sizeof(line),
                              "[%zu] %s  %s  offset=%zu  size=%zuB",
                              I, tag_strings_[I] + detail::log_tag_prefix_len,
                              fn_name, tasks_[I].offset, tasks_[I].size);
        if (n > 0) {
            sink(std::string_view(line, static_cast<std::size_t>(n)));
        }
    }

public:

    /// Resume expired timers and their associated coroutines.
    void tick() {
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};

        // Clean up completed tasks (e.g., tasks that completed via signal resume)
        for (std::size_t i = 0; i < num_tasks; ++i) {
            auto& meta = tasks_[i];
            if (meta.occupied && meta.handle &&
                meta.handle.done()) {
                log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                    "INF", tag_strings_[i], "completed");
                meta.occupied = false;
            }
        }

        // (a) Resume directly-runnable tasks — occupied, not done,
        //     not suspended on external event, and NOT waiting on a pending timer.
        for (std::size_t i = 0; i < num_tasks; ++i) {
            auto& meta = tasks_[i];
            if (meta.occupied && meta.handle &&
                !meta.handle.done() && !meta.suspended) {
                bool has_timer = false;
                for (std::size_t j = 0; j < timer_count_; ++j) {
                    if (timers_[j].handle == meta.handle) {
                        has_timer = true;
                        break;
                    }
                }
                if (!has_timer) {
                    meta.handle.resume();
                    if (meta.handle.done()) {
                        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                            "INF", tag_strings_[i], "completed");
                        meta.occupied = false;
                    }
                }
            }
        }

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
            std::size_t task_idx = find_slot_for_handle(h);
            if (task_idx < num_tasks) {
                log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                    "INF", tag_strings_[task_idx],
                    "timer expired, resuming");
                tasks_[task_idx].suspended = false;
            }
            h.resume();
            if (h.done()) {
                if (task_idx < num_tasks) {
                    log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                        "INF", tag_strings_[task_idx], "completed");
                    tasks_[task_idx].occupied = false;
                }
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

    // -----------------------------------------------------------------------
    // Pool status
    // -----------------------------------------------------------------------

    /// Returns true if the reserved pool is too small to hold all tasks.
    bool pool_exhausted() const noexcept { return pool_overflow_; }
};

// ---------------------------------------------------------------------------
// make_engine — build an engine from runtime specs
// ---------------------------------------------------------------------------

/// Build an engine from a list of bound/free specs.
///
/// Usage:
///   auto eng = make_engine<Config, Clock>(
///       register_instance<"TAG"_tag>(my_obj),  // explicit tag
///       register_instance(my_obj),              // auto-generated tag
///       register_task<"TAG"_tag, &my_fn>(),     // free function with tag
///       register_task<&my_fn>()                 // free function, auto-tag
///   );
///
/// Each bound is unfolded into one slot per member function (all sharing
/// the same self pointer).  Each free_spec produces one slot.
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
                // bound — multiple slots, all share the same self pointer
                for (std::size_t i = 0; i < SpecT::num_fns; ++i) {
                    self_ptrs[idx++] = static_cast<void*>(spec.self);
                }
            } else {
                // free_spec — one slot, no self pointer
                self_ptrs[idx++] = nullptr;
            }
        }(specs),
        ...
    );

    return eng_t{self_ptrs};
}

}  // namespace cgx::reactor

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

// -------------------------------------------------------------------------
// Slot: owns coroutine-frame storage and tracks occupancy.
// -------------------------------------------------------------------------

template <std::size_t SlotSize>
struct slot {
    alignas(std::max_align_t) std::byte storage[SlotSize];
    task current_task;
    bool occupied = false;
    bool suspended = true;  // true if task is suspended (waiting for timer/signal/etc)
    void* self = nullptr;   // instance pointer for member-function tasks (null for free fns)
    std::size_t frame_size = 0;  // actual coroutine frame size (set by probe)
};

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

    // Find the slot index for a given coroutine handle (runtime lookup)
    std::size_t find_slot_for_handle(std::coroutine_handle<> h) const noexcept {
        for (std::size_t i = 0; i < num_tasks; ++i) {
            if (slots_[i].current_task.handle() == h) {
                return i;
            }
        }
        return num_tasks; // not found
    }

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
        constexpr auto idx = detail::index_of_fn<Fn, Entries::fn...>();
        static_assert(idx < num_tasks,
                      "Task not registered with this engine. "
                      "Make sure the function pointer is included in a spec "
                      "passed to make_engine(...).");
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

    // -----------------------------------------------------------------------
    // Internal: trigger the coroutine at a given slot index
    // -----------------------------------------------------------------------

    template <std::size_t I, typename... Args>
    error trigger_slot(Args&&... args) {
        auto& s = slots_[I];

        if (s.occupied) {
            log::detail::log_impl<Config, log_level::warn, Logger, ClockType>(
                "WRN", tag_strings_[I],
                "already running, trigger rejected");
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

        // Dispatch: member function vs free function
        using desc = typename detail::type_at<I, Entries...>::type;
        using fn_type = decltype(desc::fn);
        if constexpr (std::is_member_function_pointer_v<fn_type>) {
            using Class = typename desc::class_type;
            auto* obj = static_cast<Class*>(s.self);
            s.current_task = task{(obj->*desc::fn)(std::forward<Args>(args)...)};
        } else {
            s.current_task = task{desc::fn(std::forward<Args>(args)...)};
        }

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

        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", tag_strings_[I], "triggered");

        return error::ok;
    }

    // -----------------------------------------------------------------------
    // Internal: try to trigger a slot by instance + function pointer match
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
            if (slots_[I].self == &obj && entry_t::fn == fn) {
                if (slots_[I].occupied) {
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
    engine(std::array<void*, num_tasks> self_ptrs) : slots_{} {
        for (std::size_t i = 0; i < num_tasks; ++i) {
            slots_[i].self = self_ptrs[i];
        }
        init_tag_strings();
        probe_frame_sizes();
    }

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
    /// Searches all slots for one where the self pointer matches \p obj AND
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
        [this, &sink]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((dump_one_line_sink<Is>(sink)), ...);
        }(std::make_index_sequence<num_tasks>{});
        return r;
    }

private:
    // -----------------------------------------------------------------------
    // Frame-size probing: creates each coroutine at construction to capture
    // the actual frame size, then immediately destroys it.
    // -----------------------------------------------------------------------

    void probe_frame_sizes() {
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((probe_frame_size<Is>()), ...);
        }(std::make_index_sequence<num_tasks>{});
    }

    template <std::size_t I>
    void probe_frame_size() {
        auto& s = slots_[I];
        using entry_t = typename detail::type_at<I, Entries...>::type;
        using fn_type = std::remove_cv_t<decltype(entry_t::fn)>;

        std::size_t actual_size = 0;
        bool probed = false;

        if constexpr (std::is_member_function_pointer_v<fn_type>) {
            using Class = typename entry_t::class_type;
            auto* obj = static_cast<Class*>(s.self);

            if constexpr (std::is_invocable_v<fn_type, Class*>) {
                if (obj) {
                    detail::current_task_allocator = {s.storage, sizeof(s.storage), &actual_size};
                    {
                        task t = (obj->*entry_t::fn)();
                        t.handle().destroy();
                    }
                    detail::current_task_allocator = {};
                    s.frame_size = actual_size;
                    probed = true;
                }
            }
        } else {
            if constexpr (std::is_invocable_v<fn_type>) {
                detail::current_task_allocator = {s.storage, sizeof(s.storage), &actual_size};
                {
                    task t = entry_t::fn();
                    t.handle().destroy();
                }
                detail::current_task_allocator = {};
                s.frame_size = actual_size;
                probed = true;
            }
        }

        if (!probed) {
            s.frame_size = slot_storage_size();
        }
    }

    template <std::size_t I>
    void dump_one_line_log() const {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        constexpr auto entry_fn = entry_t::fn;
        const char* fn_name = detail::fn_name<entry_fn>();
        char line[256];
        std::snprintf(line, sizeof(line),
                      "[%zu] %s  %s  reserved  frame=~%zuB",
                      I, tag_strings_[I] + detail::log_tag_prefix_len,
                      fn_name, slots_[I].frame_size);
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
                              "[%zu] %s  %s  reserved  frame=~%zuB",
                              I, tag_strings_[I] + detail::log_tag_prefix_len,
                              fn_name, slots_[I].frame_size);
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
            auto& s = slots_[i];
            if (s.occupied && s.current_task.handle() &&
                s.current_task.handle().done()) {
                log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                    "INF", tag_strings_[i], "completed");
                s.occupied = false;
            }
        }

        // (a) Resume directly-runnable tasks — occupied, not done,
        //     not suspended on external event, and NOT waiting on a pending timer.
        for (std::size_t i = 0; i < num_tasks; ++i) {
            auto& s = slots_[i];
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
                        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                            "INF", tag_strings_[i], "completed");
                        s.occupied = false;
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
            std::size_t slot_idx = find_slot_for_handle(h);
            if (slot_idx < num_tasks) {
                log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                    "INF", tag_strings_[slot_idx],
                    "timer expired, resuming");
                slots_[slot_idx].suspended = false;
            }
            h.resume();
            if (h.done()) {
                if (slot_idx < num_tasks) {
                    log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
                        "INF", tag_strings_[slot_idx], "completed");
                    slots_[slot_idx].occupied = false;
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

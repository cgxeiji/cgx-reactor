#pragma once

#include <cgx/reactor/task.hpp>
#include <cgx/reactor/config.hpp>
#include <cgx/reactor/error.hpp>
#include <cgx/reactor/logger.hpp>
#include <cgx/reactor/timer.hpp>
#include <cgx/reactor/task_list.hpp>

#include <algorithm>
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

// ---------------------------------------------------------------------------
// Forward declarations
// ---------------------------------------------------------------------------

class engine;                       // abstract base interface
struct task_handle;                 // non-templated completion handle
struct task_completion_awaiter;     // non-templated completion awaiter

template <typename Config, typename Clock, typename Logger = no_logger, typename... Entries>
class basic_engine;                 // concrete templated engine (renamed from `engine`)

// ---------------------------------------------------------------------------
// detail::engine_from_desc_list — build concrete engine type from descriptors
// ---------------------------------------------------------------------------

namespace detail {

template <typename Config, typename Clock, typename Logger, typename List>
struct engine_from_desc_list;

template <typename Config, typename Clock, typename Logger, typename... Ds>
struct engine_from_desc_list<Config, Clock, Logger, detail::type_list<Ds...>> {
    using type = basic_engine<Config, Clock, Logger, Ds...>;
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

// -------------------------------------------------------------------------
// fnv1a — FNV-1a 32-bit hash of a string view
//
// Used to derive a stable per-function UID from `extract_fn_name<Fn>()`.
// Constant-expression evaluable.
// -------------------------------------------------------------------------

constexpr std::uint32_t fnv1a(std::string_view s) noexcept {
    std::uint32_t h = 0x811c9dc5u;
    for (char c : s) {
        h ^= static_cast<std::uint8_t>(c);
        h *= 0x01000193u;
    }
    return h;
}

// -------------------------------------------------------------------------
// uid_pair_ok — pairwise check for the compile-time distinctness assert
//
// Two UIDs are "ok" if they are different (no collision), OR if they
// belong to the same function (multi-instance registration, which is
// legal and expected).  Factored as a free constexpr function so the
// collision-detection LOGIC can be unit-tested independently of any
// real hash collisions.
// -------------------------------------------------------------------------

constexpr bool uid_pair_ok(std::uint32_t a, std::uint32_t b, bool same_fn) noexcept {
    return a != b || same_fn;
}

// -------------------------------------------------------------------------
// next_pow2 — smallest power of 2 ≥ n, with floor 1
//
// Used to size the UID hashmap (open-addressing, linear probing) so the
// index can be computed with `idx & (size - 1)`.
// -------------------------------------------------------------------------

constexpr std::size_t next_pow2(std::size_t n) noexcept {
    if (n <= 1) return 1;
    std::size_t p = 1;
    while (p < n) p <<= 1;
    return p;
}

}  // namespace detail

// ---------------------------------------------------------------------------
// task_uid — strongly-typed 32-bit UID for a registered task
//
// The UID is the FNV-1a hash of the function's pretty name.  Strongly
// typed to prevent mixing with raw uint32 values.
// ---------------------------------------------------------------------------

struct task_uid {
    std::uint32_t value;
    friend constexpr bool operator==(task_uid, task_uid) = default;
};

/// Compile-time task UID: `task_uid_v<&fn>` is the UID of the function
/// `fn` (free or member function NTTP).  Computable without any engine
/// type — identical UIDs at engine registration and call sites for the
/// same fn; distinct UIDs for distinct fns (with high probability under
/// FNV-1a; the engine enforces distinctness for distinct fns at compile
/// time, see `basic_engine`'s static_assert).
template <auto Fn>
constexpr task_uid task_uid_v = task_uid{ detail::fnv1a(detail::extract_fn_name<Fn>().view()) };

// ---------------------------------------------------------------------------
// task_completion_awaiter — non-templated completion awaiter
//
// Suspends the calling coroutine until the targeted task completes.
// Uses the abstract `engine` virtuals (`task_is_done`,
// `register_completion_waiter`) for dispatch.
//
// Method bodies are defined out-of-line below, after `engine` is complete,
// because inline member function bodies cannot perform member access
// through a forward-declared type.
// ---------------------------------------------------------------------------

struct task_completion_awaiter {
    engine* self;
    std::size_t task_idx;

    bool await_ready() const noexcept;
    bool await_suspend(std::coroutine_handle<> h) noexcept;
    void await_resume() const noexcept {}
};

// ---------------------------------------------------------------------------
// task_handle — non-templated handle to a triggered task
//
// Defined BEFORE `engine` so the abstract base's templated
// `trigger(Class&, task_uid)` wrapper can return a complete
// `task_handle` (its body is parsed at the point of definition).
// `done()` returns a `task_completion_awaiter` aggregate (complete at
// this point).
// ---------------------------------------------------------------------------

struct task_handle {
    engine* self;
    std::size_t task_idx;
    error err;

    constexpr error error() const noexcept { return err; }

    task_completion_awaiter done() const noexcept {
        return {self, task_idx};
    }
};

// ---------------------------------------------------------------------------
// engine — abstract base class (interface)
//
// The non-trigger engine surface is virtual on this base so that engine
// references can be passed around without the concrete `basic_engine<...>`
// template parameters.  The concrete `basic_engine<...>` (below) publicly
// inherits this class and provides the implementation.  Trigger methods
// (templated and instance-based) remain on the concrete class only.
//
// Lifetime: derived destructors run before the base destructor, so a
// `basic_engine` destroyed via `engine*` still cleans up its task handles.
// ---------------------------------------------------------------------------

class engine {
public:
    /// Concrete run loop: tick() + 1ms sleep, forever.
    void run() {
        for (;;) {
            tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    virtual ~engine() = default;

    // ----- non-trigger surface -----

    /// Advance the engine by one tick (resume ready tasks, fire expired timers).
    virtual void tick() = 0;

    /// Static engine report (task count, pool sizes, etc.).
    virtual engine_report report() const noexcept = 0;

    /// Dump via Logger::print() (zero-cost no-op when Logger == no_logger).
    virtual engine_report dump() const = 0;

    /// Type-erased sink dump.  `sink` must be a captureless callable
    /// invocable as `void(void*, std::string_view)`.  `ctx` is forwarded
    /// as the first argument to every sink invocation.
    virtual engine_report dump_erased(
        void (*sink)(void*, std::string_view),
        void* ctx) const = 0;

    /// Type-erased sink wrapper.  Type-erases `sink` into a function
    /// pointer + context, then calls `dump_erased`.  The sink lambda
    /// must be captureless (function-pointer-convertible).
    template <typename Sink>
    engine_report dump(Sink&& sink) const {
        using SinkT = std::remove_reference_t<Sink>;
        auto trampoline = +[](void* ctx, std::string_view line) {
            (*static_cast<SinkT*>(ctx))(line);
        };
        void* ctx = const_cast<void*>(static_cast<const void*>(&sink));
        return dump_erased(trampoline, ctx);
    }

    /// True iff any task's probed frame size overflowed the reserved pool.
    virtual bool pool_exhausted() const noexcept = 0;

    /// True iff the task at `idx` is already finished (or unallocated).
    virtual bool task_is_done(std::size_t idx) const noexcept = 0;

    /// Register `h` as the single completion waiter for task `idx`.
    /// Returns false if a waiter is already registered (caller does not suspend).
    virtual bool register_completion_waiter(
        std::size_t idx, std::coroutine_handle<> h) noexcept = 0;

    // ----- UID-based trigger surface -----

    /// Trigger a task by UID.  Non-blocking: returns immediately with
    /// a `task_handle` whose `error()` indicates the result.
    /// `task_not_registered` if the UID is not present in any instance.
    virtual task_handle trigger(task_uid uid) = 0;

    /// Type-safe wrapper: trigger a specific object's task by UID.
    /// Walks the same probe chain as `trigger(uid)` and returns the
    /// first slot whose `self` matches `&obj`.
    template <typename Class>
    task_handle trigger(Class& obj, task_uid uid) {
        return trigger_obj(static_cast<void*>(std::addressof(obj)), uid);
    }

protected:
    /// Type-erased instance trigger.  `trigger(Class&, task_uid)`
    /// above casts `&obj` to `void*` and dispatches here.
    virtual task_handle trigger_obj(void* obj, task_uid uid) = 0;

    engine() = default;
};

// ---------------------------------------------------------------------------
// task_completion_awaiter method bodies (out-of-line, after `engine` complete)
// ---------------------------------------------------------------------------

inline bool task_completion_awaiter::await_ready() const noexcept {
    return self->task_is_done(task_idx);
}

inline bool task_completion_awaiter::await_suspend(
    std::coroutine_handle<> h) noexcept {
    return self->register_completion_waiter(task_idx, h);
}

/// Compile-time task-registering coroutine engine.
template <typename Config, typename ClockType, typename Logger, typename... Entries>
class basic_engine : public engine {
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
    // UID hashmap — open-addressing, linear probing, zero-heap
    //
    // Sized to next power of 2 ≥ 2 * num_tasks (load factor ≤ 50%).
    // For 0 tasks the table has 1 entry which is never occupied; lookup
    // always returns num_tasks (= 0).
    //
    // Built in `build_uid_table()` from `tasks_[i].uid` (filled by
    // `init_uids()`).  Multi-instance registration produces linear
    // probe chains — `find_slot_by_uid` returns the FIRST slot in the
    // chain (the first registered instance, matching the NTTP-based
    // trigger's behavior); `find_slot_by_uid_and_obj` walks the same
    // chain and matches `self`.
    // -----------------------------------------------------------------------

    struct uid_entry {
        std::uint32_t uid = 0;
        std::size_t slot = 0;
        bool occupied = false;
    };

    static constexpr std::size_t uid_table_size = detail::next_pow2(2 * num_tasks);
    std::array<uid_entry, uid_table_size> uid_table_{};

    void build_uid_table() noexcept {
        const std::size_t mask = uid_table_size - 1;
        for (std::size_t i = 0; i < num_tasks; ++i) {
            std::uint32_t u = tasks_[i].uid;
            std::size_t idx = u & mask;
            while (uid_table_[idx].occupied) {
                idx = (idx + 1) & mask;
            }
            uid_table_[idx] = {u, i, true};
        }
    }

    // -----------------------------------------------------------------------
    // Single invoke table — O(1) slot→execute dispatch
    //
    // The UID→slot hashmap gives O(1) lookup of the slot index from a
    // UID.  The invoke table then dispatches from slot index to the
    // coroutine in O(1): index by `idx`, call the per-`I` thunk that
    // returns a `task`.  The heavy method (execute_scratchpad,
    // trigger_reserved_argfree, probe_frame_size) sets up the engine
    // state, calls the thunk, and handles the returned handle.
    //
    // The thunk is the irreducible per-task call trampoline: it
    // captures the entry's `fn` pointer + class type via the template
    // parameter `I`, and dispatches member-vs-free + arg-free-guard
    // via `if constexpr`.  Arg-taking fns return `task{}` (empty
    // handle) as a sentinel; the caller detects and bails.
    //
    // The other per-I initializers (init_fn_names, init_uids,
    // build_uid_table, init_flags) are trivial constexpr-data builders
    // — this is the only per-I pack expansion that does real work.
    // -----------------------------------------------------------------------

    template <std::size_t I>
    static task invoke_thunk(void* self_ptr) noexcept {
        using entry_t = typename detail::type_at<I, Entries...>::type;
        using fn_type = decltype(entry_t::fn);
        if constexpr (std::is_member_function_pointer_v<fn_type>) {
            using Class = typename entry_t::class_type;
            auto* eng = static_cast<basic_engine*>(self_ptr);
            auto* obj = static_cast<Class*>(eng->tasks_[I].self);
            if constexpr (std::is_invocable_v<fn_type, Class*>) {
                return (obj->*entry_t::fn)();
            } else {
                return task{};  // arg-taking fn: sentinel
            }
        } else {
            if constexpr (std::is_invocable_v<fn_type>) {
                return entry_t::fn();
            } else {
                return task{};  // arg-taking fn: sentinel
            }
        }
    }

    using invoke_fn = task (*)(void*);
    std::array<invoke_fn, num_tasks> invoke_table_{};

    void build_invoke_table() noexcept {
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((invoke_table_[Is] = &invoke_thunk<Is>), ...);
        }(std::make_index_sequence<num_tasks>{});
    }

    // init_flags() — set per-slot `is_scratchpad` from the compile-time
    // entry descriptor.  Must run BEFORE probe_frame_sizes() (which reads
    // it) and BEFORE dispatch_by_idx() (which routes by it).  Trivial
    // per-I pack expansion: reads a constexpr bool and stores it.
    template <std::size_t I>
    static constexpr bool entry_is_scratchpad() noexcept {
        return detail::type_at<I, Entries...>::type::is_scratchpad;
    }
    template <std::size_t... Is>
    void init_flags_impl(std::index_sequence<Is...>) noexcept {
        ((tasks_[Is].is_scratchpad = entry_is_scratchpad<Is>()), ...);
    }
    void init_flags() noexcept {
        init_flags_impl(std::make_index_sequence<num_tasks>{});
    }

    std::size_t find_slot_by_uid(std::uint32_t u) const noexcept {
        if (uid_table_size == 0) return num_tasks;
        const std::size_t mask = uid_table_size - 1;
        std::size_t idx = u & mask;
        while (uid_table_[idx].occupied) {
            if (uid_table_[idx].uid == u) return uid_table_[idx].slot;
            idx = (idx + 1) & mask;
        }
        return num_tasks;
    }

    std::size_t find_slot_by_uid_and_obj(std::uint32_t u, void* obj) const noexcept {
        if (uid_table_size == 0) return num_tasks;
        const std::size_t mask = uid_table_size - 1;
        std::size_t idx = u & mask;
        while (uid_table_[idx].occupied) {
            if (uid_table_[idx].uid == u && tasks_[uid_table_[idx].slot].self == obj) {
                return uid_table_[idx].slot;
            }
            idx = (idx + 1) & mask;
        }
        return num_tasks;
    }

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
    /// Awaiter returned by trigger() for scratchpad tasks.  Stays a
    /// nested type of `basic_engine` because it accesses private
    /// members (e.g. `tasks_`, `scratchpad_waiter_count_`).  Its
    /// `await_resume` returns the namespace-scope `task_handle`.
    struct scratchpad_trigger_awaiter {
        basic_engine* self;
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

        task_handle await_resume() const noexcept {
            // Check the actual task state — the waiter may have been
            // resumed by try_resume_waiter() which sets occupied=true
            // and scratch_offset, but doesn't update the allocated flag.
            auto& meta = self->tasks_[task_idx];
            if (meta.occupied && meta.scratch_offset != self->scratch_unused)
                return {self, task_idx, error::ok};
            return {self, task_idx, allocated ? error::ok : error::capacity_exceeded};
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
                auto resumed_handle = w.handle;
                auto resumed_idx = w.task_index;
                // Remove waiter before resuming to prevent re-entrancy issues
                // (the resumed coroutine may trigger new tasks and add waiters).
                for (std::size_t j = i; j + 1 < scratchpad_waiter_count_; ++j)
                    scratchpad_waiters_[j] = scratchpad_waiters_[j + 1];
                --scratchpad_waiter_count_;
                execute_scratchpad(resumed_idx, alloc);
                resumed_handle.resume();
            } else {
                break;
            }
        }
    }

    /// Complete a task: free scratchpad memory if applicable, clear
    /// metadata, resume completion waiter, and try to resume next
    /// scratchpad waiter.
    void complete_task(std::size_t i) noexcept {
        auto& meta = tasks_[i];

        if (meta.is_scratchpad && meta.scratch_offset != scratch_unused) {
            scratchpad_free(meta.scratch_offset, meta.size);
            meta.scratch_offset = scratch_unused;
        }

        meta.handle = {};
        meta.occupied = false;
        meta.suspended = true;

        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", fn_names_[i], "completed");

        // Resume completion waiter if any
        if (meta.has_completion_waiter) {
            meta.has_completion_waiter = false;
            auto h = meta.completion_waiter;
            meta.completion_waiter = {};
            // Clear the suspended flag on the waiting task before
            // resuming it (register_completion_waiter set it to keep
            // the direct-resume path at bay).  If the waiter
            // suspends again on its own timer, the timer's resume
            // path will set suspended=false as before.
            std::size_t waiter_idx = find_slot_for_handle(h);
            if (waiter_idx < num_tasks) {
                tasks_[waiter_idx].suspended = false;
            }
            h.resume();
        }

        // Resume next scratchpad waiter (if space freed)
        if (meta.is_scratchpad) try_resume_waiter();
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
        // Leading `std::size_t{0}` makes the fold well-formed for empty packs
        // (e.g. an engine with zero tasks).  Pre-refactor this was masked
        // because `report()` was non-virtual; the vtable generation now
        // forces eager instantiation, exposing the empty-pack case.
        return (std::size_t{0} + ... + (is_scratchpad_entry<Is>() ? std::size_t{1} : std::size_t{0}));
    }
    static constexpr std::size_t num_scratchpad = count_scratchpad_impl(std::make_index_sequence<num_tasks>{});
    static constexpr std::size_t num_reserved = num_tasks - num_scratchpad;

    // -----------------------------------------------------------------------
    // Compile-time distinctness check — O(N log N) sort+adjacent
    //
    // Equal UIDs are LEGAL only for the same function (multi-instance
    // registration).  For distinct functions, equal UIDs are a FNV-1a
    // hash collision, which is a hard error.
    //
    // Algorithm:
    //   1. Collect N (uid, index) pairs into a constexpr array.
    //   2. Sort by uid (constexpr insertion sort, O(N²) work in a
    //      single constexpr function — one instantiation, cheap).
    //      The KEY win vs the prior O(N²) pairwise check: the
    //      number of TYPE INSTANTIATIONS drops to O(N) for the
    //      adjacent check below, with each check O(log N) via
    //      `detail::type_at` (divide-and-conquer std::tuple_element_t).
    //   3. The adjacent check is only CORRECT after sorting: without
    //      the sort, declaration-adjacent pairs are the only ones
    //      compared, and non-adjacent collisions are silently missed.
    //      Sorting groups equal UIDs together so the check catches
    //      ALL equal-uid pairs.
    //   4. For adjacent sorted entries, check: uid[i] == uid[i+1] ⇒
    //      same fn.  Uses `detail::uid_pair_ok` for the predicate.
    //
    // Guarantee: equal UID ⇒ same fn; different-fn collision → hard
    // error.
    // -----------------------------------------------------------------------

    // Each entry is (uid, original_index).  The original index is needed
    // to look up the entry's `fn` at compile time for the adjacent check.
    struct sorted_uid_entry {
        std::uint32_t uid;
        std::size_t index;
        constexpr sorted_uid_entry() noexcept : uid(0), index(0) {}
        constexpr sorted_uid_entry(std::uint32_t u, std::size_t i) noexcept
            : uid(u), index(i) {}
    };

    template <std::size_t I>
    static constexpr std::uint32_t entry_uid() noexcept {
        // `fn` is a value (function pointer NTTP), not a type — so no `typename`.
        return task_uid_v<detail::type_at<I, Entries...>::type::fn>.value;
    }

    template <std::size_t... Is>
    static constexpr std::array<sorted_uid_entry, num_tasks>
    collect_sorted_uids(std::index_sequence<Is...>) {
        // Collect (uid, index) pairs.
        std::array<sorted_uid_entry, num_tasks> arr = {{
            sorted_uid_entry{entry_uid<Is>(), Is}...
        }};
        // Constexpr insertion sort by uid (ties broken by index for
        // stability).  Sorting groups equal UIDs together so the
        // adjacent check below catches all equal-uid pairs.
        for (std::size_t i = 1; i < num_tasks; ++i) {
            sorted_uid_entry key = arr[i];
            std::size_t j = i;
            while (j > 0 && (arr[j - 1].uid > key.uid ||
                             (arr[j - 1].uid == key.uid &&
                              arr[j - 1].index > key.index))) {
                arr[j] = arr[j - 1];
                --j;
            }
            arr[j] = key;
        }
        return arr;
    }

    static constexpr std::array<sorted_uid_entry, num_tasks> sorted_uids_ =
        collect_sorted_uids(std::make_index_sequence<num_tasks>{});

    // Check adjacent pair: if uids match, fns must match.
    template <std::size_t I>
    static constexpr bool check_sorted_adjacent() {
        if constexpr (I + 1 >= num_tasks) {
            return true;
        } else {
            constexpr auto ui = sorted_uids_[I].uid;
            constexpr auto uj = sorted_uids_[I + 1].uid;
            constexpr auto idx_i = sorted_uids_[I].index;
            constexpr auto idx_j = sorted_uids_[I + 1].index;
            using entry_i = typename detail::type_at<idx_i, Entries...>::type;
            using entry_j = typename detail::type_at<idx_j, Entries...>::type;
            // Function pointer comparison is only valid when the types
            // match.  Different fn types → different fns → same=false.
            if constexpr (std::is_same_v<decltype(entry_i::fn),
                                         decltype(entry_j::fn)>) {
                constexpr bool same = (entry_i::fn == entry_j::fn);
                return detail::uid_pair_ok(ui, uj, same);
            } else {
                return detail::uid_pair_ok(ui, uj, false);
            }
        }
    }

    // O(N) fold over adjacent checks.  For num_tasks==0 the fold is
    // `(true && ...)` which is well-defined (empty pack → true).
    template <std::size_t... Is>
    static constexpr bool check_all_sorted_adjacent(std::index_sequence<Is...>) {
        return (check_sorted_adjacent<Is>() && ... && true);
    }

    static_assert(
        check_all_sorted_adjacent(std::make_index_sequence<num_tasks>{}),
        "Duplicate task UIDs for different functions (FNV-1a hash collision). "
        "Equal UIDs are allowed only for the same function (multi-instance).");

    void init_fn_names() {
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((fn_names_[Is] = detail::format_tag<Entries::fn>()), ...);
        }(std::make_index_sequence<num_tasks>{});
    }

    // Fill `task_meta::uid` per slot.  Mirrors `init_fn_names()`'s
    // structure: a single pack-expansion lambda iterates each entry in
    // declaration order and writes its UID into `tasks_[I].uid`.
    void init_uids() {
        [this]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((tasks_[Is].uid = task_uid_v<Entries::fn>.value), ...);
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
        return static_cast<basic_engine*>(ctx)->add_timer(wake, h);
    }

    static void mark_suspended(void* ctx, std::coroutine_handle<> h) noexcept {
        auto* self = static_cast<basic_engine*>(ctx);
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
    // Scratchpad coroutine creation — single method, runtime slot idx
    //
    // Uses the invoke_table_ thunk to call the coroutine, reads
    // per-slot state from `tasks_[idx]`.  No template parameter, no
    // per-I instantiation.  Called only for scratchpad slots (by
    // `try_scratchpad` and `try_resume_waiter`).
    // -----------------------------------------------------------------------

    void execute_scratchpad(std::size_t idx, std::size_t alloc) noexcept {
        auto& meta = tasks_[idx];
        meta.scratch_offset = alloc;
        if (meta.handle) { meta.handle.destroy(); meta.handle = {}; }

        detail::current_task_allocator = {&scratchpad_pool_[alloc], meta.size, nullptr};
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};
        meta.suspended = false;
        meta.occupied = true;

        // Thunk returns a task; if it returns an empty task (arg-taking
        // fn), the slot is already marked occupied above — undo that
        // and bail.  In practice, scratchpad thunks always succeed
        // (arg-taking fns can't be scratchpad tasks).
        task t = invoke_table_[idx](this);
        if (!t) {
            meta.occupied = false;
            meta.suspended = true;
            detail::current_external_suspension_registrar = {};
            detail::current_timer_registrar = {};
            return;
        }
        meta.handle = t.handle();
        meta.handle.resume();
        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};

        if (meta.handle.done()) {
            complete_task(idx);
        }

        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", fn_names_[idx], "scratchpad triggered");
    }

    std::array<timer_entry, Config::max_timers> timers_{};
    std::size_t timer_count_ = 0;

    // -----------------------------------------------------------------------
    // Internal: trigger a reserved task at a given index
    // -----------------------------------------------------------------------

    template <std::size_t I, typename... Args>
    error trigger_reserved(Args&&... args) {
        auto& meta = tasks_[I];

        using entry_t = typename detail::type_at<I, Entries...>::type;
        using fn_type = decltype(entry_t::fn);

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

        // Arg-free guard: the interface `trigger(task_uid)` calls
        // this with no args (`Args = {}`).  The actual call below is
        // wrapped in an invocability check so arg-taking fns return
        // `task_not_registered` instead of failing to compile.  The
        // templated `trigger<Fn>(args...)` on `basic_engine` supplies
        // matching `Args...` and always takes the call branch.
        if constexpr (std::is_member_function_pointer_v<fn_type>) {
            using Class = typename entry_t::class_type;
            auto* obj = static_cast<Class*>(meta.self);
            if constexpr (std::is_invocable_v<fn_type, Class*, Args...>) {
                meta.handle = (obj->*entry_t::fn)(std::forward<Args>(args)...).handle();
            } else {
                return error::task_not_registered;
            }
        } else {
            if constexpr (std::is_invocable_v<fn_type, Args...>) {
                meta.handle = entry_t::fn(std::forward<Args>(args)...).handle();
            } else {
                return error::task_not_registered;
            }
        }

        meta.handle.resume();
        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};

        if (meta.handle.done()) {
            complete_task(I);
        } else {
            meta.occupied = true;
        }

        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", fn_names_[I], "triggered");
        return error::ok;
    }

    // -----------------------------------------------------------------------
    // Internal: try to trigger a task by instance + function pointer match
    // -----------------------------------------------------------------------

    template <std::size_t I, typename Class, typename Fn, typename... Args>
    bool try_trigger_instance(Class& obj, Fn fn, task_handle& result, Args&&... args) {
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
                        result = {this, I, error::task_already_running};
                    } else {
                        // Forward args via compile-time dispatch
                        result = {this, I, trigger_reserved<I>(std::forward<Args>(args)...)};
                    }
                }
                return true;
            }
        }
        return false;
    }

    task_handle try_scratchpad(std::size_t idx) {
        auto& meta = tasks_[idx];
        if (meta.occupied) return {this, idx, error::task_already_running};

        // FIFO ordering: if waiters exist, can't jump ahead.
        if (scratchpad_waiter_count_ > 0) return {this, idx, error::capacity_exceeded};

        std::size_t alloc = scratchpad_allocate(meta.size);
        if (alloc == Config::scratchpad_pool_size) return {this, idx, error::capacity_exceeded};

        execute_scratchpad(idx, alloc);
        return {this, idx, error::ok};
    }

    // -----------------------------------------------------------------------
    // Runtime-index arg-free reserved trigger
    //
    // Uses the invoke_table_ thunk to call the coroutine arg-free.
    // If the thunk returns an empty task (arg-taking fn sentinel),
    // returns `error::task_not_registered`.  Called only for reserved
    // slots (by `dispatch_by_idx`).
    // -----------------------------------------------------------------------

    error trigger_reserved_argfree(std::size_t idx) noexcept {
        auto& meta = tasks_[idx];
        if (pool_overflow_) {
            log::detail::log_impl<Config, log_level::error, Logger, ClockType>(
                "ERR", fn_names_[idx], "reserved pool exhausted");
            return error::capacity_exceeded;
        }
        if (meta.occupied) {
            log::detail::log_impl<Config, log_level::warn, Logger, ClockType>(
                "WRN", fn_names_[idx], "already running");
            return error::task_already_running;
        }
        if (meta.handle) { meta.handle.destroy(); meta.handle = {}; }

        detail::current_task_allocator = {&pool_[meta.offset], meta.size, nullptr};
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};
        meta.suspended = false;

        // Thunk returns a task; empty task = arg-taking fn.
        task t = invoke_table_[idx](this);
        if (!t) {
            detail::current_external_suspension_registrar = {};
            detail::current_timer_registrar = {};
            return error::task_not_registered;
        }
        meta.handle = t.handle();
        meta.handle.resume();
        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};

        if (meta.handle.done()) {
            complete_task(idx);
        } else {
            meta.occupied = true;
        }

        log::detail::log_impl<Config, log_level::info, Logger, ClockType>(
            "INF", fn_names_[idx], "triggered");
        return error::ok;
    }

    /// Dispatch by runtime index: scratchpad → try_scratchpad;
    /// reserved → trigger_reserved_argfree.  Caller must have already
    /// verified `idx < num_tasks`.
    task_handle dispatch_by_idx(std::size_t idx) noexcept {
        if (tasks_[idx].is_scratchpad) {
            return try_scratchpad(idx);
        }
        return {this, idx, trigger_reserved_argfree(idx)};
    }

public:
    basic_engine() = delete;

    basic_engine(std::array<void*, num_tasks> self_ptrs) : tasks_{} {
        for (std::size_t i = 0; i < num_tasks; ++i) {
            tasks_[i].self = self_ptrs[i];
            tasks_[i].scratch_offset = scratch_unused;
        }
        init_fn_names();
        init_uids();
        build_uid_table();
        build_invoke_table();
        init_flags();
        probe_frame_sizes();
    }

    basic_engine(const basic_engine&) = delete;
    basic_engine& operator=(const basic_engine&) = delete;

    ~basic_engine() {
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
            return task_handle{this, idx, trigger_reserved<idx>(std::forward<Args>(args)...)};
        }
    }

    // Non-const member function (instance-based)
    // Instance-based dispatch is always non-blocking (uses try_trigger semantics
    // for scratchpad tasks).  For blocking behavior, use NTTP-based trigger<&fn>().
    template <typename Class, typename Ret, typename... FnArgs, typename... Args>
    task_handle trigger(Class& obj, Ret (Class::*fn)(FnArgs...), Args&&... args) {
        task_handle result{this, num_tasks, error::task_not_registered};
        [this, &obj, fn, &result, &args...]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((result.err == error::task_not_registered
                  ? (try_trigger_instance<Is>(obj, fn, result, std::forward<Args>(args)...), 0)
                  : 0),
             ...);
        }(std::make_index_sequence<num_tasks>{});
        return result;
    }

    // Const member function
    template <typename Class, typename Ret, typename... FnArgs, typename... Args>
    task_handle trigger(Class& obj, Ret (Class::*fn)(FnArgs...) const, Args&&... args) {
        task_handle result{this, num_tasks, error::task_not_registered};
        [this, &obj, fn, &result, &args...]<std::size_t... Is>(std::index_sequence<Is...>) {
            ((result.err == error::task_not_registered
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
    task_handle try_trigger(Args&&... args) {
        constexpr auto idx = slot_index<Fn>();
        using entry_t = typename detail::type_at<idx, Entries...>::type;

        if constexpr (entry_t::is_scratchpad) {
            return try_scratchpad(idx);
        } else {
            return task_handle{this, idx, trigger_reserved<idx>(std::forward<Args>(args)...)};
        }
    }

    // -------------------------------------------------------------------
    // engine interface overrides
    // -------------------------------------------------------------------

    engine_report report() const noexcept override {
        engine_report r{};
        r.task_count = num_tasks;
        r.reserved_count = num_reserved;
        r.scratchpad_count = num_scratchpad;
        r.scratchpad_size = Config::scratchpad_pool_size;
        return r;
    }

    // Compile-time engine report.  All fields are constexpr from
    // `Config` + `sizeof...(Entries)`.  Usable in static_assert and
    // deployment-time prints.  NO frame sizes (coroutines aren't
    // constexpr in C++20).
    static constexpr engine_static_report static_report() noexcept {
        return engine_static_report{
            sizeof...(Entries),
            num_reserved,
            num_scratchpad,
            Config::reserved_pool_size,
            Config::scratchpad_pool_size,
            Config::max_timers
        };
    }

    /// Dump via Logger::print().
    engine_report dump() const override {
        auto r = report();
        dump_pool_summary_log();
        dump_scratchpad_summary_log();
        for (std::size_t i = 0; i < num_tasks; ++i) {
            dump_one_line_log(i);
        }
        return r;
    }

    /// Type-erased sink dump.  Dispatches to the erased helpers below.
    engine_report dump_erased(void (*sink)(void*, std::string_view),
                              void* ctx) const override {
        auto r = report();
        dump_pool_summary_sink_erased(sink, ctx);
        dump_scratchpad_summary_sink_erased(sink, ctx);
        for (std::size_t i = 0; i < num_tasks; ++i) {
            dump_one_line_sink_erased(i, sink, ctx);
        }
        return r;
    }

    /// Dump via custom sink (templated; can accept capturing lambdas).
    /// Coexists with the base class's templated `dump<Sink>` wrapper.
    template <typename Sink>
    engine_report dump(Sink&& sink) const {
        auto r = report();
        dump_pool_summary_sink(sink);
        dump_scratchpad_summary_sink(sink);
        for (std::size_t i = 0; i < num_tasks; ++i) {
            dump_one_line_sink(i, sink);
        }
        return r;
    }

    bool pool_exhausted() const noexcept override { return pool_overflow_; }

    bool task_is_done(std::size_t idx) const noexcept override {
        // OOB guard: a `task_handle` constructed with `idx = num_tasks`
        // (returned by `trigger(uid)` for an unregistered UID) calls
        // this with OOB idx.  Treat OOB as "done" so `co_await h.done()`
        // returns immediately instead of reading past `tasks_`.
        if (idx >= num_tasks) return true;
        auto& meta = tasks_[idx];
        return !meta.occupied || (meta.handle && meta.handle.done());
    }

    bool register_completion_waiter(std::size_t idx,
                                    std::coroutine_handle<> h) noexcept override {
        // Same OOB guard as `task_is_done`; don't suspend on bad idx.
        if (idx >= num_tasks) return false;
        auto& meta = tasks_[idx];
        if (meta.has_completion_waiter) return false;
        meta.completion_waiter = h;
        meta.has_completion_waiter = true;
        // Mark the WAITING task as suspended so the direct-resume
        // path in tick() does not incorrectly resume it.  The
        // completion-wait contract: the waiter is only resumed when
        // complete_task() fires for the waitee.
        std::size_t waiter_idx = find_slot_for_handle(h);
        if (waiter_idx < num_tasks) {
            tasks_[waiter_idx].suspended = true;
        }
        return true;
    }

    // -------------------------------------------------------------------
    // UID-based trigger overrides
    // -------------------------------------------------------------------

    task_handle trigger(task_uid uid) override {
        std::size_t idx = find_slot_by_uid(uid.value);
        if (idx >= num_tasks) {
            return {this, num_tasks, error::task_not_registered};
        }
        return dispatch_by_idx(idx);
    }

    task_handle trigger_obj(void* obj, task_uid uid) override {
        std::size_t idx = find_slot_by_uid_and_obj(uid.value, obj);
        if (idx >= num_tasks) {
            return {this, num_tasks, error::task_not_registered};
        }
        return dispatch_by_idx(idx);
    }

private:
    // -----------------------------------------------------------------------
    // Frame-size probing — single method, runtime slot idx
    //
    // Reads `tasks_[idx].is_scratchpad` (set by `init_flags()`) and
    // uses the invoke_table_ thunk to call the coroutine for probing.
    // The thunk's arg-free guard handles arg-taking tasks (returns
    // empty task → falls back to `default_frame_size`).
    // -----------------------------------------------------------------------

    void probe_frame_sizes() {
        std::size_t current_offset = 0;
        for (std::size_t i = 0; i < num_tasks; ++i) {
            probe_frame_size(i, current_offset);
        }
    }

    void probe_frame_size(std::size_t idx, std::size_t& current_offset) {
        auto& meta = tasks_[idx];
        constexpr std::size_t min_probe_space = 64;

        if (meta.is_scratchpad) {
            meta.offset = 0;
            if (pool_overflow_) { meta.size = 0; return; }
            std::size_t remaining = Config::reserved_pool_size - current_offset;
            bool probed = false;
            if (remaining >= min_probe_space) {
                detail::current_task_allocator = {&pool_[current_offset], remaining, &meta.size};
                task t = invoke_table_[idx](this);
                if (t) {
                    t.handle().destroy();
                    probed = true;
                }
                detail::current_task_allocator = {};
            }
            if (!probed) meta.size = detail::default_frame_size;
            return;
        }

        // Reserved task
        if (pool_overflow_) { meta.offset = 0; meta.size = 0; return; }
        current_offset = (current_offset + alignof(std::max_align_t) - 1) & ~(alignof(std::max_align_t) - 1);
        meta.offset = current_offset;
        std::size_t remaining = Config::reserved_pool_size - current_offset;
        bool can_probe = (remaining >= min_probe_space);
        bool probed = false;
        if (can_probe) {
            detail::current_task_allocator = {&pool_[current_offset], remaining, &meta.size};
            task t = invoke_table_[idx](this);
            if (t) {
                t.handle().destroy();
                probed = true;
            }
            detail::current_task_allocator = {};
        }
        if (!probed) meta.size = detail::default_frame_size;
        current_offset += meta.size;
        if (current_offset > Config::reserved_pool_size) {
            pool_overflow_ = true;
            meta.offset = 0;
            meta.size = 0;
        }
    }

    // -----------------------------------------------------------------------
    // Dump helpers — templated (used by basic_engine::dump<Sink>)
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
        if (n > 0) {
            std::string_view sv{line, static_cast<std::size_t>(n)};
            sink(sv);
        }
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
        if (n > 0) {
            std::string_view sv{line, static_cast<std::size_t>(n)};
            sink(sv);
        }
    }

    // One non-templated method per dump kind.  Reads per-slot
    // `is_scratchpad` (runtime, set by init_flags()) and `fn_names_[i]`
    // from the data arrays.
    void dump_one_line_log(std::size_t i) const {
        const auto& meta = tasks_[i];
        char line[256];
        if (meta.is_scratchpad) {
            std::snprintf(line, sizeof(line), "[%zu] <%s>  scratchpad  size=%zuB",
                          i, fn_names_[i], meta.size);
        } else {
            std::snprintf(line, sizeof(line), "[%zu] <%s>  reserved  offset=%zu  size=%zuB",
                          i, fn_names_[i], meta.offset, meta.size);
        }
        log::detail::log_impl<Config, log_level::info, Logger, ClockType>("INF", fn_names_[i], "%s", line);
    }

    template <typename Sink>
    void dump_one_line_sink(std::size_t i, Sink& sink) const {
        const auto& meta = tasks_[i];
        char line[256];
        int n;
        if (meta.is_scratchpad) {
            n = std::snprintf(line, sizeof(line), "[%zu] <%s>  scratchpad  size=%zuB",
                              i, fn_names_[i], meta.size);
        } else {
            n = std::snprintf(line, sizeof(line), "[%zu] <%s>  reserved  offset=%zu  size=%zuB",
                              i, fn_names_[i], meta.offset, meta.size);
        }
        if (n > 0) {
            std::string_view sv{line, static_cast<std::size_t>(n)};
            sink(sv);
        }
    }

    // -----------------------------------------------------------------------
    // Dump helpers — type-erased (used by dump_erased)
    // -----------------------------------------------------------------------

    void dump_pool_summary_sink_erased(
            void (*sink)(void*, std::string_view), void* ctx) const {
        std::size_t used = total_pool_used();
        double pct = Config::reserved_pool_size > 0
            ? (static_cast<double>(used) / Config::reserved_pool_size) * 100.0 : 0.0;
        char line[256];
        int n = std::snprintf(line, sizeof(line), "Reserved pool: %zuB / %zuB used (%.1f%%)",
                              used, static_cast<std::size_t>(Config::reserved_pool_size), pct);
        if (n > 0) {
            // Local variable avoids the constrained `string_view(It, End)`
            // overload (a libstdc++ 14 / GCC 14 bug: it instantiates its
            // noexcept expression before the function is defined when the
            // construction appears in an eagerly-instantiated context such
            // as a virtual override).  Construction of the local picks the
            // simple `(const char*, size_t)` overload.
            std::string_view sv{line, static_cast<std::size_t>(n)};
            sink(ctx, sv);
        }
    }

    void dump_scratchpad_summary_sink_erased(
            void (*sink)(void*, std::string_view), void* ctx) const {
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
        if (n > 0) {
            std::string_view sv{line, static_cast<std::size_t>(n)};
            sink(ctx, sv);
        }
    }

    // One non-templated type-erased dump helper.  The sink callback
    // and its context are passed through directly (no capture).
    void dump_one_line_sink_erased(
            std::size_t i,
            void (*sink)(void*, std::string_view), void* ctx) const {
        const auto& meta = tasks_[i];
        char line[256];
        int n;
        if (meta.is_scratchpad) {
            n = std::snprintf(line, sizeof(line), "[%zu] <%s>  scratchpad  size=%zuB",
                              i, fn_names_[i], meta.size);
        } else {
            n = std::snprintf(line, sizeof(line), "[%zu] <%s>  reserved  offset=%zu  size=%zuB",
                              i, fn_names_[i], meta.offset, meta.size);
        }
        if (n > 0) {
            std::string_view sv{line, static_cast<std::size_t>(n)};
            sink(ctx, sv);
        }
    }

public:

    /// Resume expired timers and their associated coroutines.
    void tick() override {
        detail::current_timer_registrar = {&timer_registrar_add, this};
        detail::current_external_suspension_registrar = {&mark_suspended, this};

        // Clean up completed scratchpad tasks (reserved tasks complete inline).
        for (std::size_t i = 0; i < num_tasks; ++i) {
            auto& meta = tasks_[i];
            if (meta.occupied && meta.handle && meta.handle.done()) {
                complete_task(i);
            }
        }

        // (a) Resume directly-runnable tasks.
        //
        // A task is "directly runnable" iff it is occupied, its
        // coroutine handle is not done, it has not been marked
        // suspended (by `mark_suspended` for external events, or by
        // `register_completion_waiter` for completion-wait), and it
        // is NOT waiting on a timer.  The suspended flag is the
        // single source of truth for "this task is waiting on
        // something the engine should not resume directly here";
        // completion-wait suspends the waiting task, timer-waits are
        // detected below via `has_timer`.
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
                        complete_task(i);
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
                    complete_task(task_idx);
                }
            }
        }

        detail::current_external_suspension_registrar = {};
        detail::current_timer_registrar = {};
    }
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

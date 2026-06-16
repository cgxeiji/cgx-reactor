#pragma once

#include <coroutine>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace cgx::reactor {

// ---------------------------------------------------------------------------
// Compile-time tag (variadic chars, null-terminated for string output)
// ---------------------------------------------------------------------------

/// Compile-time character tag for task identification / debugging.
template <char... Cs>
struct tag {
    static constexpr char value[sizeof...(Cs) + 1] = {Cs..., '\0'};
    static constexpr std::size_t size = sizeof...(Cs);
};

template <char... Cs>
constexpr char tag<Cs...>::value[sizeof...(Cs) + 1];

// ---------------------------------------------------------------------------
// User-defined literal for tags — `"DISP"_tag` → tag<'D','I','S','P'>
//
// This is the C++20 template UDL form (P1040R6).  It is standard, but
// clang emits a `-Wgnu-string-literal-operator-template` warning because
// the syntax originated as a GCC extension.  The warning is suppressed
// locally here so users get clean builds.
//
// Usage at the call site requires the operator to be in scope:
//   using cgx::reactor::operator""_tag;       // (or)
//   using namespace cgx::reactor;
// ---------------------------------------------------------------------------

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wgnu-string-literal-operator-template"

template <typename CharT, CharT... Cs>
constexpr tag<Cs...> operator""_tag() noexcept {
    return {};
}

#pragma clang diagnostic pop

// ---------------------------------------------------------------------------
// make_tag — escape hatch for cases where the tag chars are not a literal
// (rare; useful in metaprogramming that builds tags from other values).
// ---------------------------------------------------------------------------

/// Construct a tag from explicit characters.
/// Usage:  make_tag<'H','E','L','O'>()
template <char... Cs>
constexpr tag<Cs...> make_tag() noexcept {
    return {};
}

// ---------------------------------------------------------------------------
// scratch<Fn> — marks a task as scratchpad (one-shot, pool-allocated)
// ---------------------------------------------------------------------------

template <auto Fn>
struct scratch {
    static constexpr auto fn = Fn;
    using fn_type = decltype(Fn);
};

/// Variable template so scratch<Fn> can be used as an NTTP in task_list:
///   task_list<scratch_v<&method>, &other_method>
template <auto Fn>
constexpr scratch<Fn> scratch_v{};

// ---------------------------------------------------------------------------
// Task list — declared in the user's class header to enumerate member tasks
// ---------------------------------------------------------------------------

template <auto... MemFns>
struct task_list {};

// ---------------------------------------------------------------------------
// Bound spec — wraps a class instance and its member-function task list
// ---------------------------------------------------------------------------

template <typename Class_, auto Tag_, auto... MemFns_>
struct bound {
    Class_* self;
    using tag_t = decltype(Tag_);
    static constexpr auto tag_value = Tag_;
    using class_type = Class_;
    static constexpr std::size_t num_fns = sizeof...(MemFns_);
};

// ---------------------------------------------------------------------------
// Free spec — wraps a free function as a task
// ---------------------------------------------------------------------------

template <auto Tag_, auto Fn_>
struct free_spec {
    using tag_t = decltype(Tag_);
    static constexpr auto tag_value = Tag_;
    static constexpr std::size_t num_fns = 1;
};

// ---------------------------------------------------------------------------
// Helper: unwrap task_list to extract MemFn pack
// ---------------------------------------------------------------------------

namespace detail {

template <typename T>
struct unwrap_task_list_helper;

template <auto... MemFns>
struct unwrap_task_list_helper<task_list<MemFns...>> {
    template <typename Class, auto Tag>
    using bound_type = bound<Class, Tag, MemFns...>;
};

} // namespace detail

// ---------------------------------------------------------------------------
// register_instance — read Class::reactor_tasks, return bound<...>
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// task_meta: per-task metadata for the reserved-pool engine.
// ---------------------------------------------------------------------------

namespace detail {

struct task_meta {
    std::coroutine_handle<> handle;
    bool occupied = false;
    bool suspended = true;
    bool is_scratchpad = false;
    void* self = nullptr;
    std::size_t offset = 0;          // reserved pool offset
    std::size_t size = 0;             // coroutine frame size
    std::size_t scratch_offset = 0;   // offset in scratchpad pool (0 = not allocated)
};

} // namespace detail

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Engine report — diagnostics from engine::dump()
// ---------------------------------------------------------------------------

struct engine_report {
    std::size_t task_count;
    std::size_t reserved_count;
    std::size_t scratchpad_count;
    std::size_t scratchpad_size;
};

template <auto Tag = tag<>{}, typename Class>
auto register_instance(Class& obj) {
    return typename detail::unwrap_task_list_helper<
        typename Class::reactor_tasks
    >::template bound_type<Class, Tag>{&obj};
}

// ---------------------------------------------------------------------------
// register_task — wrap a free function as a free_spec
// ---------------------------------------------------------------------------

// Single-argument overload: no explicit tag (auto-generate)
template <auto Fn>
constexpr free_spec<tag<>{}, Fn> register_task() noexcept {
    return {};
}

// Two-argument overload: explicit tag + function pointer
template <auto Tag, auto Fn>
constexpr free_spec<Tag, Fn> register_task() noexcept {
    return {};
}

// ---- detail metaprogramming helpers (used by engine.hpp) -----------------

namespace detail {

// Simple type list
template <typename...>
struct type_list {};

// Concatenate two type lists
template <typename A, typename B>
struct concat;

template <typename... As, typename... Bs>
struct concat<type_list<As...>, type_list<Bs...>> {
    using type = type_list<As..., Bs...>;
};

// Type at index
template <std::size_t I, typename... Ts>
struct type_at;

template <typename T, typename... Rest>
struct type_at<0, T, Rest...> {
    using type = T;
};

template <std::size_t I, typename T, typename... Rest>
struct type_at<I, T, Rest...> : type_at<I - 1, Rest...> {};

// Traits to detect and unwrap scratch<> wrappers
template <typename T>
struct is_scratch_type : std::false_type {};

template <auto Fn>
struct is_scratch_type<scratch<Fn>> : std::true_type {};

template <auto Entry>
constexpr auto unwrap_entry() noexcept {
    if constexpr (is_scratch_type<decltype(Entry)>::value) {
        return decltype(Entry)::fn;
    } else {
        return Entry;
    }
}

// Count total slots from a pack of specs
template <typename... Specs>
struct count_slots;

template <>
struct count_slots<> : std::integral_constant<std::size_t, 0> {};

template <typename Class, auto Tag, auto... MemFns, typename... Rest>
struct count_slots<bound<Class, Tag, MemFns...>, Rest...>
    : std::integral_constant<std::size_t,
                             sizeof...(MemFns) + count_slots<Rest...>::value> {};

template <auto Tag, auto Fn, typename... Rest>
struct count_slots<free_spec<Tag, Fn>, Rest...>
    : std::integral_constant<std::size_t, 1 + count_slots<Rest...>::value> {};

// Per-slot descriptor: carries function pointer (NTTP), tag (NTTP), class type,
// and a flag indicating whether the task uses the scratchpad pool.
template <auto Fn_, auto Tag_, typename Class_, bool IsScratchpad_ = false>
struct task_descriptor {
    static constexpr auto fn = Fn_;
    using tag_type = decltype(Tag_);
    static constexpr auto tag_value = Tag_;
    using class_type = Class_;
    static constexpr bool is_scratchpad = IsScratchpad_;
};

// Flatten a pack of specs into a flat type_list of task_descriptors
template <typename... Specs>
struct spec_unfolder;

template <>
struct spec_unfolder<> {
    using type = type_list<>;
};

template <typename Class, auto Tag, auto... MemFns, typename... Rest>
struct spec_unfolder<bound<Class, Tag, MemFns...>, Rest...> {
    using rest = typename spec_unfolder<Rest...>::type;
    
    template <auto MF>
    using normalized_desc = task_descriptor<
        unwrap_entry<MF>(),
        Tag,
        Class,
        is_scratch_type<decltype(MF)>::value
    >;
    
    using type = typename concat<
        type_list<normalized_desc<MemFns>...>,
        rest
    >::type;
};

template <auto Tag, auto Fn, typename... Rest>
struct spec_unfolder<free_spec<Tag, Fn>, Rest...> {
    using rest = typename spec_unfolder<Rest...>::type;
    using type = typename concat<
        type_list<task_descriptor<Fn, Tag, void, false>>,
        rest
    >::type;
};

// Compile-time index of a function pointer among NTTPs
template <auto Target, auto First, auto... Rest>
consteval std::size_t index_of_fn() {
    if constexpr (std::is_same_v<decltype(First), decltype(Target)>) {
        if constexpr (First == Target) {
            return 0;
        } else if constexpr (sizeof...(Rest) == 0) {
            return static_cast<std::size_t>(-1);  // not found
        } else {
            return 1 + index_of_fn<Target, Rest...>();
        }
    } else {
        // Types don't match, skip this one
        if constexpr (sizeof...(Rest) == 0) {
            return static_cast<std::size_t>(-1);  // not found
        } else {
            return 1 + index_of_fn<Target, Rest...>();
        }
    }
}

} // namespace detail

} // namespace cgx::reactor

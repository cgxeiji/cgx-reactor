# cgx-reactor Glossary

Domain language for the cgx-reactor coroutine-based reactive scheduler library, targeting embedded systems (RP2040) and PC testing environments.

## Language

### Core Concepts

**Engine**:
The reactor instance that manages task scheduling and timer queue. Template-parameterized for compile-time task registration and configuration. Constructed via `make_engine<Config, Clock>(specs...)`.
_Avoid_: Reactor, scheduler, dispatcher

**Task**:
A coroutine registered with the engine at compile time. Can be fire-and-return (suspends, runs to completion, slot becomes idle) or infinite-loop (periodic sensor reading) or reactive-loop (waits for external events). Each task has a fixed slot, no dynamic allocation.
_Avoid_: Coroutine, fiber, thread, job

**Signal**:
A standalone pub/sub primitive for inter-coroutine communication. Tasks can `listen()` to suspend until the signal fires, or `fire(value)` to broadcast to all listeners. The engine doesn't manage signals — they resume coroutines directly.
_Avoid_: Event, message, broadcast

**Channel**:
A standalone point-to-point communication primitive with a bounded buffer. Unlike signals (broadcast), channels implement work distribution — each value is consumed by exactly one consumer. Provides blocking `push()`/`pop()` with awaiters, non-blocking `try_push()` for ISR contexts, and `close()` to signal shutdown. `pop()` is const-qualified so a `const channel&` can receive (mirrors Go's receive-only `<-chan T` pattern). The engine doesn't manage channels — they resume coroutines directly via awaiters.
_Avoid_: Queue, pipe, mailbox, FIFO (though implementation is a FIFO ring buffer)

**Clock**:
A concept wrapping `std::chrono` types. Provides `now()` returning `time_point`. Default implementation uses `std::chrono::steady_clock`. RP2040 provides a clock wrapping Pico SDK timer. Tests provide a mock clock.
_Avoid_: Timer, time source

**Delay MS**:
Awaitable that suspends the current coroutine for a relative duration. Guarantees the coroutine is resumed *at least* N ms after suspension, but periodic use accumulates drift because each delay measures from the resume time, not from a fixed epoch. For drift-free periodic scheduling, use `delay_until` or `delay_quantized`.
_Avoid_: sleep, wait

**Delay Until**:
Awaitable that suspends the current coroutine until a specific absolute time point. Does not accumulate drift when used in a periodic loop (`next += period` after each wake). The caller manages the epoch. Returns `error::capacity_exceeded` if the timer queue is full.
_Avoid_: delay_absolute, delay_at

**Delay Quantized**:
Awaitable that suspends until the next grid-aligned tick relative to the clock's epoch. Snaps to multiples of the interval — e.g., with a 100ms interval, always wakes at 100ms, 200ms, 300ms… regardless of when the call occurs. Zero drift, no state. Returns `error::capacity_exceeded` if the timer queue is full.
_Avoid_: delay_periodic, delay_aligned

**Tag**:
A compile-time character sequence (typically 4 chars) used to identify a task slot for debugging and error messages. Constructed via the `"DISP"_tag` user-defined literal (C++20 template UDL, P1040R6) or the escape hatch `make_tag<'D','I','S','P'>()`. The UDL requires a `using` declaration at the call site (`using cgx::reactor::operator""_tag;`).
_Avoid_: Label, id, name

**Reactor Tasks**:
The mandatory `using reactor_tasks = …` alias in every class that exposes member functions as reactor tasks. Its type is `task_list<MemFns...>` (the engine-provided template). The alias is the *definition-site* registration — adding a method to the class and listing it in `reactor_tasks` is enough; `main.cpp` does not need to be edited.
```cpp
class MyDriver {
    cgx::reactor::task run_loop();
    cgx::reactor::task fire_once(int);
    using reactor_tasks = cgx::reactor::task_list<
        &MyDriver::run_loop, &MyDriver::fire_once>;
};
```
_Avoid_: Task set, task list (as a runtime concept), task table, task registry

### Task Lifecycle States

**Idle**:
Task slot is available. Task can be triggered. No coroutine frame is active.

**Running**:
Task has been triggered and is executing. Cannot be triggered again until it suspends or completes.

**Suspended**:
Task is waiting on something (timer expiry, signal fire, etc.). Not running, not idle. The suspension mechanism is internal — from the domain perspective, the task is simply not runnable.

### Configuration

**Config**:
Compile-time policy struct passed as template parameter to engine. Specifies max_timers, max_signal_listeners, optionally task_frame_size (defaults to 1024 bytes), and min_level (log level filter, defaults to `log_level::info`).
_Avoid_: Settings, options, parameters

### Engine Operations

**Trigger**:
Transitions a task from Idle → Running. Calls the task coroutine function with provided arguments. For member-function tasks, the instance pointer is already stored in the slot (from `register_instance`), so the call site does NOT pass the object.
Returns `error::task_already_running` if the task is already active.
_Avoid_: Start, launch, invoke

**Tick**:
Engine method that processes expired timers and resumes their associated coroutines. Called repeatedly from the event loop. Collects all expired timers first (maintaining FIFO order), then resumes them — ensuring signal broadcasts reach all listeners that resumed in the same tick.

### Registration

**Register Task**:
Helper that wraps a free function as a `free_spec<Tag, Fn>` for use with `make_engine`.
```cpp
register_task<"TAG"_tag, &free_fn>()
```
_Avoid_: Bind, attach, add

**Register Instance**:
Helper that reads a class's `reactor_tasks` alias and returns a `bound<Class, Tag, MemFns...>` for use with `make_engine`.
```cpp
register_instance<"TAG"_tag>(my_obj)
```

**Limitation (planned fix)**: when multiple instances of the same class are registered, the engine allocates one slot per member function per instance, but the trigger API (`eng.trigger<&Class::method>()`) always resumes the **first** instance's slot. The tag is currently used for debugging/error messages only, not for dispatch. Workarounds: use distinct method names per instance, or wait for the per-tag dispatch feature.
_Avoid_: Bind object, attach instance

**Make Engine**:
Compile-time builder that unfolds `bound` and `free_spec` specs into typed slot entries and returns an `engine` instance.
```cpp
auto eng = make_engine<Config, Clock>(specs...);
```
_Avoid_: Create engine, build engine

### Logger

**Logger**:
Compile-time policy type (`cgx::reactor::logger`) that the user specializes with a `print(const char* msg)` method. The reactor formats log messages (clock, level, tag, message) and calls `logger::print()` with the final string. Default is `no_logger` (empty `print()`, compiler eliminates all logging).
_Avoid_: Log handler, log backend, log sink

**No Logger**:
The default logger type (`cgx::reactor::no_logger`). Its `print()` method is empty. When used with `if constexpr` on `Config::min_level`, the compiler eliminates all log formatting and calls — zero codegen.
_Avoid_: Null logger, void logger

**Log**:
Static API for log calls. Provides `log::info()`, `log::debug()`, `log::warn()`, `log::error()`. Each is a variadic template that formats a message (printf-style) with clock timestamp, level, and tag prefix, then calls `logger::print()`.
_Avoid_: log_info, log_debug (free functions)

**Log Level**:
Compile-time enum class (`cgx::reactor::log_level`) with values: `debug`, `info`, `warn`, `error`. Used with `Config::min_level` to filter log output at compile time.
_Avoid_: Severity, log priority

### Error Handling

**Error**:
Enum class representing failure conditions (e.g., `task_already_running`, `capacity_exceeded`, `closed`). Functions return `error` or `std::optional<T>` + `error`. No exceptions.
_Avoid_: Exception, status code, result

## Relationships

- An **Engine** is constructed via **make_engine** from **free_spec** and **bound** entries
- A **free_spec** is produced by **register_task** from a free function pointer and a **tag**
- A **bound** is produced by **register_instance** from a class instance and its **reactor_tasks** alias
- A **Task** transitions through: **Idle** → (trigger) → **Running** → (suspend) → **Suspended** → (resume) → **Running** or **Idle**
- A **Tick** resumes expired timer-suspended tasks; signal-suspended tasks resume directly via fire()
- A **Task** can `listen()` to a **Signal** (suspends until signal fires)
- A **Task** can `fire()` a **Signal** (broadcasts to all suspended listeners)
- A **Task** can `co_await channel.push(value)` to send (suspends if buffer full)
- A **Task** can `co_await channel.pop()` to receive (suspends if buffer empty, returns `std::optional<T>`) — `pop()` is const-qualified, so a `const channel&` can receive (mirrors Go's `<-chan T`)
- A **Task** can call `channel.try_push(value)` for non-blocking send (ISR-safe)
- An **Engine** uses a **Clock** to determine timer expiry
- A **Signal** is standalone — not managed by the engine
- A **Channel** is standalone — not managed by the engine
- An **Engine** optionally accepts a **Logger** template parameter (third param in `make_engine<Config, Clock, Logger>(...)`, defaults to **no_logger**)
- **Signal** and **Channel** also accept an optional **Logger** template parameter (third param, defaults to **no_logger**)
- The **Logger** is a compile-time policy — when `no_logger` is used, all log formatting is eliminated by the compiler via double `if constexpr`
- **Log Level** filtering is controlled by `Config::min_level` — messages below the threshold are eliminated at compile time
- **Signal** logs with `<reactor::signal>` tag: fire broadcast, listener registered, capacity exceeded
- **Channel** logs with `<reactor::channel>` tag: push/pop/try_push outcomes, close events

## Example dialogue

> **Dev:** "When I trigger a task that's already running, what happens?"
> **Domain expert:** "You get an `error::task_already_running`. No queuing, no undefined behavior."

> **Dev:** "How does the event loop work?"
> **Domain expert:** "You call `tick()` in a loop. Tick processes expired timers and resumes those tasks. If tasks suspend on signals, they resume directly when `fire()` is called — not via tick. The event loop just keeps ticking."

> **Dev:** "Can I have multiple instances of the same task type?"
> **Domain expert:** "No — each registered function pointer is a singleton with a fixed slot. If you need multiple instances, use different classes or register each instance with a different tag (each tag creates a separate slot for each member function)."

> **Dev:** "How does a task wait for a UART byte?"
> **Domain expert:** "You have two options depending on the pattern. For broadcast (multiple consumers want the byte): create a `signal<uint8_t> uart_byte`. The task does `auto b = co_await uart_byte.listen()`. When the UART ISR receives a byte, it calls `uart_byte.fire(b)`, which resumes all listeners. For point-to-point (one consumer processes the byte): create a `channel<uint8_t, 16> uart_rx`. The task does `auto b = co_await uart_rx.pop()`. The ISR calls `uart_rx.try_push(b)` (non-blocking). The channel buffers up to 16 bytes, and each byte goes to exactly one consumer."

> **Dev:** "When should I use a signal vs a channel?"
> **Domain expert:** "Use **signal** for broadcast: one producer, multiple consumers, all get the same value (e.g., sensor reading → display + logger + controller). Use **channel** for work distribution: one producer, one or more consumers, each value consumed by exactly one (e.g., ISR byte stream → processor task, command queue → worker). Signals are fire-and-forget; channels provide backpressure (push blocks if full)."

> **Dev:** "How do I register a member function as a task?"
> **Domain expert:** "Add a `using reactor_tasks = task_list<&MyClass::method1, &MyClass::method2>;` alias in your class, then call `register_instance<"TAG"_tag>(obj)` when creating the engine. The hero example is `examples/member_task/` — two mock sensor drivers plus a serial printer consumer, all wired through `register_instance` and `make_engine`."

> **Dev:** "Is the timer queue part of the engine or a signal?"
> **Domain expert:** "The timer queue is managed by the engine internally. Signals are standalone — they resume coroutines directly. They're separate mechanisms that serve different purposes."

> **Dev:** "How do I enable logging?"
> **Domain expert:** "Pass a logger type as the third template parameter to `make_engine`: `make_engine<Config, Clock, my_logger>(...)`. Your logger needs a `static void print(const char* msg)` method. By default, `no_logger` is used and all logging is eliminated at compile time — zero cost."

> **Dev:** "What does the log output look like?"
> **Domain expert:** "Each message is formatted as `{clock_ms} [{LEVEL}] <reactor::task::TAG> message`. For example: `12345 [INF] <reactor::task::TEMP> triggered`. The clock is raw milliseconds from `Clock::now()`, the level is INF/DBG/WRN/ERR, and the tag is the task's registered tag."

> **Dev:** "Can I filter which log levels are shown?"
> **Domain expert:** "Yes — set `Config::min_level` to `log_level::debug`, `log_level::info`, `log_level::warn`, or `log_level::error`. Messages below the threshold are eliminated at compile time. The default is `log_level::info`, so debug messages are suppressed unless you change it."

> **Dev:** "Can a const channel reference receive data?"
> **Domain expert:** "Yes — `pop()` is const-qualified, so `const channel<int, 16>&` can pop. This mirrors Go's receive-only `<-chan T` pattern. A consumer takes a const reference, a producer needs a mutable one. The compiler enforces the contract: you can't push through a const reference."

> **Dev:** "My periodic task drifts — each iteration is slightly longer than the target period."
> **Domain expert:** "That's `delay_ms` accumulating drift — each delay measures from the resume time, not from a fixed epoch. Use `delay_until` if you want to manage the epoch yourself (`next += period`), or `delay_quantized` if you want automatic grid alignment. Both give zero drift. See `examples/timer/` for a side-by-side comparison."

> **Dev:** "What's the difference between `delay_until` and `delay_quantized`?"
> **Domain expert:** "`delay_until(time_point)` takes an absolute time — you control the epoch. `delay_quantized(interval)` snaps to the clock's epoch grid automatically. If you're doing `next += 100ms` in a loop, use `delay_until`. If you just want 'every 100ms tick', use `delay_quantized`. Both are drift-free; `delay_quantized` is simpler when you don't need epoch control."

## Flagged ambiguities

- **"Reactor" vs "Engine"**: We use **Engine** as the user-facing type name. "Reactor" is the pattern name, not a type.
- **"Task" vs "Coroutine"**: A **Task** is a specific coroutine registered with the engine. Not all coroutines are tasks (e.g., helper coroutines).
- **"Signal" vs "Channel"**: **Signal** is broadcast pub/sub (N listeners get every value). **Channel** (in `cgx::reactor::channel<T,Capacity>`) is point-to-point queue-based communication — one consumer takes each value. Both are standalone primitives with no engine dependency.
- **"Idle worker"**: Initially discussed but rejected for barebones. All tasks are equal — each has a fixed slot. No dynamic task assignment.
- **"Temporary task"**: Clarified as "task that fires and returns" — not a dynamically allocated task, but a registered task type that can be triggered, runs to completion, and becomes idle again.

# cgx-reactor Architecture

This document describes the architecture of cgx-reactor, a header-only C++20 coroutine-based reactive scheduler for embedded systems.

## Design Principles

1. **Zero heap allocation** — all memory is statically allocated at compile time
2. **No exceptions** — error handling via return codes
3. **Header-only** — single-include for easy integration
4. **Compile-time task registration** — tasks are registered via NTTPs (non-type template parameters) or via `register_instance`/`register_task` helpers, eliminating runtime overhead
5. **Cooperative scheduling** — tasks voluntarily yield via suspension points

## Core Components

### Engine

The `engine` class is the central scheduler. It:
- Manages a fixed array of task slots (compile-time sized)
- Maintains a timer queue for delayed suspensions
- Provides `trigger()` to start tasks and `tick()` to advance time

**Template parameters:**
```cpp
template <typename Config,          // Configuration policy
          typename Clock,           // clock concept (std::chrono-compatible)
          typename... Entries>      // Typed slot entries (descriptors)
class engine;
```

**Construction (via `make_engine`):**
```cpp
auto eng = make_engine<Config, Clock>(
    register_task<"TAG"_tag, &free_task>(),
    register_task<"TAG"_tag, &another_task>()
);
```

**With member-function tasks:**
```cpp
auto eng = make_engine<Config, Clock>(
    register_instance<"DISP"_tag>(display)
);
```

### Task

A task is a coroutine registered with the engine. Each task has a dedicated slot containing:
- Storage for the coroutine frame (placement-new allocated)
- State: `idle`, `running`, or `suspended`
- Handle to the coroutine
- Self pointer (for member-function tasks) or nullptr (for free-function tasks)

**Task lifecycle:**
1. **Idle** — slot is empty, task can be triggered
2. **Running** — coroutine is executing
3. **Suspended** — coroutine is waiting (on timer or signal)

### Tag

A compile-time 4-character identifier for debugging and error messages:

```cpp
template <char... Cs>
struct tag { ... };
```

Tags are normally constructed via the `"DISP"_tag` user-defined literal (C++20 template UDL, P1040R6). The escape hatch `make_tag<'D','I','S','P'>()` is available when the tag chars are not known as a string literal at the call site (e.g. metaprogramming). The UDL requires a `using` declaration in scope:

```cpp
using cgx::reactor::operator""_tag;   // bring the UDL into scope
```

### Task List

Classes with member-function tasks expose a `reactor_tasks` alias:

```cpp
class MyDriver {
    task run_loop();
    task fire_once(int);
    using reactor_tasks = task_list<&MyDriver::run_loop, &MyDriver::fire_once>;
};
```

### Signal

A broadcast pub/sub mechanism for inter-task communication. Signals:
- Are standalone objects (not managed by engine)
- Support multiple concurrent listeners
- Resume suspended coroutines directly via `fire()`

**Example:**
```cpp
signal<int> data_ready;

task producer() {
    data_ready.fire(42);  // resumes all listeners
}

task consumer() {
    int value = co_await data_ready.listen();  // suspends until fired
}
```

### Channel

A point-to-point bounded buffer for inter-task communication. Channels:
- Are standalone objects (not managed by engine)
- Implement work distribution (each value consumed by exactly one consumer)
- Provide blocking `push()`/`pop()` with awaiters
- Provide non-blocking `try_push()` for ISR contexts
- Support `close()` to signal shutdown
- Resume suspended coroutines directly via awaiters

**Example:**
```cpp
channel<uint8_t, 16> uart_rx;  // 16-byte buffer

// ISR context (non-blocking)
void UART_ISR() {
    uart_rx.try_push(UART->DR);
}

// Task context (blocking)
task consumer() {
    while (auto byte = co_await uart_rx.pop()) {
        process(*byte);  // each byte consumed by exactly one consumer
    }
    // pop() returns std::nullopt when channel is closed
}
```

**Signal vs Channel:**
- **Signal**: broadcast (all listeners get the value), fire-and-forget, no buffering
- **Channel**: point-to-point (one consumer per value), backpressure (blocks if full), bounded buffer

## Data Flow

The architecture follows a vertical data flow pattern with clear layering:

```
┌─────────────────────────────────────────────────────────────┐
│                        User Code                             │
│                   (tasks, business logic)                    │
└──────────────────┬──────────────────────┬───────────────────┘
                   │ co_await             │ trigger() / fire()
                   ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   Suspension Layer                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐ │
│  │ delay_ms()  │  │  listen()   │  │   custom awaiters   │ │
│  └──────┬──────┘  └──────┬──────┘  └──────────┬──────────┘ │
│         │                │                     │             │
└─────────┼────────────────┼─────────────────────┼─────────────┘
          │                │                     │
          ▼                ▼                     ▼
┌─────────────────────────────────────────────────────────────┐
│                   Engine Layer                                │
│  ┌──────────────────────────────────────────────────────┐  │
│  │                    Timer Queue                         │  │
│  │  (suspended tasks waiting for time-based wake-up)     │  │
│  └──────────────────────┬───────────────────────────────┘  │
│                         │                                    │
│  ┌──────────────────────┴───────────────────────────────┐  │
│  │                    Task Slots                          │  │
│  │  [task_0]  [task_1]  [task_2]  ...  [task_N]          │  │
│  └──────────────────────────────────────────────────────┘  │
│                         │                                    │
│  ┌──────────────────────┴───────────────────────────────┐  │
│  │                    Event Loop                          │  │
│  │         tick() — advances time, resumes tasks         │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Suspension Flow

Tasks suspend via awaitables. Two primary mechanisms:

### Timer-based Suspension

```
Task execution
    ↓
co_await delay_ms(100)
    ↓
delay_ms::await_suspend()
    ↓
Registers with timer queue via thread-local registrar
    ↓
Coroutine suspends → task state: suspended
    ↓
[time passes]
    ↓
tick() detects expiry
    ↓
Resumes coroutine → task state: running
    ↓
Task continues execution
```

### Signal-based Suspension

```
Task execution
    ↓
co_await signal.listen()
    ↓
listen_awaiter::await_suspend()
    ↓
Registers with signal's listener list
    ↓
Coroutine suspends → task state: suspended
    ↓
[signal fires]
    ↓
signal::fire() directly resumes coroutine → task state: running
    ↓
Task continues execution
```

### Channel-based Suspension

```
Task execution
    ↓
co_await channel.pop()
    ↓
pop_awaiter::await_suspend()
    ↓
If buffer has data → copy to result, return false (no suspend)
If buffer empty → register in consumer wait queue, suspend
    ↓
[producer pushes]
    ↓
producer's push_awaiter writes to buffer or hands off to waiting consumer
    ↓
Directly resumes waiting consumer → task state: running
    ↓
Task continues execution
```

## Registration Flow

### Free-function tasks

```
User code:  register_task<"TAG"_tag, &free_fn>()
    ↓
make_engine unfolds the free_spec into a task_descriptor
    ↓
engine stores fn as NTTP for compile-time dispatch
```

### Member-function tasks

```
Driver class:  using reactor_tasks = task_list<&Driver::method1, &Driver::method2>;
    ↓
User code:  register_instance<"TAG"_tag>(obj)
    ↓
register_instance reads Class::reactor_tasks and returns bound<Class, Tag, MemFns...>
    ↓
make_engine unfolds the bound into one task_descriptor per member function
    ↓
engine stores each fn as NTTP, plus the self pointer in the slot
```

## Memory Model

All memory is statically allocated:

1. **Task slots** — fixed-size array in engine, one per registered task
2. **Coroutine frames** — placement-new allocated into slot storage
3. **Timer queue** — fixed-size array (configurable via `Config::max_timers`)
4. **Signal listeners** — fixed-size array per signal (configurable via `Config::max_signal_listeners`)
5. **Channel buffers** — fixed-size ring buffer per channel (compile-time `Capacity` template parameter)
6. **Channel wait queues** — fixed-size arrays per channel (bounded by `Capacity`)

**No dynamic allocation occurs at runtime.**

## Thread-Local Context

To avoid passing engine references through every coroutine, the engine uses thread-local registrars:

- `current_timer_registrar` — set by engine before resuming task, used by `delay_ms` to register timers
- `current_external_suspension_registrar` — set by engine, used by awaitables to notify engine of suspension

This pattern allows awaitables to interact with the engine without explicit parameter passing.

## Error Handling

All operations return `error` codes:
- `error::ok` — success
- `error::task_already_running` — tried to trigger a running task
- `error::capacity_exceeded` — timer queue, listener queue, or channel full
- `error::listener_limit_exceeded` — too many concurrent listeners
- `error::closed` — channel was closed by the producer

No exceptions are thrown.

## Configuration

The `Config` struct provides compile-time constants:

```cpp
struct Config {
    static constexpr size_t max_timers = 16;
    static constexpr size_t max_signal_listeners = 8;
    static constexpr size_t task_frame_size = 1024;
};
```

- `max_timers` — maximum concurrent timer suspensions
- `max_signal_listeners` — maximum concurrent listeners per signal
- `task_frame_size` — maximum coroutine frame size (bytes)

## Clock Abstraction

The engine is parameterized on a `Clock` type satisfying the clock concept:

```cpp
template <typename Clock>
concept clock = requires {
    typename Clock::time_point;
    typename Clock::duration;
    { Clock::now() } -> std::same_as<typename Clock::time_point>;
};
```

This allows:
- `std::chrono::steady_clock` for production
- Mock clocks for deterministic testing

## Limitations

- **No preemption** — tasks must explicitly yield via suspension
- **No priority** — all tasks are treated equally
- **No dynamic task registration** — tasks must be known at compile time
- **No cross-engine signals** — signals are local to a single engine instance
- **Drift in periodic tasks** — `delay_ms` causes drift; `delay_until` is deferred

## Future Directions

See [roadmap.md](roadmap.md) for planned features:
- `delay_until()` for drift-free periodic tasks
- Task lifecycle management (`cancel()`, `join()`)
- RP2040 clock implementation

## Examples

Four runnable examples live under `examples/`:

| Directory | What it shows |
|-----------|---------------|
| `examples/basic/` | Free-function tasks wired through `register_task` + `make_engine`. Producer/consumer over a signal. The simplest end-to-end demo. |
| `examples/error_handling/` | Exercises every error code (`task_already_running`, `capacity_exceeded`, `listener_limit_exceeded`) with a deliberately small `Config` so the limits are easy to hit. |
| `examples/member_task/` | The hero example for the member-function API. Three classes — two mock sensors and a serial printer consumer — all using `reactor_tasks` aliases and `register_instance`. Run with `make example_member_task`; produces interleaved temperature/pressure readings over ~3 seconds. |
| `examples/channel/` | Demonstrates point-to-point communication with `channel<T, Capacity>`. Shows ISR-to-task data flow with a UART receiver pushing bytes into a channel, and a processor task consuming them. Illustrates blocking `pop()` for task contexts and non-blocking `try_push()` for ISR contexts. Run with `make example_channel`. |

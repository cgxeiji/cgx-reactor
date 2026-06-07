# cgx-reactor Architecture

This document describes the architecture of cgx-reactor, a header-only C++20 coroutine-based reactive scheduler for embedded systems.

## Design Principles

1. **Zero heap allocation** — all memory is statically allocated at compile time
2. **No exceptions** — error handling via return codes
3. **Header-only** — single-include for easy integration
4. **Compile-time task registration** — tasks are registered via NTTPs (non-type template parameters), eliminating runtime overhead
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
          auto... TaskFunctions>    // Task function pointers (NTTPs)
class engine;
```

**Example:**
```cpp
engine<Config, std::chrono::steady_clock, &sensor_task, &display_task> reactor;
```

### Task

A task is a coroutine function registered with the engine. Each task has a dedicated slot containing:
- Storage for the coroutine frame (placement-new allocated)
- State: `idle`, `running`, or `suspended`
- Handle to the coroutine

**Task lifecycle:**
1. **Idle** — slot is empty, task can be triggered
2. **Running** — coroutine is executing
3. **Suspended** — coroutine is waiting (on timer or signal)

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

## Memory Model

All memory is statically allocated:

1. **Task slots** — fixed-size array in engine, one per registered task
2. **Coroutine frames** — placement-new allocated into slot storage
3. **Timer queue** — fixed-size array (configurable via `Config::max_timers`)
4. **Signal listeners** — fixed-size array per signal (configurable via `Config::max_signal_listeners`)

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
- `error::queue_full` — timer or listener queue exhausted
- `error::listener_limit_exceeded` — too many concurrent listeners

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
- Queue-based channels (point-to-point communication)
- Task lifecycle management (`cancel()`, `join()`)
- RP2040 clock implementation

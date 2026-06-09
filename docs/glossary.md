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
_Avoid_: Channel, event, message, broadcast

**Clock**:
A concept wrapping `std::chrono` types. Provides `now()` returning `time_point`. Default implementation uses `std::chrono::steady_clock`. RP2040 provides a clock wrapping Pico SDK timer. Tests provide a mock clock.
_Avoid_: Timer, time source

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
Compile-time policy struct passed as template parameter to engine. Specifies max_timers, max_signal_listeners, and optionally task_frame_size (defaults to 1024 bytes).
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

### Error Handling

**Error**:
Enum class representing failure conditions (e.g., `task_already_running`, `queue_full`, `invalid_task`). Functions return `error` or `std::optional<T>` + `error`. No exceptions.
_Avoid_: Exception, status code, result

## Relationships

- An **Engine** is constructed via **make_engine** from **free_spec** and **bound** entries
- A **free_spec** is produced by **register_task** from a free function pointer and a **tag**
- A **bound** is produced by **register_instance** from a class instance and its **reactor_tasks** alias
- A **Task** transitions through: **Idle** → (trigger) → **Running** → (suspend) → **Suspended** → (resume) → **Running** or **Idle**
- A **Tick** resumes expired timer-suspended tasks; signal-suspended tasks resume directly via fire()
- A **Task** can `listen()` to a **Signal** (suspends until signal fires)
- A **Task** can `fire()` a **Signal** (broadcasts to all suspended listeners)
- An **Engine** uses a **Clock** to determine timer expiry
- A **Signal** is standalone — not managed by the engine

## Example dialogue

> **Dev:** "When I trigger a task that's already running, what happens?"
> **Domain expert:** "You get an `error::task_already_running`. No queuing, no undefined behavior."

> **Dev:** "How does the event loop work?"
> **Domain expert:** "You call `tick()` in a loop. Tick processes expired timers and resumes those tasks. If tasks suspend on signals, they resume directly when `fire()` is called — not via tick. The event loop just keeps ticking."

> **Dev:** "Can I have multiple instances of the same task type?"
> **Domain expert:** "No — each registered function pointer is a singleton with a fixed slot. If you need multiple instances, use different classes or register each instance with a different tag (each tag creates a separate slot for each member function)."

> **Dev:** "How does a task wait for a UART byte?"
> **Domain expert:** "You create a `signal<uint8_t> uart_byte`. The task does `auto b = co_await uart_byte.listen()`. When the UART ISR receives a byte, it calls `uart_byte.fire(b)`, which resumes the task."

> **Dev:** "How do I register a member function as a task?"
> **Domain expert:** "Add a `using reactor_tasks = task_list<&MyClass::method1, &MyClass::method2>;` alias in your class, then call `register_instance<"TAG"_tag>(obj)` when creating the engine. The hero example is `examples/member_task/` — two mock sensor drivers plus a serial printer consumer, all wired through `register_instance` and `make_engine`."

> **Dev:** "Is the timer queue part of the engine or a signal?"
> **Domain expert:** "The timer queue is managed by the engine internally. Signals are standalone — they resume coroutines directly. They're separate mechanisms that serve different purposes."

## Flagged ambiguities

- **"Reactor" vs "Engine"**: We use **Engine** as the user-facing type name. "Reactor" is the pattern name, not a type.
- **"Task" vs "Coroutine"**: A **Task** is a specific coroutine registered with the engine. Not all coroutines are tasks (e.g., helper coroutines).
- **"Signal" vs "Channel"**: We use **Signal** for broadcast pub/sub. A **Channel** (future) would be queue-based (one listener gets the value).
- **"Idle worker"**: Initially discussed but rejected for barebones. All tasks are equal — each has a fixed slot. No dynamic task assignment.
- **"Temporary task"**: Clarified as "task that fires and returns" — not a dynamically allocated task, but a registered task type that can be triggered, runs to completion, and becomes idle again.

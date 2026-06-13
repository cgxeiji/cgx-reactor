# cgx-reactor Roadmap

## 2026-06-09 — Task Registration Refactor ✓

Completed — all items in this release have been implemented and tested.

- **~~Member-function tasks~~** — True member functions as coroutine tasks. The user class exposes a `using reactor_tasks = task_list<&Class::method, ...>` alias and registers via `register_instance<"TAG"_tag>(obj)`. Self pointer is stored in the engine slot, not in NTTPs. Dispatched automatically in `trigger<&Class::method>()`.
- **~~`register_task` / `register_instance` / `make_engine` API~~** — `make_engine<Config, Clock>(specs...)` unfolds `bound` and `free_spec` entries into the engine's typed slot list at compile time.
- **~~Tagged slots~~** — Each task slot carries a compile-time `tag<Cs...>` (via `"TAG"_tag` UDL or `make_tag<'T','A','G'>()` as escape hatch) for debugging.
- **~~Drop old `engine<…, &fn1, &fn2>` API~~** — The old NTTP-list API is removed. All examples and tests use the new registration helpers.
- **~~Test coverage~~** — Existing tests (task, timer, signal) rewritten; new `test_member_task.cpp` covers member-function tasks, multi-instance, and combined free+member registration.
- **~~`member_task` hero example~~** — New `examples/member_task/` showcases the full member-class pattern: two mock sensor drivers and a serial printer consumer, all using `reactor_tasks` aliases and `register_instance`. Run with `make example_member_task`.

## 2026-06-07 — Barebones POC ✓

All barebones items are complete (this milestone was reached before the refactor; the refactor builds on it).

- **~~Channels (queue-based)~~** — broadcast signals chosen for barebones; queue-based channels (single listener consumes value) deferred until use case demands it
- **~~Generic awaiter concept~~** — implemented via thread-local registrar pattern (`current_timer_registrar`, `current_external_suspension_registrar`); awaitables find their engine context without explicit parameters. A formal C++20 `concept` could refine this later but isn't blocking.
- **~~`delay_until(time_point)`~~** — absolute-time awaitable for drift-free periodic tasks; `delay_ms` sufficient for barebones
- **~~`yield` awaitable~~** — cooperative CPU sharing; not needed for barebones task counts
- **~~Task lifecycle (`cancel()`, `join()`)~~** — `final_suspend = suspend_always` leaves the door open; no API surface added yet
- **~~Multiple instances per task type~~** — singleton per type for barebones; parameterized instances (e.g., one task per sensor) deferred
- **~~Idle workers / dynamic task assignment~~** — all tasks compile-time registered; dynamic assignment from a worker pool deferred
- **~~Event-based triggering (GPIO, UART)~~** — consumer-side concern; library only provides `trigger()` API; higher-level event wiring deferred
- **~~Reactor-managed signals (Model B)~~** — signals are standalone (Model A) for barebones; central event loop polling deferred until coordination needs arise
- **~~RP2040 clock implementation~~** — concept defined, but Pico SDK integration is consumer-side; a reference implementation may be added as an example
- **~~Custom awaitable examples~~** — `next_byte` for UART etc. deferred; architecture supports them (signals resume handles directly)

## 2026-06-11 — Channels ✓

- **~~Queue-based channels~~** — `channel<T, Capacity>` implemented as a standalone primitive (no engine dependency). Ring buffer, blocking push/pop with awaiters, non-blocking try_push, close() with waiter wakeup. Direct resume semantics (same as signals). Error enum extended: `queue_full` renamed to `capacity_exceeded`, `closed` added.

## 2026-06-13 — Logger ✓

- **~~Composable logger~~** — `cgx::reactor::logger` compile-time policy type. Default `no_logger` produces zero codegen (double `if constexpr` eliminates all formatting). User enables via `make_engine<Config, Clock, my_logger>(...)`. Provides `log::info/debug/warn/error` variadic API with printf-style formatting.
- **~~Log format~~** — `{clock_ms} [{LEVEL}] <reactor::task::TAG> message`. Clock is raw `Clock::now()` in ms. Levels: INF/DBG/WRN/ERR. Tag is the task's registered tag.
- **~~Level filtering~~** — `Config::min_level` compile-time filter (default: `log_level::info`). Messages below threshold eliminated at compile time.
- **~~Engine + timer log points~~** — 6 log points: trigger, already-running warning, timer registered, timer capacity exceeded, timer expired, task completed.
- **~~Signal/channel logging~~** — Logger template param added to signal and channel (third param, default `no_logger`). Signal logs: fire broadcast, listener registered, capacity exceeded. Channel logs: push/pop/try_push outcomes, close events. Tags: `<reactor::signal>`, `<reactor::channel>`.
- **~~Logger example~~** — `examples/logger/` demonstrates custom stdout_logger with engine, signal, and channel logging.
- **~~Test coverage~~** — 13 tests in `test_logger.cpp`: custom capture, timestamp format, level filtering, no_logger suppression, tag format, full timer flow, already-running, capacity exceeded.
- **~~Const channel pop~~** — `pop()` is now const-qualified, mirroring `signal::listen()`. A `const channel&` can receive (Go's `<-chan T` pattern). Internal buffer/wait-queue members are `mutable`. `push()`/`try_push()`/`close()` remain non-const.

## Upcoming

- **`delay_until(time_point)`** — absolute-time awaitable for drift-free periodic tasks
- **Task lifecycle (`cancel()`, `join()`)** — explicit management of running tasks
- **RP2040 clock implementation** — reference implementation for Pico SDK
- **Custom awaitable examples** — `next_byte` for UART, `wait_for_pin` for GPIO

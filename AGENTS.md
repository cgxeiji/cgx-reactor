# AGENTS.md

`cgx-reactor` is a C++20 header-only coroutine-based reactive scheduler for embedded systems (RP2040) and PC testing. Public source lives in `include/cgx/reactor/`, single-include entry point `cgx/reactor.hpp`.

## Read first

Before doing anything in this repo, read in order:

1. `docs/glossary.md` — exact domain terms (Engine, Task, Signal, Channel, Tag, Scratchpad, …).
2. `docs/architecture.md` — engine, task lifecycle, suspension, memory model, error model.

Use the glossary terms verbatim in code, comments, logs, and commit messages. The glossary is the source of truth; do not re-derive terminology from intuition.

## Hard constraints (non-negotiable)

- **Zero heap allocation.** Every byte of memory is statically allocated: reserved pool, scratchpad pool, timer queue, signal listener arrays, channel ring buffers. No `new`, `malloc`, `std::make_shared`, heap-allocated coroutine frames, runtime `std::vector` for storage. Coroutine frames are placement-new into pool storage.
- **Header-only.** All public code lives under `include/`. Do not add `.cpp` files. The build target is `INTERFACE` (see `CMakeLists.txt`).
- **C++20 minimum.** Coroutines, concepts, NTTPs, `if constexpr`, template UDLs are all in use. Do not reach for C++23-only features.
- **No exceptions.** All error paths return `cgx::reactor::error` codes (or `std::optional<T>` + `error`). No `throw` and no `try`/`catch` in the library.
- **No backwards compatibility.** The library is pre-release, has never shipped. Refactor freely. Drop old APIs without deprecation paths. Do not preserve transitional forms (e.g. the old NTTP-list engine API).
- **No dynamic task registration.** Tasks are known at compile time via `register_task` / `register_instance` / `make_engine`. No runtime task creation.

## Examples vs. embedded tests (different things — do not conflate)

- **`examples/<name>/`** — host-runnable *showcase* of how to use the library. Built via `make example_<name>`. The single embedded entry, `examples/rp2040_pico/`, is a demo, not a test.
- **`embedded_test/rp2040/<feature>/`** — TDD-style test of a library feature on real RP2040 hardware. Cross-compiled with `arm-none-eabi-gcc`, flashed with `picotool`. *Only create one when the user explicitly asks for an embedded test of a specific feature.* Use `examples/rp2040_pico/` as the structural reference (CMakeLists, Makefile, `pico_clock.hpp`, README pattern).

## Build & verify

- Host tests: `make test` (runs `cmake --build build --target cgx-reactor-tests && ctest --test-dir build --output-on-failure`).
- Host examples: `make example_<name>` (see top-level `Makefile` for the list).
- Embedded test on Pico: from its own folder, `make && make flash`.

A change is "done" when host tests pass. Embedded tests are run on explicit user request.

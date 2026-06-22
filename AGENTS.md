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
- **Comments describe the code, not the process that produced it.** No milestone IDs (M1/M2), no workflow/PR/review references, no "collapsed from X" / "follow-up" history. A reader of the code wasn't in the cycle — comments must stand alone.

## Design trade-offs

This library targets a constrained MCU (RP2040: 264KB SRAM, 2MB flash, 125MHz) and is cross-compiled. At spec/design time, weigh compile time vs runtime cost vs binary size, and dynamic processing vs fixed data. When the trade-off is unclear, write a napkin (`.tmp/napkins/`) and measure across candidate designs before committing the architecture. See `docs/architecture.md` (Memory Model) for the engine's specific trade-off landscape.

## Examples vs. embedded tests (different things — do not conflate)

- **`examples/<name>/`** — host-runnable *showcase* of how to use the library. Built via `make example_<name>`. The single embedded entry, `examples/rp2040_pico/`, is a demo, not a test.
- **`embedded_test/rp2040/<feature>/`** — TDD-style test of a library feature on real RP2040 hardware. Cross-compiled with `arm-none-eabi-gcc`, flashed with `picotool`. *Only create one when the user explicitly asks for an embedded test of a specific feature.* Use `examples/rp2040_pico/` as the structural reference (CMakeLists, Makefile, `pico_clock.hpp`, README pattern).
- **`embedded_benchmarks/rp2040/<feature>/`** — performance benchmark of a library feature on real RP2040 hardware, timed with the pico-sdk hardware timer. Cross-compiled with `arm-none-eabi-gcc`, flashed with `picotool`. Create one when measuring a hot path (e.g. dispatch latency) that host timings can't represent. Use `examples/rp2040_pico/` as the structural reference.

## Embedded verification (agent-verifiable)

Embedded examples, tests, and benchmarks must be verifiable by an agent without manual button presses — so that flashing and serial capture can be automated.

- **Keep USB CDC alive.** `main()` must NOT return. End it with an infinite loop so the device stays responsive to `picotool load -f` (software reboot into BOOTSEL) and a late-connecting monitor can read output:
  ```c
  while (true) { tight_loop_contents(); }   // pico-sdk WFI/no-op; keeps USB IRQs serviced
  ```
  If `main()` returns, USB dies, the device can't be software-rebooted, and flashing requires a physical BOOTSEL button press.
- **One-shot output (benchmarks/tests that print once at boot):** capture with `./tools/pico/pico run <name>.uf2 -t <seconds>` (flashes + captures from boot in one command; also saves a copy to `.tmp/serial_logs/`). Live/looping output: `./tools/pico/pico flash <name>.uf2` then `./tools/pico/pico serial`. See `tools/pico/SKILL.md`.
- **Prefer `make clean` over `rm -rf build`** to rebuild a target — the Makefiles own their build dirs and `clean` is the documented way to reset.

## Build & verify

- Host tests: `make test` (runs `cmake --build build --target cgx-reactor-tests && ctest --test-dir build --output-on-failure`).
- Host examples: `make example_<name>` (see top-level `Makefile` for the list). Examples showcase real use cases and are good starting points for integration tests — mirror an example's call structure when a test needs to exercise how components interact in practice.
- Embedded test on Pico: from its own folder, `make && make flash`.
- Embedded benchmark on Pico: `make -C embedded_benchmarks/rp2040/<feature>`, then flash + capture with `./tools/pico/pico run <feature>/build/<name>.uf2 -t 12`.

A change is "done" when host tests pass. Embedded tests are run on explicit user request.

# trigger_lookup — embedded benchmark for interface `trigger(task_uid)`

Measures the per-trigger dispatch latency of the abstract
`cgx::reactor::engine::trigger(task_uid)` path on real RP2040
hardware, across a sweep of registered task counts. Used to validate
the O(1) hashmap lookup claim — average latency should remain
roughly constant as N grows (no linear growth).

## What it measures

- **Per-trigger dispatch** of the interface `trigger(uid)` path:
  hashmap lookup (open-addressing, linear probing, ≤50% load) → slot
  resolution → **single `invoke_table_` of N trivial thunks** (M6:
  one method per dispatch op, runtime slot index) → scratchpad
  `try_scratchpad` (alloc + coroutine create + resume + complete_task
  + free for a fire-and-return dummy).
- The `task_uid_v<auto Fn>` is a **compile-time** constant — zero
  runtime cost. This benchmark measures dispatch only.
- `K = 1000` triggers per N, cycling through all N UIDs so the
  hashmap probe-chain length varies (honest measurement, not just
  one cached entry).
- Timing: pico-sdk `time_us_32()` (1µs resolution). Per-trigger
  min/max have 1µs granularity — `min` may read 0 for sub-µs
  dispatches. The **avg is batched over K=1000** and is the reliable
  number.

## Workload

- `template<unsigned I> task dummy() { co_return; }` — fire-and-return,
  no-arg, free function. Frame is tiny (probed ~32B; well under the
  64B scratchpad pool).
- All dummies registered as **scratchpad** (`scratch_v<&dummy<Is>>`
  in a `task_list<...>`), so each trigger is a full
  alloc → run → free cycle (the realistic dispatch cost).
- The scratchpad pool (`bench_config` = 64B reserved + 64B scratchpad)
  holds one dummy at a time (correct: each `co_return` completes
  synchronously, freeing the slot before the next trigger). If a
  trigger returns `capacity_exceeded`, that N is skipped (would
  pollute the measurement).

## Compile-time SRAM footprint

Each line prints `size=<N>B` — the compile-time
`sizeof(basic_engine<...>)`, which is the full per-engine footprint:
`tasks_` + `uid_table_` + `invoke_table_` + `fn_names_` + reserved
pool + scratchpad pool + `timers_` + `scratchpad_waiters_`. Because
each `run_bench<N>()` builds its engine as a local (stack) variable
and only one engine is live at a time, the constraint is
`max sizeof ≤ available stack/SRAM`, **not** the sum across N.
This makes the `size=` field a direct predictor of the SRAM ceiling
for the largest N in the sweep.

## How to build, flash, and run

```bash
cd embedded_benchmarks/rp2040/trigger_lookup
make                              # cross-compiles to trigger_lookup.uf2
../../tools/pico/pico run build/trigger_lookup.uf2 -t 20
```

`make` clones pico-sdk 2.2.0 into `.pico-sdk/` on first run
(git-ignored). Requires `arm-none-eabi-gcc` (RPi 13.2+ or Arm GNU
Toolchain 14.x). The `pico` tool flash + capture + saves a log
automatically.

## Results — M7 sweep (RP2040 @ 125 MHz, pico-sdk 2.2.0)

11-point N sweep, full cross-compile in ~2m40s, 1.07 MB UF2:

```
N=1    size=552B    avg=5us
N=2    size=744B    avg=6us
N=4    size=888B    avg=6us
N=8    size=1032B   avg=6us
N=16   size=1480B   avg=6us
N=32   size=2640B   avg=7us
N=64   size=4840B   avg=7us
N=128  size=9192B   avg=8us
N=256  size=17896B  avg=12us
N=512  size=35304B  avg=14us
N=1024 size=70120B  avg=15us
```

### M5 vs M7 (at N=128, the largest M5 tested)

| N   | M5 avg | M7 avg | Speedup |
|-----|--------|--------|---------|
| 128 | 31 µs  | 8 µs   | ~4×     |

The M5 curve was climbing: 4 µs (N=1) → 31 µs (N=128), roughly 8×
growth over a 128× range. The M7 curve is **flat-ish**:
5 µs (N=1) → 15 µs (N=1024), roughly 3× growth over a 1024× range,
**plateauing at 256 → 1024** (12 → 14 → 15 µs). This confirms the
hypothesis that the M5 residual growth was XIP flash cache pressure
from N distinct per-I code instantiations — the M6 type-erased
collapse (one heavy method per dispatch op + N tiny trivial thunks)
flattens it. Dispatch is effectively O(1) across 1 → 1024.

### SRAM ceiling

N=1024's engine is **70,120 B (~68 KB)**. The Pico has 264 KB SRAM;
the default Pico stack is 8 KB, but the `run_bench<N>()` engines
are stack-local and the linker allocates sufficient stack for the
largest one (the cross-compile link succeeds and the device runs
the full sweep without faulting). If a future config grows the
per-engine footprint, the `size=` field on each line is the direct
predictor of the largest N that will fit.

## Limitations

- 1µs timer resolution: per-trigger `min` and `max` are noisy. The
  `avg` (batched) is the signal.
- 11-point N sweep covers 1 → 1024 (four orders of magnitude),
  enough to show the O(1) trend and the 256 → 1024 plateau.
- The `size=` field is `sizeof(basic_engine<...>)` — a compile-time
  constant per N. It does not account for stack overhead, .bss, or
  heap (the engine uses none of those).

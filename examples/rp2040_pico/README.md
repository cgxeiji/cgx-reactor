# rp2040_pico — cgx::reactor on Pi Pico (RP2040)

Self-contained bare-metal example that runs the `cgx::reactor` coroutine
library on a Raspberry Pi Pico, driven by a 1 ms pico-sdk timer + WFI loop.

## Prerequisites

* `arm-none-eabi-gcc` (Raspberry Pi 13.2.Rel1 or newer)
* `cmake` (3.13+)
* `picotool` (for `make flash`)
* `git` (auto-clones pico-sdk on first build)

## Build

```sh
make
```

On first run, this clones `pico-sdk 2.2.0` into `.pico-sdk/` (git-ignored)
and configures cmake with `PICO_BOARD=pico`. The build artifact is
`build/rp2040_pico.uf2`.

## Flash

Hold **BOOTSEL** on the Pico, plug in USB, then:

```sh
make flash
```

## Monitor

```sh
make monitor
```

## What it does

* **Scratchpad init_led** (one-shot): configures `PICO_DEFAULT_LED_PIN`
  (GP25) with staged simulated init delays. Slot is reclaimed on completion.
* **Scratchpad init_temp** (one-shot): enables the on-die ADC temperature
  sensor with staged simulated init delays. Slot is reclaimed on completion.
* **blink** (reserved loop): toggles the onboard LED every 500 ms.
* **temp** (reserved loop): reads the on-die ADC temperature sensor and
  prints the CPU temperature every 1 s.
* **dump** (reserved loop): prints engine task-memory layout every 5 s.
* **1 ms tick timer** + `__wfi` event loop: low-power; wakes the core
  from WFI on each tick to drive `eng.tick()`.

## Notes

This example is **standalone** — it is not part of the host `make examples`
build in the parent cgx-reactor repo. It cross-compiles with
`arm-none-eabi-gcc` and would break a host build.

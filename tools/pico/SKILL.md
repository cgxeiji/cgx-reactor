---
name: pico
description: Flash and capture serial output from a connected RP2040 Pico for embedded examples, tests, and benchmarks. Use when verifying embedded firmware on hardware — flashing a .uf2, reading USB CDC output, or capturing one-shot benchmark/test output from boot.
---

# pico — agent playbook

Use `./tools/pico/pico` to flash firmware and capture serial output from
a connected Pico **without manual BOOTSEL button presses**. This is the
verification tool for embedded examples, tests, and benchmarks.

## When to use which command

- **Verifying a benchmark or test that prints once at boot** → `run`:
  ```sh
  ./tools/pico/pico run path/to/build/firmware.uf2 -t 12
  ```
  Flashes + captures output from boot in one shot. `-t` is the capture
  window in seconds (default 15). This is the default for one-shot
  output — `serial` will miss it (the app has already printed by the
  time you connect).

- **Flashing an interactive/looping app, then monitoring live** →
  `flash` then `serial`:
  ```sh
  ./tools/pico/pico flash path/to/build/firmware.uf2
  ./tools/pico/pico serial
  ```

- **Checking what's connected** → `info`:
  ```sh
  ./tools/pico/pico info
  ```

## The agent-verifiable convention (from AGENTS.md)

For `flash`/`run` to work without a physical BOOTSEL press, the flashed
app **must keep USB CDC alive** — end `main()` with an infinite loop:
```c
while (true) { tight_loop_contents(); }
```
If `main()` returns, USB dies, the device can't be software-rebooted,
and `flash`/`run` fall back to telling you to press BOOTSEL. Any
embedded example/test/benchmark you write or edit must follow this.

## Verify loop (typical)

1. Build: `make -C <embedded_dir>` (use `make clean` to rebuild, not
   `rm -rf build`).
2. Flash + capture: `./tools/pico/pico run <dir>/build/<name>.uf2 -t 12`.
3. Read the captured output; assert the expected lines/values. A copy is
   saved to `.tmp/serial_logs/<ts>_run.log` (gitignored) for later reference.
4. If flashing fails with the BOOTSEL fallback, ask the user to hold
   BOOTSEL and tap reset, then re-run `run`.

## Gotchas

- One-shot output (benchmark prints once) → `run`, not `serial`.
- `reboot app` is best-effort (the app must service the reboot request);
  prefer `flash`/`run` which use `picotool load -f` directly.
- Multiple Picos: disconnect all but one (the tool uses the first
  `/dev/cu.usbmodem*`).

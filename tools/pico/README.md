# pico — agent-verifiable RP2040 flash + serial tool

A single Python script (`./pico`) that wraps `picotool` and raw termios
serial I/O so an agent (or human) can flash firmware and capture USB CDC
output **without manual BOOTSEL button presses or hand-written capture
scripts**.

Standard library only — no `pyserial`, no other dependencies beyond
`picotool` on PATH and a Pico connected via USB.

## Requirements

- Python 3 (stdlib only).
- `picotool` on PATH.
- A Pico connected via USB. The flashed app must keep USB CDC alive
  (see the agent-verifiable convention in `../../AGENTS.md`) so that
  `flash`/`run` can reboot it without a physical BOOTSEL press.

## Subcommands

| Command | Description |
|---|---|
| `pico info [--force]` | Show device state. Non-disruptive by default (queries BOOTSEL devices + checks for a CDC tty). `--force` reboots a running app to query it (disruptive). |
| `pico flash <uf2>` | Load a `.uf2` onto the connected Pico (`picotool load -f`). The app must keep USB alive so picotool can reboot it into BOOTSEL; otherwise the tool prints a BOOTSEL-button fallback. |
| `pico reboot [app\|bootsel]` | Reboot the device (default `app`). May fail if the running app doesn't service the reboot request — use `flash` instead. |
| `pico serial [-t SEC] [-T SEC]` | Live-read USB CDC output. `-t` = idle timeout (stop after SEC seconds of silence); `-T` = total timeout. Ctrl-C to stop. |
| `pico run <uf2> [-t SEC]` | **Flash + capture serial from boot** in one command. Flashes (which reboots to BOOTSEL then back to app) and captures the app's output from the moment it boots. `-t` = capture window in seconds (default 15). This is the command for one-shot output (benchmarks, tests that print once). |

## Typical flows

**Capture one-shot output from a benchmark/test that prints once at boot:**
```sh
./tools/pico/pico run embedded_benchmarks/rp2040/trigger_lookup/build/trigger_lookup.uf2 -t 12
```

**Flash an interactive/looping app, then monitor live:**
```sh
./tools/pico/pico flash examples/rp2040_pico/build/rp2040_pico.uf2
./tools/pico/pico serial
```

**Check what's connected:**
```sh
./tools/pico/pico info
```

## How `run` works

`run` spawns a reader thread that waits for the CDC device to vanish
(the `picotool load` reboots the device into BOOTSEL), reappear (the app
boots and CDC enumerates), then captures output for the capture window.
`picotool load -f` runs concurrently in the main thread. This is the
coordinated flash+capture operation that would otherwise require
hand-coded scripts.

## Saved logs

`run` and `serial` tee captured output to `.tmp/serial_logs/<ts>_<cmd>.log`
(gitignored). The path is printed to stderr. Lets you reference/diff runs
later without re-flashing.

## Gotchas

- **One-shot output needs `run`, not `serial`.** An app that prints once
  at boot then idles will have already printed by the time `serial`
  connects. `run` flashes (rebooting the app) and catches the output
  from boot.
- **The app must keep USB alive.** If `main()` returns, USB CDC dies and
  the device can't be software-rebooted — `flash`/`run` will fail with a
  BOOTSEL-button fallback. Embedded apps should end `main()` with an
  infinite loop (`while (true) { tight_loop_contents(); }`).
- **`reboot app` is best-effort.** A running app must service the
  picotool reboot request; many CDC apps don't. Prefer `flash` (which
  uses `load -f` and handles the reboot-to-BOOTSEL itself).
- **Multiple devices.** The tool uses the first `/dev/cu.usbmodem*` it
  finds. If you have multiple Picos connected, disconnect all but one.

# cgx-reactor Roadmap

## 2026-06-07 — Barebones POC

- **Channels (queue-based)** — broadcast signals chosen for barebones; queue-based channels (single listener consumes value) deferred until use case demands it
- **Generic awaiter concept** — reactor manages timers directly for now; a `concept awaiter { is_ready(); resume(); }` would allow reactor-managed non-timer awaitables; deferred to keep barebones simple
- **`delay_until(time_point)`** — absolute-time awaitable for drift-free periodic tasks; `delay_ms` sufficient for barebones
- **`yield` awaitable** — cooperative CPU sharing; not needed for barebones task counts
- **Task lifecycle (`cancel()`, `join()`)** — `final_suspend = suspend_always` leaves the door open; no API surface added yet
- **Multiple instances per task type** — singleton per type for barebones; parameterized instances (e.g., one task per sensor) deferred
- **Idle workers / dynamic task assignment** — all tasks compile-time registered; dynamic assignment from a worker pool deferred
- **Event-based triggering (GPIO, UART)** — consumer-side concern; library only provides `trigger()` API; higher-level event wiring deferred
- **Reactor-managed signals (Model B)** — signals are standalone (Model A) for barebones; central event loop polling deferred until coordination needs arise
- **RP2040 clock implementation** — concept defined, but Pico SDK integration is consumer-side; a reference implementation may be added as an example
- **Custom awaitable examples** — `next_byte` for UART etc. deferred; architecture supports them (signals resume handles directly)

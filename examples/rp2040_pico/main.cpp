// rp2040_pico example — cgx::reactor running on a Pi Pico (RP2040)
//
// Uses pico-sdk 2.2.0 (board=pico) with the cgx::reactor header-only library
// at ../../include. Demonstrates:
//   * Two one-shot scratchpad tasks (init_led, init_temp) with staged
//     simulated-long-init delays for GPIO and ADC setup
//   * Two reserved-loop tasks (blink, temp) recurring via delay_ms<pico_clock>
//   * A reserved-loop task (dump) that periodically reports engine state
//   * A reserved member task `coordinator::bootstrap` that drives the
//     inits to completion via the abstract `engine&` interface, then
//     triggers the blink/temp/dump loops. Triggered once from main()
//     using the concrete engine's instance-based trigger with arg
//     forwarding (`eng.trigger(coord, &coordinator::bootstrap, eng)`).
//   * A 1 ms repeating timer + __wfi event loop (low-power, no std::thread)
//
// The reactor is driven exclusively through full `cgx::reactor::` namespace
// qualifiers — no `using namespace`, no alias.

#include <cgx/reactor.hpp>

#include <cstdio>
#include <string_view>

#include "hardware/adc.h"
#include "hardware/sync.h"
#include "hardware/timer.h"
#include "pico/stdlib.h"
#include "pico_clock.hpp"

// -----------------------------------------------------------------------
// Dump sink — file-scope (stateless) function so the C thunk that the
// coordinator's dumper calls can reference it without captures.
// -----------------------------------------------------------------------

static void dump_sink(std::string_view s) {
    std::printf("%.*s\n", static_cast<int>(s.size()), s.data());
}

// -----------------------------------------------------------------------
// CPU temperature — pico-sdk 2.x has no adc_read_temp(); we implement the
// documented formula ourselves:
//   T = 27 - (V - 0.706) / 0.001721   (V from 12-bit ADC, 3.3 V reference)
// -----------------------------------------------------------------------

static float read_cpu_temp_c() {
    adc_select_input(ADC_TEMPERATURE_CHANNEL_NUM);
    const uint16_t raw = adc_read();
    const float voltage = static_cast<float>(raw) * 3.3f / 4096.0f;
    return 27.0f - (voltage - 0.706f) / 0.001721f;
}

// -----------------------------------------------------------------------
// Coordinator — owns the reactor tasks, including the bootstrap driver
// -----------------------------------------------------------------------

class coordinator {
public:
    // Type-erased dumper: set from main() once the engine exists.
    void set_dumper(void (*fn)(void*), void* ctx) {
        dump_fn_ = fn;
        dump_ctx_ = ctx;
    }

    // One-shot scratchpad init — runs exactly once, then reclaims the slot.
    cgx::reactor::task init_led() {
        gpio_init(PICO_DEFAULT_LED_PIN);
        gpio_set_dir(PICO_DEFAULT_LED_PIN, GPIO_OUT);
        std::printf("[init_led] simulating long init times\n");
        std::printf("[init_led] stage 1/4\n");
        co_await cgx::reactor::delay_ms<pico_clock>(200);
        std::printf("[init_led] stage 2/4\n");
        co_await cgx::reactor::delay_ms<pico_clock>(300);
        std::printf("[init_led] stage 3/4\n");
        co_await cgx::reactor::delay_ms<pico_clock>(200);
        std::printf("[init_led] stage 4/4\n");
        co_await cgx::reactor::delay_ms<pico_clock>(100);
        std::printf("[init_led] LED ready\n");
        co_return;
    }

    cgx::reactor::task init_temp() {
        adc_init();
        adc_set_temp_sensor_enabled(true);
        std::printf("[init_temp] simulating long init times\n");
        std::printf("[init_temp] stage 1/3\n");
        co_await cgx::reactor::delay_ms<pico_clock>(500);
        std::printf("[init_temp] stage 2/3\n");
        co_await cgx::reactor::delay_ms<pico_clock>(50);
        std::printf("[init_temp] stage 3/3\n");
        co_await cgx::reactor::delay_ms<pico_clock>(1000);
        std::printf("[init_temp] ADC temp sensor ready\n");
        co_return;
    }

    // Reserved loop — blink the onboard LED at ~2 Hz.
    cgx::reactor::task blink() {
        for (;;) {
            gpio_put(PICO_DEFAULT_LED_PIN, !gpio_get(PICO_DEFAULT_LED_PIN));
            co_await cgx::reactor::delay_ms<pico_clock>(500);
        }
    }

    // Reserved loop — print the CPU temperature every second.
    cgx::reactor::task temp() {
        for (;;) {
            const float t = read_cpu_temp_c();
            std::printf("[temp] CPU: %.2f C\n", static_cast<double>(t));
            co_await cgx::reactor::delay_ms<pico_clock>(1000);
        }
    }

    // Reserved loop — dump the engine's task memory every 5 seconds.
    cgx::reactor::task dump() {
        for (;;) {
            if (dump_fn_) dump_fn_(dump_ctx_);
            co_await cgx::reactor::delay_ms<pico_clock>(5000);
        }
    }

    // Reserved member task — runs the init sequence via the abstract
    // `engine&` interface, then triggers the recurring loops.
    // Takes `engine&` so the probe falls back to `default_frame_size`
    // (1024B); the reserved pool (default 8192B) accommodates the
    // 4 reserved tasks (bootstrap + blink + temp + dump).
    cgx::reactor::task bootstrap(cgx::reactor::engine& eng) {
        // Interface `trigger(uid)` is non-blocking (returns a
        // `task_handle`); the `co_await h.done()` calls are what
        // suspend bootstrap until each init completes.
        std::printf("[bootstrap] init starting...\n");
        std::printf("[bootstrap] init_led() triggering\n");
        auto h_led = eng.trigger(
            cgx::reactor::task_uid_v<&coordinator::init_led>);
        if (h_led.error() != cgx::reactor::error::ok) co_return;
        std::printf("[bootstrap] init_temp() triggering\n");
        auto h_temp = eng.trigger(
            cgx::reactor::task_uid_v<&coordinator::init_temp>);
        if (h_temp.error() != cgx::reactor::error::ok) co_return;
        co_await h_led.done();
        co_await h_temp.done();
        std::printf("[bootstrap] init completed\n");

        eng.trigger(cgx::reactor::task_uid_v<&coordinator::blink>);
        eng.trigger(cgx::reactor::task_uid_v<&coordinator::temp>);
        eng.trigger(cgx::reactor::task_uid_v<&coordinator::dump>);
        std::printf("[bootstrap] blink/temp/dump triggered\n");
        co_return;
    }

    using reactor_tasks = cgx::reactor::task_list<
        cgx::reactor::scratch_v<&coordinator::init_led>,
        cgx::reactor::scratch_v<&coordinator::init_temp>,
        &coordinator::blink,
        &coordinator::temp,
        &coordinator::dump,
        &coordinator::bootstrap
    >;

private:
    void (*dump_fn_)(void*) = nullptr;
    void* dump_ctx_ = nullptr;
};

// -----------------------------------------------------------------------
// 1 ms tick timer — wakes main loop from WFI, drives eng.tick()
// -----------------------------------------------------------------------

static volatile bool tick_pending = false;

/// Repeating-timer callback (C linkage — no captures allowed).
/// Only sets a flag; the actual tick() runs in the main-loop context.
bool timer_callback([[maybe_unused]] repeating_timer_t* rt) {
    tick_pending = true;
    return true;  // keep repeating
}

// -----------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------

int main() {
    stdio_init_all();
    sleep_ms(2000);  // wait for USB CDC enumeration

    std::printf("\n=== rp2040_pico (cgx::reactor on RP2040) ===\n\n");

    coordinator coord;

    auto eng = cgx::reactor::make_engine<cgx::reactor::default_config, pico_clock>(
        cgx::reactor::register_instance(coord));

    // The engine type is not nameable here without decltype(eng) — wrap
    // eng.dump(sink) in a stateless C thunk so the coordinator can call
    // it from its own `dump` task. The sink is a file-scope function so
    // the thunk needs no captures.
    coord.set_dumper(+[](void* p) {
        using engine_t = std::remove_reference_t<decltype(eng)>;
        static_cast<engine_t*>(p)->dump(dump_sink);
    }, &eng);

    // (1) initial state dump
    std::printf("=== engine: initial ===\n");
    eng.dump(dump_sink);

    // Trigger the bootstrap member task via the concrete engine's
    // instance-based trigger. `eng` is `basic_engine&`; the bootstrap
    // takes `engine&` (abstract), so the arg forwards via the
    // concrete overload's argument forwarding. The trigger runs
    // bootstrap until the first `co_await h_led.done()` suspends
    // (init_led and init_temp are scratchpad tasks, each suspended on
    // their first delay_ms at this point).
    eng.trigger(coord, &coordinator::bootstrap, eng);

    // (2) post-trigger dump: bootstrap is suspended on h_led.done(),
    //     init_led and init_temp are both running (suspended on their
    //     first delay_ms), blink/temp/dump loops not yet triggered.
    std::printf("=== engine: bootstrap running (inits pending) ===\n");
    eng.dump(dump_sink);

    // 1 ms repeating timer — wakes the main loop from WFI each millisecond.
    repeating_timer_t tick_timer;
    add_repeating_timer_ms(-1, timer_callback, nullptr, &tick_timer);

    // Event loop (low-power: WFI, wake on timer IRQ).
    while (true) {
        __wfi();
        if (tick_pending) {
            tick_pending = false;
            eng.tick();
        }
    }
}

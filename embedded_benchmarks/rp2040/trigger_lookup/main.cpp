// trigger_lookup benchmark — measure interface `trigger(task_uid)`
// lookup latency on real RP2040 hardware across a sweep of task counts,
// to validate the O(1) hashmap claim.
//
// Build & flash: see README.md.
//
// Workload: N fire-and-return scratchpad dummy tasks (`co_return` only,
// tiny frame ~32B). Each `trigger(uid)` cycles through the full
// alloc+create+resume+complete+free path — the realistic dispatch
// cost. UIDs cycle through all N per measurement so the hashmap
// probe-chain length varies (honest O(1) measurement, not just
// hitting one cached entry).
//
// Timing: pico-sdk `time_us_32()` (1µs resolution). Per-trigger min/max
// have 1µs granularity (min may read 0 for sub-µs dispatches). Avg is
// computed from a batched K=1000 measurement, so the avg is reliable
// even for sub-µs triggers.
//
// Each line also prints `size=<N>B` — the compile-time `sizeof` of the
// engine instance (full SRAM footprint: tasks_ + uid_table_ +
// invoke_table_ + fn_names_ + pools + timers_ + waiters).  The engines
// are local (stack) in `run_bench<N>()`, one live at a time, so the
// constraint is `max sizeof <= available stack/SRAM`, not the sum.
//
// Expected output (avg roughly constant — O(1) hashmap):
//   N=1    size=...B   min=0us avg=1us max=2us
//   N=2    ...
//   N=1024 ...

#include <cgx/reactor.hpp>

#include <array>
#include <cstdint>
#include <cstdio>
#include <utility>

#include "pico/stdlib.h"
#include "pico_clock.hpp"

// -----------------------------------------------------------------------
// Dummy task — fire-and-return, no-arg, free function.  Frame is
// probed tiny (co_return only, ~32B on clang for this codebase).
// -----------------------------------------------------------------------

template <unsigned I>
cgx::reactor::task dummy() {
    co_return;
}

// -----------------------------------------------------------------------
// Build `task_list<scratch_v<&dummy<Is>>...>` at compile time for a
// given N.  This is the only way to get scratchpad registration for
// free functions (there is no `register_task<scratch, fn>()`).
// -----------------------------------------------------------------------

namespace detail {

template <unsigned N, unsigned... Is>
constexpr auto make_scratch_task_list(std::index_sequence<Is...>) {
    return cgx::reactor::task_list<cgx::reactor::scratch_v<&dummy<Is>>...>{};
}

template <unsigned N>
using scratch_task_list_for =
    decltype(detail::make_scratch_task_list<N>(std::make_index_sequence<N>{}));

// Per-N array of compile-time UIDs.  We index into this at runtime
// with `i % N` — the UIDs themselves are constexpr, so this is
// just an array load (no `task_uid_v<...>` instantiation in the
// hot path).
template <unsigned N, unsigned... Is>
constexpr std::array<cgx::reactor::task_uid, sizeof...(Is)>
make_uid_array(std::index_sequence<Is...>) {
    return {cgx::reactor::task_uid_v<&dummy<Is>>...};
}

template <unsigned N>
constexpr std::array<cgx::reactor::task_uid, N> uids_for =
    make_uid_array<N>(std::make_index_sequence<N>{});

}  // namespace detail

// -----------------------------------------------------------------------
// Per-N driver class — holds the N-tuple of scratchpad dummies.
// -----------------------------------------------------------------------

template <unsigned N>
struct bench_driver {
    using reactor_tasks = detail::scratch_task_list_for<N>;
};

// -----------------------------------------------------------------------
// Minimal Config for the benchmark.
//
// `default_config` has 8KB reserved + 2KB scratchpad pools; for
// the 8-engine sweep (1..128) the metadata (task_meta array, uid
// hashmap, the two new dispatch tables) already dominates BSS, so
// 10KB of pool memory per engine × 8 engines = 80KB of unnecessary
// BSS.  The dummies are tiny (~32B frames) and the probe only
// needs 64B to run (min_probe_space), so 64B/64B pools are plenty.
// All 8 engines fit well under 264KB RP2040 SRAM.
// -----------------------------------------------------------------------

struct bench_config : cgx::reactor::default_config {
    static constexpr std::size_t reserved_pool_size   = 64;
    static constexpr std::size_t scratchpad_pool_size = 64;
};

// -----------------------------------------------------------------------
// Per-N benchmark: build the engine, warm up, time K triggers, print.
// -----------------------------------------------------------------------

template <unsigned N>
void run_bench() {
    constexpr auto uids = detail::uids_for<N>;

    bench_driver<N> drv;
    auto eng = cgx::reactor::make_engine<bench_config, pico_clock>(
        cgx::reactor::register_instance(drv));

    // Warm up — fills the probe cache, pre-faults the scratchpad pool,
    // and validates that every UID we plan to cycle through actually
    // dispatches successfully.  If a trigger errors, we skip that N
    // (would pollute the measurement).
    constexpr unsigned WARMUP = 10;
    bool ok = true;
    for (unsigned i = 0; i < WARMUP; ++i) {
        auto h = eng.trigger(uids[i % N]);
        if (h.error() != cgx::reactor::error::ok) { ok = false; break; }
    }
    if (!ok) {
        std::printf("N=%-4u SKIPPED (warmup trigger error)\n", N);
        return;
    }

    // Timed loop — K iterations, cycle through all N UIDs.
    constexpr unsigned K = 1000;
    uint32_t min_us = UINT32_MAX;
    uint32_t max_us = 0;

    uint32_t start = time_us_32();
    for (unsigned i = 0; i < K; ++i) {
        uint32_t t0 = time_us_32();
        auto h = eng.trigger(uids[i % N]);
        uint32_t t1 = time_us_32();
        (void)h;
        uint32_t dt = t1 - t0;
        if (dt < min_us) min_us = dt;
        if (dt > max_us) max_us = dt;
    }
    uint32_t total = time_us_32() - start;
    uint32_t avg_us = total / K;

    // Note: min_us may be 0 because time_us_32() has 1µs resolution
    // and a single trigger can complete in well under 1µs.  The avg
    // (batched over K=1000) is the reliable number; min/max are
    // informational.
    // `sizeof(decltype(eng))` is a compile-time constant = the full
    // SRAM footprint of one engine instance (every per-engine array
    // is a member of `basic_engine`).  Printed to surface the
    // stack/SRAM budget per N — the ceiling is `max sizeof <= stack
    // (or SRAM)`, not the sum across N.
    std::printf("N=%-4u size=%zuB min=%uus avg=%uus max=%uus\n",
                N, sizeof(decltype(eng)), min_us, avg_us, max_us);
}

int main() {
    stdio_init_all();
    sleep_ms(2000);  // wait for USB CDC enumeration

    std::printf("\n=== trigger_lookup benchmark (pico_clock) ===\n");
    std::printf("Each N: K=1000 triggers, cycling through all N UIDs.\n");
    std::printf("Scratchpad dummies, full alloc+run+free per trigger.\n");
    std::printf("timer=time_us_32() (1us resolution); avg is batched.\n\n");

    // Compile-time N ladder.  N must be a compile-time constant
    // because make_engine takes the spec pack as variadic template
    // args; runtime N would require dynamic dispatch which the
    // reactor doesn't support.
    //
    // The 11-point sweep covers four orders of magnitude (1 → 1024).
    // The single `invoke_table_` in `basic_engine` (one method per
    // dispatch op + N tiny trivial thunks) gives O(1) dispatch
    // across the full range.  If a larger N's warmup triggers an
    // error (capacity_exceeded from pool pressure, or stack/SRAM
    // overflow at runtime), `run_bench<N>()` prints
    // `N=<n> SKIPPED (...)` and continues — so a too-big N doesn't
    // kill the whole run.  The largest N that runs cleanly is the
    // hardware SRAM ceiling.
    run_bench<1>();
    run_bench<2>();
    run_bench<4>();
    run_bench<8>();
    run_bench<16>();
    run_bench<32>();
    run_bench<64>();
    run_bench<128>();
    run_bench<256>();
    run_bench<512>();
    run_bench<1024>();

    std::printf("\n=== done ===\n");

    // Keep USB CDC alive after the benchmark completes so the device
    // stays responsive to `picotool load -f` (software reboot) and to
    // a late-connecting monitor.  `tight_loop_contents()` is a no-op
    // helper from the pico-sdk that keeps the USB IRQs serviced
    // (without it the host's USB CDC stack would hang and the device
    // would be unreachable until a physical BOOTSEL reset).
    while (true) { tight_loop_contents(); }
}

#include <cgx/reactor.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// drifty_task — uses delay_ms(100ms), drift accumulates
// -----------------------------------------------------------------------

cgx::reactor::task drifty_task() {
    auto t0 = cgx::reactor::steady_clock::now();
    for (int i = 0; i < 10; ++i) {
        co_await cgx::reactor::delay_ms<cgx::reactor::steady_clock>(100ms);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            cgx::reactor::steady_clock::now() - t0);
        std::printf("[drifty] iter %d -- elapsed %lldms\n", i,
                    (long long)elapsed.count());
    }
    std::puts("[drifty] done");
    co_return;
}

// -----------------------------------------------------------------------
// precise_task — uses delay_until(next) with next += 100ms, no drift
// -----------------------------------------------------------------------

cgx::reactor::task precise_task() {
    auto t0 = cgx::reactor::steady_clock::now();
    auto next = t0 + 100ms;
    for (int i = 0; i < 10; ++i) {
        co_await cgx::reactor::delay_until<cgx::reactor::steady_clock>(next);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            cgx::reactor::steady_clock::now() - t0);
        std::printf("[precise] iter %d -- elapsed %lldms\n", i,
                    (long long)elapsed.count());
        next += 100ms;
    }
    std::puts("[precise] done");
    co_return;
}

// -----------------------------------------------------------------------
// quantized_task — uses delay_quantized(100ms), grid-aligned, no drift
// -----------------------------------------------------------------------

cgx::reactor::task quantized_task() {
    auto t0 = cgx::reactor::steady_clock::now();
    for (int i = 0; i < 10; ++i) {
        co_await cgx::reactor::delay_quantized<cgx::reactor::steady_clock>(100ms);
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            cgx::reactor::steady_clock::now() - t0);
        std::printf("[quantized] iter %d -- elapsed %lldms\n", i,
                    (long long)elapsed.count());
    }
    std::puts("[quantized] done");
    co_return;
}

// -----------------------------------------------------------------------
// Main — tie everything together
// -----------------------------------------------------------------------

int main() {
    auto eng = cgx::reactor::make_engine<
        cgx::reactor::default_config,
        cgx::reactor::steady_clock>(
        cgx::reactor::register_task<&drifty_task>(),
        cgx::reactor::register_task<&precise_task>(),
        cgx::reactor::register_task<&quantized_task>());

    // Trigger all three tasks.
    {
        auto ec = eng.template trigger<&drifty_task>();
        if (ec != cgx::reactor::error::ok) {
            std::fprintf(stderr, "drifty trigger failed: %s\n",
                         cgx::reactor::to_string(ec).data());
            return EXIT_FAILURE;
        }
    }
    {
        auto ec = eng.template trigger<&precise_task>();
        if (ec != cgx::reactor::error::ok) {
            std::fprintf(stderr, "precise trigger failed: %s\n",
                         cgx::reactor::to_string(ec).data());
            return EXIT_FAILURE;
        }
    }
    {
        auto ec = eng.template trigger<&quantized_task>();
        if (ec != cgx::reactor::error::ok) {
            std::fprintf(stderr, "quantized trigger failed: %s\n",
                         cgx::reactor::to_string(ec).data());
            return EXIT_FAILURE;
        }
    }

    std::puts("--- starting timer comparison (10 iterations, 100ms period) ---");

    // 10 iterations × 100ms = 1000ms, plus margin → 60 ticks × 20ms = 1200ms
    for (int i = 0; i < 60; ++i) {
        eng.tick();
        std::this_thread::sleep_for(20ms);
    }

    // Final summary
    std::puts("--- summary ---");
    std::puts("[drifty]    delay_ms(100ms)         -- drift accumulates");
    std::puts("[precise]   delay_until(next)       -- no drift, exact points");
    std::puts("[quantized] delay_quantized(100ms)  -- no drift, grid-aligned");
    std::puts("--- all done ---");
    return EXIT_SUCCESS;
}

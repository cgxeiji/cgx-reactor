#include <cgx/reactor.hpp>

#include <cstdio>
#include <cstdlib>

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Signal for inter-task data transfer
// -----------------------------------------------------------------------

using signal_t = cgx::reactor::signal<std::uint8_t, 4>;
signal_t data_signal;

// -----------------------------------------------------------------------
// Fire-and-return task — demonstrates a one-shot coroutine
// -----------------------------------------------------------------------

cgx::reactor::task hello_task() {
    std::puts("[hello] fire-and-return task executed");
    co_return;
}

// -----------------------------------------------------------------------
// Producer task — periodic, fires signal with incrementing value
// -----------------------------------------------------------------------

cgx::reactor::task producer_task(std::uint8_t& value) {
    for (int i = 0; i < 5; ++i) {
        co_await cgx::reactor::delay_ms<cgx::reactor::steady_clock>(200ms);
        std::printf("[producer] firing signal with value %u\n", value);
        data_signal.fire(value);
        ++value;
    }
    std::puts("[producer] done");
    co_return;
}

// -----------------------------------------------------------------------
// Consumer task — reactive, waits on signal and prints received values
//
// The consumer listens immediately and suspends on the signal.  When the
// producer fires the signal the consumer is resumed directly (not via
// tick()).  The delay_ms in the producer ensures timed events stay in sync.
// -----------------------------------------------------------------------

cgx::reactor::task consumer_task() {
    for (int i = 0; i < 5; ++i) {
        std::uint8_t val = co_await data_signal.listen();
        std::printf("[consumer] received %u\n", val);
    }
    std::puts("[consumer] done");
    co_return;
}

cgx::reactor::task consumer2_task() {
    for (int i = 0; i < 3; ++i) {
        std::uint8_t val = co_await data_signal.listen();
        std::printf("[consumer2] received %u\n", val);
    }
    std::puts("[consumer2] done");
    co_return;
}

// -----------------------------------------------------------------------
// Main — tie everything together
// -----------------------------------------------------------------------

int main() {
    auto eng = cgx::reactor::make_engine<
        cgx::reactor::default_config,
        cgx::reactor::steady_clock>(
        cgx::reactor::register_task<&hello_task>(),
        cgx::reactor::register_task<&producer_task>(),
        cgx::reactor::register_task<&consumer_task>(),
        cgx::reactor::register_task<&consumer2_task>());

    // Trigger order matters: consumer FIRST so its timer sits at a lower
    // array index and is processed before the producer's timer in tick().
    // This ensures the consumer is already listening when the producer fires.

    // --- Consumer task ---
    {
        auto ec = eng.template trigger<&consumer_task>();
        if (ec != cgx::reactor::error::ok) {
            std::fprintf(stderr, "consumer trigger failed: %s\n",
                         cgx::reactor::to_string(ec).data());
            return EXIT_FAILURE;
        }
    }

    {
        auto ec = eng.template trigger<&consumer2_task>();
        if (ec != cgx::reactor::error::ok) {
            std::fprintf(stderr, "consumer2 trigger failed: %s\n",
                         cgx::reactor::to_string(ec).data());
            return EXIT_FAILURE;
        }
    }

    // --- Producer task ---
    std::uint8_t value = 1;
    {
        auto ec = eng.template trigger<&producer_task>(value);
        if (ec != cgx::reactor::error::ok) {
            std::fprintf(stderr, "producer trigger failed: %s\n",
                         cgx::reactor::to_string(ec).data());
            return EXIT_FAILURE;
        }
    }

    // --- Fire-and-return task ---
    {
        auto ec = eng.template trigger<&hello_task>();
        if (ec != cgx::reactor::error::ok) {
            std::fprintf(stderr, "hello trigger failed: %s\n",
                         cgx::reactor::to_string(ec).data());
            return EXIT_FAILURE;
        }
    }

    // --- Event loop ---
    // Run enough ticks to complete the periodic producer (5 × 200ms) plus
    // some margin.  Real sleep between ticks so the steady_clock advances.
    std::puts("--- starting event loop ---");
    for (int i = 0; i < 30; ++i) {
        eng.tick();
        std::this_thread::sleep_for(200ms);
    }

    std::puts("--- all done ---");
    return EXIT_SUCCESS;
}

#include <cgx/reactor.hpp>
#include <cstdio>
#include <thread>

namespace cr = cgx::reactor;
using cr::operator""_tag;

// ---------------------------------------------------------------------------
// Custom logger — prints to stdout
// ---------------------------------------------------------------------------

struct stdout_logger {
    static void print(const char* msg) {
        printf("%s\n", msg);
    }
};

// ---------------------------------------------------------------------------
// Signal — fire every 300ms; listener receives values
// ---------------------------------------------------------------------------

cr::signal<int, 8, stdout_logger> my_signal;

cr::task signal_firer() {
    int val = 0;
    for (;;) {
        co_await cr::delay_ms<cr::steady_clock>(300);
        my_signal.fire(++val);
        printf("[signal_firer] fired %d\n", val);
    }
}

cr::task signal_listener() {
    for (;;) {
        int val = co_await my_signal.listen();
        printf("[signal_listener] received %d\n", val);
    }
}

// ---------------------------------------------------------------------------
// Channel — producer pushes every 250ms; consumer pops
// ---------------------------------------------------------------------------

cr::channel<int, 4, stdout_logger> my_channel;

cr::task channel_producer() {
    int val = 0;
    for (;;) {
        co_await cr::delay_ms<cr::steady_clock>(250);
        auto ec = co_await my_channel.push(++val);
        printf("[channel_producer] pushed %d: %s\n", val,
               cr::to_string(ec).data());
    }
}

cr::task channel_consumer() {
    for (;;) {
        auto result = co_await my_channel.pop();
        if (result) {
            printf("[channel_consumer] received %d\n", *result);
        } else {
            printf("[channel_consumer] channel closed\n");
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Timer-based tasks (from original example)
// ---------------------------------------------------------------------------

cr::task delayed_increment(int& counter) {
    for (;;) {
        co_await cr::delay_ms<cr::steady_clock>(100);
        ++counter;
        printf("[delayed_increment] counter incremented to %d\n", counter);
    }
}

cr::task periodic_loop(int& counter) {
    for (;;) {
        co_await cr::delay_ms<cr::steady_clock>(50);
        printf("[periodic_loop] counter = %d\n", counter);
        ++counter;
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main() {
    int fire_cnt = 0;
    int loop_cnt = 0;

    auto eng = cr::make_engine<cr::default_config, cr::steady_clock,
                               stdout_logger>(
        cr::register_task<"FIRE"_tag, &delayed_increment>(),
        cr::register_task<"LOOP"_tag, &periodic_loop>(),
        cr::register_task<"SIGF"_tag, &signal_firer>(),
        cr::register_task<"SIGL"_tag, &signal_listener>(),
        cr::register_task<"CHNP"_tag, &channel_producer>(),
        cr::register_task<"CHNC"_tag, &channel_consumer>());

    std::puts("=== cgx::reactor logging example ===\n");
    std::puts("--- triggering all tasks ---");
    eng.trigger<&delayed_increment>(fire_cnt);
    eng.trigger<&periodic_loop>(loop_cnt);
    eng.trigger<&signal_firer>();
    eng.trigger<&signal_listener>();
    eng.trigger<&channel_producer>();
    eng.trigger<&channel_consumer>();

    std::puts("\n--- event loop (~2.5s) ---");
    for (int i = 0; i < 500; ++i) {
        eng.tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        if (i % 100 == 0)
            std::fflush(stdout);
    }

    // Close the channel to demonstrate channel-close logging.
    puts("\n--- closing channel ---");
    my_channel.close();
    eng.tick();

    printf("\nfire_cnt = %d, loop_cnt = %d\n", fire_cnt, loop_cnt);
    std::puts("\n=== logger example complete ===");
}

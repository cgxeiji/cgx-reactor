#include <cgx/reactor.hpp>
#include <iostream>

using namespace cgx::reactor;
using namespace std::chrono_literals;

// Configuration with small limits to trigger errors
struct small_config {
    static constexpr std::size_t max_timers = 2;           // Only 2 timers allowed
    static constexpr std::size_t max_signal_listeners = 2; // Only 2 listeners allowed
    static constexpr std::size_t reserved_pool_size = 8192;
    static constexpr std::size_t scratchpad_pool_size = 2048;
    static constexpr log_level min_level = log_level::info;
};

// Micro-pool config for overflow demo — too small for any real coroutine
struct tiny_pool_config {
    static constexpr std::size_t max_timers = 2;
    static constexpr std::size_t max_signal_listeners = 2;
    static constexpr std::size_t reserved_pool_size = 16;
    static constexpr std::size_t scratchpad_pool_size = 2048;
    static constexpr log_level min_level = log_level::info;
};

using signal_t = cgx::reactor::signal<int, 2>;
signal_t limited_signal;  // Signal with capacity for 2 listeners

// ---------------------------------------------------------------------------
// Error 1: task_already_running
// ---------------------------------------------------------------------------

task long_running_task() {
    std::cout << "[long_running] Started\n";
    co_await delay_ms<steady_clock>(50);
    std::cout << "[long_running] Finished\n";
    co_return;
}

// ---------------------------------------------------------------------------
// Error 2: capacity_exceeded - three tasks that all try to add timers
// ---------------------------------------------------------------------------

task timer_task_a() {
    std::cout << "[timer_a] Adding timer...\n";
    auto ec = co_await delay_ms<steady_clock>(100);
    if (ec != error::ok) {
        std::cout << "[timer_a] Timer failed: " << to_string(ec) << "\n";
    } else {
        std::cout << "[timer_a] Timer completed\n";
    }
    co_return;
}

task timer_task_b() {
    std::cout << "[timer_b] Adding timer...\n";
    auto ec = co_await delay_ms<steady_clock>(100);
    if (ec != error::ok) {
        std::cout << "[timer_b] Timer failed: " << to_string(ec) << "\n";
    } else {
        std::cout << "[timer_b] Timer completed\n";
    }
    co_return;
}

task timer_task_c() {
    std::cout << "[timer_c] Adding timer...\n";
    auto ec = co_await delay_ms<steady_clock>(100);
    if (ec != error::ok) {
        std::cout << "[timer_c] Timer failed: " << to_string(ec) << "\n";
    } else {
        std::cout << "[timer_c] Timer completed\n";
    }
    co_return;
}

// ---------------------------------------------------------------------------
// Error 3: listener_limit_exceeded - three tasks that all try to listen
// ---------------------------------------------------------------------------

task listener_task_1() {
    std::cout << "[listener1] Attempting to listen...\n";
    auto awaiter = limited_signal.listen();
    int val = co_await awaiter;
    if (awaiter.ec_ != error::ok) {
        std::cout << "[listener1] Listen failed: " << to_string(awaiter.ec_) << "\n";
    } else {
        std::cout << "[listener1] Received: " << val << "\n";
    }
    co_return;
}

task listener_task_2() {
    std::cout << "[listener2] Attempting to listen...\n";
    auto awaiter = limited_signal.listen();
    int val = co_await awaiter;
    if (awaiter.ec_ != error::ok) {
        std::cout << "[listener2] Listen failed: " << to_string(awaiter.ec_) << "\n";
    } else {
        std::cout << "[listener2] Received: " << val << "\n";
    }
    co_return;
}

task listener_task_3() {
    std::cout << "[listener3] Attempting to listen...\n";
    auto awaiter = limited_signal.listen();
    int val = co_await awaiter;
    if (awaiter.ec_ != error::ok) {
        std::cout << "[listener3] Listen failed: " << to_string(awaiter.ec_) << "\n";
    } else {
        std::cout << "[listener3] Received: " << val << "\n";
    }
    co_return;
}

// ---------------------------------------------------------------------------
// Error 4: pool overflow — tiny pool can't hold any real coroutine
// ---------------------------------------------------------------------------

task any_coroutine() {
    // This coroutine is too large for a 16-byte reserved pool.
    std::cout << "[pool] This message never appears — pool too small\n";
    co_return;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    std::cout << "=== Error Handling Example ===\n\n";

    auto eng = make_engine<small_config, steady_clock>(
        register_task<"LONG"_tag, &long_running_task>(),
        register_task<"TMA_"_tag, &timer_task_a>(),
        register_task<"TMB_"_tag, &timer_task_b>(),
        register_task<"TMC_"_tag, &timer_task_c>(),
        register_task<"LST1"_tag, &listener_task_1>(),
        register_task<"LST2"_tag, &listener_task_2>(),
        register_task<"LST3"_tag, &listener_task_3>());

    // Error 1: task_already_running
    std::cout << "--- Error 1: task_already_running ---\n";
    auto ec = eng.template trigger<&long_running_task>();
    std::cout << "First trigger: " << to_string(ec) << "\n";

    ec = eng.template trigger<&long_running_task>();
    std::cout << "Second trigger (should fail): " << to_string(ec) << "\n";

    std::cout << "\n";

    // Error 2: capacity_exceeded
    std::cout << "--- Error 2: capacity_exceeded (timers) ---\n";
    std::cout << "Triggering 3 tasks that each add a timer (max_timers=2)...\n";
    eng.template trigger<&timer_task_a>();
    eng.template trigger<&timer_task_b>();
    eng.template trigger<&timer_task_c>();
    eng.tick();  // All three run, but only 2 timers succeed

    std::cout << "\n";

    // Error 3: listener_limit_exceeded
    std::cout << "--- Error 3: listener_limit_exceeded ---\n";
    std::cout << "Triggering 3 tasks that each listen on signal (max_listeners=2)...\n";
    eng.template trigger<&listener_task_1>();
    eng.template trigger<&listener_task_2>();
    eng.template trigger<&listener_task_3>();
    eng.tick();  // First two suspend, third fails immediately

    std::cout << "\nFiring signal to resume listeners...\n";
    limited_signal.fire(42);
    eng.tick();

    std::cout << "\n";

    // Error 4: pool overflow
    std::cout << "--- Error 4: pool overflow ---\n";
    {
        auto tiny_eng = make_engine<tiny_pool_config, steady_clock>(
            register_task<"OVER"_tag, &any_coroutine>());

        std::cout << "pool_exhausted(): " << (tiny_eng.pool_exhausted() ? "true" : "false") << "\n";
        ec = tiny_eng.template trigger<&any_coroutine>();
        std::cout << "Trigger result: " << to_string(ec) << "\n";
    }  // tiny_eng destroyed here

    std::cout << "\n=== Example Complete ===\n";
    return 0;
}

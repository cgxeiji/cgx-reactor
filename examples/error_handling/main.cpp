#include <cgx/reactor.hpp>
#include <iostream>

using namespace cgx::reactor;
using namespace std::chrono_literals;

// Configuration with small limits to trigger errors
struct small_config {
    static constexpr std::size_t max_tasks = 7;            // 7 tasks total
    static constexpr std::size_t max_timers = 2;           // Only 2 timers allowed
    static constexpr std::size_t max_signal_listeners = 2; // Only 2 listeners allowed
    static constexpr std::size_t task_frame_size = 64;
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
// Error 2: queue_full - three tasks that all try to add timers
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
// Main
// ---------------------------------------------------------------------------

int main() {
    std::cout << "=== Error Handling Example ===\n\n";
    
    engine<small_config, steady_clock,
           &long_running_task,
           &timer_task_a, &timer_task_b, &timer_task_c,
           &listener_task_1, &listener_task_2, &listener_task_3> eng;
    
    // Error 1: task_already_running
    std::cout << "--- Error 1: task_already_running ---\n";
    auto ec = eng.trigger<&long_running_task>();
    std::cout << "First trigger: " << to_string(ec) << "\n";
    
    ec = eng.trigger<&long_running_task>();
    std::cout << "Second trigger (should fail): " << to_string(ec) << "\n";
    
    std::cout << "\n";
    
    // Error 2: queue_full
    std::cout << "--- Error 2: queue_full ---\n";
    std::cout << "Triggering 3 tasks that each add a timer (max_timers=2)...\n";
    eng.trigger<&timer_task_a>();
    eng.trigger<&timer_task_b>();
    eng.trigger<&timer_task_c>();
    eng.tick();  // All three run, but only 2 timers succeed
    
    std::cout << "\n";
    
    // Error 3: listener_limit_exceeded
    std::cout << "--- Error 3: listener_limit_exceeded ---\n";
    std::cout << "Triggering 3 tasks that each listen on signal (max_listeners=2)...\n";
    eng.trigger<&listener_task_1>();
    eng.trigger<&listener_task_2>();
    eng.trigger<&listener_task_3>();
    eng.tick();  // First two suspend, third fails immediately
    
    std::cout << "\nFiring signal to resume listeners...\n";
    limited_signal.fire(42);
    eng.tick();
    
    std::cout << "\n=== Example Complete ===\n";
    return 0;
}

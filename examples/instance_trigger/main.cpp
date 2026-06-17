#include <cgx/reactor.hpp>

#include <cmath>
#include <cstdio>
#include <cstdlib>

namespace cr = cgx::reactor;

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// mock_temperature_sensor — reads a temperature register periodically
// -----------------------------------------------------------------------

class mock_temperature_sensor {
    cr::signal<float> signal_;
    float base_temp_;
    float noise_amp_;
    unsigned tick_ = 0;

public:
    mock_temperature_sensor(float base_temp_c, float noise_amplitude_c)
        : base_temp_(base_temp_c), noise_amp_(noise_amplitude_c) {}

    auto on_temperature() const { return signal_.listen(); }

    cr::task poll_loop() {
        while (true) {
            co_await cr::delay_ms<cr::steady_clock>(100ms);
            float t = base_temp_ + noise_amp_ * std::sin(static_cast<float>(tick_) * 0.3f);
            ++tick_;
            std::printf("[temp] %.1f°C\n", static_cast<double>(t));
            signal_.fire(t);
        }
    }

    using reactor_tasks = cr::task_list<&mock_temperature_sensor::poll_loop>;
};

// -----------------------------------------------------------------------
// serial_printer — listens to two temperature sensors and prints combos
// -----------------------------------------------------------------------

class serial_printer {
    float last_temp_a_ = 0.0f;
    float last_temp_b_ = 0.0f;

public:
    cr::task print_loop(mock_temperature_sensor const& sensor_a,
                        mock_temperature_sensor const& sensor_b) {
        while (true) {
            last_temp_a_ = co_await sensor_a.on_temperature();
            last_temp_b_ = co_await sensor_b.on_temperature();
            std::printf("[term] sensor_a=%.1f°C  sensor_b=%.1f°C\n",
                        static_cast<double>(last_temp_a_),
                        static_cast<double>(last_temp_b_));
        }
    }

    using reactor_tasks = cr::task_list<&serial_printer::print_loop>;
};

// -----------------------------------------------------------------------
// Main — wire two sensor instances + printer, dump, then run briefly
// -----------------------------------------------------------------------

int main() {
    std::puts("=== instance_trigger example starting ===");

    mock_temperature_sensor sensor_a{23.0f, 5.0f};
    mock_temperature_sensor sensor_b{30.0f, 3.0f};
    serial_printer         printer;

    // Register all three instances.
    auto eng = cr::make_engine<cr::default_config, cr::steady_clock>(
        cr::register_instance(sensor_a),
        cr::register_instance(sensor_b),
        cr::register_instance(printer));

    // Trigger each sensor by instance (instance-based dispatch).
    auto ec = eng.trigger(sensor_a, &mock_temperature_sensor::poll_loop);
    if (ec != cr::error::ok) {
        std::fprintf(stderr, "sensor_a trigger failed: %s\n",
                     cr::to_string(ec).data());
        return EXIT_FAILURE;
    }

    ec = eng.trigger(sensor_b, &mock_temperature_sensor::poll_loop);
    if (ec != cr::error::ok) {
        std::fprintf(stderr, "sensor_b trigger failed: %s\n",
                     cr::to_string(ec).data());
        return EXIT_FAILURE;
    }

    // Printer takes sensor references as arguments.
    ec = eng.trigger(printer, &serial_printer::print_loop, sensor_a, sensor_b);
    if (ec != cr::error::ok) {
        std::fprintf(stderr, "printer trigger failed: %s\n",
                     cr::to_string(ec).data());
        return EXIT_FAILURE;
    }

    // Dump engine layout using a custom sink (no_logger suppresses the default dump).
    std::puts("");
    std::puts("--- Engine dump ---");
    eng.dump([](std::string_view line) {
        std::printf("%s\n", line.data());
    });
    std::puts("--- End dump ---");
    std::puts("");

    // Event loop — run for ~1.5 seconds (30 ticks × 50 ms).
    for (int i = 0; i < 30; ++i) {
        eng.tick();
        std::this_thread::sleep_for(50ms);
    }

    std::puts("=== instance_trigger example done ===");
    return EXIT_SUCCESS;
}

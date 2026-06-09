#include <cgx/reactor.hpp>

#include <cmath>
#include <cstdio>
#include <cstdlib>

// NOTE: We do NOT use `using namespace cgx::reactor` here because POSIX
// `<sys/signal.h>` (pulled in via standard headers) declares `signal()`
// in the global namespace, which clashes with `cgx::reactor::signal`.
// Instead we alias the namespace and bring in the tag literal separately.
namespace cr = cgx::reactor;
using cr::operator""_tag;

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Signals — file-scope, one per sensor type
// -----------------------------------------------------------------------

cr::signal<float> temperature_signal;
cr::signal<float> pressure_signal;

// -----------------------------------------------------------------------
// mock_temperature_sensor — pretends to read a temperature register
// -----------------------------------------------------------------------

class mock_temperature_sensor {
    float base_temp_;
    float noise_amp_;
    unsigned tick_ = 0;

public:
    mock_temperature_sensor(float base_temp_c, float noise_amplitude_c)
        : base_temp_(base_temp_c), noise_amp_(noise_amplitude_c) {}

    cr::task poll_loop() {
        while (true) {
            co_await cr::delay_ms<cr::steady_clock>(100ms);
            float t = base_temp_ + noise_amp_ * std::sin(static_cast<float>(tick_) * 0.3f);
            ++tick_;
            std::printf("[temp] %.1f°C\n", static_cast<double>(t));
            temperature_signal.fire(t);
        }
    }

    using reactor_tasks = cr::task_list<&mock_temperature_sensor::poll_loop>;
};

// -----------------------------------------------------------------------
// mock_pressure_sensor — same shape, different period, different signal
// -----------------------------------------------------------------------

class mock_pressure_sensor {
    float base_pressure_;
    float noise_amp_;
    unsigned tick_ = 0;

public:
    mock_pressure_sensor(float base_pressure_kpa, float noise_amplitude_kpa)
        : base_pressure_(base_pressure_kpa), noise_amp_(noise_amplitude_kpa) {}

    cr::task poll_loop() {
        while (true) {
            co_await cr::delay_ms<cr::steady_clock>(150ms);
            float p = base_pressure_ + noise_amp_ * std::cos(static_cast<float>(tick_) * 0.2f);
            ++tick_;
            std::printf("[pres] %.1f kPa\n", static_cast<double>(p));
            pressure_signal.fire(p);
        }
    }

    using reactor_tasks = cr::task_list<&mock_pressure_sensor::poll_loop>;
};

// -----------------------------------------------------------------------
// serial_printer — listens to both signals, prints combined line
//
// The printer preserves the most recent value from each sensor and
// alternates which signal it waits on, so it always blends the newest
// reading from one side with the cached value from the other.
// -----------------------------------------------------------------------

class serial_printer {
    float last_temp_ = 0.0f;
    float last_pressure_ = 0.0f;

public:
    cr::task print_loop() {
        // Always wait for the first reading from both before printing.
        last_temp_     = co_await temperature_signal.listen();
        last_pressure_ = co_await pressure_signal.listen();
        std::printf("[term] T=%.1f°C  P=%.1f kPa\n",
                    static_cast<double>(last_temp_),
                    static_cast<double>(last_pressure_));

        // Thereafter alternate: listen to temperature, then pressure.
        while (true) {
            last_temp_ = co_await temperature_signal.listen();
            std::printf("[term] T=%.1f°C  P=%.1f kPa\n",
                        static_cast<double>(last_temp_),
                        static_cast<double>(last_pressure_));

            last_pressure_ = co_await pressure_signal.listen();
            std::printf("[term] T=%.1f°C  P=%.1f kPa\n",
                        static_cast<double>(last_temp_),
                        static_cast<double>(last_pressure_));
        }
    }

    using reactor_tasks = cr::task_list<&serial_printer::print_loop>;
};

// -----------------------------------------------------------------------
// Main — wire everything together
// -----------------------------------------------------------------------

int main() {
    std::puts("=== member_task example starting ===");

    mock_temperature_sensor temp_sensor{23.0f, 5.0f};
    mock_pressure_sensor   pressure_sensor{101.3f, 2.0f};
    serial_printer         printer;

    auto eng = cr::make_engine<cr::default_config, cr::steady_clock>(
        cr::register_instance<"TEMP"_tag>(temp_sensor),
        cr::register_instance<"PRES"_tag>(pressure_sensor),
        cr::register_instance<"TERM"_tag>(printer));

    // Trigger all three tasks.
    auto ec = eng.template trigger<&mock_temperature_sensor::poll_loop>();
    if (ec != cr::error::ok) {
        std::fprintf(stderr, "temp trigger failed: %s\n",
                     cr::to_string(ec).data());
        return EXIT_FAILURE;
    }

    ec = eng.template trigger<&mock_pressure_sensor::poll_loop>();
    if (ec != cr::error::ok) {
        std::fprintf(stderr, "pressure trigger failed: %s\n",
                     cr::to_string(ec).data());
        return EXIT_FAILURE;
    }

    ec = eng.template trigger<&serial_printer::print_loop>();
    if (ec != cr::error::ok) {
        std::fprintf(stderr, "printer trigger failed: %s\n",
                     cr::to_string(ec).data());
        return EXIT_FAILURE;
    }

    // Event loop — run for ~3 seconds (60 ticks × 50 ms).
    for (int i = 0; i < 60; ++i) {
        eng.tick();
        std::this_thread::sleep_for(50ms);
    }

    std::puts("=== member_task example done ===");
    return EXIT_SUCCESS;
}

#include <cgx/reactor.hpp>

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace cr = cgx::reactor;

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// uart_receiver: mocks ISR waking up, reading a byte, pushing to channel
// -----------------------------------------------------------------------

class uart_receiver {
    cr::channel<char, 1> tx_channel_;  // Size 1: ISR-to-task handoff

public:
    /// Return a pop_awaiter for the next received byte.
    auto on_byte() { return tx_channel_.pop(); }

    /// Simulate ISR waking up and receiving bytes (very fast - every 10ms).
    cr::task receive_loop() {
        std::string_view message = "Hello, reactor!";
        for (char c : message) {
            co_await cr::delay_ms<cr::steady_clock>(10ms);  // ISR fires every 10ms
            std::printf("[rx] got: '%c'\n", c);

            // ISR pushes to channel (non-blocking, size 1)
            auto ec = tx_channel_.try_push(c);
            if (ec == cr::error::capacity_exceeded) {
                std::printf("[rx] channel full, dropping '%c'\n", c);
            }
        }

        // Send null terminator
        co_await cr::delay_ms<cr::steady_clock>(10ms);
        std::printf("[rx] got: '\\0'\n");
        tx_channel_.try_push('\0');

        // Keep alive
        co_await cr::delay_ms<cr::steady_clock>(5000ms);
        tx_channel_.close();
    }

    using reactor_tasks = cr::task_list<&uart_receiver::receive_loop>;
};

// -----------------------------------------------------------------------
// processor: two-stage pipeline
//   fast_buffer: quickly reads from channel into local buffer
//   process: slowly iterates over local buffer
// -----------------------------------------------------------------------

class processor {
    static constexpr size_t BUFFER_SIZE = 32;
    char buffer_[BUFFER_SIZE];
    size_t write_pos_ = 0;  // Where fast_buffer writes
    size_t read_pos_ = 0;   // Where process reads

public:
    /// Fast path: co_awaits channel, accumulates bytes into local buffer.
    /// No analysis — just receives and buffers as fast as possible.
    cr::task fast_buffer(uart_receiver& rx) {
        std::printf("[fast_buffer] waiting for data...\n");

        while (auto byte = co_await rx.on_byte()) {
            char c = *byte;

            // Accumulate byte immediately (no delay, no analysis)
            if (write_pos_ < BUFFER_SIZE - 1) {
                buffer_[write_pos_++] = c;
                if (c == '\0') {
                    std::printf("[fast_buffer] buffered: '\\0'\n");
                } else {
                    std::printf("[fast_buffer] buffered: '%c'\n", c);
                }
            }
        }

        std::printf("[fast_buffer] channel closed\n");
    }

    /// Slow path: checks buffer for new bytes, prints when 0x00 is received.
    cr::task process() {
        std::printf("[process] waiting for data...\n");

        while (true) {
            // Wait for new data
            if (read_pos_ >= write_pos_) {
                co_await cr::delay_ms<cr::steady_clock>(10ms);
                continue;
            }

            // Process bytes one by one
            char c = buffer_[read_pos_];
            if (c == '\0') {
                std::printf("[process] parsing: '\\0'\n");
            } else {
                std::printf("[process] parsing: '%c'\n", c);
            }
            
            // Check for end of message
            if (c == '\0') {
                // Print complete message (excluding the null terminator)
                std::printf("[process] complete: \"");
                for (size_t i = 0; i < read_pos_; ++i) {
                    std::printf("%c", buffer_[i]);
                }
                std::printf("\"\n");
                
                // Reset buffer
                write_pos_ = 0;
                read_pos_ = 0;
            } else {
                co_await cr::delay_ms<cr::steady_clock>(200ms);
                ++read_pos_;
            }
        }
    }

    using reactor_tasks = cr::task_list<&processor::fast_buffer, &processor::process>;
};

// -----------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------

int main() {
    std::puts("=== channel example starting ===");

    uart_receiver rx;
    processor proc;

    auto eng = cr::make_engine<cr::default_config, cr::steady_clock>(
        cr::register_instance(rx),
        cr::register_instance(proc));

    // Start processor tasks first
    auto h = eng.template trigger<&processor::fast_buffer>(rx);
    if (h.error() != cr::error::ok) {
        std::fprintf(stderr, "fast_buffer trigger failed\n");
        return EXIT_FAILURE;
    }

    h = eng.template trigger<&processor::process>();
    if (h.error() != cr::error::ok) {
        std::fprintf(stderr, "process trigger failed\n");
        return EXIT_FAILURE;
    }

    // Start receiver
    h = eng.template trigger<&uart_receiver::receive_loop>();
    if (h.error() != cr::error::ok) {
        std::fprintf(stderr, "receive_loop trigger failed\n");
        return EXIT_FAILURE;
    }

    // Run long enough for everything to complete
    for (int i = 0; i < 100; ++i) {
        eng.tick();
        std::this_thread::sleep_for(50ms);
    }

    std::puts("=== channel example done ===");
    return EXIT_SUCCESS;
}

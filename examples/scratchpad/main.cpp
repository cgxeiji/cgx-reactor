#include <cgx/reactor.hpp>

#include <array>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Config: 2080B scratchpad pool.
// -----------------------------------------------------------------------

struct demo_config : cgx::reactor::default_config {
    static constexpr std::size_t scratchpad_pool_size = 2080;
    static constexpr cgx::reactor::log_level min_level = cgx::reactor::log_level::debug;
};

// -----------------------------------------------------------------------
// Logger.
// -----------------------------------------------------------------------

static auto g_start = cgx::reactor::steady_clock::now();

struct timed_logger {
    static void print(const char* msg) noexcept {
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      cgx::reactor::steady_clock::now() - g_start).count();
        std::printf("t=%4lldms [log] %s\n", (long long)ms, msg);
    }
};

static void say(const char* fmt, ...) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  cgx::reactor::steady_clock::now() - g_start).count();
    std::printf("t=%4lldms  ", (long long)ms);
    va_list a; va_start(a, fmt); std::vprintf(fmt, a); va_end(a);
    std::printf("\n");
}

// -----------------------------------------------------------------------
// Signal handling.
// -----------------------------------------------------------------------

static volatile std::sig_atomic_t g_shutdown = 0;

static void on_sigint(int) {
    g_shutdown = 1;
}

// -----------------------------------------------------------------------
// Four scratchpad tasks.
//
// Golden example:
//   Pool: 2080B
//   A(900B, 1s) + B(900B, 2s) = 1800B → 280B free
//   C(2000B, 3s) → doesn't fit (needs 2000B, only 280B free), waits
//   D( 56B, 1s)  → WOULD fit (280B > 56B), but waits behind C (FIFO)
//
//   A finishes → 1180B free → C still doesn't fit (2000 > 1180), D still waits
//   B finishes → 2080B free → C fits! C allocated. D fits! D allocated.
//
//   A + B + D = 1856 ≤ 2080 (D fits with A+B)
//   C + D = 2056 > 2080 (D doesn't fit with C — C uses almost all pool)
// -----------------------------------------------------------------------

class coordinator {
public:
    // A: ~900B frame, 1s timer
    cgx::reactor::task task_a() {
        std::array<char, 948> b{}; b.fill(0);
        say("A: start (1s)");
        co_await cgx::reactor::delay_ms<cgx::reactor::steady_clock>(1000ms);
        (void)b[0]; say("A: done"); co_return;
    }
    // B: ~1000B frame, 2s timer
    cgx::reactor::task task_b() {
        std::array<char, 948> b{}; b.fill(0);
        say("B: start (2s)");
        co_await cgx::reactor::delay_ms<cgx::reactor::steady_clock>(2000ms);
        (void)b[0]; say("B: done"); co_return;
    }
    // C: ~2000B frame, 3s timer
    cgx::reactor::task task_c() {
        std::array<char, 1944> b{}; b.fill(0);
        say("C: start (3s) — large");
        co_await cgx::reactor::delay_ms<cgx::reactor::steady_clock>(3000ms);
        (void)b[0]; say("C: done"); co_return;
    }
    // D: ~56B frame (coroutine overhead), 1s timer
    cgx::reactor::task task_d() {
        say("D: start (1s) — tiny");
        co_await cgx::reactor::delay_ms<cgx::reactor::steady_clock>(1000ms);
        say("D: done"); co_return;
    }

    using reactor_tasks = cgx::reactor::task_list<
        cgx::reactor::scratch_v<&coordinator::task_a>,
        cgx::reactor::scratch_v<&coordinator::task_b>,
        cgx::reactor::scratch_v<&coordinator::task_c>,
        cgx::reactor::scratch_v<&coordinator::task_d>
    >;
};

// -----------------------------------------------------------------------
// Schedule coroutine — orchestrates triggering and waiting.
// Not registered as a task; manually created and resumed.
// -----------------------------------------------------------------------

template <typename E>
cgx::reactor::task schedule(E& eng) {
    say("Schedule: starting...\n");

    // Step 1: Try-trigger A and B (should succeed, pool enough for both).
    auto ha = eng.template try_trigger<&coordinator::task_a>();
    say("  A: try_trigger -> %s", cgx::reactor::to_string(ha.error()).data());

    auto hb = eng.template try_trigger<&coordinator::task_b>();
    say("  B: try_trigger -> %s", cgx::reactor::to_string(hb.error()).data());

    say("");

    // Step 2: Trigger C using blocking trigger (waits in scratchpad FIFO list).
    say("  C: co_await trigger (blocks until pool space opens)...");
    auto hc = co_await eng.template trigger<&coordinator::task_c>();
    say("  C: trigger done -> %s", cgx::reactor::to_string(hc.error()).data());

    // Step 3: Trigger D using blocking trigger (waits behind C due to FIFO).
    say("  D: co_await trigger (blocks behind C)...");
    auto hd = co_await eng.template trigger<&coordinator::task_d>();
    say("  D: trigger done -> %s", cgx::reactor::to_string(hd.error()).data());

    say("");

    // Step 4: Wait for completion of all four tasks.
    say("  Waiting for completion...");
    co_await ha.done();   say("  A: completed");
    co_await hb.done();   say("  B: completed");
    co_await hc.done();  say("  C: completed");
    co_await hd.done();  say("  D: completed");

    say("");
    say("Schedule: all tasks complete! Shutting down.");
    g_shutdown = 1;
}

// -----------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------

int main() {
    struct sigaction sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_sigint;
    sigaction(SIGINT, &sa, nullptr);
    g_start = cgx::reactor::steady_clock::now();

    std::puts("\n=== Scratchpad — FIFO Ordering with task_handle ===\n");
    std::puts("Pool: 2080B shared among 4 tasks.\n");
    std::puts("  A(1000B, 1s) + B(1000B, 2s) = 2000B → 80B free");
    std::puts("  C(2000B, 3s) → doesn't fit (needs 2000B, only 80B free), waits");
    std::puts("  D( 56B, 1s)  → WOULD fit (80B > 56B), but waits behind C (FIFO)\n");
    std::puts("  A done → 1080B free → C still doesn't fit (2000 > 1080), D waits");
    std::puts("  B done → 2080B free → C fits! C allocated. D fits! D allocated.\n");
    std::puts("Key: A + B + D = 2056 < 2080 (D fits alongside A+B)");
    std::puts("     But FIFO forces D to wait behind C.\n");

    coordinator coord;
    auto eng = cgx::reactor::make_engine<demo_config, cgx::reactor::steady_clock, timed_logger>(
        cgx::reactor::register_instance(coord));

    auto dump = [&] {
        std::vector<std::string> l;
        eng.dump([&](std::string_view s) { l.push_back(std::string(s)); });
        for (auto& s : l) std::printf("  %s\n", s.data());
    };

    say("Initial");
    dump();

    // Create and start the schedule coroutine.
    auto s = schedule(eng);
    s.handle().resume();

    say("\nEvent loop — press Ctrl+C to exit\n");

    while (!g_shutdown) {
        eng.tick();
        std::this_thread::sleep_for(1ms);
    }

    say("\nFinal");
    dump();
    say("Done\n");
    return EXIT_SUCCESS;
}

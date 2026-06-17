#include <cgx/reactor.hpp>

#include <array>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using namespace std::chrono_literals;
namespace cr = cgx::reactor;

// -----------------------------------------------------------------------
// Config: 2040B scratchpad pool.
// -----------------------------------------------------------------------

struct demo_config : cr::default_config {
    static constexpr std::size_t scratchpad_pool_size = 2080;
    static constexpr cr::log_level min_level = cr::log_level::debug;
};

// -----------------------------------------------------------------------
// Logger.
// -----------------------------------------------------------------------

static auto g_start = cr::steady_clock::now();

struct timed_logger {
    static void print(const char* msg) noexcept {
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      cr::steady_clock::now() - g_start).count();
        std::printf("t=%4lldms [log] %s\n", (long long)ms, msg);
    }
};

static void say(const char* fmt, ...) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  cr::steady_clock::now() - g_start).count();
    std::printf("t=%4lldms  ", (long long)ms);
    va_list a; va_start(a, fmt); std::vprintf(fmt, a); va_end(a);
    std::printf("\n");
}

// -----------------------------------------------------------------------
// Four scratchpad tasks.
//
// Golden example:
//   Pool: 2040B
//   A(900B, 1s) + B(900B, 2s) = 1800B → 240B free
//   C(2000B, 3s) → doesn't fit (needs 2000B, only 240B free), waits
//   D( 56B, 1s)  → WOULD fit (240B > 56B), but waits behind C (FIFO)
//
//   A finishes → 1140B free → C still doesn't fit (2000 > 1140), D still waits
//   B finishes → 2040B free → C fits! C allocated. D fits! D allocated.
//
//   A + B + D = 1856 ≤ 2040 (D fits with A+B)
//   C + D = 2056 > 2040 (D doesn't fit with C — C uses almost all pool)
// -----------------------------------------------------------------------

class coordinator {
public:
    // A: ~900B frame, 1s timer
    cr::task task_a() {
        std::array<char, 948> b{}; b.fill(0);
        say("A: start (1s)");
        co_await cr::delay_ms<cr::steady_clock>(1000ms);
        (void)b[0]; say("A: done"); co_return;
    }
    // B: ~1000B frame, 2s timer
    cr::task task_b() {
        std::array<char, 948> b{}; b.fill(0);
        say("B: start (2s)");
        co_await cr::delay_ms<cr::steady_clock>(2000ms);
        (void)b[0]; say("B: done"); co_return;
    }
    // C: ~2000B frame, 3s timer
    cr::task task_c() {
        std::array<char, 1944> b{}; b.fill(0);
        say("C: start (3s) — large");
        co_await cr::delay_ms<cr::steady_clock>(3000ms);
        (void)b[0]; say("C: done"); co_return;
    }
    // D: ~56B frame (coroutine overhead), 1s timer
    cr::task task_d() {
        say("D: start (1s) — tiny");
        co_await cr::delay_ms<cr::steady_clock>(1000ms);
        say("D: done"); co_return;
    }

    using reactor_tasks = cr::task_list<
        cr::scratch_v<&coordinator::task_a>,
        cr::scratch_v<&coordinator::task_b>,
        cr::scratch_v<&coordinator::task_c>,
        cr::scratch_v<&coordinator::task_d>
    >;
};

// -----------------------------------------------------------------------
// Driver helper.
// -----------------------------------------------------------------------

template <typename E, auto Fn>
cr::task driver(E& eng) {
    auto ec = co_await eng.template trigger<Fn>();
    say("  trigger: %s", cr::to_string(ec).data());
}

// -----------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------

int main() {
    g_start = cr::steady_clock::now();

    std::puts("\n=== Scratchpad — FIFO Ordering ===\n");
    std::puts("Pool: 2080B shared among 4 tasks.  Each driver co_awaits trigger().\n");
    std::puts("Drivers staggered 100ms apart.\n");
    std::puts("  A(1000B, 1s) + B(1000B, 2s) = 2000B → 80B free");
    std::puts("  C(2000B, 3s) → doesn't fit (needs 2000B, only 80B free), waits");
    std::puts("  D( 56B, 1s)  → WOULD fit (80B > 56B), but waits behind C (FIFO)\n");
    std::puts("  A done → 1080B free → C still doesn't fit (2000 > 1080), D waits");
    std::puts("  B done → 2080B free → C fits! C allocated. D fits! D allocated.\n");
    std::puts("Key: A + B + D = 2056 < 2080 (D fits alongside A+B)");
    std::puts("     But FIFO forces D to wait behind C.\n");

    coordinator coord;
    auto eng = cr::make_engine<demo_config, cr::steady_clock, timed_logger>(
        cr::register_instance(coord));

    auto dump = [&] {
        std::vector<std::string> l;
        eng.dump([&](std::string_view s) { l.push_back(std::string(s)); });
        for (auto& s : l) std::printf("  %s\n", s.data());
    };

    say("Initial");
    dump();

    auto da = driver<decltype(eng), &coordinator::task_a>(eng);
    auto db = driver<decltype(eng), &coordinator::task_b>(eng);
    auto dc = driver<decltype(eng), &coordinator::task_c>(eng);
    auto dd = driver<decltype(eng), &coordinator::task_d>(eng);

    // Staggered launch: A at t=0, B at t=100, C at t=200, D at t=300.
    da.handle().resume();
    say("A: triggered");
    std::this_thread::sleep_for(100ms);

    db.handle().resume();
    say("B: triggered");
    std::this_thread::sleep_for(100ms);

    say("Pool near full (~2000B used) — C blocks (needs ~2000B, ~80B free)");
    dc.handle().resume();
    std::this_thread::sleep_for(100ms);

    say("D would fit (~240B > ~56B), but waits behind C (FIFO)");
    dd.handle().resume();

    say("Event loop\n");

    for (int i = 0; i < 1000; ++i) {
        eng.tick();
        std::this_thread::sleep_for(10ms);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      cr::steady_clock::now() - g_start).count();
        if (ms >= 8000) break;
    }

    say("Final");
    dump();
    say("Done\n");
    return EXIT_SUCCESS;
}

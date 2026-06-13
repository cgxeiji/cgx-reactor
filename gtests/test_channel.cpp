#include <cgx/reactor.hpp>
#include <mock_clock.hpp>

#include <gtest/gtest.h>

#include <coroutine>

namespace {

namespace cr = cgx::reactor;
using cgx::reactor::operator""_tag;

using namespace std::chrono_literals;

// -----------------------------------------------------------------------
// Templated helper tasks — Capacity is part of the function signature,
// so each <N> is a separate function pointer that can be passed as NTTP.
// -----------------------------------------------------------------------

template <std::size_t Cap>
cr::task push_one(cr::channel<int, Cap>& ch, int val, cr::error& out) {
    out = co_await ch.push(val);
    co_return;
}

template <std::size_t Cap>
cr::task pop_one(cr::channel<int, Cap>& ch, std::optional<int>& out) {
    out = co_await ch.pop();
    co_return;
}

template <std::size_t Cap>
cr::task push_many(cr::channel<int, Cap>& ch, int start, int n, cr::error* out) {
    for (int i = 0; i < n; ++i)
        out[i] = co_await ch.push(start + i);
    co_return;
}

template <std::size_t Cap>
cr::task pop_many(cr::channel<int, Cap>& ch, int n, std::optional<int>* out) {
    for (int i = 0; i < n; ++i)
        out[i] = co_await ch.pop();
    co_return;
}

template <std::size_t Cap>
cr::task ping_pong_producer(cr::channel<int, Cap>& ch, int& out) {
    auto ec = co_await ch.push(1);
    EXPECT_EQ(ec, cr::error::ok);
    auto val = co_await ch.pop();
    out = val.value_or(-1);
    co_return;
}

template <std::size_t Cap>
cr::task ping_pong_consumer(cr::channel<int, Cap>& ch, int& out) {
    auto val = co_await ch.pop();
    out = val.value_or(-1);
    auto ec = co_await ch.push(2);
    EXPECT_EQ(ec, cr::error::ok);
    co_return;
}

template <std::size_t Cap>
cr::task pop_on_closed_channel(cr::channel<int, Cap>& ch, std::optional<int>& out) {
    out = co_await ch.pop();
    co_return;
}

// -----------------------------------------------------------------------
// Test 1 — push with no waiting consumer, then pop
// -----------------------------------------------------------------------

TEST(ChannelTest, PushThenPop) {
    cr::channel<int, 4> ch;
    cr::error push_ec = cr::error::ok;
    std::optional<int> pop_val;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"PSH1"_tag, &push_one<4>>(),
        cr::register_task<"POP1"_tag, &pop_one<4>>());

    // Push a value — buffer has space, immediate.
    auto ec = eng.template trigger<&push_one<4>>(ch, 42, push_ec);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(push_ec, cr::error::ok);

    // Pop the value — buffer has data, immediate.
    ec = eng.template trigger<&pop_one<4>>(ch, pop_val);
    ASSERT_EQ(ec, cr::error::ok);
    ASSERT_TRUE(pop_val.has_value());
    EXPECT_EQ(*pop_val, 42);
}

// -----------------------------------------------------------------------
// Test 2 — buffer fills, pop drains
// -----------------------------------------------------------------------

TEST(ChannelTest, BufferFillAndDrain) {
    cr::channel<int, 3> ch;
    cr::error push_ec[3] = {};
    std::optional<int> pop_vals[3] = {};

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"PSH2"_tag, &push_many<3>>(),
        cr::register_task<"POP2"_tag, &pop_many<3>>());

    // Fill buffer with 3 values.
    auto ec = eng.template trigger<&push_many<3>>(ch, 10, 3, push_ec);
    ASSERT_EQ(ec, cr::error::ok);
    for (int i = 0; i < 3; ++i)
        EXPECT_EQ(push_ec[i], cr::error::ok) << "i=" << i;

    // Drain — all 3 should be available.
    ec = eng.template trigger<&pop_many<3>>(ch, 3, pop_vals);
    ASSERT_EQ(ec, cr::error::ok);
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(pop_vals[i].has_value()) << "i=" << i;
        EXPECT_EQ(*pop_vals[i], 10 + i) << "i=" << i;
    }
}

// -----------------------------------------------------------------------
// Test 3 — blocking push (buffer full) resolved by consumer pop
// -----------------------------------------------------------------------

TEST(ChannelTest, BlockingPushResolvedByPop) {
    cr::channel<int, 1> ch;
    cr::error push_ec = cr::error::ok;
    std::optional<int> pop_val;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"PSH3"_tag, &push_one<1>>(),
        cr::register_task<"POP3"_tag, &pop_one<1>>());

    // Fill the single slot.
    auto ec = eng.template trigger<&push_one<1>>(ch, 10, push_ec);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(push_ec, cr::error::ok);

    // Push again — buffer full, producer suspends.
    ec = eng.template trigger<&push_one<1>>(ch, 20, push_ec);
    ASSERT_EQ(ec, cr::error::ok);

    // Pop — takes 10, then the producer's 20 moves into the buffer and
    // the producer is resumed (push_ec set to ok).
    ec = eng.template trigger<&pop_one<1>>(ch, pop_val);
    ASSERT_EQ(ec, cr::error::ok);
    ASSERT_TRUE(pop_val.has_value());
    EXPECT_EQ(*pop_val, 10);
    // The producer's push_awaiter was resumed with ec = ok.
    EXPECT_EQ(push_ec, cr::error::ok);
}

// -----------------------------------------------------------------------
// Test 4 — blocking pop (empty buffer) resolved by producer push
// -----------------------------------------------------------------------

TEST(ChannelTest, BlockingPopResolvedByPush) {
    cr::channel<int, 1> ch;
    std::optional<int> pop_val;
    cr::error push_ec = cr::error::ok;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"POP4"_tag, &pop_one<1>>(),
        cr::register_task<"PSH4"_tag, &push_one<1>>());

    // Pop on empty buffer — consumer suspends.
    auto ec = eng.template trigger<&pop_one<1>>(ch, pop_val);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_FALSE(pop_val.has_value());

    // Push — the consumer is waiting, so the value is handed off directly.
    ec = eng.template trigger<&push_one<1>>(ch, 99, push_ec);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(push_ec, cr::error::ok);
    ASSERT_TRUE(pop_val.has_value());
    EXPECT_EQ(*pop_val, 99);
}

// -----------------------------------------------------------------------
// Test 5 — try_push returns capacity_exceeded when buffer full
// -----------------------------------------------------------------------

TEST(ChannelTest, TryPushFull) {
    cr::channel<int, 2> ch;

    EXPECT_EQ(ch.try_push(1), cr::error::ok);
    EXPECT_EQ(ch.try_push(2), cr::error::ok);
    EXPECT_EQ(ch.try_push(3), cr::error::capacity_exceeded);
    EXPECT_EQ(ch.size(), 2u);
}

// -----------------------------------------------------------------------
// Test 6 — try_push returns closed after close()
// -----------------------------------------------------------------------

TEST(ChannelTest, TryPushClosed) {
    cr::channel<int, 4> ch;

    ch.close();
    EXPECT_TRUE(ch.is_closed());
    EXPECT_EQ(ch.try_push(1), cr::error::closed);
}

// -----------------------------------------------------------------------
// Test 7 — close wakes waiting consumer with nullopt
// -----------------------------------------------------------------------

TEST(ChannelTest, CloseWakesConsumer) {
    cr::channel<int, 1> ch;
    std::optional<int> pop_val;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"POP7"_tag, &pop_one<1>>());

    // Pop on empty buffer — suspends.
    auto ec = eng.template trigger<&pop_one<1>>(ch, pop_val);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_FALSE(pop_val.has_value());

    // Close wakes the consumer with empty optional.
    ch.close();
    ASSERT_FALSE(pop_val.has_value());
}

// -----------------------------------------------------------------------
// Test 8 — close wakes waiting producer with error::closed
// -----------------------------------------------------------------------

TEST(ChannelTest, CloseWakesProducer) {
    cr::channel<int, 1> ch;
    cr::error push_ec = cr::error::ok;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"PSH8"_tag, &push_one<1>>());

    // Fill the single slot.
    ASSERT_EQ(ch.try_push(1), cr::error::ok);

    // Push on full buffer — suspends.
    auto ec = eng.template trigger<&push_one<1>>(ch, 2, push_ec);
    ASSERT_EQ(ec, cr::error::ok);

    // Close wakes the producer with closed error.
    ch.close();
    EXPECT_EQ(push_ec, cr::error::closed);
}

// -----------------------------------------------------------------------
// Test 9 — ping-pong: producer pushes, consumer pops, then both switch
// -----------------------------------------------------------------------

TEST(ChannelTest, PingPong) {
    cr::channel<int, 2> ch;
    int prod_val = 0;
    int cons_val = 0;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"PROD"_tag, &ping_pong_producer<2>>(),
        cr::register_task<"CONS"_tag, &ping_pong_consumer<2>>());

    // Trigger consumer FIRST so it is already waiting when the producer
    // pushes 1 (direct handoff).  Then consumer pops 1 and pushes 2 back.
    auto ec = eng.template trigger<&ping_pong_consumer<2>>(ch, cons_val);
    ASSERT_EQ(ec, cr::error::ok);

    // Trigger producer: pushes 1 (handoff to consumer), then pops
    // (receives 2 from consumer).
    ec = eng.template trigger<&ping_pong_producer<2>>(ch, prod_val);
    ASSERT_EQ(ec, cr::error::ok);

    EXPECT_EQ(cons_val, 1);
    EXPECT_EQ(prod_val, 2);
}

// -----------------------------------------------------------------------
// Test 10 — consumer pop on already-closed channel returns nullopt
// -----------------------------------------------------------------------

TEST(ChannelTest, PopOnClosedChannel) {
    cr::channel<int, 4> ch;
    ch.close();
    std::optional<int> result;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"PCLS"_tag, &pop_on_closed_channel<4>>());

    auto ec = eng.template trigger<&pop_on_closed_channel<4>>(ch, result);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_FALSE(result.has_value());
}

// -----------------------------------------------------------------------
// Test 11 — capacity() is constexpr, size() is runtime
// -----------------------------------------------------------------------

TEST(ChannelTest, CapacityAndSize) {
    cr::channel<int, 5> ch;
    static_assert(ch.capacity() == 5, "capacity should be constexpr");
    EXPECT_EQ(ch.size(), 0u);
    ch.try_push(10);
    EXPECT_EQ(ch.size(), 1u);
    ch.try_push(20);
    EXPECT_EQ(ch.size(), 2u);
}

// -----------------------------------------------------------------------
// Test 12 — push on already-closed channel returns closed (H1 fix)
// -----------------------------------------------------------------------

template <std::size_t Cap>
cr::task push_on_closed_simple(cr::channel<int, Cap>& ch, cr::error& out) {
    out = co_await ch.push(42);
    co_return;
}

TEST(ChannelTest, PushOnClosedChannel) {
    cr::channel<int, 4> ch;
    ch.close();
    cr::error push_ec = cr::error::ok;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"POCL"_tag, &push_on_closed_simple<4>>());

    auto ec = eng.template trigger<&push_on_closed_simple<4>>(ch, push_ec);
    ASSERT_EQ(ec, cr::error::ok);
    EXPECT_EQ(push_ec, cr::error::closed);
}

// Distinct push tasks for testing multiple waiters (same channel capacity,
// different function pointers so each has its own engine slot).

// Distinct push/pop tasks for testing multiple waiters (need different
// function pointers so each has its own engine slot).

cr::task push_a(cr::channel<int, 4>& ch, int val, cr::error& out) {
    out = co_await ch.push(val);
    co_return;
}
cr::task push_b(cr::channel<int, 4>& ch, int val, cr::error& out) {
    out = co_await ch.push(val);
    co_return;
}
cr::task push_c(cr::channel<int, 4>& ch, int val, cr::error& out) {
    out = co_await ch.push(val);
    co_return;
}

cr::task cons_a(cr::channel<int, 4>& ch, std::optional<int>& out) {
    out = co_await ch.pop();
    co_return;
}
cr::task cons_b(cr::channel<int, 4>& ch, std::optional<int>& out) {
    out = co_await ch.pop();
    co_return;
}
cr::task cons_c(cr::channel<int, 4>& ch, std::optional<int>& out) {
    out = co_await ch.pop();
    co_return;
}

// -----------------------------------------------------------------------
// Test 13 — multiple producers waiting on full buffer, drained in FIFO order
//
// Wait queues are sized to Capacity, so use Capacity=4 to support 3 waiters.
// -----------------------------------------------------------------------

TEST(ChannelTest, MultipleProducersFIFO) {
    cr::channel<int, 4> ch;
    cr::error ec1 = cr::error::ok;
    cr::error ec2 = cr::error::ok;
    cr::error ec3 = cr::error::ok;
    std::optional<int> v1, v2, v3, v4, v5, v6, v7;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"PA"_tag, &push_a>(),
        cr::register_task<"PB"_tag, &push_b>(),
        cr::register_task<"PC"_tag, &push_c>(),
        cr::register_task<"PQ"_tag, &pop_one<4>>());

    // Fill the buffer so pushes block.
    ASSERT_EQ(ch.try_push(0), cr::error::ok);
    ASSERT_EQ(ch.try_push(1), cr::error::ok);
    ASSERT_EQ(ch.try_push(2), cr::error::ok);
    ASSERT_EQ(ch.try_push(3), cr::error::ok);
    ASSERT_EQ(ch.size(), 4u);

    // Three producers queue up (buffer full).
    ASSERT_EQ(eng.template trigger<&push_a>(ch, 10, ec1), cr::error::ok);
    ASSERT_EQ(eng.template trigger<&push_b>(ch, 20, ec2), cr::error::ok);
    ASSERT_EQ(eng.template trigger<&push_c>(ch, 30, ec3), cr::error::ok);

    // Pops 1-4: drain the 4 initial values.  Each pop wakes one producer,
    // whose value moves into the buffer at the position just vacated.
    ASSERT_EQ(eng.template trigger<&pop_one<4>>(ch, v1), cr::error::ok);
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(*v1, 0);
    EXPECT_EQ(ec1, cr::error::ok);  // push_a succeeded

    ASSERT_EQ(eng.template trigger<&pop_one<4>>(ch, v2), cr::error::ok);
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(*v2, 1);
    EXPECT_EQ(ec2, cr::error::ok);  // push_b succeeded

    ASSERT_EQ(eng.template trigger<&pop_one<4>>(ch, v3), cr::error::ok);
    ASSERT_TRUE(v3.has_value());
    EXPECT_EQ(*v3, 2);
    EXPECT_EQ(ec3, cr::error::ok);  // push_c succeeded

    ASSERT_EQ(eng.template trigger<&pop_one<4>>(ch, v4), cr::error::ok);
    ASSERT_TRUE(v4.has_value());
    EXPECT_EQ(*v4, 3);

    // Pops 5-7: drain the three producer values now in the buffer.
    ASSERT_EQ(eng.template trigger<&pop_one<4>>(ch, v5), cr::error::ok);
    ASSERT_TRUE(v5.has_value());
    EXPECT_EQ(*v5, 10);

    ASSERT_EQ(eng.template trigger<&pop_one<4>>(ch, v6), cr::error::ok);
    ASSERT_TRUE(v6.has_value());
    EXPECT_EQ(*v6, 20);

    ASSERT_EQ(eng.template trigger<&pop_one<4>>(ch, v7), cr::error::ok);
    ASSERT_TRUE(v7.has_value());
    EXPECT_EQ(*v7, 30);
}

// -----------------------------------------------------------------------
// Test 14 — multiple consumers waiting on empty buffer, filled in FIFO order
// -----------------------------------------------------------------------

TEST(ChannelTest, MultipleConsumersFIFO) {
    cr::channel<int, 4> ch;
    std::optional<int> v1, v2, v3;
    cr::error ec1 = cr::error::ok, ec2 = cr::error::ok, ec3 = cr::error::ok;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"CA"_tag, &cons_a>(),
        cr::register_task<"CB"_tag, &cons_b>(),
        cr::register_task<"CC"_tag, &cons_c>(),
        cr::register_task<"P4A"_tag, &push_a>(),
        cr::register_task<"P4B"_tag, &push_b>(),
        cr::register_task<"P4C"_tag, &push_c>());

    // Three consumers queue up (buffer empty).
    ASSERT_EQ(eng.template trigger<&cons_a>(ch, v1), cr::error::ok);
    ASSERT_EQ(eng.template trigger<&cons_b>(ch, v2), cr::error::ok);
    ASSERT_EQ(eng.template trigger<&cons_c>(ch, v3), cr::error::ok);

    // Push values — each wakes the next consumer in FIFO order.
    ASSERT_EQ(eng.template trigger<&push_a>(ch, 100, ec1), cr::error::ok);
    ASSERT_TRUE(v1.has_value());
    EXPECT_EQ(*v1, 100);

    ASSERT_EQ(eng.template trigger<&push_b>(ch, 200, ec2), cr::error::ok);
    ASSERT_TRUE(v2.has_value());
    EXPECT_EQ(*v2, 200);

    ASSERT_EQ(eng.template trigger<&push_c>(ch, 300, ec3), cr::error::ok);
    ASSERT_TRUE(v3.has_value());
    EXPECT_EQ(*v3, 300);
}

// -----------------------------------------------------------------------
// Test 15 — const channel& can call pop() (mirrors Go's <-chan T pattern)
// -----------------------------------------------------------------------

template <std::size_t Cap>
cr::task pop_one_const(const cr::channel<int, Cap>& ch,
                       std::optional<int>& out) {
    out = co_await ch.pop();
    co_return;
}

TEST(ChannelTest, ConstChannelCanPop) {
    cr::channel<int, 4> ch;
    std::optional<int> val;
    cr::error ec;

    auto eng = cr::make_engine<cr::default_config, cr::test::mock_clock>(
        cr::register_task<"CPUS"_tag, &push_one<4>>(),
        cr::register_task<"CPOP"_tag, &pop_one_const<4>>());

    // Push a value into the channel first.
    ASSERT_EQ(eng.template trigger<&push_one<4>>(ch, 42, ec), cr::error::ok);
    ASSERT_EQ(ec, cr::error::ok);

    // Pop through a const reference — verifies pop() is const-qualified.
    const auto& cref = ch;
    ASSERT_EQ(eng.template trigger<&pop_one_const<4>>(cref, val),
              cr::error::ok);
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, 42);
}

}  // anonymous namespace

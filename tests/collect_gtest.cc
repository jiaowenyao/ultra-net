#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <vector>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include "ultranet/collect/collect.hpp"
#include "ultranet/coroutine/execution_context.hpp"
#include "ultranet/coroutine/thread_pool.hpp"

using namespace ynet::async;
using namespace ynet::async::collect;
using namespace ynet::async::scheduling;

namespace {

struct payload {
    std::vector<uint8_t> bytes;
};

struct seen_batch {
    std::vector<payload> records;
};

struct tcp_state {
    int fd = -1;
    collect_config cfg;
    collect_result result;
    std::vector<seen_batch> batches;
    int calls = 0;
    bool sink_ok = true;
};

struct udp_state {
    int fd = -1;
    collect_config cfg;
    collect_result result;
    std::vector<payload> records;
    int peer_len = 0;
    bool sink_ok = true;
};

struct emit_state {
    int fd = -1;
    collect_config cfg;
    const record_span* records = nullptr;
    size_t count = 0;
    emit_result result;
    bool close_after = false;
};

collect_config long_wait() {
    collect_config cfg;
    cfg.batch_delay = std::chrono::hours(1);
    return cfg;
}

bool write_frame(int fd, const void* data, size_t n) {
    uint32_t len = static_cast<uint32_t>(n);
    if (::write(fd, &len, sizeof(len)) != static_cast<ssize_t>(sizeof(len))) {
        return false;
    }
    if (n > 0 && ::write(fd, data, n) != static_cast<ssize_t>(n)) {
        return false;
    }
    return true;
}

void copy_batch(const record_batch& batch, seen_batch& one) {
    for (size_t i = 0; i < batch.size(); ++i) {
        auto span = batch.at(i);
        one.records.push_back(payload{
            std::vector<uint8_t>(span.data, span.data + span.size)});
    }
}

Task<void> run_tcp(tcp_state& st) {
    st.result = co_await collect_tcp(st.fd, st.cfg, [&st](const record_batch& batch) {
        st.calls += 1;
        seen_batch one;
        copy_batch(batch, one);
        st.batches.push_back(std::move(one));
        return st.sink_ok;
    });
}

Task<void> run_udp(udp_state& st) {
    st.result = co_await collect_udp(st.fd, st.cfg, [&st](const record_batch& batch) {
        for (size_t i = 0; i < batch.size(); ++i) {
            auto span = batch.at(i);
            st.peer_len = static_cast<int>(span.peer_len);
            st.records.push_back(payload{
                std::vector<uint8_t>(span.data, span.data + span.size)});
        }
        return st.sink_ok;
    });
}

Task<void> run_emit_tcp(emit_state& st) {
    st.result = co_await emit_tcp(st.fd, st.cfg, st.records, st.count);
    if (st.close_after) {
        ::close(st.fd);
        st.fd = -1;
    }
}

Task<void> run_emit_udp(emit_state& st) {
    st.result = co_await emit_udp(st.fd, st.cfg, st.records, st.count);
}

int udp_bound(sockaddr_in& addr) {
    int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        return -1;
    }
    addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

} // namespace

TEST(CollectTest, TcpCountFlushesRemainderOnClose) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);

    tcp_state st;
    st.fd = sv[0];
    st.cfg = long_wait();
    st.cfg.batch_records = 2;
    pool.submit(run_tcp(st).release());

    const char* a = "aaaa";
    const char* b = "bbbb";
    const char* c = "cccc";
    ASSERT_TRUE(write_frame(sv[1], a, 4));
    ASSERT_TRUE(write_frame(sv[1], b, 4));
    ASSERT_TRUE(write_frame(sv[1], c, 4));
    ::close(sv[1]);
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    ::close(sv[0]);

    ASSERT_EQ(st.result.err, 0);
    EXPECT_EQ(st.result.stats.records, 3u);
    EXPECT_EQ(st.result.stats.batches, 2u);
    ASSERT_EQ(st.batches.size(), 2u);
    ASSERT_EQ(st.batches[0].records.size(), 2u);
    ASSERT_EQ(st.batches[1].records.size(), 1u);
    EXPECT_EQ(st.batches[0].records[0].bytes, std::vector<uint8_t>(a, a + 4));
    EXPECT_EQ(st.batches[0].records[1].bytes, std::vector<uint8_t>(b, b + 4));
    EXPECT_EQ(st.batches[1].records[0].bytes, std::vector<uint8_t>(c, c + 4));
}

TEST(CollectTest, TcpRecordLargerThanBatchBytesIsOwnBatch) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);

    tcp_state st;
    st.fd = sv[0];
    st.cfg = long_wait();
    st.cfg.batch_bytes = 4;
    st.cfg.max_record_bytes = 64;
    pool.submit(run_tcp(st).release());

    std::vector<uint8_t> body(8, 7);
    ASSERT_TRUE(write_frame(sv[1], body.data(), body.size()));
    ::close(sv[1]);
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    ::close(sv[0]);
    EXPECT_EQ(st.result.err, 0);
    EXPECT_EQ(st.result.stats.batches, 1u);
    ASSERT_EQ(st.batches.size(), 1u);
    EXPECT_EQ(st.batches[0].records.size(), 1u);
}

TEST(CollectTest, TcpDelayFlushesWithoutClose) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);

    tcp_state st;
    st.fd = sv[0];
    st.cfg.batch_records = 100;
    st.cfg.batch_bytes = 1024 * 1024;
    st.cfg.batch_delay = std::chrono::milliseconds(20);
    st.sink_ok = false;
    pool.submit(run_tcp(st).release());

    const char body[4] = {1, 2, 3, 4};
    ASSERT_TRUE(write_frame(sv[1], body, 4));
    ASSERT_TRUE(pool.wait_all_for(std::chrono::milliseconds(200)));
    ::close(sv[0]);
    ::close(sv[1]);
    EXPECT_EQ(st.calls, 1);
    EXPECT_EQ(st.result.err, 0);
}

TEST(CollectTest, TcpOversizeLengthStopsAfterCompleteRecord) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);

    tcp_state st;
    st.fd = sv[0];
    st.cfg = long_wait();
    st.cfg.max_record_bytes = 8;
    pool.submit(run_tcp(st).release());

    const char body[4] = {9, 9, 9, 9};
    ASSERT_TRUE(write_frame(sv[1], body, 4));
    uint32_t big = 9;
    ASSERT_EQ(::write(sv[1], &big, sizeof(big)), static_cast<ssize_t>(sizeof(big)));
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    ::close(sv[0]);
    ::close(sv[1]);
    EXPECT_EQ(st.result.err, EMSGSIZE);
    EXPECT_EQ(st.result.stats.records, 1u);
    ASSERT_EQ(st.batches.size(), 1u);
    EXPECT_EQ(st.batches[0].records.size(), 1u);
}

TEST(CollectTest, UdpTwoDatagrams) {
    sockaddr_in recv_addr{};
    int recv_fd = udp_bound(recv_addr);
    ASSERT_GE(recv_fd, 0);
    int send_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(send_fd, 0);
    WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);

    udp_state st;
    st.fd = recv_fd;
    st.cfg = long_wait();
    st.cfg.batch_records = 2;
    st.sink_ok = false;
    pool.submit(run_udp(st).release());

    const char a[4] = {1, 2, 3, 4};
    const char b[4] = {5, 6, 7, 8};
    ASSERT_EQ(::sendto(send_fd, a, 4, 0, reinterpret_cast<sockaddr*>(&recv_addr), sizeof(recv_addr)), 4);
    ASSERT_EQ(::sendto(send_fd, b, 4, 0, reinterpret_cast<sockaddr*>(&recv_addr), sizeof(recv_addr)), 4);
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    ::close(recv_fd);
    ::close(send_fd);
    EXPECT_EQ(st.result.err, 0);
    ASSERT_EQ(st.records.size(), 2u);
    EXPECT_GT(st.peer_len, 0);
    EXPECT_EQ(st.records[0].bytes, std::vector<uint8_t>(a, a + 4));
    EXPECT_EQ(st.records[1].bytes, std::vector<uint8_t>(b, b + 4));
}

TEST(CollectTest, UdpOversizeThenSmall) {
    sockaddr_in recv_addr{};
    int recv_fd = udp_bound(recv_addr);
    ASSERT_GE(recv_fd, 0);
    int send_fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    ASSERT_GE(send_fd, 0);
    WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);

    udp_state st;
    st.fd = recv_fd;
    st.cfg = long_wait();
    st.cfg.batch_records = 1;
    st.cfg.max_record_bytes = 8;
    st.sink_ok = false;
    pool.submit(run_udp(st).release());

    std::vector<uint8_t> big(32, 1);
    const char small[4] = {4, 3, 2, 1};
    ASSERT_EQ(::sendto(send_fd, big.data(), big.size(), 0,
                       reinterpret_cast<sockaddr*>(&recv_addr), sizeof(recv_addr)),
              static_cast<ssize_t>(big.size()));
    ASSERT_EQ(::sendto(send_fd, small, 4, 0,
                       reinterpret_cast<sockaddr*>(&recv_addr), sizeof(recv_addr)),
              4);
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    ::close(recv_fd);
    ::close(send_fd);
    EXPECT_EQ(st.result.stats.dropped_oversize, 1u);
    ASSERT_EQ(st.records.size(), 1u);
    EXPECT_EQ(st.records[0].bytes, std::vector<uint8_t>(small, small + 4));
}

TEST(CollectTest, SinkFalseStops) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    WorkStealingThreadPool pool(1);
    ExecutionContext::Scope scope(&pool);

    tcp_state st;
    st.fd = sv[0];
    st.cfg = long_wait();
    st.cfg.batch_records = 1;
    st.sink_ok = false;
    pool.submit(run_tcp(st).release());

    const char a[4] = {1, 1, 1, 1};
    const char b[4] = {2, 2, 2, 2};
    ASSERT_TRUE(write_frame(sv[1], a, 4));
    ASSERT_TRUE(write_frame(sv[1], b, 4));
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    ::close(sv[0]);
    ::close(sv[1]);
    EXPECT_EQ(st.calls, 1);
    EXPECT_EQ(st.result.err, 0);
    EXPECT_EQ(st.result.stats.records, 1u);
}

TEST(CollectGuide, SocketPairRoundTrip) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);

    const char a[4] = {'w', 'x', 'y', 'z'};
    const char b[4] = {1, 2, 3, 4};
    const char c[4] = {5, 6, 7, 8};
    record_span records[3]{};
    records[0] = record_span{reinterpret_cast<const uint8_t*>(a), 4, nullptr, 0};
    records[1] = record_span{reinterpret_cast<const uint8_t*>(b), 4, nullptr, 0};
    records[2] = record_span{reinterpret_cast<const uint8_t*>(c), 4, nullptr, 0};

    emit_state send;
    send.fd = sv[0];
    send.cfg = long_wait();
    send.cfg.batch_records = 2;
    send.records = records;
    send.count = 3;
    send.close_after = true;

    tcp_state recv;
    recv.fd = sv[1];
    recv.cfg = send.cfg;
    pool.submit(run_emit_tcp(send).release());
    pool.submit(run_tcp(recv).release());
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    if (send.fd >= 0) {
        ::close(send.fd);
    }
    ::close(sv[1]);

    EXPECT_EQ(send.result.records, 3u);
    EXPECT_EQ(send.result.err, 0);
    EXPECT_EQ(recv.result.stats.records, 3u);
    EXPECT_EQ(recv.result.err, 0);
    ASSERT_EQ(recv.batches.size(), 2u);
    ASSERT_EQ(recv.batches[0].records.size(), 2u);
    ASSERT_EQ(recv.batches[1].records.size(), 1u);
    EXPECT_EQ(recv.batches[0].records[0].bytes, std::vector<uint8_t>(a, a + 4));
    EXPECT_EQ(recv.batches[0].records[1].bytes, std::vector<uint8_t>(b, b + 4));
    EXPECT_EQ(recv.batches[1].records[0].bytes, std::vector<uint8_t>(c, c + 4));
}

TEST(CollectTest, EmitStopsOnOversizeRecord) {
    int sv[2] = {-1, -1};
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
    WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);

    const char ok[4] = {1, 2, 3, 4};
    std::vector<uint8_t> huge(32, 9);
    record_span records[2]{};
    records[0] = record_span{reinterpret_cast<const uint8_t*>(ok), 4, nullptr, 0};
    records[1] = record_span{huge.data(), huge.size(), nullptr, 0};

    emit_state send;
    send.fd = sv[0];
    send.cfg = long_wait();
    send.cfg.max_record_bytes = 8;
    send.records = records;
    send.count = 2;
    send.close_after = true;

    tcp_state recv;
    recv.fd = sv[1];
    recv.cfg = send.cfg;
    pool.submit(run_emit_tcp(send).release());
    pool.submit(run_tcp(recv).release());
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    if (send.fd >= 0) {
        ::close(send.fd);
    }
    ::close(sv[1]);
    EXPECT_EQ(send.result.records, 1u);
    EXPECT_EQ(send.result.err, EMSGSIZE);
    EXPECT_EQ(recv.result.err, 0);
    ASSERT_EQ(recv.batches.size(), 1u);
    ASSERT_EQ(recv.batches[0].records.size(), 1u);
    EXPECT_EQ(recv.batches[0].records[0].bytes, std::vector<uint8_t>(ok, ok + 4));
}

TEST(CollectTest, EmitUdpRoundTrip) {
    sockaddr_in recv_addr{};
    int recv_fd = udp_bound(recv_addr);
    ASSERT_GE(recv_fd, 0);
    sockaddr_in send_addr{};
    int send_fd = udp_bound(send_addr);
    ASSERT_GE(send_fd, 0);
    WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);

    const char a[4] = {8, 7, 6, 5};
    const char b[4] = {4, 3, 2, 1};
    record_span records[2]{};
    records[0] = record_span{reinterpret_cast<const uint8_t*>(a), 4,
                             reinterpret_cast<sockaddr*>(&recv_addr), sizeof(recv_addr)};
    records[1] = record_span{reinterpret_cast<const uint8_t*>(b), 4,
                             reinterpret_cast<sockaddr*>(&recv_addr), sizeof(recv_addr)};

    emit_state send;
    send.fd = send_fd;
    send.cfg = long_wait();
    send.cfg.batch_records = 2;
    send.records = records;
    send.count = 2;

    udp_state recv;
    recv.fd = recv_fd;
    recv.cfg = send.cfg;
    recv.sink_ok = false;
    pool.submit(run_udp(recv).release());
    pool.submit(run_emit_udp(send).release());
    ASSERT_TRUE(pool.wait_all_for(std::chrono::seconds(2)));
    ::close(recv_fd);
    ::close(send_fd);
    EXPECT_EQ(send.result.err, 0);
    EXPECT_EQ(send.result.records, 2u);
    EXPECT_EQ(recv.result.err, 0);
    ASSERT_EQ(recv.records.size(), 2u);
    EXPECT_GT(recv.peer_len, 0);
    EXPECT_EQ(recv.records[0].bytes, std::vector<uint8_t>(a, a + 4));
    EXPECT_EQ(recv.records[1].bytes, std::vector<uint8_t>(b, b + 4));
}

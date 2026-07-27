// RecvMultishotAwaiter 单元测试
//
// 测试 CQE handler → state → awaiter 的完整链路，
// 不依赖实际的 io_uring（纯逻辑测试）。

#include <gtest/gtest.h>
#include "ultranet/io/recv_multishot.hpp"

using namespace ynet::async::io;

// ── Handler 测试 ───────────────────────────────────────────────────────

// handler 在无等待者时正常存储数据
TEST(RecvMultishotTest, HandlerStoresChunkWhenNoWaiter)
{
    RecvMultishotState state;
    on_recv_chunk_handler(&state, 512, 0x00010000);  // buffer_id=1, res=512

    EXPECT_TRUE(state.chunk_ready);
    EXPECT_FALSE(state.stopped);
    EXPECT_EQ(state.last_res, 512);
    EXPECT_EQ(state.last_flags, 0x00010000u);
}

// handler 在有等待者时恢复协程并清除 waiter
TEST(RecvMultishotTest, HandlerResumesWaiter)
{
    RecvMultishotState state;

    // 使用 std::noop_coroutine() 得到合法的空操作协程句柄，resume() 安全无副作用
    state.waiter = std::noop_coroutine();
    on_recv_chunk_handler(&state, 256, 0x00020000);

    // waiter 应该被清除（恢复后置空）
    EXPECT_EQ(state.waiter, nullptr);
    EXPECT_TRUE(state.chunk_ready);
}

// EOF (res=0) 设置 stopped
TEST(RecvMultishotTest, EofSetsStopped)
{
    RecvMultishotState state;
    on_recv_chunk_handler(&state, 0, 0);

    EXPECT_TRUE(state.chunk_ready);
    EXPECT_TRUE(state.stopped);
}

// 错误 (res<0) 设置 stopped
TEST(RecvMultishotTest, ErrorSetsStopped)
{
    RecvMultishotState state;
    on_recv_chunk_handler(&state, -ECONNRESET, 0);

    EXPECT_TRUE(state.chunk_ready);
    EXPECT_TRUE(state.stopped);
}

// ── RecvChunkResult 测试 ───────────────────────────────────────────────

// buffer_id 从 flags 高位提取
TEST(RecvChunkResultTest, BufferIdExtraction)
{
    RecvChunkResult r{512, 0x00030000};  // buffer_id=3
    EXPECT_EQ(r.buffer_id(), 3u);
    EXPECT_FALSE(r.is_eof());
    EXPECT_FALSE(r.is_error());
}

// buffer_id=0 边界值
TEST(RecvChunkResultTest, BufferIdZero)
{
    RecvChunkResult r{128, 0x00000000};
    EXPECT_EQ(r.buffer_id(), 0u);
}

// buffer_id 最大值 (0xFFFF)
TEST(RecvChunkResultTest, BufferIdMax)
{
    RecvChunkResult r{64, 0xFFFF0000};
    EXPECT_EQ(r.buffer_id(), 0xFFFFu);
}

// EOF 检测
TEST(RecvChunkResultTest, EofDetection)
{
    RecvChunkResult r{0, 0};
    EXPECT_TRUE(r.is_eof());
    EXPECT_FALSE(r.is_error());
}

// 错误检测
TEST(RecvChunkResultTest, ErrorDetection)
{
    RecvChunkResult r{-ECONNREFUSED, 0};
    EXPECT_FALSE(r.is_eof());
    EXPECT_TRUE(r.is_error());
}

// ── RecvMultishotAwaiter 测试 ──────────────────────────────────────────

// await_ready 在数据就绪时返回 true
TEST(RecvMultishotAwaiterTest, AwaitReadyWhenChunkAvailable)
{
    RecvMultishotState state;
    state.chunk_ready = true;

    RecvMultishotAwaiter awaiter{&state};
    EXPECT_TRUE(awaiter.await_ready());
}

// await_ready 在 stopped 时返回 true
TEST(RecvMultishotAwaiterTest, AwaitReadyWhenStopped)
{
    RecvMultishotState state;
    state.stopped = true;

    RecvMultishotAwaiter awaiter{&state};
    EXPECT_TRUE(awaiter.await_ready());
}

// await_ready 在无数据时返回 false
TEST(RecvMultishotAwaiterTest, AwaitNotReadyInitially)
{
    RecvMultishotState state;

    RecvMultishotAwaiter awaiter{&state};
    EXPECT_FALSE(awaiter.await_ready());
}

// await_suspend 存储协程句柄
TEST(RecvMultishotAwaiterTest, AwaitSuspendStoresHandle)
{
    RecvMultishotState state;
    RecvMultishotAwaiter awaiter{&state};

    auto handle = std::noop_coroutine();
    awaiter.await_suspend(handle);

    EXPECT_EQ(state.waiter, handle);
}

// await_resume 返回正确结果并清除 ready 标志
TEST(RecvMultishotAwaiterTest, AwaitResumeReturnsAndClears)
{
    RecvMultishotState state;
    state.last_res = 1024;
    state.last_flags = 0x00050000;
    state.chunk_ready = true;

    RecvMultishotAwaiter awaiter{&state};
    auto result = awaiter.await_resume();

    EXPECT_EQ(result.res, 1024);
    EXPECT_EQ(result.buffer_id(), 5u);
    EXPECT_EQ(state.chunk_ready, false);  // 标志已被清除
}

// ── 完整流程模拟测试 ───────────────────────────────────────────────────

// 模拟 "handler 先到达 → 协程后 await" 的流程
TEST(RecvMultishotIntegrationTest, HandlerBeforeAwait)
{
    RecvMultishotState state;

    // 步骤1：CQE 先到达
    on_recv_chunk_handler(&state, 512, 0x00010000);

    // 步骤2：协程 await（直接获取数据，无需暂停）
    RecvMultishotAwaiter awaiter{&state};
    EXPECT_TRUE(awaiter.await_ready());

    auto chunk = awaiter.await_resume();
    EXPECT_EQ(chunk.res, 512);
    EXPECT_EQ(chunk.buffer_id(), 1u);

    // 步骤3：下一个 await 需要等待（数据已消费）
    RecvMultishotAwaiter awaiter2{&state};
    EXPECT_FALSE(awaiter2.await_ready());
}

// 模拟 "EOF 处理" 流程
TEST(RecvMultishotIntegrationTest, EofFlow)
{
    RecvMultishotState state;
    on_recv_chunk_handler(&state, 0, 0);

    RecvMultishotAwaiter awaiter{&state};
    EXPECT_TRUE(awaiter.await_ready());

    auto chunk = awaiter.await_resume();
    EXPECT_TRUE(chunk.is_eof());
    EXPECT_TRUE(state.stopped);
}

// 模拟 "错误处理" 流程
TEST(RecvMultishotIntegrationTest, ErrorFlow)
{
    RecvMultishotState state;
    on_recv_chunk_handler(&state, -ENOBUFS, 0);

    RecvMultishotAwaiter awaiter{&state};
    EXPECT_TRUE(awaiter.await_ready());

    auto chunk = awaiter.await_resume();
    EXPECT_TRUE(chunk.is_error());
    EXPECT_EQ(chunk.res, -ENOBUFS);
    EXPECT_TRUE(state.stopped);
}

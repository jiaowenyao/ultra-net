#pragma once

// RecvMultishotAwaiter — 将 multishot recv CQE 流桥接到协程 co_await 模式。
//
// 机制：
//   1. submit_multishot_recv(fd, bgid) 提交 1 个 multishot recv SQE
//   2. 每个 CQE 携带一个 buffer ring 数据块（buffer_id + 字节数）
//   3. on_recv_chunk_handler 将数据写入 RecvMultishotState
//   4. 协程通过 co_await RecvMultishotAwaiter{state} 等待下一个数据块
//
// 注意：所有操作在同一 worker 线程上执行（CQE handler 和协程），无需锁。

#include "io_engine.hpp"
#include "io_callback.hpp"
#include <coroutine>

namespace ynet::async::io {

// ── Per-fd multishot recv 状态 ─────────────────────────────────────────
//
// 由 CQE handler（写）和等待协程（读）共享。
// 单线程访问（同 worker 线程），无需原子操作。

struct RecvMultishotState {
    // 等待下一个数据块的协程句柄
    std::coroutine_handle<> waiter{nullptr};

    // 最新 CQE 结果
    int last_res{0};
    unsigned last_flags{0};

    // 有可用数据块等待消费
    bool chunk_ready{false};

    // multishot recv 已终止（res=0 连接关闭，或 res<0 错误）
    bool stopped{false};
};

// ── CQE handler ────────────────────────────────────────────────────────
//
// 由 on_io_completion 在线程池 worker 线程上调用。
// 将 CQE 结果存入 state，若协程正在等待则恢复它。

inline void on_recv_chunk_handler(void* ctx, int res, unsigned cqe_flags)
{
    auto* state = static_cast<RecvMultishotState*>(ctx);
    state->last_res = res;
    state->last_flags = cqe_flags;
    state->chunk_ready = true;

    if (res <= 0)
    {
        state->stopped = true;
    }

    // 恢复等待中的协程
    if (state->waiter)
    {
        auto h = state->waiter;
        state->waiter = nullptr;
        h.resume();
    }
}

// ── 数据块结果 ─────────────────────────────────────────────────────────

struct RecvChunkResult {
    int res;       // >0=字节数, 0=EOF, <0=错误码
    unsigned flags; // CQE flags（buffer_id 在高 16 位）

    // 从 CQE flags 中提取 buffer_id
    unsigned buffer_id() const
    {
        return flags >> 16;
    }

    bool is_eof() const
    {
        return res == 0;
    }

    bool is_error() const
    {
        return res < 0;
    }
};

// ── 协程 awaitable ─────────────────────────────────────────────────────
//
// 用法：
//   RecvMultishotState state;
//   // ... 提交 multishot recv SQE ...
//   while (true) {
//       auto chunk = co_await RecvMultishotAwaiter{&state};
//       if (chunk.is_eof() || chunk.is_error()) break;
//       // 处理 chunk ...
//   }

class RecvMultishotAwaiter {
public:
    explicit RecvMultishotAwaiter(RecvMultishotState* state) noexcept
        : m_state(state) {}

    // 如果数据已就绪或已停止，跳过暂停直接获取
    bool await_ready() const noexcept
    {
        return m_state->chunk_ready || m_state->stopped;
    }

    // 暂停协程，等待 CQE 恢复
    void await_suspend(std::coroutine_handle<> h) noexcept
    {
        m_state->waiter = h;
    }

    // 协程恢复后返回最新的数据块
    RecvChunkResult await_resume() noexcept
    {
        RecvChunkResult r{m_state->last_res, m_state->last_flags};
        m_state->chunk_ready = false;
        return r;
    }

private:
    RecvMultishotState* m_state;
};

// ── 提交 multishot recv SQE ────────────────────────────────────────────
//
// 每个 fd 只调用一次。返回的 IoCallback 由调用方管理生命周期。
// IoCallback 必须在 multishot recv 持续期间保持存活（直到 fd 关闭或 cancel）。
//
// 返回 nullptr 表示引擎不可用或 SQE 池满。

inline IoCallback* submit_multishot_recv(int fd, unsigned bgid)
{
    auto* engine = IoUringEngine::current();
    if (!engine)
    {
        return nullptr;
    }

    auto* sqe = engine->get_sqe();
    if (!sqe)
    {
        return nullptr;
    }

    // IoCallback 必须堆分配——multishot 生命周期跨越多次 CQE
    auto* cb = new IoCallback{};
    cb->m_is_multishot = true;
    cb->m_multishot_handler = on_recv_chunk_handler;
    // m_multishot_ctx 由调用方在提交后设置为对应的 RecvMultishotState*

    // 使用 buffer ring：buf=nullptr, len=0，内核从指定 ring 中选择 buffer
    io_uring_prep_recv_multishot(sqe, fd, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = static_cast<__u16>(bgid);
    io_uring_sqe_set_data(sqe, cb);
    engine->increment_pending();
    engine->submit_now();

    return cb;
}

} // namespace ynet::async::io

#pragma once

// RecvMultishotAwaiter — 将 multishot recv CQE 流桥接到协程 co_await 模式。
//
// 机制：
//   1. submit_multishot_recv(fd, bgid) 提交 1 个 multishot recv SQE
//   2. 每个 CQE 携带一个 buffer ring 数据块（buffer_id + 字节数）
//   3. on_recv_chunk_handler 把数据块排进队列，而不是覆盖单个槽
//   4. 协程通过 co_await RecvMultishotAwaiter{state} 取走下一个数据块
//
// CQE 完成线程和协程可能不是同一个 worker（write 完成后的 resubmit 可被偷走），
// 所以队列用 mutex 保护。resume 发生在解锁之后。

#include "io_engine.hpp"
#include "io_callback.hpp"
#include "ultranet/buffer/buffer.h"
#include <coroutine>
#include <deque>
#include <mutex>

namespace ynet::async::io {

struct RecvChunkResult {
    int res;        // >0=字节数, 0=EOF, <0=错误码
    unsigned flags; // CQE flags（buffer_id 在高 16 位）

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

// 非终止的成功 CQE 带 IORING_CQE_F_MORE。没有 MORE，或 res<=0，表示 multishot 结束。
inline bool multishot_is_terminal(int res, unsigned cqe_flags)
{
    if (res <= 0) {
        return true;
    }
    return (cqe_flags & IORING_CQE_F_MORE) == 0;
}

// ── Per-fd multishot recv 状态 ─────────────────────────────────────────

struct RecvMultishotState {
    mutable std::mutex mu;
    std::deque<RecvChunkResult> queue;
    std::coroutine_handle<> waiter{nullptr};
    bool stopped{false};
    // 拥有者已析构或 cleanup：不再 resume，由终止 CQE 释放 IoCallback。
    bool abandoned{false};
    IoCallback* owner_cb{nullptr};
    BufferGroup* bg{nullptr};
};

inline void on_recv_chunk_handler(void* ctx, int res, unsigned cqe_flags)
{
    auto* state = static_cast<RecvMultishotState*>(ctx);
    if (!state) {
        return;
    }

    const bool terminal = multishot_is_terminal(res, cqe_flags);
    std::coroutine_handle<> to_resume;
    bool dispose = false;
    IoCallback* cb = nullptr;
    BufferGroup* bg = nullptr;
    unsigned bid = cqe_flags >> 16;

    {
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->abandoned) {
            bg = state->bg;
            if (terminal) {
                dispose = true;
                cb = state->owner_cb;
                state->owner_cb = nullptr;
            }
        } else {
            state->queue.push_back(RecvChunkResult{res, cqe_flags});
            if (terminal) {
                state->stopped = true;
            }
            to_resume = state->waiter;
            state->waiter = nullptr;
        }
    }

    if (bg && res > 0) {
        bg->return_buffer(bid, 0);
        bg->advance_ring(1);
    }
    if (to_resume) {
        to_resume.resume();
    }
    if (dispose && cb) {
        cb->m_dispose = true;
    }
}

class RecvMultishotAwaiter {
public:
    explicit RecvMultishotAwaiter(RecvMultishotState* state) noexcept
        : m_state(state) {}

    bool await_ready() noexcept
    {
        std::lock_guard<std::mutex> lock(m_state->mu);
        return !m_state->queue.empty() || m_state->stopped;
    }

    void await_suspend(std::coroutine_handle<> h) noexcept
    {
        bool resume_now = false;
        {
            std::lock_guard<std::mutex> lock(m_state->mu);
            if (!m_state->queue.empty() || m_state->stopped) {
                resume_now = true;
            } else {
                m_state->waiter = h;
            }
        }
        if (resume_now) {
            h.resume();
        }
    }

    RecvChunkResult await_resume() noexcept
    {
        std::lock_guard<std::mutex> lock(m_state->mu);
        if (m_state->queue.empty()) {
            return RecvChunkResult{0, 0};
        }
        RecvChunkResult r = m_state->queue.front();
        m_state->queue.pop_front();
        return r;
    }

private:
    RecvMultishotState* m_state;
};

// 每个 fd 只调用一次。返回的 IoCallback 由调用方管理生命周期。
// IoCallback 必须活到终止 CQE 被完成线程处理完。

inline IoCallback* submit_multishot_recv(int fd, unsigned bgid)
{
    auto* engine = IoUringEngine::current();
    if (!engine) {
        return nullptr;
    }

    auto* sqe = engine->get_sqe();
    if (!sqe) {
        return nullptr;
    }

    auto* cb = new IoCallback{};
    cb->m_is_multishot = true;
    cb->m_multishot_handler = on_recv_chunk_handler;

    io_uring_prep_recv_multishot(sqe, fd, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    sqe->buf_group = static_cast<__u16>(bgid);
    io_uring_sqe_set_data(sqe, cb);
    engine->increment_pending();
    engine->submit_now();

    return cb;
}

} // namespace ynet::async::io

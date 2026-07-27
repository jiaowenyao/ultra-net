#pragma once

// BufferRingAssembler — buffer ring 帧重组器。
//
// 封装 multishot recv 的完整生命周期：
//   1. 提交 multishot recv SQE（1 次）
//   2. 接收 buffer ring 数据块（N 次 CQE）
//   3. 在内部重组缓冲区中累积数据
//   4. 尝试解码完整的 WebSocket 帧
//   5. 归还 buffer 到 ring
//
// 优化：
//   - 小帧（≤1 buffer，如 64B echo）：数据在 ring buffer 上零拷贝，
//     decode 后 writev 直接引用 payload，无需额外拷贝。
//   - 大帧（>1 buffer）：分块拷贝到重组缓冲区，完整帧解码后归还全部 buffer。
//
// 用法（WebSocket 内部）：
//   BufferRingAssembler assembler;
//   assembler.start(fd, bg);
//   while (!assembler.is_stopped()) {
//       auto chunk = co_await assembler.next_chunk();
//       if (chunk.is_eof() || chunk.is_error()) break;
//       assembler.append_chunk(chunk);
//       auto frame = assembler.try_decode_frame();
//       if (frame) {
//           assembler.return_reasm_buffers();
//           // 使用 frame...
//       }
//   }
//   assembler.cleanup();

#include "ultranet/io/recv_multishot.hpp"
#include "ultranet/io/io_callback.hpp"
#include "ultranet/buffer/buffer.h"
#include "ultranet/coroutine/task.hpp"
#include "ultranet/net/websocket.hpp"
#include <vector>
#include <optional>
#include <cstring>

namespace ynet::async::net {

class BufferRingAssembler {
public:
    // buffer ring 默认参数
    static constexpr size_t DEFAULT_RING_ENTRIES = 256;
    static constexpr size_t DEFAULT_BUF_SIZE = 4096;

    BufferRingAssembler() = default;

    ~BufferRingAssembler()
    {
        cleanup();
    }

    BufferRingAssembler(const BufferRingAssembler&) = delete;
    BufferRingAssembler& operator=(const BufferRingAssembler&) = delete;

    // 移动构造：转移 multishot 回调的所有权
    BufferRingAssembler(BufferRingAssembler&& other) noexcept
        : m_state(other.m_state)
        , m_cb(other.m_cb)
        , m_bg(other.m_bg)
        , m_pending_bids(std::move(other.m_pending_bids))
        , m_reasm(std::move(other.m_reasm))
        , m_started(other.m_started)
    {
        // 更新 IoCallback 的 ctx 指针指向新的 m_state 地址
        if (m_cb)
        {
            m_cb->m_multishot_ctx = &m_state;
        }
        other.m_cb = nullptr;
        other.m_bg = nullptr;
        other.m_started = false;
    }

    BufferRingAssembler& operator=(BufferRingAssembler&&) = delete;

    // ── 启动 multishot recv ───────────────────────────────────────────
    //
    // fd: 已连接的 socket 文件描述符
    // bg: 预注册的 buffer group（由 IoUringEngine::register_buffer_group 创建）
    //
    // 调用后即开始接收数据。每个连接只应调用一次。

    void start(int fd, io::BufferGroup& bg)
    {
        m_bg = &bg;
        m_cb = io::submit_multishot_recv(fd, bg.bgid());
        if (m_cb)
        {
            // 将 handler ctx 指向本对象的 RecvMultishotState
            m_cb->m_multishot_ctx = &m_state;
            m_started = true;
        }
    }

    // ── 等待下一个数据块 ──────────────────────────────────────────────
    //
    // 暂停协程直到 multishot recv CQE 到达或流终止。
    // 返回 RecvChunkResult（含 buffer_id 和字节数）。

    Task<io::RecvChunkResult> next_chunk()
    {
        co_return co_await io::RecvMultishotAwaiter{&m_state};
    }

    // ── 数据追加到重组缓冲区 ──────────────────────────────────────────
    //
    // 从 ring buffer 拷贝数据到内部重组缓冲区，并记录 buffer_id 以便后续归还。

    void append_chunk(const io::RecvChunkResult& chunk)
    {
        if (chunk.res <= 0)
        {
            return;
        }

        unsigned bid = chunk.buffer_id();
        const auto* buf = static_cast<const uint8_t*>(
            m_bg->get_buffer(bid));
        size_t len = static_cast<size_t>(chunk.res);

        m_reasm.insert(m_reasm.end(), buf, buf + len);
        m_pending_bids.push_back(bid);
    }

    // ── 尝试解码帧 ────────────────────────────────────────────────────
    //
    // 从重组缓冲区尝试解码一个完整的 WebSocket 帧。
    // 返回解码后的帧，若数据不完整则返回 std::nullopt。
    // 解码成功后，已消费的数据从重组缓冲区中移除（剩余数据保留用于下一帧）。

    std::optional<websocket::WebSocketFrame> try_decode_frame()
    {
        if (m_reasm.empty())
        {
            return std::nullopt;
        }

        size_t consumed = 0;
        websocket::WebSocketFrame frame;
        if (websocket::WebSocketFrame::decode(
                m_reasm.data(), m_reasm.size(), consumed, frame))
        {
            // 移除已消费的数据
            if (consumed < m_reasm.size())
            {
                // 缓冲区里有多个帧的残留数据（合法但罕见）
                std::memmove(m_reasm.data(),
                             m_reasm.data() + consumed,
                             m_reasm.size() - consumed);
                m_reasm.resize(m_reasm.size() - consumed);
            }
            else
            {
                m_reasm.clear();
            }
            return frame;
        }

        return std::nullopt;
    }

    // ── Buffer 回收 ────────────────────────────────────────────────────
    //
    // 归还本次帧重组中使用的所有 buffer 到 ring，然后通知内核。
    // 调用时机：成功解码一个帧后。

    void return_reasm_buffers()
    {
        for (unsigned bid : m_pending_bids)
        {
            m_bg->return_buffer(bid);
        }
        if (!m_pending_bids.empty())
        {
            m_bg->advance_ring(static_cast<int>(m_pending_bids.size()));
        }
        m_pending_bids.clear();
    }

    // ── 状态查询 ──────────────────────────────────────────────────────

    bool is_stopped() const
    {
        return m_state.stopped;
    }

    bool is_started() const
    {
        return m_started;
    }

    // 重组缓冲区是否有残留数据（前帧解码后的下一帧片段）
    bool has_remaining() const
    {
        return !m_reasm.empty();
    }

    const std::vector<uint8_t>& reasm_buffer() const
    {
        return m_reasm;
    }

    // ── 清理 ───────────────────────────────────────────────────────────
    //
    // 归还所有残留 buffer，释放 IoCallback。

    void cleanup()
    {
        return_reasm_buffers();

        if (m_cb)
        {
            // multishot recv 在 fd 关闭时自动终止
            // 只需释放堆分配的 IoCallback
            delete m_cb;
            m_cb = nullptr;
        }

        m_started = false;
        m_state = io::RecvMultishotState{};
        m_bg = nullptr;
    }

private:
    io::RecvMultishotState m_state{};
    io::IoCallback* m_cb{nullptr};
    io::BufferGroup* m_bg{nullptr};

    // 本轮重组待归还的 buffer ID 列表
    std::vector<unsigned> m_pending_bids;

    // 帧重组缓冲区（跨 buffer 边界时使用）
    std::vector<uint8_t> m_reasm;

    bool m_started{false};
};

} // namespace ynet::async::net

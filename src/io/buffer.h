// src/io/buffer.hpp
#pragma once

#include <atomic>
#include <memory>
#include <liburing.h>

namespace ynet::async::io {

// Buffer Group - liburing 2.5 兼容版本
class BufferGroup {
public:
    BufferGroup(unsigned gid, size_t entries = 1024, size_t buf_size = 4096);

    ~BufferGroup();

    // 禁止拷贝移动
    BufferGroup(const BufferGroup&) = delete;
    BufferGroup& operator=(const BufferGroup&) = delete;

    // 获取buffer指针（通过bid）
    void* get_buffer(unsigned bid) const noexcept;

    // 分配新的buffer ID（用于用户管理）
    unsigned allocate_bid() noexcept;
    // liburing 2.5 没有io_uring_buf_ring_add，我们只做注册
    // 内核会自动管理buffer ring
    void release_buffer(unsigned) noexcept;

    unsigned bgid() const noexcept;

    size_t entries() const noexcept { return m_entries; }
    size_t buf_size() const noexcept { return m_buf_size; }

private:
    unsigned m_gid;
    size_t m_entries;
    size_t m_buf_size;

    std::unique_ptr<char[]> m_buffers;
    std::unique_ptr<io_uring_buf[]> m_bufs;
    io_uring_buf_reg m_reg{};
    std::atomic<unsigned> m_next_bid{0};
};

} // namespace ynet::async::io


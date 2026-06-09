#pragma once

#include <atomic>
#include <memory>
#include <liburing.h>

namespace ynet::async::io {

// Buffer Group - io_uring registered buffer ring for zero-copy I/O.
// Compatible with liburing >= 2.5.  The kernel manages buffer lifecycles
// automatically once the ring is registered.
class BufferGroup {
public:
    BufferGroup(unsigned gid, size_t entries = 1024, size_t buf_size = 4096);
    ~BufferGroup();

    BufferGroup(const BufferGroup&) = delete;
    BufferGroup& operator=(const BufferGroup&) = delete;

    void* get_buffer(unsigned bid) const noexcept;
    unsigned allocate_bid() noexcept;
    void release_buffer(unsigned) noexcept;
    unsigned bgid() const noexcept;
    size_t entries() const noexcept { return m_entries; }
    size_t buf_size() const noexcept { return m_buf_size; }

private:
    unsigned m_gid;
    size_t m_entries;
    size_t m_buf_size;
    bool m_registered{false};

    std::unique_ptr<char[]> m_buffers;
    std::unique_ptr<io_uring_buf[]> m_bufs;
    io_uring_buf_reg m_reg{};
    std::atomic<unsigned> m_next_bid{0};
};

} // namespace ynet::async::io

#pragma once

#include <atomic>
#include <memory>
#include <cassert>
#include <system_error>
#include <liburing.h>

namespace ynet::async::io {

class BufferGroup {
public:
    BufferGroup(unsigned gid, size_t entries, size_t buf_size, struct io_uring* ring);
    ~BufferGroup();
    BufferGroup(const BufferGroup&) = delete;
    BufferGroup& operator=(const BufferGroup&) = delete;
    void* get_buffer(unsigned bid) const noexcept;
    unsigned allocate_bid() noexcept;
    void release_buffer(unsigned) noexcept;
    unsigned bgid() const noexcept { return m_gid; }
    size_t entries() const noexcept { return m_entries; }
    size_t buf_size() const noexcept { return m_buf_size; }

private:
    unsigned m_gid; size_t m_entries; size_t m_buf_size;
    struct io_uring* m_ring; bool m_registered{false};
    std::unique_ptr<char[]> m_buffers;
    std::unique_ptr<io_uring_buf[]> m_bufs;
    io_uring_buf_reg m_reg{};
    std::atomic<unsigned> m_next_bid{0};
};

inline BufferGroup::BufferGroup(unsigned gid, size_t entries, size_t buf_size,
                                 struct io_uring* ring)
    : m_gid(gid), m_entries(entries), m_buf_size(buf_size), m_ring(ring) {
    assert((entries & (entries - 1)) == 0);
    m_buffers = std::make_unique<char[]>(entries * buf_size);
    const size_t rs = entries * sizeof(io_uring_buf);
    void* rm = nullptr;
    if (posix_memalign(&rm, 4096, rs) != 0) throw std::bad_alloc();
    m_bufs.reset(static_cast<io_uring_buf*>(rm));
    for (size_t i = 0; i < entries; ++i)
        m_bufs[i] = {reinterpret_cast<__u64>(m_buffers.get()+i*buf_size),
                     static_cast<__u32>(buf_size), static_cast<__u16>(i), 0};
    m_reg = {reinterpret_cast<__u64>(m_bufs.get()), static_cast<__u32>(entries),
             static_cast<__u16>(gid), 0, {0,0,0}};
    int ret = io_uring_register_buf_ring(m_ring, &m_reg, 0);
    if (ret < 0) throw std::system_error(-ret, std::system_category(), "register buffer ring");
    m_registered = true;
}
inline BufferGroup::~BufferGroup() { if (m_registered) io_uring_unregister_buf_ring(m_ring, m_gid); }
inline void* BufferGroup::get_buffer(unsigned bid) const noexcept { return m_buffers.get()+(bid%m_entries)*m_buf_size; }
inline unsigned BufferGroup::allocate_bid() noexcept { return m_next_bid.fetch_add(1,std::memory_order_acq_rel)%m_entries; }
inline void BufferGroup::release_buffer(unsigned) noexcept {}

} // namespace ynet::async::io

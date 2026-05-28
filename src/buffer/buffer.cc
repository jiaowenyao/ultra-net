#include "ultranet/buffer/buffer.h"
#include "ultranet/io/io_engine.hpp"
#include <stdlib.h>
#include <cassert>

namespace ynet::async::io {

BufferGroup::BufferGroup(unsigned gid, size_t entries, size_t buf_size)
    : m_gid(gid)
    , m_entries(entries)
    , m_buf_size(buf_size) {

    assert((entries & (entries - 1)) == 0);

    // 1. 分配buffer内存
    m_buffers = std::make_unique<char[]>(entries * buf_size);

    // 2. 分配io_uring_buf数组（用于注册）
    const size_t ring_size = entries * sizeof(io_uring_buf);
    void* ring_mem = nullptr;
    if (posix_memalign(&ring_mem, 4096, ring_size) != 0) {
        throw std::bad_alloc();
    }
    m_bufs.reset(static_cast<io_uring_buf*>(ring_mem));
    // m_bufs = std::make_unique<io_uring_buf[]>(entries);

    // 3. 初始化每个buffer
    for (size_t i = 0; i < entries; ++i) {
        m_bufs[i] = io_uring_buf{
            .addr = reinterpret_cast<__u64>(m_buffers.get() + i * buf_size),
            .len = static_cast<__u32>(buf_size),
            .bid = static_cast<__u16>(i),
            .resv = 0
        };
    }

    // 4. 注册buffer ring - liburing 2.5 正确用法
    m_reg = io_uring_buf_reg{
        .ring_addr = reinterpret_cast<__u64>(m_bufs.get()),
        .ring_entries = static_cast<__u32>(entries),
        .bgid = static_cast<__u16>(gid),
        .flags = 0,
        .resv = {0, 0, 0}
    };

    // 5. 注册到io_uring
    auto* ring = IoUringEngine::current()->get_ring();
    int ret = io_uring_register_buf_ring(ring, &m_reg, 0);
    if (ret < 0) {
        throw std::system_error(-ret, std::system_category(),
            "failed to register buffer ring");
    }
    m_registered = true;
}

BufferGroup::~BufferGroup() {
    if (m_registered) {
        auto* ring = IoUringEngine::current()->get_ring();
        io_uring_unregister_buf_ring(ring, m_gid);
    }
}


// 获取buffer指针（通过bid）
void* BufferGroup::get_buffer(unsigned bid) const noexcept {
    return m_buffers.get() + (bid % m_entries) * m_buf_size;
}

// 分配新的buffer ID（用于用户管理）
unsigned BufferGroup::allocate_bid() noexcept {
    return m_next_bid.fetch_add(1, std::memory_order_acq_rel) % m_entries;
}

// liburing 2.5 没有io_uring_buf_ring_add，我们只做注册
// 内核会自动管理buffer ring
void BufferGroup::release_buffer(unsigned) noexcept {
    // 在liburing 2.5中，注册的buffer ring由内核自动管理
    // 不需要显式释放
}

unsigned BufferGroup::bgid() const noexcept { return m_gid; }

} // namespace ynet::async::io


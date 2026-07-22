#include "ultranet/io/io_engine.hpp"
#include "ultranet/buffer/buffer.h"

namespace ynet::async::io {

IoUringEngine::~IoUringEngine() {
    if (m_ring.ring_fd >= 0) {
        io_uring_submit(&m_ring);
        m_buffer_groups.clear();
        io_uring_queue_exit(&m_ring);
    }
}

BufferGroup& IoUringEngine::register_buffer_group(unsigned gid,
        size_t entries, size_t buf_size) {
    auto it = m_buffer_groups.find(gid);
    if (it != m_buffer_groups.end()) {
        return *it->second;
    }
    auto group = std::make_unique<BufferGroup>(gid, entries, buf_size, &m_ring);
    auto* ptr = group.get();
    m_buffer_groups[gid] = std::move(group);
    return *ptr;
}

void* IoUringEngine::get_buffer(unsigned gid, unsigned bid) noexcept {
    auto it = m_buffer_groups.find(gid);
    if (it != m_buffer_groups.end()) {
        return it->second->get_buffer(bid);
    }
    return nullptr;
}

} // namespace ynet::async::io

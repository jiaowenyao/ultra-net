// async/io/read.hpp
#include <liburing.h>
#include "async/io/io_awaitable.hpp"

namespace ynet::async::io {

class Read : public IoOperation<Read> {
public:
    // 父操作：创建 multishot reader
    explicit Read(int fd, unsigned bgid = 1) noexcept
        : IoOperation<Read>(
            [](io_uring_sqe* sqe, int f, unsigned b) {
                // liburing 2.5 multishot read
                io_uring_prep_recv_multishot(sqe, f, nullptr, 4096, 0);
                sqe->flags |= IOSQE_BUFFER_SELECT;
                sqe->buf_group = static_cast<__u16>(b);
                sqe->flags |= IOSQE_FIXED_FILE;
            },
            fd, bgid
        )
        , m_fd(fd)
        , m_bgid(bgid) {
    }

    // 子操作
    Read(int fd, Read& parent) noexcept
        : IoOperation<Read>()
        , m_fd(fd)
        , m_parent(&parent) {
        m_is_parent = false;
        m_bgid = parent.m_bgid;
    }

    // 不需要析构release，纯BufferGroup不需要手动归还
    IoResult<std::span<char>> resume() noexcept {
        if (!m_is_parent) {
            return m_parent->get_data();
        }

        if (m_callback.m_result < 0) {
            return std::unexpected(make_io_error(m_callback.m_result));
        }

        // 解析结果：低16位是长度，高16位是buffer ID
        size_t bytes = static_cast<size_t>(m_callback.m_result & 0xFFFF);
        unsigned bid = static_cast<unsigned>((m_callback.m_result >> 16) & 0xFFFF);

        // 获取buffer - 纯BufferGroup直接通过bid索引
        void* buffer = IoUringContext::current()->get_buffer(m_bgid, bid);

        // 重置，准备下一次读取
        m_callback.m_completed = false;
        m_callback.m_result = 0;

        return std::span<char>(
            static_cast<char*>(buffer),
            bytes
        );
    }

    IoResult<std::span<char>> get_data() noexcept {
        if (m_parent->m_callback.m_result < 0) {
            return std::unexpected(make_io_error(m_parent->m_callback.m_result));
        }

        size_t bytes = static_cast<size_t>(m_parent->m_callback.m_result & 0xFFFF);
        unsigned bid = static_cast<unsigned>((m_parent->m_callback.m_result >> 16) & 0xFFFF);

        void* buffer = IoUringContext::current()->get_buffer(m_parent->m_bgid, bid);

        m_parent->m_callback.m_completed = false;
        m_parent->m_callback.m_result = 0;

        return std::span<char>(
            static_cast<char*>(buffer),
            bytes
        );
    }

    bool cancel() noexcept override {
        if (!m_is_parent) return false;
        return IoOperation<Read>::cancel();
    }

private:
    int m_fd;
    unsigned m_bgid{0};
    Read* m_parent{nullptr};
};

} // namespace ynet::async::io

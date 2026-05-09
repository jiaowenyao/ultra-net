#include <liburing.h>
#include "ultranet/io/io_awaitable.hpp"

namespace ynet::async::io {

// Read - 支持单次和 multishot 读取
class Read : public IoOperation<Read> {
public:
    // 父操作：创建 multishot reader
    explicit Read(int fd, unsigned bgid = 1) noexcept
        : IoOperation<Read>(
            [](io_uring_sqe* sqe, int f, unsigned b) {
                io_uring_prep_recv_multishot(sqe, f, nullptr, 0, 0);
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

    // 析构时释放子操作
    ~Read() {
        if (!m_is_parent && m_parent) {
            // 通知父操作这个子操作已结束
        }
    }

    // 不需要析构release，纯BufferGroup不需要手动归还
    IoResult<std::span<char>> resume() noexcept {
        // 解析结果：低16位是长度，高16位是buffer ID
        if (m_callback.m_result < 0) {
            // 处理错误
            if (m_callback.m_result == -ECANCELED) {
                return std::unexpected(make_io_error(ECANCELED));
            }
            if (m_callback.m_result == -EAGAIN) {
                // 资源暂时不可用，不算错误
                return std::unexpected(make_io_error(EAGAIN));
            }
            return std::unexpected(make_io_error(m_callback.m_result));
        }

        size_t bytes = static_cast<size_t>(m_callback.m_result & 0xFFFF);
        unsigned bid = static_cast<unsigned>((m_callback.m_result >> 16) & 0xFFFF);

        // 获取buffer - 纯BufferGroup直接通过bid索引
        void* buffer = IoUringEngine::current()->get_buffer(m_bgid, bid);

        if (!buffer) {
            return std::unexpected(make_io_error(ENOMEM));
        }

        // 重置，准备下一次读取
        m_callback.m_completed = false;
        m_callback.m_result = 0;

        return std::span<char>(
            static_cast<char*>(buffer),
            bytes
        );
    }

    IoResult<std::span<char>> get_data() noexcept {
        if (m_callback.m_result < 0) {
            return std::unexpected(make_io_error(m_callback.m_result));
        }

        size_t bytes = static_cast<size_t>(m_callback.m_result & 0xFFFF);
        unsigned bid = static_cast<unsigned>((m_callback.m_result >> 16) & 0xFFFF);

        void* buffer = IoUringEngine::current()->get_buffer(m_bgid, bid);

        m_callback.m_completed = false;
        m_callback.m_result = 0;

        return std::span<char>(
            static_cast<char*>(buffer),
            bytes
        );
    }

    // 取消 multishot 读取
    void cancel() noexcept override {
        if (!m_is_parent) return;
        IoOperation<Read>::cancel();
    }

    // 重新提交
    void resubmit() override {
        if (m_is_parent && m_sqe) {
            auto* ctx = IoUringEngine::current();
            auto* new_sqe = ctx->get_sqe();
            if (new_sqe) {
                *new_sqe = *m_sqe;
                io_uring_sqe_set_data(new_sqe, &m_callback);
                m_sqe = new_sqe;
                ctx->increment_pending();
            }
        }
    }

private:
    int m_fd;
    unsigned m_bgid{0};
    Read* m_parent{nullptr};
};

} // namespace ynet::async::io

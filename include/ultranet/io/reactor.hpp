// IoReactor — 每线程的 io_uring 事件循环。
// 管理线程局部的 io_uring 引擎生命周期，处理 CQE 并执行回调。
// 由 WorkStealingThreadPool::worker_thread 持有。
#pragma once

#include "io_engine.hpp"
#include "io_callback.hpp"
#include <sys/eventfd.h>
#include <unistd.h>

namespace ynet::async::io {

class IoReactor {
public:
    using CompletionHandler = void (*)(void* ctx, io_uring_cqe* cqe);

    IoReactor(int event_fd, CompletionHandler on_completion, void* ctx,
              IoUringEngineConfig config = IoUringEngineConfig{})
        : m_event_fd(event_fd)
        , m_on_completion(on_completion)
        , m_ctx(ctx) {
        // 初始化线程局部 io_uring 引擎
        IoUringEngine::init_thread_local(config);
        submit_eventfd_read();
    }

    ~IoReactor() {
        // 销毁线程局部 io_uring 引擎，释放 ring 资源
        IoUringEngine::destroy_thread_local();
    }

    IoReactor(const IoReactor&) = delete;
    IoReactor& operator=(const IoReactor&) = delete;

    // 处理所有待处理的 CQE，不阻塞。
    // eventfd CQE 在这里被拦截消费，其他 CQE 交给 m_on_completion 回调。
    void poll() {
        auto* engine = IoUringEngine::current();
        if (!engine) {
            return;
        }

        engine->for_each_cqe([this](io_uring_cqe* cqe) {
            auto* cb = reinterpret_cast<IoCallback*>(io_uring_cqe_get_data(cqe));
            if (!cb) {
                return;
            }

            cb->m_result = cqe->res;
            cb->m_completed = true;

            // eventfd 唤醒：读取计数器并重新注册
            if (cb == &m_wakeup_cb) {
                uint64_t val;
                ::read(m_event_fd, &val, sizeof(val));
                submit_eventfd_read();
                return;
            }

            m_on_completion(m_ctx, cqe);
        });

        // CQE 溢出恢复：高负载下 CQ 环溢出时 CQE 被静默丢弃，
        // pending_ops 计数会膨胀。减半以使系统恢复。
        if (engine->has_cq_overflow()) {
            engine->adjust_pending_ops_on_overflow();
        }
    }

    // 提交待处理的 SQE 并等待至少一个 CQE。
    // 使用 100ms 超时确保关闭检查能及时响应。
    void wait_for_events() {
        auto* engine = IoUringEngine::current();
        if (!engine) {
            return;
        }
        engine->flush_submit();
        struct __kernel_timespec ts = {0, 100000000};  // 100ms
        io_uring_cqe* cqe = nullptr;
        engine->wait_cqe(&cqe, 1, &ts);
    }

    IoUringEngine* engine() noexcept { return IoUringEngine::current(); }

private:
    // 向 io_uring 提交 eventfd 读操作，用于线程唤醒
    void submit_eventfd_read() {
        auto* engine = IoUringEngine::current();
        if (!engine) {
            return;
        }
        auto* sqe = engine->get_sqe();
        if (sqe) {
            io_uring_prep_read(sqe, m_event_fd, &m_eventfd_buf,
                              sizeof(m_eventfd_buf), 0);
            io_uring_sqe_set_data(sqe, &m_wakeup_cb);
            engine->increment_pending();
            engine->submit_now();
        }
    }

    int m_event_fd;
    CompletionHandler m_on_completion;
    void* m_ctx;
    uint64_t m_eventfd_buf{0};
    IoCallback m_wakeup_cb{};
};

} // namespace ynet::async::io

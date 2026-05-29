#pragma once

#include "io_engine.hpp"
#include "io_callback.hpp"
#include <sys/eventfd.h>
#include <unistd.h>

namespace ynet::async::io {

class IoReactor {
public:
    using CompletionHandler = void (*)(void* ctx, io_uring_cqe* cqe);

    IoReactor(int event_fd, CompletionHandler on_completion, void* ctx)
        : m_engine_scope()
        , m_event_fd(event_fd)
        , m_on_completion(on_completion)
        , m_ctx(ctx) {
        submit_eventfd_read();
    }

    // 处理所有待处理的 CQE，不阻塞
    void poll() {
        auto* engine = IoUringEngine::current();
        if (!engine) return;

        engine->for_each_cqe([this](io_uring_cqe* cqe) {
            auto* cb = reinterpret_cast<IoCallback*>(io_uring_cqe_get_data(cqe));
            if (!cb) return;

            cb->m_result = cqe->res;
            cb->m_completed = true;

            if (cb == &m_wakeup_cb) {
                uint64_t val;
                ::read(m_event_fd, &val, sizeof(val));
                submit_eventfd_read();
                return;
            }

            m_on_completion(m_ctx, cqe);
        });
    }

    // 提交所有待处理的 SQE 并等待 CQE
    void wait_for_events() {
        auto* engine = IoUringEngine::current();
        if (!engine) return;
        engine->flush_submit();
        struct __kernel_timespec ts = {5, 0};
        io_uring_cqe* cqe = nullptr;
        engine->wait_cqe(&cqe, 1, &ts);
    }

    IoUringEngine* engine() noexcept { return IoUringEngine::current(); }

private:
    void submit_eventfd_read() {
        auto* engine = IoUringEngine::current();
        if (!engine) return;
        auto* sqe = engine->get_sqe();
        if (sqe) {
            io_uring_prep_read(sqe, m_event_fd, &m_eventfd_buf, sizeof(m_eventfd_buf), 0);
            io_uring_sqe_set_data(sqe, &m_wakeup_cb);
            engine->increment_pending();
            engine->submit_now();
        }
    }

    IoUringEngine::Scope m_engine_scope;
    int m_event_fd;
    CompletionHandler m_on_completion;
    void* m_ctx;
    uint64_t m_eventfd_buf{0};
    IoCallback m_wakeup_cb{};
};

} // namespace ynet::async::io

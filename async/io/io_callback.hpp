#pragma once

#include <coroutine>
#include <atomic>
#include <chrono>
#include <cassert>


namespace ynet::async::io {

struct IoCallback {

    std::coroutine_handle<> m_handle{nullptr};
    int m_result{0};
    bool m_completed{false};
    void* m_operation{nullptr};
    std::chrono::steady_clock::time_point m_deadline{};

};


} // namespace ynet::async::io



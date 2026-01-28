#include "context.h"

namespace ynet::async {


Scheduler* SchedulerContext::current() noexcept {
    return t_current_scheduler;
}

Scheduler* SchedulerContext::set_current(Scheduler* scheduler) noexcept {
    t_previous_scheduler = t_current_scheduler;
    t_current_scheduler = scheduler;
    return t_previous_scheduler;
}

void SchedulerContext::restore() noexcept {
    t_current_scheduler = t_previous_scheduler;
    t_previous_scheduler = nullptr;
}

SchedulerContext::ScopedGuard::ScopedGuard(Scheduler* scheduler)
    : m_prev(set_current(scheduler))
    , m_active(true) {}

SchedulerContext::ScopedGuard::~ScopedGuard() {
    if (m_active) {
        set_current(m_prev);
    }
}

SchedulerContext::ScopedGuard::ScopedGuard(SchedulerContext::ScopedGuard&& other) noexcept
    : m_prev(other.m_prev)
    , m_active(other.m_active) {
    other.m_active = false;
}

} // namespace ynet::async




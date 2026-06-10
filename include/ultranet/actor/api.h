// Ultra-Net Actor Framework — Public API
#pragma once

#include "ultranet/actor/core/message.h"
#include "ultranet/actor/core/mailbox.h"
#include "ultranet/actor/core/actor.h"
#include "ultranet/actor/core/actor_ref.h"

namespace ynet::actor {

static uint64_t s_next_id = 0;

template <typename T, typename... Args>
actor_ref<T> spawn(actor_system& system, actor_config cfg = actor_config{}, Args&&... args) {
    auto* a = new actor<T>(&system, std::move(cfg), std::forward<Args>(args)...);
    return actor_ref<T>(++s_next_id, a);
}

} // namespace ynet::actor

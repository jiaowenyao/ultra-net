// Compile-time type hash for actor message dispatch.
// Uses FNV-1a over typeid(T).name() — consistent within a single build.
#pragma once

#include <cstdint>
#include <typeinfo>

namespace ynet::actor {

template <typename T>
inline uint64_t actor_type_hash() {
    const char* name = typeid(T).name();
    uint64_t h = 14695981039346656037ULL;
    while (*name) {
        h ^= static_cast<uint8_t>(*name++);
        h *= 1099511628211ULL;
    }
    return h;
}

} // namespace ynet::actor

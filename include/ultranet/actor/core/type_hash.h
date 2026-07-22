// Compile-time type hash for actor message dispatch.
//
// By default, uses FNV-1a over typeid(T).name(), which is consistent within
// a single build but NOT across compilers or compiler versions.
//
// For distributed use (where messages may cross compiler boundaries),
// annotate message types with a stable string tag:
//
//   struct ping_msg {
//       static constexpr const char* actor_type = "ping";
//       int id;
//   };
//
// The tag is hashed via FNV-1a, making it stable across compilers.
#pragma once

#include <cstdint>
#include <typeinfo>

namespace ynet::actor {

namespace detail {
    // FNV-1a hash of a string.
    inline constexpr uint64_t fnv1a(const char* s) {
        uint64_t h = 14695981039346656037ULL;
        while (*s) {
            h ^= static_cast<uint8_t>(*s++);
            h *= 1099511628211ULL;
        }
        return h;
    }
}

// Primary template: use typeid(T).name() (compiler-dependent).
template <typename T>
inline uint64_t actor_type_hash() {
    // Detect if T::actor_type exists at compile time.
    if constexpr (requires { T::actor_type; }) {
        // Use the stable user-provided tag.
        // constexpr evaluation ensures zero runtime cost.
        return detail::fnv1a(T::actor_type);
    } else {
        // Fallback: compiler-dependent name.
        const char* name = typeid(T).name();
        uint64_t h = 14695981039346656037ULL;
        while (*name) {
            h ^= static_cast<uint8_t>(*name++);
            h *= 1099511628211ULL;
        }
        return h;
    }
}

} // namespace ynet::actor

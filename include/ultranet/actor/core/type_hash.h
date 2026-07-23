// 编译期类型哈希 — 用于 actor 消息分发。
//
// 默认使用 typeid(T).name() 的 FNV-1a 哈希，在同一构建内一致，
// 但不同编译器或编译器版本间不保证稳定。
//
// 在分布式场景下（消息可能跨越编译器边界），消息类型应标注稳定的字符串标签：
//
//   struct ping_msg {
//       static constexpr const char* actor_type = "ping";
//       int id;
//   };
//
// 框架通过 C++20 requires 表达式在编译期检测 T::actor_type 是否存在，
// 若存在则使用标签字符串的哈希，保证跨编译器稳定性。
#pragma once

#include <cstdint>
#include <typeinfo>

namespace ynet::actor {

// ── 编译期序列化检测 ────────────────────────────────────────────────────
// 检测消息类型是否提供了侵入式 serialize/deserialize 方法。
// 若有 → 走自定义序列化路径；若无 → 走 memcpy 快路径（需 trivially copyable）。

template <typename T>
concept serializable_msg = requires(const T& t, const uint8_t* d, size_t n) {
    t.serialize();
    T::deserialize(d, n);
};

} // namespace ynet::actor

// 重新打开以继续原有内容（避免影响后面的代码）
namespace ynet::actor {

namespace detail {
    // FNV-1a 哈希常量与计算
    inline constexpr uint64_t fnv1a(const char* s) {
        uint64_t h = 14695981039346656037ULL;
        while (*s) {
            h ^= static_cast<uint8_t>(*s++);
            h *= 1099511628211ULL;
        }
        return h;
    }
}

// 主模板：优先使用用户提供的稳定标签，退化为编译器相关名称
template <typename T>
inline uint64_t actor_type_hash() {
    if constexpr (requires { T::actor_type; }) {
        // 编译期常量求值，零运行时开销
        return detail::fnv1a(T::actor_type);
    } else {
        // 回退：编译器相关的 typeid 名称
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

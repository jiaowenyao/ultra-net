// actor_uri — 集群内每个 actor 的全局唯一标识符。
// 格式："ultra://<node_id>/<type_name>/<actor_name>"
// 示例："ultra://node-1/calculator/math-svc"、"ultra://*ping*/pinger-1"
//
// node 为 "*" 表示本地或任意节点。
#pragma once
#include <cstdint>
#include <string>
#include <format>

namespace ynet::actor {

struct actor_uri {
    std::string node;    // 节点 ID，"*" = 本地/任意
    std::string type;    // 类型名（通常为 typeid 名称）
    std::string name;    // 用户指定的 actor 名称

    // 构造本地 URI（node = "*"）
    static actor_uri make_local(const std::string& type_name,
                                const std::string& actor_name) {
        return {"*", type_name, actor_name};
    }

    // 构造指定节点的 URI
    static actor_uri make(uint64_t node_id, const std::string& type_name,
                          const std::string& actor_name) {
        return {std::to_string(node_id), type_name, actor_name};
    }

    std::string to_string() const {
        return std::format("ultra://{}/{}/{}", node, type, name);
    }

    bool operator==(const actor_uri& o) const {
        return node == o.node && type == o.type && name == o.name;
    }
};

} // namespace ynet::actor

// std::unordered_map 等容器所需的哈希支持
template <>
struct std::hash<ynet::actor::actor_uri> {
    size_t operator()(const ynet::actor::actor_uri& u) const {
        return std::hash<std::string>{}(u.to_string());
    }
};

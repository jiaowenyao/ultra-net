// actor_uri — global unique identifier for every actor in the cluster.
// Format: "ultra://<node_id>/<type_name>/<actor_name>"
// Examples: "ultra://node-1/calculator/math-svc", "ultra://*ping*/pinger-1"
#pragma once
#include <cstdint>
#include <string>
#include <format>

namespace ynet::actor {

struct actor_uri {
    std::string node;    // "*" = any/local
    std::string type;    // typeid name
    std::string name;    // user-assigned name

    static actor_uri make_local(const std::string& type_name, const std::string& actor_name) {
        return {"*", type_name, actor_name};
    }
    static actor_uri make(uint64_t node_id, const std::string& type_name, const std::string& actor_name) {
        return {std::to_string(node_id), type_name, actor_name};
    }
    std::string to_string() const {
        return std::format("ultra://{}/{}/{}", node, type, name);
    }
    bool operator==(const actor_uri& o) const { return node==o.node && type==o.type && name==o.name; }
};

} // namespace ynet::actor

template <>
struct std::hash<ynet::actor::actor_uri> {
    size_t operator()(const ynet::actor::actor_uri& u) const {
        return std::hash<std::string>{}(u.to_string());
    }
};

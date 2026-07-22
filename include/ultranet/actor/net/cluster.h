// Cluster — 基于 gossip 的去中心化节点发现。
// 每个节点定期与对等节点交换已知节点列表，自动发现新加入的节点
// 并检测宕机节点（基于超时）。
#pragma once

#include <vector>
#include <unordered_map>
#include <random>
#include <mutex>
#include <chrono>
#include <cstdint>
#include <string>
#include <cstring>

namespace ynet::actor::net {

using node_id_t = uint64_t;

// ── 节点信息 ──────────────────────────────────────────────────────────────

struct node_info {
    node_id_t id;
    std::string addr;
    uint64_t last_seen = 0;
};

// ── 集群成员管理 ──────────────────────────────────────────────────────────

class cluster {
public:
    cluster(node_id_t self_id, const std::string& self_addr)
        : m_self{self_id, self_addr, now_ms()}
        , m_id(self_id) {}

    // 添加种子节点地址（用于初始发现）
    void add_seed(const std::string& addr) {
        m_seeds.push_back(addr);
    }

    node_id_t self_id() const {
        return m_id;
    }

    // 获取当前存活节点列表（排除超时节点）
    std::vector<node_info> live_nodes(uint64_t timeout_ms = 3000) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::vector<node_info> out;
        out.push_back(m_self);

        uint64_t now = now_ms();
        for (auto& [id, info] : m_peers) {
            if (now - info.last_seen < timeout_ms) {
                out.push_back(info);
            }
        }
        return out;
    }

    // 标记节点为活跃（更新 last_seen 时间戳）
    void mark_seen(node_id_t id, const std::string& addr) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto& node = m_peers[id];
        node.id = id;
        node.addr = addr;
        node.last_seen = now_ms();
    }

    // ── Gossip 消息构造与解析 ──────────────────────────────────────────

    // 构造 gossip 消息：包含当前已知节点列表。
    // 线格式：[self_id:8][count:4][(id:8)(addr_len:4)(addr:var)(last_seen:8)]*
    std::vector<uint8_t> build_gossip() {
        auto nodes = live_nodes();
        std::lock_guard<std::mutex> lock(m_mutex);

        std::vector<uint8_t> buffer;
        // 辅助 lambda：将值按原始字节追加到 buffer
        auto write_value = [&buffer](auto value) {
            const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&value);
            buffer.insert(buffer.end(), ptr, ptr + sizeof(value));
        };

        // 写入源节点 ID 和节点数量
        write_value(m_id);
        write_value(static_cast<uint32_t>(nodes.size()));

        // 写入每个节点的信息
        for (auto& node : nodes) {
            write_value(node.id);

            uint32_t addr_len = static_cast<uint32_t>(node.addr.size());
            write_value(addr_len);
            buffer.insert(buffer.end(), node.addr.begin(), node.addr.end());

            write_value(node.last_seen);
        }
        return buffer;
    }

    // 解析并应用收到的 gossip 消息，更新本地节点视图
    void apply_gossip(const uint8_t* data, size_t length) {
        // 辅助 lambda：从 data 中读取一个值并推进指针
        auto read_value = [&data](auto& value) {
            std::memcpy(&value, data, sizeof(value));
            data += sizeof(value);
        };

        // 解析源节点 ID 和节点数量
        node_id_t src_id = 0;
        uint32_t count = 0;
        read_value(src_id);
        read_value(count);

        std::lock_guard<std::mutex> lock(m_mutex);

        for (uint32_t i = 0; i < count; ++i) {
            node_id_t id = 0;
            std::string addr;
            uint64_t seen = 0;

            read_value(id);

            uint32_t addr_len = 0;
            read_value(addr_len);
            addr.assign(reinterpret_cast<const char*>(data), addr_len);
            data += addr_len;

            read_value(seen);

            // 过滤自身
            if (id == m_id) {
                continue;
            }

            // 仅在新信息或更新的时间戳时更新
            auto it = m_peers.find(id);
            if (it == m_peers.end() || seen > it->second.last_seen) {
                m_peers[id] = {id, addr, seen};
            }
        }

        // 更新源节点的活跃时间
        auto it = m_peers.find(src_id);
        if (it != m_peers.end()) {
            it->second.last_seen = now_ms();
        }
    }

    const std::vector<std::string>& seeds() const {
        return m_seeds;
    }

private:
    node_info m_self;
    node_id_t m_id;
    std::vector<std::string> m_seeds;
    std::unordered_map<node_id_t, node_info> m_peers;
    std::mutex m_mutex;

    // 获取当前时间戳（毫秒）
    static uint64_t now_ms() {
        auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
    }
};

} // namespace ynet::actor::net

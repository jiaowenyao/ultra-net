// Cluster — decentralized node discovery via gossip.
// Each node periodically exchanges known-node lists with peers.
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

struct node_info {
    node_id_t id;
    std::string addr;
    uint64_t last_seen = 0;
};

class cluster {
public:
    cluster(node_id_t self_id, const std::string& self_addr)
        : m_self{self_id, self_addr, now_ms()}
        , m_id(self_id) {}

    void add_seed(const std::string& addr) {
        m_seeds.push_back(addr);
    }

    node_id_t self_id() const {
        return m_id;
    }

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

    void mark_seen(node_id_t id, const std::string& addr) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto& node = m_peers[id];
        node.id = id;
        node.addr = addr;
        node.last_seen = now_ms();
    }

    // Build a gossip message containing our known-node list.
    // Uses by-value capture in the helper lambda to avoid dangling references.
    std::vector<uint8_t> build_gossip() {
        auto nodes = live_nodes();
        std::lock_guard<std::mutex> lock(m_mutex);

        std::vector<uint8_t> buffer;
        auto write_value = [&buffer](auto value) {
            const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&value);
            buffer.insert(buffer.end(), ptr, ptr + sizeof(value));
        };

        write_value(m_id);
        write_value(static_cast<uint32_t>(nodes.size()));

        for (auto& node : nodes) {
            write_value(node.id);

            uint32_t addr_len = static_cast<uint32_t>(node.addr.size());
            write_value(addr_len);
            buffer.insert(buffer.end(), node.addr.begin(), node.addr.end());

            write_value(node.last_seen);
        }
        return buffer;
    }

    void apply_gossip(const uint8_t* data, size_t length) {
        auto read_value = [&data](auto& value) {
            std::memcpy(&value, data, sizeof(value));
            data += sizeof(value);
        };

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

            if (id == m_id) {
                continue;
            }

            auto it = m_peers.find(id);
            if (it == m_peers.end() || seen > it->second.last_seen) {
                m_peers[id] = {id, addr, seen};
            }
        }

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

    static uint64_t now_ms() {
        auto now = std::chrono::steady_clock::now().time_since_epoch();
        return static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::milliseconds>(now).count());
    }
};

} // namespace ynet::actor::net

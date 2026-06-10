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

namespace ynet::actor::net {

using node_id_t = uint64_t;

struct node_info {
    node_id_t id; std::string addr; uint64_t last_seen=0;
};

class cluster {
public:
    cluster(node_id_t self_id, const std::string& self_addr)
        : m_self{self_id, self_addr, now_ms()}, m_id(self_id) {}

    void add_seed(const std::string& addr) { m_seeds.push_back(addr); }
    node_id_t self_id() const { return m_id; }

    std::vector<node_info> live_nodes(uint64_t timeout_ms=3000) {
        std::lock_guard<std::mutex> lk(m_mtx);
        std::vector<node_info> out{m_self};
        auto now=now_ms();
        for(auto&[id,info]:m_peers) if(now-info.last_seen<timeout_ms) out.push_back(info);
        return out;
    }

    void mark_seen(node_id_t id, const std::string& addr) {
        std::lock_guard<std::mutex> lk(m_mtx);
        auto& n=m_peers[id]; n.id=id; n.addr=addr; n.last_seen=now_ms();
    }

    std::vector<uint8_t> build_gossip() {
        auto nodes=live_nodes(); std::lock_guard<std::mutex> lk(m_mtx);
        std::vector<uint8_t> buf; auto w=[&](auto v){auto*p=(uint8_t*)&v;buf.insert(buf.end(),p,p+sizeof(v));};
        w(m_id); w((uint32_t)nodes.size());
        for(auto&n:nodes){w(n.id); uint32_t al=(uint32_t)n.addr.size(); w(al);
            buf.insert(buf.end(),n.addr.begin(),n.addr.end()); w(n.last_seen); }
        return buf;
    }

    void apply_gossip(const uint8_t* d, size_t len) {
        auto r=[&](auto& v){std::memcpy(&v,d, sizeof(v));d+=sizeof(v);};
        node_id_t src; uint32_t cnt; r(src); r(cnt);
        std::lock_guard<std::mutex> lk(m_mtx);
        for(uint32_t i=0;i<cnt;++i){node_id_t id;std::string addr;uint64_t seen;r(id);
            uint32_t al;r(al);addr.assign((const char*)d,al);d+=al;r(seen);
            if(id==m_id)continue;
            auto it=m_peers.find(id);if(it==m_peers.end()||seen>it->second.last_seen)m_peers[id]={id,addr,seen};
        }
        auto it=m_peers.find(src);if(it!=m_peers.end())it->second.last_seen=now_ms();
    }

    const std::vector<std::string>& seeds() const { return m_seeds; }

private:
    node_info m_self; node_id_t m_id; std::vector<std::string> m_seeds;
    std::unordered_map<node_id_t,node_info> m_peers; std::mutex m_mtx;
    static uint64_t now_ms(){return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();}
};

} // namespace ynet::actor::net

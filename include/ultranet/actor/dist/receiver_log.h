// 接收方 0x04 日志。receiver_log_path 为空时不创建。
// 记录小端：magic RCV1、type、sender_node_id、msg_id、body_len、body、crc32。
// CRC-32/ISO-HDLC 覆盖 type||sender_node_id||msg_id||body_len||body，不覆盖 magic。
// 数据记录 fsync 之前留在用户态缓冲。满 32 条或这组第一条已过 2ms 才 write 再 fsync。
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

#include "ultranet/actor/core/actor_uri.h"
#include "ultranet/actor/dist/remote_log.h"
#include "ultranet/actor/dist/serialization.h"

namespace ynet::actor {

inline void append_le_u32(std::vector<uint8_t>& out, uint32_t value) {
    uint8_t bytes[4];
    std::memcpy(bytes, &value, sizeof(value));
    out.insert(out.end(), bytes, bytes + sizeof(bytes));
}

inline void append_le_u64(std::vector<uint8_t>& out, uint64_t value) {
    uint8_t bytes[8];
    std::memcpy(bytes, &value, sizeof(value));
    out.insert(out.end(), bytes, bytes + sizeof(bytes));
}

class receiver_log {
public:
    struct key {
        uint64_t sender = 0;
        uint64_t msg_id = 0;
        bool operator==(const key& other) const {
            return sender == other.sender && msg_id == other.msg_id;
        }
    };

    struct key_hash {
        size_t operator()(const key& item) const noexcept {
            return static_cast<size_t>(item.sender ^ (item.msg_id * 0x9E3779B97F4A7C15ull));
        }
    };

    struct ack_item {
        uint64_t reply_conn_id = 0;
        uint64_t msg_id = 0;
    };

    struct flush_result {
        bool ok = true;
        std::vector<key> data_keys;
        std::vector<ack_item> acks;
    };

    explicit receiver_log(std::string path) : m_path(std::move(path)) {
        scan_existing();
        m_fd = ::open(m_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    }

    ~receiver_log() {
        if (m_fd >= 0) {
            ::close(m_fd);
        }
    }

    receiver_log(const receiver_log&) = delete;
    receiver_log& operator=(const receiver_log&) = delete;

    int torn() const { return m_torn; }

    uint64_t fsynced_data_count() const {
        return m_fsynced_data.load(std::memory_order_acquire);
    }

    uint64_t fsynced_handled_count() const {
        return m_fsynced_handled.load(std::memory_order_acquire);
    }

    bool executed(uint64_t sender, uint64_t msg_id) const {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return false;
        }
        return it->second.executed;
    }

    bool handled_durable(uint64_t sender, uint64_t msg_id) const {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return false;
        }
        return it->second.handled;
    }

    bool has_data(uint64_t sender, uint64_t msg_id) const {
        std::lock_guard<std::mutex> lock(m_mu);
        return m_index.find(key{sender, msg_id}) != m_index.end();
    }

    bool persisted(uint64_t sender, uint64_t msg_id) const {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return false;
        }
        return it->second.persisted;
    }

    bool enqueued(uint64_t sender, uint64_t msg_id) const {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return false;
        }
        return it->second.enqueued;
    }

    void set_enqueued(uint64_t sender, uint64_t msg_id, bool value) {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return;
        }
        it->second.enqueued = value;
    }

    void mark_executed(uint64_t sender, uint64_t msg_id) {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return;
        }
        it->second.executed = true;
        it->second.enqueued = false;
    }

    void drop_key(uint64_t sender, uint64_t msg_id) {
        std::lock_guard<std::mutex> lock(m_mu);
        m_index.erase(key{sender, msg_id});
    }

    std::vector<uint8_t> copy_frame(uint64_t sender, uint64_t msg_id) const {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return {};
        }
        return it->second.frame;
    }

    uint64_t reply_conn(uint64_t sender, uint64_t msg_id) const {
        std::lock_guard<std::mutex> lock(m_mu);
        auto it = m_index.find(key{sender, msg_id});
        if (it == m_index.end()) {
            return 0;
        }
        return it->second.reply_conn_id;
    }

    bool buffer_data(uint64_t sender, uint64_t msg_id, std::vector<uint8_t> frame,
                     uint64_t reply_conn_id) {
        std::lock_guard<std::mutex> lock(m_mu);
        key id{sender, msg_id};
        if (m_index.find(id) != m_index.end()) {
            return false;
        }
        entry item;
        item.frame = std::move(frame);
        item.reply_conn_id = reply_conn_id;
        item.persisted = false;
        m_index.emplace(id, item);
        open_group_locked();
        pending_rec rec;
        rec.type = 1;
        rec.sender = sender;
        rec.msg_id = msg_id;
        rec.reply_conn_id = reply_conn_id;
        rec.body = m_index[id].frame;
        m_pending.push_back(std::move(rec));
        return true;
    }

    void buffer_handled(uint64_t sender, uint64_t msg_id, uint64_t reply_conn_id) {
        std::lock_guard<std::mutex> lock(m_mu);
        key id{sender, msg_id};
        auto it = m_index.find(id);
        if (it == m_index.end()) {
            return;
        }
        it->second.reply_conn_id = reply_conn_id;
        if (it->second.handled) {
            return;
        }
        for (const auto& rec : m_pending) {
            if (rec.type == 2 && rec.sender == sender && rec.msg_id == msg_id) {
                return;
            }
        }
        open_group_locked();
        pending_rec rec;
        rec.type = 2;
        rec.sender = sender;
        rec.msg_id = msg_id;
        rec.reply_conn_id = reply_conn_id;
        m_pending.push_back(std::move(rec));
    }

    flush_result flush_ready() {
        std::vector<pending_rec> batch;
        {
            std::lock_guard<std::mutex> lock(m_mu);
            if (m_pending.empty()) {
                return {};
            }
            bool full = m_pending.size() >= 32;
            bool due = false;
            if (m_group_open) {
                auto age = std::chrono::steady_clock::now() - m_group_start;
                due = age >= std::chrono::milliseconds(2);
            }
            if (!full && !due) {
                return {};
            }
            batch.swap(m_pending);
            m_group_open = false;
        }
        return commit_batch(batch);
    }

    struct replay_item {
        uint64_t sender = 0;
        uint64_t msg_id = 0;
        std::vector<uint8_t> frame;
    };

    std::vector<replay_item> take_for_actor(const std::string& type, const std::string& name) {
        std::lock_guard<std::mutex> lock(m_mu);
        std::vector<replay_item> out;
        for (auto& [id, item] : m_index) {
            if (!item.persisted || item.executed || item.enqueued || item.frame.empty()) {
                continue;
            }
            actor_uri uri;
            uint8_t flags = 0;
            uint64_t msg_id = 0;
            uint64_t sender = 0;
            uint64_t msg_hash = 0;
            std::vector<uint8_t> payload;
            if (!dist::unpack_routed(item.frame.data(), item.frame.size(),
                                     uri, flags, msg_id, sender, msg_hash, payload)) {
                continue;
            }
            if (uri.type != type || uri.name != name) {
                continue;
            }
            item.enqueued = true;
            replay_item ready;
            ready.sender = id.sender;
            ready.msg_id = id.msg_id;
            ready.frame = item.frame;
            out.push_back(std::move(ready));
        }
        return out;
    }

    std::vector<key> waiting_keys() const {
        std::lock_guard<std::mutex> lock(m_mu);
        std::vector<key> out;
        for (const auto& [id, item] : m_index) {
            if (item.persisted && !item.executed && !item.enqueued) {
                out.push_back(id);
            }
        }
        return out;
    }

private:
    struct entry {
        std::vector<uint8_t> frame;
        uint64_t reply_conn_id = 0;
        bool persisted = false;
        bool executed = false;
        bool enqueued = false;
        bool handled = false;
    };

    struct pending_rec {
        uint8_t type = 0;
        uint64_t sender = 0;
        uint64_t msg_id = 0;
        uint64_t reply_conn_id = 0;
        std::vector<uint8_t> body;
    };

    void open_group_locked() {
        if (!m_group_open) {
            m_group_open = true;
            m_group_start = std::chrono::steady_clock::now();
        }
    }

    void scan_existing() {
        std::ifstream in(m_path, std::ios::binary);
        if (!in) {
            m_torn = 0;
            return;
        }
        std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                  std::istreambuf_iterator<char>());
        size_t off = 0;
        bool bad = false;
        while (off < bytes.size()) {
            if (bytes.size() - off < 29) {
                bad = true;
                break;
            }
            if (bytes[off] != 0x52 || bytes[off + 1] != 0x43
                || bytes[off + 2] != 0x56 || bytes[off + 3] != 0x31) {
                bad = true;
                break;
            }
            const uint8_t* cursor = bytes.data() + off + 4;
            uint8_t type = cursor[0];
            uint64_t sender = read_le_u64(cursor + 1);
            uint64_t msg_id = read_le_u64(cursor + 9);
            uint32_t body_len = read_le_u32(cursor + 17);
            if (bytes.size() - off < 29u + body_len) {
                bad = true;
                break;
            }
            uint32_t expect = crc32_iso_hdlc(cursor, 21u + body_len);
            uint32_t got = read_le_u32(cursor + 21 + body_len);
            if (expect != got) {
                bad = true;
                break;
            }
            key id{sender, msg_id};
            if (type == 1) {
                if (m_index.find(id) == m_index.end()) {
                    entry item;
                    item.frame.assign(cursor + 21, cursor + 21 + body_len);
                    item.persisted = true;
                    m_index.emplace(id, std::move(item));
                }
            } else if (type == 2) {
                auto it = m_index.find(id);
                if (it != m_index.end()) {
                    it->second.handled = true;
                }
            }
            off += 29u + body_len;
        }
        if (bad || off != bytes.size()) {
            m_torn = 1;
        }
    }

    flush_result commit_batch(const std::vector<pending_rec>& batch) {
        flush_result result;
        if (m_fd < 0) {
            rollback(batch);
            result.ok = false;
            return result;
        }
        std::vector<uint8_t> bytes;
        for (const auto& rec : batch) {
            std::vector<uint8_t> covered;
            covered.push_back(rec.type);
            append_le_u64(covered, rec.sender);
            append_le_u64(covered, rec.msg_id);
            append_le_u32(covered, static_cast<uint32_t>(rec.body.size()));
            covered.insert(covered.end(), rec.body.begin(), rec.body.end());
            uint32_t crc = crc32_iso_hdlc(covered.data(), covered.size());
            bytes.push_back(0x52);
            bytes.push_back(0x43);
            bytes.push_back(0x56);
            bytes.push_back(0x31);
            bytes.insert(bytes.end(), covered.begin(), covered.end());
            append_le_u32(bytes, crc);
        }
        if (!write_full(bytes.data(), bytes.size()) || ::fsync(m_fd) != 0) {
            rollback(batch);
            result.ok = false;
            return result;
        }
        std::lock_guard<std::mutex> lock(m_mu);
        for (const auto& rec : batch) {
            auto it = m_index.find(key{rec.sender, rec.msg_id});
            if (rec.type == 1) {
                if (it != m_index.end()) {
                    it->second.persisted = true;
                    if (!it->second.executed && !it->second.enqueued) {
                        result.data_keys.push_back(key{rec.sender, rec.msg_id});
                    }
                }
                m_fsynced_data.fetch_add(1, std::memory_order_release);
            } else if (rec.type == 2) {
                if (it != m_index.end()) {
                    it->second.handled = true;
                }
                m_fsynced_handled.fetch_add(1, std::memory_order_release);
                result.acks.push_back(ack_item{rec.reply_conn_id, rec.msg_id});
            }
        }
        return result;
    }

    void rollback(const std::vector<pending_rec>& batch) {
        std::lock_guard<std::mutex> lock(m_mu);
        for (const auto& rec : batch) {
            if (rec.type != 1) {
                continue;
            }
            auto it = m_index.find(key{rec.sender, rec.msg_id});
            if (it != m_index.end() && !it->second.persisted) {
                m_index.erase(it);
            }
        }
    }

    bool write_full(const uint8_t* data, size_t n) {
        size_t off = 0;
        while (off < n) {
            ssize_t wrote = ::write(m_fd, data + off, n - off);
            if (wrote < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            if (wrote == 0) {
                return false;
            }
            off += static_cast<size_t>(wrote);
        }
        return true;
    }

    std::string m_path;
    int m_fd = -1;
    int m_torn = 0;
    mutable std::mutex m_mu;
    std::unordered_map<key, entry, key_hash> m_index;
    std::vector<pending_rec> m_pending;
    bool m_group_open = false;
    std::chrono::steady_clock::time_point m_group_start{};
    std::atomic<uint64_t> m_fsynced_data{0};
    std::atomic<uint64_t> m_fsynced_handled{0};
};

} // namespace ynet::actor

// 远端 0x04 的追加日志。只在 remote_log_path 非空时打开。
// 记录小端：magic ULR1、type、msg_id、body_len、body、crc32。
// CRC-32/ISO-HDLC 覆盖 type||msg_id||body_len||body，不覆盖 magic。
#pragma once

#include <cstdint>
#include <cstring>
#include <fstream>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace ynet::actor {

inline uint32_t crc32_iso_hdlc(const uint8_t* data, size_t len) {
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; ++i) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; ++bit) {
            if ((crc & 1u) != 0u) {
                crc = (crc >> 1) ^ 0xEDB88320u;
            } else {
                crc = crc >> 1;
            }
        }
    }
    return crc ^ 0xFFFFFFFFu;
}

struct remote_log_scan {
    uint64_t complete_data = 0;
    int torn = 0;
    std::vector<std::vector<uint8_t>> unacked_frames;
};

inline uint32_t read_le_u32(const uint8_t* p) {
    uint32_t v = 0;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline uint64_t read_le_u64(const uint8_t* p) {
    uint64_t v = 0;
    std::memcpy(&v, p, sizeof(v));
    return v;
}

inline remote_log_scan scan_remote_log(const std::string& path) {
    remote_log_scan out;
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        return out;
    }
    std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                              std::istreambuf_iterator<char>());
    struct record {
        uint8_t type = 0;
        uint64_t msg_id = 0;
        std::vector<uint8_t> body;
    };
    std::vector<record> records;
    size_t off = 0;
    bool bad = false;
    while (off < bytes.size()) {
        if (bytes.size() - off < 21) {
            bad = true;
            break;
        }
        if (bytes[off] != 0x55 || bytes[off + 1] != 0x4C
            || bytes[off + 2] != 0x52 || bytes[off + 3] != 0x31) {
            bad = true;
            break;
        }
        const uint8_t* cursor = bytes.data() + off + 4;
        uint8_t type = cursor[0];
        uint64_t msg_id = read_le_u64(cursor + 1);
        uint32_t body_len = read_le_u32(cursor + 9);
        if (bytes.size() - off < 21u + body_len) {
            bad = true;
            break;
        }
        uint32_t expect = crc32_iso_hdlc(cursor, 13u + body_len);
        uint32_t got = read_le_u32(cursor + 13 + body_len);
        if (expect != got) {
            bad = true;
            break;
        }
        record rec;
        rec.type = type;
        rec.msg_id = msg_id;
        if (body_len > 0) {
            rec.body.assign(cursor + 13, cursor + 13 + body_len);
        }
        records.push_back(std::move(rec));
        off += 21u + body_len;
    }
    if (bad || off != bytes.size()) {
        out.torn = 1;
    }
    std::unordered_set<uint64_t> acked;
    for (const auto& rec : records) {
        if (rec.type == 2) {
            acked.insert(rec.msg_id);
        }
    }
    for (const auto& rec : records) {
        if (rec.type != 1) {
            continue;
        }
        out.complete_data++;
        if (acked.find(rec.msg_id) == acked.end()) {
            out.unacked_frames.push_back(rec.body);
        }
    }
    return out;
}

class remote_log {
public:
    explicit remote_log(std::string path) : m_path(std::move(path)) {
        std::ifstream in(m_path, std::ios::binary);
        if (in) {
            std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(in)),
                                      std::istreambuf_iterator<char>());
            size_t off = 0;
            while (off + 21 <= bytes.size()) {
                if (bytes[off] != 0x55 || bytes[off + 1] != 0x4C
                    || bytes[off + 2] != 0x52 || bytes[off + 3] != 0x31) {
                    break;
                }
                const uint8_t* cursor = bytes.data() + off + 4;
                uint32_t body_len = read_le_u32(cursor + 9);
                if (off + 21u + body_len > bytes.size()) {
                    break;
                }
                uint32_t expect = crc32_iso_hdlc(cursor, 13u + body_len);
                uint32_t got = read_le_u32(cursor + 13 + body_len);
                if (expect != got) {
                    break;
                }
                if (cursor[0] == 1) {
                    m_data_ids.insert(read_le_u64(cursor + 1));
                }
                off += 21u + body_len;
            }
        }
        m_fd = ::open(m_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    }

    ~remote_log() {
        if (m_fd >= 0) {
            ::close(m_fd);
        }
    }

    remote_log(const remote_log&) = delete;
    remote_log& operator=(const remote_log&) = delete;

    bool append_data(uint64_t msg_id, const std::vector<uint8_t>& body) {
        std::lock_guard<std::mutex> lock(m_mu);
        if (m_data_ids.find(msg_id) != m_data_ids.end()) {
            return true;
        }
        if (!append_record_locked(1, msg_id, body.data(),
                                  static_cast<uint32_t>(body.size()))) {
            return false;
        }
        m_data_ids.insert(msg_id);
        return true;
    }

    bool append_ack(uint64_t msg_id) {
        std::lock_guard<std::mutex> lock(m_mu);
        return append_record_locked(2, msg_id, nullptr, 0);
    }

private:
    bool append_record_locked(uint8_t type, uint64_t msg_id,
                              const uint8_t* body, uint32_t body_len) {
        if (m_fd < 0) {
            return false;
        }
        std::vector<uint8_t> rec;
        rec.push_back(0x55);
        rec.push_back(0x4C);
        rec.push_back(0x52);
        rec.push_back(0x31);
        rec.push_back(type);
        uint8_t id_bytes[8];
        std::memcpy(id_bytes, &msg_id, sizeof(msg_id));
        rec.insert(rec.end(), id_bytes, id_bytes + 8);
        uint8_t len_bytes[4];
        std::memcpy(len_bytes, &body_len, sizeof(body_len));
        rec.insert(rec.end(), len_bytes, len_bytes + 4);
        if (body_len > 0 && body != nullptr) {
            rec.insert(rec.end(), body, body + body_len);
        }
        uint32_t crc = crc32_iso_hdlc(rec.data() + 4, rec.size() - 4);
        uint8_t crc_bytes[4];
        std::memcpy(crc_bytes, &crc, sizeof(crc));
        rec.insert(rec.end(), crc_bytes, crc_bytes + 4);
        size_t written = 0;
        while (written < rec.size()) {
            ssize_t n = ::write(m_fd, rec.data() + written, rec.size() - written);
            if (n < 0) {
                return false;
            }
            written += static_cast<size_t>(n);
        }
        if (::fsync(m_fd) != 0) {
            return false;
        }
        return true;
    }

    std::string m_path;
    int m_fd = -1;
    std::mutex m_mu;
    std::unordered_set<uint64_t> m_data_ids;
};

} // namespace ynet::actor

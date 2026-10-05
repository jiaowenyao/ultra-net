// 发送与采集。TCP 用本机序 uint32 长度前缀，UDP 一个数据报一条记录。
// 调用方拥有 fd。本接口不 bind、不 close、不落盘。
#pragma once

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <vector>

#include <sys/socket.h>

#include "ultranet/coroutine/task.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/recvfrom.hpp"
#include "ultranet/net/sendto.hpp"
#include "ultranet/net/write.hpp"

namespace ynet::async::collect {

struct record_span {
    const uint8_t* data = nullptr;
    size_t size = 0;
    // TCP 为 nullptr。UDP 只在 sink 返回前有效。
    const sockaddr* peer = nullptr;
    socklen_t peer_len = 0;
};

class record_batch {
public:
    record_batch() = default;
    record_batch(const record_batch&) = delete;
    record_batch& operator=(const record_batch&) = delete;

    size_t size() const { return m_items.size(); }

    record_span at(size_t index) const {
        if (index >= m_items.size()) {
            return {};
        }
        const item& one = m_items[index];
        record_span span;
        span.size = one.size;
        if (one.size > 0) {
            span.data = m_bytes.data() + one.offset;
        }
        if (one.peer_len > 0) {
            span.peer = reinterpret_cast<const sockaddr*>(&m_peers[one.peer_index]);
            span.peer_len = one.peer_len;
        }
        return span;
    }

    size_t payload_bytes() const { return m_payload; }

    void clear() {
        m_bytes.clear();
        m_peers.clear();
        m_items.clear();
        m_payload = 0;
    }

    void add(const uint8_t* data, size_t n, const sockaddr* peer, socklen_t peer_len) {
        item one;
        one.offset = m_bytes.size();
        one.size = n;
        if (n > 0 && data != nullptr) {
            m_bytes.insert(m_bytes.end(), data, data + n);
        }
        if (peer != nullptr && peer_len > 0) {
            sockaddr_storage stored{};
            socklen_t copy_len = peer_len;
            if (copy_len > sizeof(stored)) {
                copy_len = static_cast<socklen_t>(sizeof(stored));
            }
            std::memcpy(&stored, peer, copy_len);
            one.peer_index = m_peers.size();
            one.peer_len = copy_len;
            m_peers.push_back(stored);
        }
        m_items.push_back(one);
        m_payload += n;
    }

private:
    struct item {
        size_t offset = 0;
        size_t size = 0;
        size_t peer_index = 0;
        socklen_t peer_len = 0;
    };

    std::vector<uint8_t> m_bytes;
    std::vector<sockaddr_storage> m_peers;
    std::vector<item> m_items;
    size_t m_payload = 0;
};

struct collect_config {
    size_t batch_records = 256;
    size_t batch_bytes = 256 * 1024;
    std::chrono::milliseconds batch_delay{2};
    size_t max_record_bytes = 64 * 1024;
};

struct collect_stats {
    uint64_t records = 0;
    uint64_t batches = 0;
    uint64_t record_bytes = 0;
    uint64_t dropped_oversize = 0;
};

struct collect_result {
    collect_stats stats;
    int err = 0;
};

struct emit_result {
    uint64_t records = 0;
    uint64_t record_bytes = 0;
    uint64_t writes = 0;
    int err = 0;
};

using batch_sink = std::function<bool(const record_batch&)>;

inline bool config_ok(const collect_config& cfg) {
    return cfg.batch_records > 0 && cfg.batch_bytes > 0 && cfg.max_record_bytes > 0;
}

inline Task<bool> write_complete(int fd, const uint8_t* data, size_t n, int& err) {
    size_t off = 0;
    while (off < n) {
        auto wrote = co_await io::Write(fd, data + off, n - off);
        if (!wrote) {
            err = wrote.error().value();
            co_return false;
        }
        if (*wrote == 0) {
            err = EIO;
            co_return false;
        }
        off += *wrote;
    }
    co_return true;
}

struct emit_group {
    std::vector<uint8_t> buf;
    size_t grouped = 0;
    size_t grouped_wire = 0;
    size_t grouped_payload = 0;
};

inline Task<bool> flush_emit_group(int fd, emit_group& group, emit_result& result) {
    if (group.buf.empty()) {
        co_return true;
    }
    bool ok = co_await write_complete(fd, group.buf.data(), group.buf.size(), result.err);
    if (!ok) {
        co_return false;
    }
    result.records += group.grouped;
    result.record_bytes += group.grouped_payload;
    result.writes += 1;
    group.buf.clear();
    group.grouped = 0;
    group.grouped_wire = 0;
    group.grouped_payload = 0;
    co_return true;
}

inline Task<emit_result> emit_tcp(int fd, collect_config cfg,
                                  const record_span* records, size_t count) {
    emit_result result;
    if (count == 0) {
        co_return result;
    }
    if (records == nullptr || !config_ok(cfg)) {
        result.err = EINVAL;
        co_return result;
    }
    emit_group group;
    for (size_t i = 0; i < count; ++i) {
        const record_span& rec = records[i];
        if (rec.size == 0 || rec.size > cfg.max_record_bytes || rec.data == nullptr) {
            if (!co_await flush_emit_group(fd, group, result)) {
                co_return result;
            }
            result.err = EMSGSIZE;
            co_return result;
        }
        size_t wire = 4 + rec.size;
        if (group.grouped > 0
            && (group.grouped + 1 > cfg.batch_records
                || group.grouped_wire + wire > cfg.batch_bytes)) {
            if (!co_await flush_emit_group(fd, group, result)) {
                co_return result;
            }
        }
        uint32_t len = static_cast<uint32_t>(rec.size);
        const auto* len_bytes = reinterpret_cast<const uint8_t*>(&len);
        group.buf.insert(group.buf.end(), len_bytes, len_bytes + sizeof(len));
        group.buf.insert(group.buf.end(), rec.data, rec.data + rec.size);
        group.grouped += 1;
        group.grouped_wire += wire;
        group.grouped_payload += rec.size;
        if (group.grouped >= cfg.batch_records || group.grouped_wire >= cfg.batch_bytes) {
            if (!co_await flush_emit_group(fd, group, result)) {
                co_return result;
            }
        }
    }
    if (!co_await flush_emit_group(fd, group, result)) {
        co_return result;
    }
    co_return result;
}

inline Task<emit_result> emit_udp(int fd, collect_config cfg,
                                  const record_span* records, size_t count) {
    emit_result result;
    if (count == 0) {
        co_return result;
    }
    if (records == nullptr || !config_ok(cfg)) {
        result.err = EINVAL;
        co_return result;
    }
    const char dummy = 0;
    for (size_t i = 0; i < count; ++i) {
        const record_span& rec = records[i];
        if (rec.peer == nullptr || rec.peer_len == 0) {
            result.err = EINVAL;
            co_return result;
        }
        if (rec.size > cfg.max_record_bytes) {
            result.err = EMSGSIZE;
            co_return result;
        }
        const void* ptr = rec.data != nullptr ? static_cast<const void*>(rec.data) : &dummy;
        auto wrote = co_await io::SendTo(fd, ptr, rec.size, rec.peer, rec.peer_len);
        if (!wrote) {
            result.err = wrote.error().value();
            co_return result;
        }
        if (*wrote != rec.size) {
            result.err = EIO;
            co_return result;
        }
        result.records += 1;
        result.record_bytes += rec.size;
        result.writes += 1;
    }
    co_return result;
}

inline Task<collect_result> collect_tcp(int fd, collect_config cfg, batch_sink sink) {
    collect_result result;
    if (!sink || !config_ok(cfg)) {
        result.err = EINVAL;
        co_return result;
    }
    std::vector<uint8_t> pending;
    std::vector<uint8_t> io_buf(64 * 1024);
    record_batch batch;
    bool batch_open = false;
    auto batch_started = std::chrono::steady_clock::now();

    auto deliver = [&]() -> bool {
        if (batch.size() == 0) {
            return true;
        }
        result.stats.records += batch.size();
        result.stats.record_bytes += batch.payload_bytes();
        result.stats.batches += 1;
        bool keep = sink(batch);
        batch.clear();
        batch_open = false;
        return keep;
    };

    while (true) {
        io::Read reader(fd, io_buf.data(), io_buf.size());
        if (batch_open) {
            auto age = std::chrono::steady_clock::now() - batch_started;
            auto remain = cfg.batch_delay - std::chrono::duration_cast<std::chrono::milliseconds>(age);
            if (remain <= std::chrono::milliseconds(0)) {
                if (!deliver()) {
                    result.err = 0;
                    co_return result;
                }
                continue;
            }
            if (remain < std::chrono::milliseconds(1)) {
                remain = std::chrono::milliseconds(1);
            }
            reader.with_timeout(remain);
        }
        auto got = co_await reader;
        if (!got) {
            int code = got.error().value();
            if (code == ETIMEDOUT) {
                if (batch.size() > 0) {
                    if (!deliver()) {
                        result.err = 0;
                        co_return result;
                    }
                }
                continue;
            }
            deliver();
            result.err = code;
            co_return result;
        }
        if (*got == 0) {
            deliver();
            if (!pending.empty()) {
                result.err = EPROTO;
            }
            co_return result;
        }
        pending.insert(pending.end(), io_buf.data(), io_buf.data() + *got);
        size_t off = 0;
        bool stop = false;
        while (!stop && pending.size() - off >= 4) {
            uint32_t len = 0;
            std::memcpy(&len, pending.data() + off, sizeof(len));
            if (len == 0 || len > cfg.max_record_bytes) {
                deliver();
                result.err = EMSGSIZE;
                stop = true;
                break;
            }
            if (pending.size() - off < 4u + len) {
                break;
            }
            if (batch.size() > 0
                && (batch.size() + 1 > cfg.batch_records
                    || batch.payload_bytes() + len > cfg.batch_bytes)) {
                if (!deliver()) {
                    result.err = 0;
                    stop = true;
                    break;
                }
            }
            if (batch.size() == 0) {
                batch_started = std::chrono::steady_clock::now();
                batch_open = true;
            }
            batch.add(pending.data() + off + 4, len, nullptr, 0);
            off += 4u + len;
            bool full = batch.size() >= cfg.batch_records
                || batch.payload_bytes() >= cfg.batch_bytes
                || cfg.batch_delay.count() == 0;
            if (full) {
                if (!deliver()) {
                    result.err = 0;
                    stop = true;
                    break;
                }
            }
        }
        if (stop) {
            co_return result;
        }
        if (off > 0) {
            pending.erase(pending.begin(), pending.begin() + static_cast<std::ptrdiff_t>(off));
        }
    }
}

inline Task<collect_result> collect_udp(int fd, collect_config cfg, batch_sink sink) {
    collect_result result;
    if (!sink || !config_ok(cfg)) {
        result.err = EINVAL;
        co_return result;
    }
    std::vector<uint8_t> io_buf(cfg.max_record_bytes);
    record_batch batch;
    bool batch_open = false;
    auto batch_started = std::chrono::steady_clock::now();

    auto deliver = [&]() -> bool {
        if (batch.size() == 0) {
            return true;
        }
        result.stats.records += batch.size();
        result.stats.record_bytes += batch.payload_bytes();
        result.stats.batches += 1;
        bool keep = sink(batch);
        batch.clear();
        batch_open = false;
        return keep;
    };

    while (true) {
        io::RecvFrom reader(fd, io_buf.data(), io_buf.size());
        if (batch_open) {
            auto age = std::chrono::steady_clock::now() - batch_started;
            auto remain = cfg.batch_delay - std::chrono::duration_cast<std::chrono::milliseconds>(age);
            if (remain <= std::chrono::milliseconds(0)) {
                if (!deliver()) {
                    result.err = 0;
                    co_return result;
                }
                continue;
            }
            if (remain < std::chrono::milliseconds(1)) {
                remain = std::chrono::milliseconds(1);
            }
            reader.with_timeout(remain);
        }
        auto got = co_await reader;
        if (!got) {
            int code = got.error().value();
            if (code == ETIMEDOUT) {
                if (batch.size() > 0) {
                    if (!deliver()) {
                        result.err = 0;
                        co_return result;
                    }
                }
                continue;
            }
            deliver();
            result.err = code;
            co_return result;
        }
        if ((reader.flags() & MSG_TRUNC) != 0) {
            result.stats.dropped_oversize += 1;
            continue;
        }
        if (batch.size() > 0
            && (batch.size() + 1 > cfg.batch_records
                || batch.payload_bytes() + *got > cfg.batch_bytes)) {
            if (!deliver()) {
                result.err = 0;
                co_return result;
            }
        }
        if (batch.size() == 0) {
            batch_started = std::chrono::steady_clock::now();
            batch_open = true;
        }
        const sockaddr* peer = reinterpret_cast<const sockaddr*>(&reader.source_addr());
        batch.add(io_buf.data(), *got, peer, reader.source_addr_len());
        bool full = batch.size() >= cfg.batch_records
            || batch.payload_bytes() >= cfg.batch_bytes
            || cfg.batch_delay.count() == 0;
        if (full) {
            if (!deliver()) {
                result.err = 0;
                co_return result;
            }
        }
    }
}

} // namespace ynet::async::collect

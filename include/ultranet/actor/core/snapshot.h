// 主动快照。写临时文件，fsync，rename，再 fsync 目录。
// kill -9 之后只能读到最后一次文件和目录都 fsync 成功的内容。
#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace ynet::actor {

inline bool save_snapshot(const std::string& path, const void* data, size_t len) {
    std::string tmp = path + ".tmp";
    int fd = ::open(tmp.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        return false;
    }
    const auto* bytes = static_cast<const uint8_t*>(data);
    size_t offset = 0;
    while (offset < len) {
        ssize_t wrote = ::write(fd, bytes + offset, len - offset);
        if (wrote <= 0) {
            ::close(fd);
            return false;
        }
        offset += static_cast<size_t>(wrote);
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        return false;
    }
    if (::close(fd) != 0) {
        return false;
    }
    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        return false;
    }
    auto slash = path.find_last_of('/');
    std::string dir = (slash == std::string::npos) ? std::string(".") : path.substr(0, slash);
    if (dir.empty()) {
        dir = ".";
    }
    int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd < 0) {
        return false;
    }
    int synced = ::fsync(dfd);
    ::close(dfd);
    return synced == 0;
}

inline bool load_snapshot(const std::string& path, std::vector<uint8_t>& out) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }
    struct stat st {};
    if (::fstat(fd, &st) != 0 || st.st_size < 0) {
        ::close(fd);
        return false;
    }
    out.resize(static_cast<size_t>(st.st_size));
    size_t offset = 0;
    while (offset < out.size()) {
        ssize_t got = ::read(fd, out.data() + offset, out.size() - offset);
        if (got <= 0) {
            ::close(fd);
            return false;
        }
        offset += static_cast<size_t>(got);
    }
    ::close(fd);
    return true;
}

} // namespace ynet::actor

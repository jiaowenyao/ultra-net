// 父进程在子进程 fsync 快照并写出 ready 之后发 SIGKILL，再启动恢复进程。
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>

#include "ultranet/actor/core/snapshot.h"

namespace {

bool wait_for_file(const std::filesystem::path& path, int ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (std::filesystem::exists(path)) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return std::filesystem::exists(path);
}

int write_mode(const std::string& dir) {
    uint64_t value = 42;
    std::string snap = dir + "/state.bin";
    if (!ynet::actor::save_snapshot(snap, &value, sizeof(value))) {
        return 2;
    }
    std::string ready = dir + "/ready";
    int fd = ::open(ready.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) {
        return 3;
    }
    if (::fsync(fd) != 0) {
        ::close(fd);
        return 4;
    }
    ::close(fd);
    int dfd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dfd >= 0) {
        ::fsync(dfd);
        ::close(dfd);
    }
    for (;;) {
        ::pause();
    }
}

int read_mode(const std::string& dir) {
    std::vector<uint8_t> bytes;
    if (!ynet::actor::load_snapshot(dir + "/state.bin", bytes)) {
        std::cerr << "load failed\n";
        return 5;
    }
    if (bytes.size() != sizeof(uint64_t)) {
        std::cerr << "size " << bytes.size() << "\n";
        return 6;
    }
    uint64_t value = 0;
    std::memcpy(&value, bytes.data(), sizeof(value));
    std::cout << "restored " << value << "\n";
    return value == 42 ? 0 : 7;
}

} // namespace

int main(int argc, char** argv) {
    std::string dir = argc > 2 ? argv[2] : "/tmp/ultra-snapshot-crash";
    if (argc > 1 && std::strcmp(argv[1], "write") == 0) {
        return write_mode(dir);
    }
    if (argc > 1 && std::strcmp(argv[1], "read") == 0) {
        return read_mode(dir);
    }

    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    pid_t child = ::fork();
    if (child == 0) {
        execl(argv[0], argv[0], "write", dir.c_str(), nullptr);
        _exit(127);
    }
    if (child < 0) {
        return 8;
    }
    if (!wait_for_file(std::filesystem::path(dir) / "ready", 3000)) {
        ::kill(child, SIGKILL);
        ::waitpid(child, nullptr, 0);
        std::cerr << "ready file missing\n";
        return 9;
    }
    if (::kill(child, SIGKILL) != 0) {
        return 10;
    }
    int status = 0;
    ::waitpid(child, &status, 0);
    if (!WIFSIGNALED(status) || WTERMSIG(status) != SIGKILL) {
        std::cerr << "child was not SIGKILL\n";
        return 11;
    }

    pid_t reader = ::fork();
    if (reader == 0) {
        execl(argv[0], argv[0], "read", dir.c_str(), nullptr);
        _exit(127);
    }
    int read_status = 0;
    ::waitpid(reader, &read_status, 0);
    if (!WIFEXITED(read_status) || WEXITSTATUS(read_status) != 0) {
        std::cerr << "restore failed\n";
        return 12;
    }
    std::cout << "snapshot sigkill ok\n";
    return 0;
}

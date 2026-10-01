// 两个操作系统进程的远端送达、断连重发、ask、成员和在途条数。
// 结果写到文件里，父进程只转述子进程写下的数字。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>

#include "ultranet/actor.hpp"
#include "ultranet/actor/core/snapshot.h"

using namespace ynet::actor;

struct tick_msg {
    uint64_t id = 0;
};

struct ask_q {
    int value = 0;
};
struct ask_a {
    int value = 0;
};

class Box : public actor<Box> {
public:
    std::mutex mu;
    std::unordered_set<uint64_t> seen;
    int total = 0;
    int closed_fds = 0;
    bool drop_after_100 = false;
    bool reply_asks = false;
    bool kill_on_first = false;
    bool dropped = false;

    Box() {
        register_handler<tick_msg>([this](const tick_msg& msg) {
            if (kill_on_first) {
                ::raise(SIGKILL);
            }
            bool do_drop = false;
            {
                std::lock_guard<std::mutex> lock(mu);
                total++;
                seen.insert(msg.id);
                if (drop_after_100 && !dropped && static_cast<int>(seen.size()) >= 100) {
                    dropped = true;
                    do_drop = true;
                }
            }
            if (do_drop && system() != nullptr) {
                int closed = static_cast<int>(system()->drop_inbound_connections());
                std::lock_guard<std::mutex> lock(mu);
                closed_fds = closed;
            }
        });
        register_handler<ask_q>([this](const ask_q& q) {
            if (reply_asks) {
                reply(ask_a{q.value + 1});
            }
        });
    }

    void snapshot(int& unique, int& deliveries, int& closed) {
        std::lock_guard<std::mutex> lock(mu);
        unique = static_cast<int>(seen.size());
        deliveries = total;
        closed = closed_fds;
    }
};

namespace {

void write_text(const std::filesystem::path& path, const std::string& body) {
    auto tmp = path;
    tmp += ".tmp";
    {
        std::ofstream out(tmp);
        out << body;
    }
    std::filesystem::rename(tmp, path);
}

void write_text_fsync(const std::filesystem::path& path, const std::string& body) {
    auto tmp = path;
    tmp += ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return;
    }
    size_t off = 0;
    while (off < body.size()) {
        ssize_t n = ::write(fd, body.data() + off, body.size() - off);
        if (n < 0) {
            break;
        }
        off += static_cast<size_t>(n);
    }
    ::fsync(fd);
    ::close(fd);
    std::filesystem::rename(tmp, path);
}

std::string read_text(const std::filesystem::path& path) {
    std::ifstream in(path);
    std::string body;
    std::getline(in, body);
    return body;
}

bool wait_file(const std::filesystem::path& path, int ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (std::filesystem::exists(path) && std::filesystem::file_size(path) > 0) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return std::filesystem::exists(path) && std::filesystem::file_size(path) > 0;
}

system_config base_cfg(uint64_t node_timeout, uint64_t ack_timeout);
actor_ref<Box> wait_box(actor_system& sys, int ms);

void docker_receiver(const std::filesystem::path& dir, const std::string& host) {
    auto cfg = base_cfg(8000, 60000);
    cfg.advertise_host = host;
    actor_system sys(cfg);
    auto box = sys.spawn<Box>("box");
    box.get()->reply_asks = true;
    write_text_fsync(dir / "port", std::to_string(sys.actual_port()));
    write_text_fsync(dir / "adv_recv", sys.advertised_addr());
    auto start = std::chrono::steady_clock::now();
    while (!std::filesystem::exists(dir / "stop")) {
        int unique = 0;
        int deliveries = 0;
        int closed = 0;
        box.get()->snapshot(unique, deliveries, closed);
        write_text(dir / "recv",
                   std::to_string(unique) + " " + std::to_string(deliveries));
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(60)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    int unique = 0;
    int deliveries = 0;
    int closed = 0;
    box.get()->snapshot(unique, deliveries, closed);
    write_text_fsync(dir / "recv",
                     std::to_string(unique) + " " + std::to_string(deliveries));
}

int docker_sender(const std::filesystem::path& dir, const std::string& self_host,
                  const std::string& peer_host, int count) {
    if (!wait_file(dir / "port", 10000)) {
        write_text(dir / "send", "noport 0 0 0 0");
        return 2;
    }
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(8000, 60000);
    cfg.advertise_host = self_host;
    cfg.seed_nodes.push_back(peer_host + ":" + port);
    actor_system sys(cfg);
    write_text_fsync(dir / "adv_send", sys.advertised_addr());
    auto ref = wait_box(sys, 15000);
    if (!ref.is_valid()) {
        write_text(dir / "send", "nobox 0 0 0 0");
        return 2;
    }
    int accepted = 0;
    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < count; ++i) {
        if (ref.send(tick_msg{static_cast<uint64_t>(i)})) {
            accepted++;
        }
    }
    auto deadline = t0 + std::chrono::seconds(30);
    int unique = 0;
    int deliveries = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream in(dir / "recv");
        in >> unique >> deliveries;
        if (unique == accepted && accepted > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    auto us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t0).count();
    std::vector<int64_t> ask_us;
    for (int i = 0; i < 50; ++i) {
        auto a0 = std::chrono::steady_clock::now();
        auto got = ref.ask<ask_a>(ask_q{i}, std::chrono::milliseconds(1000));
        auto dt = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - a0).count();
        if (got.has_value() && got->value == i + 1) {
            ask_us.push_back(dt);
        }
    }
    int64_t ask_p50 = 0;
    if (!ask_us.empty()) {
        std::sort(ask_us.begin(), ask_us.end());
        ask_p50 = ask_us[ask_us.size() / 2];
    }
    bool live = false;
    for (const auto& node : sys.current_live_nodes()) {
        if (node.id != sys.self_node_id()) {
            live = true;
        }
    }
    write_text_fsync(dir / "send",
                     std::to_string(accepted) + " " + std::to_string(unique) + " "
                     + std::to_string(deliveries) + " " + std::to_string(us) + " "
                     + std::to_string(ask_us.size()) + " " + std::to_string(ask_p50) + " "
                     + (live ? "live" : "dead"));
    if (accepted != count || unique != accepted || deliveries != unique
        || ask_us.size() != 50) {
        return 3;
    }
    return 0;
}

system_config base_cfg(uint64_t node_timeout, uint64_t ack_timeout) {
    system_config cfg;
    cfg.num_threads = 2;
    cfg.listen_port = 0;
    cfg.gossip_interval_ms = 200;
    cfg.node_timeout_ms = node_timeout;
    cfg.remote_ack_timeout_ms = ack_timeout;
    cfg.remote_accept_timeout_ms = 5000;
    return cfg;
}

std::string box_uri() {
    return std::string("ultra://*/") + typeid(Box).name() + "/box";
}

actor_ref<Box> wait_box(actor_system& sys, int ms) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        auto ref = sys.find<Box>(box_uri());
        if (ref.is_valid()) {
            return ref;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return {};
}

void receiver_main(const std::filesystem::path& dir, bool drop, bool reply, bool die) {
    auto cfg = base_cfg(3000, 60000);
    actor_system sys(cfg);
    auto box = sys.spawn<Box>("box");
    box.get()->drop_after_100 = drop;
    box.get()->reply_asks = reply;
    box.get()->kill_on_first = die;
    write_text(dir / "port", std::to_string(sys.actual_port()));
    write_text(dir / "self", std::to_string(sys.self_node_id()));
    auto start = std::chrono::steady_clock::now();
    while (!std::filesystem::exists(dir / "stop")) {
        int unique = 0;
        int deliveries = 0;
        int closed = 0;
        box.get()->snapshot(unique, deliveries, closed);
        write_text(dir / "recv",
                   std::to_string(unique) + " " + std::to_string(deliveries)
                   + " " + std::to_string(closed) + " "
                   + std::to_string(sys.wire_skip_count()));
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(25)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    int unique = 0;
    int deliveries = 0;
    int closed = 0;
    box.get()->snapshot(unique, deliveries, closed);
    write_text(dir / "recv",
               std::to_string(unique) + " " + std::to_string(deliveries)
               + " " + std::to_string(closed) + " "
               + std::to_string(sys.wire_skip_count()));
}

int sender_flood(const std::filesystem::path& dir, int count, uint64_t ack_timeout) {
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(3000, ack_timeout);
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        write_text(dir / "send", "0 0 0 0");
        return 2;
    }
    int accepted = 0;
    for (int i = 0; i < count; ++i) {
        if (ref.send(tick_msg{static_cast<uint64_t>(i)})) {
            accepted++;
        }
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    int unique = 0;
    int deliveries = 0;
    int closed = 0;
    int skips = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream in(dir / "recv");
        in >> unique >> deliveries >> closed >> skips;
        if (unique == accepted && accepted > 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    write_text(dir / "send",
               std::to_string(accepted) + " " + std::to_string(sys.resent_count())
               + " " + std::to_string(sys.ack_timeout_resend_count())
               + " " + std::to_string(closed));
    write_text(dir / "final_recv",
               std::to_string(unique) + " " + std::to_string(deliveries)
               + " " + std::to_string(skips));
    return 0;
}

int ask_timeout_sender(const std::filesystem::path& dir) {
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(3000, 60000);
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        write_text(dir / "ask", "missing");
        return 2;
    }
    auto start = std::chrono::steady_clock::now();
    auto got = ref.ask<ask_a>(ask_q{1}, std::chrono::milliseconds(200));
    auto wall = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    write_text(dir / "ask",
               std::string(got.has_value() ? "value" : "nullopt")
               + " " + std::to_string(wall));
    return 0;
}

int ask_p50_sender(const std::filesystem::path& dir) {
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(3000, 60000);
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        write_text(dir / "p50", "0 0");
        return 2;
    }
    std::vector<int64_t> us;
    us.reserve(200);
    int ok = 0;
    for (int i = 0; i < 200; ++i) {
        auto start = std::chrono::steady_clock::now();
        auto got = ref.ask<ask_a>(ask_q{i}, std::chrono::milliseconds(1000));
        auto dt = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start).count();
        if (got.has_value() && got->value == i + 1) {
            us.push_back(dt);
            ok++;
        }
    }
    int64_t p50 = 0;
    if (!us.empty()) {
        std::sort(us.begin(), us.end());
        p50 = us[us.size() / 2];
    }
    write_text(dir / "p50", std::to_string(ok) + " " + std::to_string(p50));
    return ok == 200 ? 0 : 3;
}

void member_main(const std::filesystem::path& dir, const std::string& seed) {
    auto cfg = base_cfg(1000, 60000);
    if (!seed.empty()) {
        cfg.seed_nodes.push_back(seed);
    }
    actor_system sys(cfg);
    sys.spawn<Box>("box");
    write_text(dir / "port", std::to_string(sys.actual_port()));
    write_text(dir / "self", std::to_string(sys.self_node_id()));
    uint64_t peer = 0;
    auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds(8)) {
        for (const auto& node : sys.current_live_nodes()) {
            if (node.id != sys.self_node_id()) {
                peer = node.id;
            }
        }
        if (peer != 0) {
            write_text(dir / "seen", std::to_string(peer));
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    while (!std::filesystem::exists(dir / "killed")) {
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(20)) {
            write_text(dir / "member", "nokill");
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    auto killed_at = std::chrono::steady_clock::now();
    bool gone = false;
    int elapsed = 0;
    while (true) {
        elapsed = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - killed_at).count());
        bool present = false;
        for (const auto& node : sys.current_live_nodes()) {
            if (node.id == peer) {
                present = true;
            }
        }
        if (peer != 0 && !present) {
            gone = true;
            break;
        }
        if (elapsed > 2500) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    write_text(dir / "member",
               std::string(gone ? "gone" : "still") + " " + std::to_string(elapsed)
               + " " + std::to_string(sys.unacked_count()));
}

int member_unacked_sender(const std::filesystem::path& dir) {
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(1000, 60000);
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        write_text(dir / "member_unacked", "nobox 0 0 0 0");
        return 2;
    }
    auto start = std::chrono::steady_clock::now();
    int accepted = 0;
    uint64_t id = 0;
    while (accepted < 20
           && std::chrono::steady_clock::now() - start < std::chrono::seconds(3)) {
        if (ref.send(tick_msg{id++})) {
            accepted++;
        }
    }
    uint64_t peer = 0;
    for (const auto& node : sys.current_live_nodes()) {
        if (node.id != sys.self_node_id()) {
            peer = node.id;
        }
    }
    auto wait_from = std::chrono::steady_clock::now();
    bool gone = peer == 0;
    int elapsed = 0;
    while (!gone) {
        elapsed = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - wait_from).count());
        bool present = false;
        for (const auto& node : sys.current_live_nodes()) {
            if (node.id == peer) {
                present = true;
            }
        }
        if (!present) {
            gone = true;
            break;
        }
        if (elapsed > 2500) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    uint64_t first = sys.recover_attempt_count();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    uint64_t second = sys.recover_attempt_count();
    write_text(dir / "member_unacked",
               std::string(gone ? "gone" : "still") + " " + std::to_string(elapsed)
               + " " + std::to_string(sys.unacked_count()) + " "
               + std::to_string(first) + " " + std::to_string(second));
    return 0;
}

void blackhole_receiver(const std::filesystem::path& dir) {
    auto cfg = base_cfg(8000, 60000);
    actor_system sys(cfg);
    auto box = sys.spawn<Box>("box");
    write_text(dir / "port", std::to_string(sys.actual_port()));
    bool armed = false;
    auto start = std::chrono::steady_clock::now();
    while (!std::filesystem::exists(dir / "stop")) {
        int unique = 0;
        int deliveries = 0;
        int closed = 0;
        box.get()->snapshot(unique, deliveries, closed);
        if (!armed && unique >= 100) {
            armed = true;
            sys.drop_inbound_connections();
            sys.blackhole_accept_for(std::chrono::milliseconds(2000));
        }
        write_text(dir / "recv",
                   std::to_string(unique) + " " + std::to_string(deliveries));
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(20)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    int unique = 0;
    int deliveries = 0;
    int closed = 0;
    box.get()->snapshot(unique, deliveries, closed);
    write_text(dir / "recv",
               std::to_string(unique) + " " + std::to_string(deliveries));
}

int blackhole_sender(const std::filesystem::path& dir) {
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(8000, 60000);
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        write_text(dir / "send", "0 0 0 dead");
        write_text(dir / "final_recv", "0 0");
        return 2;
    }
    int accepted = 0;
    for (int i = 0; i < 1000; ++i) {
        if (ref.send(tick_msg{static_cast<uint64_t>(i)})) {
            accepted++;
        }
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    int unique = 0;
    int deliveries = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream in(dir / "recv");
        in >> unique >> deliveries;
        if (unique >= 1000) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }
    bool live = false;
    for (const auto& node : sys.current_live_nodes()) {
        if (node.id != sys.self_node_id()) {
            live = true;
        }
    }
    write_text(dir / "send",
               std::to_string(accepted) + " " + std::to_string(sys.resent_count())
               + " " + std::to_string(sys.stopped_recover_count()) + " "
               + (live ? "live" : "dead"));
    write_text(dir / "final_recv",
               std::to_string(unique) + " " + std::to_string(deliveries));
    return 0;
}

void hold_receiver(const std::filesystem::path& dir) {
    auto cfg = base_cfg(8000, 60000);
    actor_system sys(cfg);
    sys.set_hold_routed(true);
    auto box = sys.spawn<Box>("box");
    write_text(dir / "port", std::to_string(sys.actual_port()));
    auto start = std::chrono::steady_clock::now();
    while (!std::filesystem::exists(dir / "replay_go")) {
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(20)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    int ignored = 0;
    int deliveries = 0;
    int closed = 0;
    box.get()->snapshot(ignored, deliveries, closed);
    int handled_before = deliveries;
    sys.release_routed_after_drop();
    write_text(dir / "ready", "1");
    while (!std::filesystem::exists(dir / "finish")) {
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(40)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    int unique = 0;
    box.get()->snapshot(unique, deliveries, closed);
    write_text(dir / "hold_result",
               std::to_string(handled_before) + " " + std::to_string(unique)
               + " " + std::to_string(deliveries));
}

int log_sender(const std::filesystem::path& dir) {
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(8000, 60000);
    auto log_path = (dir / "remote.log").string();
    cfg.remote_log_path = log_path;
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        write_text_fsync(dir / "logstat", "0 0");
        ::pause();
        return 2;
    }
    uint64_t id = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (std::chrono::steady_clock::now() < deadline) {
        ref.send(tick_msg{id++});
        auto scan = scan_remote_log(log_path);
        if (scan.complete_data >= 50) {
            break;
        }
    }
    auto scan = scan_remote_log(log_path);
    write_text_fsync(dir / "logstat",
                     std::to_string(scan.complete_data) + " " + std::to_string(scan.torn));
    ::pause();
    return 0;
}

int log_replay(const std::filesystem::path& dir) {
    auto log_path = (dir / "remote.log").string();
    auto scan = scan_remote_log(log_path);
    write_text_fsync(dir / "replay_sent", std::to_string(scan.unacked_frames.size()));
    if (!wait_file(dir / "ready", 10000)) {
        return 2;
    }
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(8000, 60000);
    cfg.remote_log_path = log_path;
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    size_t sent = 0;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sent < scan.unacked_frames.size()
           && std::chrono::steady_clock::now() < deadline) {
        if (sys.post_original_frame(scan.unacked_frames[sent])) {
            sent++;
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }
    if (sent != scan.unacked_frames.size()) {
        return 3;
    }
    // 确认进了处理函数（ack 在处理之后）再拆系统，避免写队列还在时关闭连接。
    auto ack_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (sys.unacked_count() > 0
           && std::chrono::steady_clock::now() < ack_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    if (sys.unacked_count() != 0) {
        return 3;
    }
    return 0;
}

void rcvlog_receiver(const std::filesystem::path& dir, bool hold) {
    auto cfg = base_cfg(8000, 60000);
    cfg.receiver_log_path = (dir / "receiver.log").string();
    actor_system sys(cfg);
    sys.set_hold_handler(hold);
    auto box = sys.spawn<Box>("box");
    write_text_fsync(dir / "port", std::to_string(sys.actual_port()));
    auto start = std::chrono::steady_clock::now();
    while (!std::filesystem::exists(dir / "stop")) {
        int unique = 0;
        int deliveries = 0;
        int closed = 0;
        box.get()->snapshot(unique, deliveries, closed);
        write_text_fsync(dir / "rcvstat",
                         std::to_string(sys.receiver_fsynced_data_count()) + " "
                         + std::to_string(sys.receiver_fsynced_handled_count()) + " "
                         + std::to_string(deliveries) + " "
                         + std::to_string(sys.receiver_log_torn()));
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(20)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

int rcvlog_sender(const std::filesystem::path& dir) {
    if (!wait_file(dir / "port", 5000)) {
        return 2;
    }
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(8000, 60000);
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        write_text_fsync(dir / "unacked", "missing");
        return 2;
    }
    for (int i = 0; i < 20; ++i) {
        ref.send(tick_msg{static_cast<uint64_t>(i)});
    }
    auto start = std::chrono::steady_clock::now();
    while (!std::filesystem::exists(dir / "stop")) {
        write_text_fsync(dir / "unacked", std::to_string(sys.unacked_count()));
        if (std::chrono::steady_clock::now() - start > std::chrono::seconds(20)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return 0;
}

int rcvlog_replay(const std::filesystem::path& dir) {
    auto cfg = base_cfg(8000, 60000);
    cfg.receiver_log_path = (dir / "receiver.log").string();
    actor_system sys(cfg);
    auto box = sys.spawn<Box>("box");
    auto start = std::chrono::steady_clock::now();
    int unique = 0;
    int deliveries = 0;
    int closed = 0;
    int last = -1;
    int stable = 0;
    while (std::chrono::steady_clock::now() - start < std::chrono::seconds(2)) {
        box.get()->snapshot(unique, deliveries, closed);
        if (deliveries > 0 && deliveries == last) {
            stable++;
            if (stable >= 5) {
                break;
            }
        } else {
            stable = 0;
            last = deliveries;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    box.get()->snapshot(unique, deliveries, closed);
    write_text_fsync(dir / "replay_result",
                     std::to_string(unique) + " " + std::to_string(deliveries) + " "
                     + std::to_string(sys.receiver_log_torn()));
    return 0;
}

int inflight_sender(const std::filesystem::path& dir) {
    auto port = read_text(dir / "port");
    auto cfg = base_cfg(3000, 60000);
    cfg.seed_nodes.push_back("127.0.0.1:" + port);
    actor_system sys(cfg);
    auto ref = wait_box(sys, 8000);
    if (!ref.is_valid()) {
        return 2;
    }
    uint64_t id = 0;
    int accepted = 0;
    for (;;) {
        if (ref.send(tick_msg{id++})) {
            accepted++;
            if (accepted % 100 == 0) {
                ynet::actor::save_snapshot(dir / "accepted", &accepted, sizeof(accepted));
            }
        }
    }
}

void inflight_receiver(const std::filesystem::path& dir) {
    auto cfg = base_cfg(3000, 60000);
    actor_system sys(cfg);
    auto box = sys.spawn<Box>("box");
    write_text(dir / "port", std::to_string(sys.actual_port()));
    while (!std::filesystem::exists(dir / "killed")) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int unique = 0;
    int deliveries = 0;
    int closed = 0;
    box.get()->snapshot(unique, deliveries, closed);
    write_text(dir / "inflight_recv", std::to_string(unique) + " " + std::to_string(deliveries));
}

pid_t spawn_self(const char* self, const std::vector<std::string>& args) {
    pid_t pid = ::fork();
    if (pid == 0) {
        std::vector<char*> av;
        av.push_back(const_cast<char*>(self));
        for (const auto& arg : args) {
            av.push_back(const_cast<char*>(arg.c_str()));
        }
        av.push_back(nullptr);
        ::execv(self, av.data());
        _exit(127);
    }
    return pid;
}

int wait_pid(pid_t pid) {
    int status = 0;
    if (::waitpid(pid, &status, 0) < 0) {
        return 128;
    }
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        return 128 + WTERMSIG(status);
    }
    return 128;
}

int run_pair(const char* self, const std::filesystem::path& dir,
             const std::vector<std::string>& recv_args,
             const std::vector<std::string>& send_args) {
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    pid_t recv = spawn_self(self, recv_args);
    if (!wait_file(dir / "port", 5000)) {
        ::kill(recv, SIGKILL);
        ::waitpid(recv, nullptr, 0);
        std::cerr << "no port\n";
        return 1;
    }
    pid_t send = spawn_self(self, send_args);
    int send_rc = wait_pid(send);
    write_text(dir / "stop", "1");
    int recv_rc = wait_pid(recv);
    (void)recv_rc;
    return send_rc;
}

bool parse3(const std::string& text, int& a, int& b, int& c) {
    std::stringstream in(text);
    return static_cast<bool>(in >> a >> b >> c);
}

} // namespace

int main(int argc, char** argv) {
    std::string role = argc > 1 ? argv[1] : "parent";
    std::string dir = argc > 2 ? argv[2] : "/tmp/ultra-dist-runtime";
    if (role == "recv") {
        bool drop = argc > 3 && std::string(argv[3]) == "drop";
        bool reply = argc > 3 && std::string(argv[3]) == "reply";
        bool die = argc > 3 && std::string(argv[3]) == "die";
        receiver_main(dir, drop, reply, die);
        return 0;
    }
    if (role == "flood") {
        int count = argc > 3 ? std::stoi(argv[3]) : 1000;
        uint64_t ack = argc > 4 ? std::stoull(argv[4]) : 5000;
        return sender_flood(dir, count, ack);
    }
    if (role == "ask-timeout") {
        return ask_timeout_sender(dir);
    }
    if (role == "ask-p50") {
        return ask_p50_sender(dir);
    }
    if (role == "member-unacked") {
        return member_unacked_sender(dir);
    }
    if (role == "blackhole-recv") {
        blackhole_receiver(dir);
        return 0;
    }
    if (role == "blackhole-send") {
        return blackhole_sender(dir);
    }
    if (role == "docker-recv") {
        std::string host = argc > 3 ? argv[3] : "";
        docker_receiver(dir, host);
        return 0;
    }
    if (role == "docker-send") {
        std::string self_host = argc > 3 ? argv[3] : "";
        std::string peer_host = argc > 4 ? argv[4] : "";
        int count = argc > 5 ? std::stoi(argv[5]) : 10000;
        return docker_sender(dir, self_host, peer_host, count);
    }
    if (role == "hold") {
        hold_receiver(dir);
        return 0;
    }
    if (role == "log-send") {
        return log_sender(dir);
    }
    if (role == "log-replay") {
        return log_replay(dir);
    }
    if (role == "member") {
        std::string seed = argc > 3 ? argv[3] : "";
        member_main(dir, seed);
        return 0;
    }
    if (role == "inflight-recv") {
        inflight_receiver(dir);
        return 0;
    }
    if (role == "inflight-send") {
        return inflight_sender(dir);
    }
    if (role == "rcvlog-hold") {
        rcvlog_receiver(dir, true);
        return 0;
    }
    if (role == "rcvlog-live") {
        rcvlog_receiver(dir, false);
        return 0;
    }
    if (role == "rcvlog-send") {
        return rcvlog_sender(dir);
    }
    if (role == "rcvlog-replay") {
        return rcvlog_replay(dir);
    }

    int failures = 0;

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-flood");
        int rc = run_pair(argv[0], d,
                          {"recv", d.string(), "plain"},
                          {"flood", d.string(), "1000", "5000"});
        std::ifstream send(d / "send");
        std::ifstream recv(d / "final_recv");
        int accepted = 0, resent = 0, ack_resend = 0, closed = 0;
        int unique = 0, deliveries = 0;
        send >> accepted >> resent >> ack_resend >> closed;
        recv >> unique >> deliveries;
        int duplicates = deliveries - unique;
        std::cout << "flood unique=" << unique << " accepted=" << accepted
                  << " duplicates=" << duplicates << " resent=" << resent
                  << " rc=" << rc << "\n";
        if (rc != 0 || unique != 1000 || accepted != 1000) {
            failures++;
        }
    }

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-drop");
        int rc = run_pair(argv[0], d,
                          {"recv", d.string(), "drop"},
                          {"flood", d.string(), "1000", "60000"});
        std::ifstream send(d / "send");
        std::ifstream recv(d / "final_recv");
        int accepted = 0, resent = 0, ack_resend = 0, closed = 0;
        int unique = 0, deliveries = 0, skips = 0;
        send >> accepted >> resent >> ack_resend >> closed;
        recv >> unique >> deliveries >> skips;
        std::cout << "drop unique=" << unique << " accepted=" << accepted
                  << " handler_runs=" << deliveries
                  << " wire_skips=" << skips
                  << " duplicates=" << (deliveries - unique)
                  << " resent=" << resent
                  << " ack_timeout_resend=" << ack_resend
                  << " closed_fds=" << closed
                  << " rc=" << rc << "\n";
        if (rc != 0 || unique != accepted || deliveries != unique
            || resent <= 0 || closed <= 0 || ack_resend != 0) {
            failures++;
        }
    }

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-ask-to");
        int rc = run_pair(argv[0], d,
                          {"recv", d.string(), "plain"},
                          {"ask-timeout", d.string()});
        auto line = read_text(d / "ask");
        std::cout << "ask_timeout " << line << " rc=" << rc << "\n";
        std::stringstream in(line);
        std::string kind;
        int wall = 0;
        in >> kind >> wall;
        if (rc != 0 || kind != "nullopt" || wall >= 2000) {
            failures++;
        }
    }

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-ask-p50");
        int rc = run_pair(argv[0], d,
                          {"recv", d.string(), "reply"},
                          {"ask-p50", d.string()});
        auto line = read_text(d / "p50");
        std::cout << "ask_p50 " << line << " rc=" << rc << "\n";
        std::stringstream in(line);
        int ok = 0;
        int64_t p50 = 0;
        in >> ok >> p50;
        if (rc != 0 || ok != 200) {
            failures++;
        }
    }

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-member");
        std::filesystem::remove_all(d);
        std::filesystem::create_directories(d);
        std::filesystem::create_directories(d / "a");
        std::filesystem::create_directories(d / "b");
        pid_t a = spawn_self(argv[0], {"member", (d / "a").string(), ""});
        if (!wait_file(d / "a" / "port", 5000)) {
            ::kill(a, SIGKILL);
            ::waitpid(a, nullptr, 0);
            std::cout << "member no port\n";
            failures++;
        } else {
            auto port = read_text(d / "a" / "port");
            pid_t b = spawn_self(argv[0], {"member", (d / "b").string(),
                                           "127.0.0.1:" + port});
            bool saw = wait_file(d / "b" / "seen", 8000) && wait_file(d / "a" / "seen", 8000);
            if (!saw) {
                std::cout << "member nodes did not see each other\n";
                ::kill(a, SIGKILL);
                ::kill(b, SIGKILL);
                ::waitpid(a, nullptr, 0);
                ::waitpid(b, nullptr, 0);
                failures++;
            } else {
                ::kill(a, SIGKILL);
                ::waitpid(a, nullptr, 0);
                write_text(d / "b" / "killed", "1");
                int rc = wait_pid(b);
                auto line = read_text(d / "b" / "member");
                std::cout << "member " << line << " rc=" << rc << "\n";
                std::stringstream in(line);
                std::string state;
                int elapsed = 0;
                uint64_t unacked = 0;
                in >> state >> elapsed >> unacked;
                if (state != "gone" || elapsed > 2500) {
                    failures++;
                }
            }
        }
    }

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-member-unacked");
        int rc = run_pair(argv[0], d,
                          {"recv", d.string(), "die"},
                          {"member-unacked", d.string()});
        auto line = read_text(d / "member_unacked");
        std::cout << "member-unacked " << line << " rc=" << rc << "\n";
        std::stringstream in(line);
        std::string state;
        int elapsed = 0;
        uint64_t unacked = 0;
        uint64_t first = 0;
        uint64_t second = 1;
        in >> state >> elapsed >> unacked >> first >> second;
        if (rc != 0 || state != "gone" || elapsed > 2500 || unacked < 1 || first != second) {
            failures++;
        }
    }

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-blackhole");
        int rc = run_pair(argv[0], d,
                          {"blackhole-recv", d.string()},
                          {"blackhole-send", d.string()});
        std::ifstream send(d / "send");
        std::ifstream recv(d / "final_recv");
        int accepted = 0;
        uint64_t resent = 0;
        uint64_t stopped = 1;
        std::string live;
        int unique = 0;
        int deliveries = 0;
        send >> accepted >> resent >> stopped >> live;
        recv >> unique >> deliveries;
        std::cout << "blackhole unique=" << unique
                  << " handler_runs=" << deliveries
                  << " resent=" << resent
                  << " stopped_recover=" << stopped
                  << " " << live
                  << " rc=" << rc << "\n";
        if (rc != 0 || unique != 1000 || deliveries != unique || resent == 0
            || stopped != 0 || live != "live") {
            failures++;
        }
    }

    {
        auto d = std::filesystem::path("/tmp/ultra-dist-replay");
        std::filesystem::remove_all(d);
        std::filesystem::create_directories(d);
        pid_t recv = spawn_self(argv[0], {"hold", d.string()});
        if (!wait_file(d / "port", 5000)) {
            ::kill(recv, SIGKILL);
            ::waitpid(recv, nullptr, 0);
            std::cout << "replay no port\n";
            failures++;
        } else {
            pid_t send = spawn_self(argv[0], {"log-send", d.string()});
            bool saw = wait_file(d / "logstat", 20000);
            ::kill(send, SIGKILL);
            int send_rc = wait_pid(send);
            bool signaled = send_rc == 128 + SIGKILL;
            pid_t replay = spawn_self(argv[0], {"log-replay", d.string()});
            bool saw_replay = wait_file(d / "replay_sent", 5000);
            write_text(d / "replay_go", "1");
            int replay_rc = wait_pid(replay);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            write_text(d / "finish", "1");
            wait_pid(recv);
            auto stat = read_text(d / "logstat");
            auto sent_line = read_text(d / "replay_sent");
            auto hold = read_text(d / "hold_result");
            uint64_t complete_data = 0;
            int torn = 0;
            uint64_t replay_sent = 0;
            int handled_before = -1;
            int unique = 0;
            int handler_runs = 0;
            {
                std::stringstream in(stat);
                in >> complete_data >> torn;
            }
            {
                std::stringstream in(sent_line);
                in >> replay_sent;
            }
            {
                std::stringstream in(hold);
                in >> handled_before >> unique >> handler_runs;
            }
            std::cout << "replay complete_data=" << complete_data
                      << " replay_sent=" << replay_sent
                      << " handled_before_go=" << handled_before
                      << " unique=" << unique
                      << " handler_runs=" << handler_runs
                      << " torn=" << torn
                      << " signaled=" << signaled
                      << " rc=" << replay_rc << "\n";
            if (!saw || !saw_replay || !signaled || replay_rc != 0
                || handled_before != 0 || replay_sent != complete_data
                || handler_runs != unique || unique != static_cast<int>(replay_sent)) {
                failures++;
            }
        }
    }

    auto run_rcvlog = [&](bool hold, const char* label) {
        auto d = std::filesystem::path(hold ? "/tmp/ultra-dist-rcvlog-hold"
                                            : "/tmp/ultra-dist-rcvlog-live");
        std::filesystem::remove_all(d);
        std::filesystem::create_directories(d);
        const char* recv_role = hold ? "rcvlog-hold" : "rcvlog-live";
        pid_t recv = spawn_self(argv[0], {recv_role, d.string()});
        if (!wait_file(d / "port", 5000)) {
            ::kill(recv, SIGKILL);
            ::waitpid(recv, nullptr, 0);
            std::cout << label << " no port\n";
            failures++;
            return;
        }
        pid_t send = spawn_self(argv[0], {"rcvlog-send", d.string()});
        uint64_t fsynced_data = 0;
        uint64_t fsynced_handled = 0;
        int handler_runs = -1;
        bool ready = false;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(hold ? 2 : 5);
        while (std::chrono::steady_clock::now() < deadline) {
            std::ifstream stat(d / "rcvstat");
            stat >> fsynced_data >> fsynced_handled >> handler_runs;
            uint64_t unacked = 1;
            std::ifstream un(d / "unacked");
            un >> unacked;
            if (hold) {
                if (fsynced_data >= 20 && fsynced_handled == 0 && handler_runs == 0) {
                    ready = true;
                    break;
                }
            } else if (fsynced_data >= 20 && fsynced_handled >= 20 && unacked == 0) {
                ready = true;
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        ::kill(recv, SIGKILL);
        int recv_rc = wait_pid(recv);
        bool signaled = recv_rc == 128 + SIGKILL;
        write_text(d / "stop", "1");
        ::kill(send, SIGKILL);
        ::waitpid(send, nullptr, 0);
        pid_t replay = spawn_self(argv[0], {"rcvlog-replay", d.string()});
        bool saw = wait_file(d / "replay_result", 5000);
        int replay_rc = wait_pid(replay);
        auto line = read_text(d / "replay_result");
        int unique = 0;
        int runs = 0;
        int torn = -1;
        {
            std::stringstream in(line);
            in >> unique >> runs >> torn;
        }
        std::cout << label << " fsynced_data=" << fsynced_data
                  << " fsynced_handled=" << fsynced_handled
                  << " before_runs=" << handler_runs
                  << " unique=" << unique
                  << " handler_runs=" << runs
                  << " torn=" << torn
                  << " signaled=" << signaled
                  << " rc=" << replay_rc << "\n";
        if (!ready || !signaled || !saw || replay_rc != 0
            || unique != runs
            || runs != static_cast<int>(fsynced_data)) {
            failures++;
        }
    };
    run_rcvlog(true, "rcvlog-hold");
    run_rcvlog(false, "rcvlog-live");

    {
        actor_system sys;
        auto start = std::chrono::steady_clock::now();
        auto missing = sys.find<Box>("ultra://*/no/such");
        bool accepted = missing.send(tick_msg{1});
        auto wall = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start).count();
        std::cout << "missing_uri accepted=" << accepted << " wall_ms=" << wall << "\n";
        if (accepted || wall >= 500) {
            failures++;
        }
    }

    std::cout << "dist_failures=" << failures << "\n";
    return failures == 0 ? 0 : 1;
}

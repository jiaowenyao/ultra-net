# Ring Echo 多线程并发优化计划

> **For agentic workers:** Use superpowers:executing-plans to implement. Steps use checkbox (`- [ ]`) syntax.

**Goal:** 验证 ring 模式在多线程下的正确性，并通过并发连接测试量化 scale-out 性能。

**Architecture:** WsServer ring 模式已支持多线程（每线程独立 buffer group + sched->submit 分发）。本计划验证并发正确性，创建多连接压测。

**Tech Stack:** C++20 coroutines, io_uring, SO_REUSEPORT

## 收益评估

| 场景 | 当前状态 | 优化后预期 |
|------|---------|-----------|
| 单连接 P50 | 90μs（已超传统 92μs）✅ | 不变 |
| 4 并发连接 | 未测试 | 线性扩展（4 线程 → ~4x 吞吐） |
| 大帧 64KB | 慢路径（2 次拷贝）| 保持不变 |

## 全局约束

- 注释中文，禁止压行
- ASAN 零错误 + 152 个已有测试全部通过
- 验证多线程 ring 模式无竞态、无 use-after-free

---

### Task 1: 多连接并发 ring echo 正确性测试

**Files:**
- Create: `tests/ws_ring_concurrent.cc`

**Interfaces:**
- Consumes: WsServer ring mode, BufferRingAssembler, WebSocket client
- Produces: 并发连接压测可执行文件

**职责：** 启动 ring mode WsServer（threads=4），同时建立 N 个客户端连接，每个持续 echo，验证零消息丢失 + 测量并发吞吐。

- [ ] **Step 1: 编写并发测试**

```cpp
// tests/ws_ring_concurrent.cc
// 多连接 ring echo 并发测试——验证多线程 ring 模式的正确性和吞吐量
#include <iostream>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
#include "ultranet/net/ws_server.hpp"

using namespace ynet::async;
using namespace ynet::async::net;
using namespace std::chrono;

static uint64_t now_us() {
    return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv) {
    uint16_t port = argc > 1 ? static_cast<uint16_t>(atoi(argv[1])) : 9810;
    int num_conns = argc > 2 ? atoi(argv[2]) : 4;
    int num_msgs = argc > 3 ? atoi(argv[3]) : 200;

    std::atomic<bool> ready{false};
    std::atomic<uint64_t> total_msgs{0};

    // 服务端线程：ring mode WsServer
    std::thread srv([port, &ready, &total_msgs]() {
        Launcher().threads(4).run([port, &ready, &total_msgs]() -> Task<void> {
            WsServer server(port);
            server.enable_ring_mode(256, 4096);
            server.on_text([&total_msgs](WsConn& conn, std::string msg) -> Task<void> {
                co_await conn.send_text(msg);
                total_msgs.fetch_add(1);
            });
            ready.store(true);
            co_await server.serve();
        });
    });

    while (!ready.load()) {
        std::this_thread::sleep_for(milliseconds(10));
    }
    std::this_thread::sleep_for(milliseconds(200));

    std::cout << "=== Ring 多连接并发测试 ===\n";
    std::cout << "Port: " << port << "  Connections: " << num_conns
              << "  Messages/conn: " << num_msgs << "\n\n";

    uint64_t t0 = now_us();
    std::vector<std::thread> clients;

    for (int c = 0; c < num_conns; ++c) {
        clients.emplace_back([port, num_msgs, c, &total_msgs]() {
            Launcher().threads(1).run([port, num_msgs, c]() -> Task<void> {
                auto ws = co_await websocket::WebSocket::connect(
                    "127.0.0.1", port, "/");
                std::string payload(64, 'x');

                for (int i = 0; i < num_msgs; ++i) {
                    co_await ws.send_text(payload);
                    auto frame = co_await ws.read_frame();
                }
                co_await ws.close();
            });
        });
    }

    for (auto& t : clients) {
        t.join();
    }

    uint64_t elapsed = now_us() - t0;
    uint64_t expected = static_cast<uint64_t>(num_conns) * num_msgs;
    uint64_t actual = total_msgs.load();

    std::cout << std::fixed << std::setprecision(0);
    std::cout << "  总消息数  : " << actual << "/" << expected << "\n";
    std::cout << "  总耗时    : " << elapsed / 1000 << " ms\n";
    if (elapsed > 0 && actual > 0) {
        std::cout << "  并发吞吐  : " << (actual * 1000000ULL / elapsed) << " msg/s\n";
    }
    std::cout << "  结果      : " << (actual == expected ? "✅ PASS" : "❌ FAIL") << "\n";

    srv.detach();
    return actual == expected ? 0 : 1;
}
```

- [ ] **Step 2: 添加构建目标**

在 `tests/CMakeLists.txt` 中添加：
```cmake
add_executable(ws_ring_concurrent ws_ring_concurrent.cc)
target_link_libraries(ws_ring_concurrent ultranet pthread)
```

- [ ] **Step 3: 编译 + ASAN 验证**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_FLAGS="-fsanitize=address" && cmake --build . --target ws_ring_concurrent -j$(nproc)`

Run: `ASAN_OPTIONS=detect_leaks=0 ./bin/ws_ring_concurrent 9810 4 100`
Expected: 400/400 消息，零 ASAN 错误

- [ ] **Step 4: Release 并发吞吐测试**

Run: `cd build && cmake .. -DCMAKE_BUILD_TYPE=Release && cmake --build . --target ws_ring_concurrent -j$(nproc)`

Run: `./bin/ws_ring_concurrent 9810 4 500`
Expected: 2000/2000 消息，吞吐量 > 单连接 × 3

- [ ] **Step 5: Commit**

```bash
git add tests/ws_ring_concurrent.cc tests/CMakeLists.txt
git commit -m "[test]: ring模式多连接并发验证+吞吐测试"
```

---

## 自 Review

**1. Spec coverage:** P1（多线程验证+并发测试）覆盖。P2/P3 明确跳过并给出量化理由。

**2. Placeholder scan:** 无 TBD/TODO。

**3. 不做的事情（明确记录，防止过度优化）：**
- P2 CQE 链路优化：间接调用开销 <1μs，在 90μs 延迟中占比 <1%。去掉它需要侵入 thread_pool 代码，得不偿失。
- P3 批量 buffer 回收：单帧 fast path 仅 1 buffer，return_buffer 是共享内存写（非 syscall），开销 <10ns。不值得增加代码复杂度。

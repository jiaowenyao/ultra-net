# ultra-net 代码深度解读

## 文档定位

本文档面向**熟悉 C++11，但对 C++14/17/20/23 新特性了解有限**的开发者，
从细节层面解读 ultra-net 网络库的设计与实现。读完本文档后，你将：

- 理解 C++20 协程的完整工作机制（promise、awaitable、symmetrical transfer）
- 掌握项目中使用的关键设计模式（CRTP、类型擦除、RAII+移动语义）
- 看懂模板元编程技巧（variadic template、`if constexpr`、`requires`、fold expression）
- 理解无锁数据结构的设计思路（MPSC 队列、工作窃取、原子状态机）
- 能够独立排查运行时问题（协程泄漏、内存顺序、io_uring 超时等）

本文档**不是**宏观架构文档，而是聚焦于**具体代码细节**的学习指南。

## 目录

1. [C++ 标准演进速览](#1-c-标准演进速览-c11--c23)
2. [协程深度解析](#2-协程深度解析)
3. [CRTP 模式与 I/O 操作](#3-crtp-模式与-io-操作)
4. [模板元编程](#4-模板元编程)
5. [std::variant 与 std::visit](#5-stdvariant-与-stdvisit)
6. [std::expected 与错误处理](#6-stdexpected-与错误处理)
7. [移动语义与 RAII](#7-移动语义与-raii)
8. [无锁并发编程](#8-无锁并发编程)
9. [类型擦除](#9-类型擦除)
10. [io_uring 集成](#10-io_uring-集成)
11. [实用语法糖汇总](#11-实用语法糖汇总)
12. [调试与排错指南](#12-调试与排错指南)

---

## 1. C++ 标准演进速览 (C++11 → C++23)

在深入代码之前，先了解各版本引入的关键特性。如果你已熟悉这些，可以直接跳至第 2 章。

### C++14

| 特性 | 说明 |
|------|------|
| 泛型 lambda | `auto add = [](auto a, auto b) { return a + b; };` |
| `std::make_unique` | 替代 `new` + `unique_ptr`，异常安全 |
| 变量模板 | `template<class T> constexpr T pi = T(3.14...);` |
| `[[deprecated]]` | 标记已弃用的函数/类 |

### C++17

| 特性 | 说明 | 本项目使用位置 |
|------|------|---------------|
| `std::optional<T>` | 可选值，避免空指针/null | Channel::read() 返回 `optional<T>` |
| `std::variant<Ts...>` | 类型安全的 union | UnifiedTask 存储 coroutine_handle 或 function |
| `std::string_view` | 零拷贝字符串引用 | HTTP header 查找、WebSocket 解析 |
| `if constexpr` | 编译期条件分支 | when_all 中区分 void/non-void |
| 结构化绑定 | `auto [a, b] = tuple;` | 未直接使用（when_all 使用 `std::get<I>`） |
| fold expression | `(args + ...)` 展开参数包 | when_all 的 launch 循环 |
| `std::invoke_result_t<T>` | 获取可调用对象的返回类型 | with_retry 中推导 Task<T> 的 T |
| `inline` 变量 | 头文件中定义变量不违反 ODR | 多处 thread_local 声明 |

### C++20

| 特性 | 说明 | 本项目使用位置 |
|------|------|---------------|
| **协程** | `co_await`/`co_return`/`co_yield` | 整个项目的基础 |
| `requires` 子句 | 约束模板参数 | `TaskPromise::return_value`，`when_all` |
| 指定初始化器 | `S{.x=1, .y=2}` 结构体初始化 | buffer.cc 中 `io_uring_buf{}` |
| `std::atomic_ref<T>` | 对非原子对象的原子操作 | 未使用 |
| `using enum` | 导入枚举值到当前作用域 | 未使用 |
| `std::format` | 类型安全的格式化字符串 | 日志宏 |
| `std::coroutine_handle<T>` | 协程句柄标准库 | Task 内部 |

### C++23

| 特性 | 说明 | 本项目使用位置 |
|------|------|---------------|
| `std::expected<T, E>` | 要么值、要么错误 | `IoResult<T>` = `std::expected<T, std::error_code>` |
| `std::ranges::to<T>()` | 范围转容器 | 未使用 |
| `static operator()` | 无状态函数对象 | 未使用 |

---

## 2. 协程深度解析

C++20 协程是 ultra-net 的核心。本章从零开始解释协程机制。

### 2.1 什么是协程？

协程是一个**可以暂停和恢复的函数**。与普通函数不同，普通函数只能：
1. 从头执行到尾
2. 返回一个值

协程可以：
1. 执行到一半暂停（suspend）
2. 把控制权交还给调用者
3. 之后从暂停点恢复（resume）

在 ultra-net 中，当代码写 `co_await Read(fd, buf, size)` 时：
- 协程发起一个异步读操作（向 io_uring 提交 SQE）
- 协程暂停，线程去做其他事
- 当 io_uring 完成读操作（收到 CQE），协程恢复执行，拿到读取结果

### 2.2 协程的三个角色

理解 C++20 协程需要区分三个角色：

```
┌──────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  调用者       │────>│  promise_type    │<────│  awaitable      │
│  (Caller)     │     │  (协程内部状态)   │     │  (暂停/恢复点)   │
└──────────────┘     └──────────────────┘     └─────────────────┘
```

**1. 调用者 (Caller)**：启动协程的代码。例如：
```cpp
auto task = my_coroutine();  // 调用者创建协程
pool.submit(task.release()); // 调用者提交给线程池
```

**2. Promise 类型**：协程的"内部控制块"。编译器在协程帧中嵌入一个 `promise_type` 对象，
管理协程的返回值、异常和生命周期。在 ultra-net 中：
- `TaskPromiseBase`：管理 `m_caller`（父协程句柄）、`m_ex`（异常指针）
- `TaskPromise<T>`：管理 `m_value`（`optional<T>`，存储返回值）

**3. Awaitable**：`co_await` 后面的表达式。必须实现三个方法：
- `await_ready()` → 是否需要暂停？
- `await_suspend(handle)` → 暂停时要做什么？
- `await_resume()` → 恢复时返回什么？

### 2.3 协程的生命周期（逐行解读）

以 echo_server 为例，逐步追踪协程的执行流程。

**第一步：创建协程**

```cpp
// pool.submit(echo_session(client_fd).release());
//            ^^^^^^^^^^^^^^^^^^^^^^^^^
//            调用协程函数，创建协程帧
```

当你调用一个协程函数时，编译器生成的代码大致相当于：

```cpp
// 编译器生成的伪代码
Task<void> echo_session(int fd) {
    // 1. 分配协程帧（堆上）
    auto* frame = operator new(sizeof(coroutine_frame));
    
    // 2. 在帧中构造 promise 对象
    auto* promise = new (&frame->promise) TaskPromise<void>{};
    
    // 3. 调用 initial_suspend
    auto initial_awaitable = promise->initial_suspend();  // suspend_always
    if (!initial_awaitable.await_ready()) {
        // 4. 协程暂停！控制权返回给调用者
        initial_awaitable.await_suspend(frame->handle);
        return Task<void>{frame->handle};  // 返回 Task 对象
    }
    // ... 用户代码在 resume 后继续 ...
}
```

关键点：
- `initial_suspend()` 返回 `std::suspend_always`，意味着协程一开始就暂停
- 协程帧在**堆上**分配（`TaskPromiseBase` 自定义了 `operator new`）
- **为什么必须堆分配？** 防止 HALO 优化把帧放在栈上，协程提交到线程池后原栈被销毁导致 use-after-free

**第二步：提交到线程池**

```cpp
pool.submit(echo_session(client_fd).release());
//         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
//         Task<void> 的 release() 返回协程句柄
```

`release()` 从 Task 中取出协程句柄，Task 变为空。线程池接管句柄的所有权。
线程池把句柄放入 `UnifiedTask`（`variant<coroutine_handle<>, function<void()>>`），
然后通过 MPSC 队列发送给 worker 线程。

**第三步：Worker 恢复协程**

```cpp
// thread_pool.hpp, process_io_completions 循环中
void operator()() {
    std::visit([](auto& task) {
        using T = std::decay_t<decltype(task)>;
        if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
            if (task && !task.done()) task.resume();  // 恢复协程！
        }
    }, m_task);
}
```

`task.resume()` 让协程从初始暂停点继续执行，进入用户代码体的第一行。

**第四步：co_await 一个 I/O 操作**

```cpp
auto n = co_await Read(fd, buf, sizeof(buf));
```

编译器将其转换为：

```cpp
// co_await Read(fd, buf, sizeof(buf)) 的展开
auto&& awaitable = Read(fd, buf, sizeof(buf));

// 检查是否需要暂停
if (!awaitable.await_ready()) {
    // 需要暂停：向 io_uring 提交 SQE
    awaitable.await_suspend(current_coroutine_handle);
    // 控制权返回到线程池的调用者
    return;  // 实际上是 final_suspend 的处理
}

// 恢复后执行（CQE 返回时）
auto n = awaitable.await_resume();
```

**第五步：IO 完成，协程恢复**

当 io_uring CQE 到来时，worker 线程调用 `on_io_completion(cqe)`：

```cpp
// thread_pool.hpp:on_io_completion
auto* cb = reinterpret_cast<IoCallback*>(cqe->user_data);
cb->m_result = cqe->res;       // 存储内核返回的结果
cb->m_completed = true;
// 恢复等待的协程
cb->m_handle.resume();
```

协程从 `await_resume()` 处恢复，拿到 `IoResult<size_t>` 结果，继续执行。

**第六步：协程结束**

当协程执行到最后的 `co_return`（或函数末尾），编译器调用 `promise.final_suspend()`：

```cpp
// TaskFinalAwaiter::await_suspend
std::coroutine_handle<> await_suspend(std::coroutine_handle<TaskPromise> h) noexcept {
    auto& promise = h.promise();
    promise.notify_complete();  // 通知线程池：活跃任务数减一
    
    auto caller = promise.m_caller.exchange(nullptr, std::memory_order_acq_rel);
    if (caller && !caller.done()) {
        return caller;  // 链式恢复父协程
    }
    // 没有父协程 → 销毁当前帧
    return std::noop_coroutine();  // h.destroy() 会被调用
}
```

**`wait_all()` 怎么知道所有任务完成了？**

线程池维护一个 `m_active_tasks` 原子计数器：
- `submit()` 时 +1
- `notify_complete()` 时 -1
- `wait_all()` 自旋/等待直到计数器为 0

### 2.4 Awaitable 协议详解

一个类型要能被 `co_await`，必须实现三个方法（或通过 `operator co_await` 转换）：

```cpp
struct MyAwaitable {
    // 1. await_ready: 是否需要暂停？
    //    返回 true → 不暂停，直接调用 await_resume
    //    返回 false → 暂停，调用 await_suspend
    bool await_ready() const noexcept;
    
    // 2. await_suspend: 暂停时做什么？
    //    参数是当前协程的句柄
    //    返回 void → 无条件暂停
    //    返回 bool → true 暂停/false 不暂停
    //    返回 coroutine_handle → 恢复指定的协程（对称转移）
    void/bool/coroutine_handle<> await_suspend(coroutine_handle<> h);
    
    // 3. await_resume: 恢复时返回什么？
    //    返回值就是 co_await 表达式的结果
    T await_resume();
};
```

ultra-net 中有两类 awaitable：

| 类型 | await_ready | await_suspend | 示例 |
|------|------------|---------------|------|
| 异步 I/O | `return m_completed` | 提交 SQE + 挂起 | Read, Write, Accept |
| 同步操作 | `return true` | 不调用 | Bind, Listen |

`Bind` 和 `Listen` 的 `await_ready()` 直接执行系统调用并返回 `true`，所以 `co_await Bind(...)` 实际上是**同步执行**的，不会挂起协程。这是合理的，因为 `bind()` 和 `listen()` 是非阻塞的。

### 2.5 对称转移（Symmetric Transfer）

这是协程性能优化的关键概念。看 `TaskFinalAwaiter::await_suspend` 的第 49 行：

```cpp
if (caller && !caller.done()) {
    return caller;  // 返回父协程句柄
}
```

当 `await_suspend` 返回一个 `coroutine_handle` 时，C++ 运行时会**直接恢复那个协程**，
跳过当前协程的 `await_resume`。这意味着：

1. 子协程结束 → 直接跳转到父协程继续执行
2. 避免了调度器的往返
3. 调用栈不会无限增长（tail call optimization 等价物）

没有对称转移的话，每次子协程结束都需要：
```
子协程结束 → 返回调度器 → 调度器恢复父协程 → 父协程继续
```

有了对称转移：
```
子协程结束 → 直接跳转到父协程
```

### 2.6 Task 的 operator co_await 设计

`Task<T>` 本身不是一个 awaitable，但它提供了 `operator co_await`：

```cpp
// lvalue 版本（Task& 被 co_await）
auto operator co_await() const & noexcept {
    return Awaitable{*this};  // 返回引用语义
}

// rvalue 版本（Task&& 被 co_await）
auto operator co_await() const && noexcept {
    return Awaitable{*this};  // 返回移动语义
}
```

`Awaitable::await_resume` 中：
- lvalue 版本返回 `T&`（不移动，保留在原协程帧中）
- rvalue 版本使用 `if constexpr (is_void_v<T>)` 区分，非 void 时返回 `T&&`（移动出来）

这意味着：
```cpp
Task<std::string> task = get_string();
auto s = co_await task;         // lvalue → 返回引用，不移动
auto s = co_await get_string(); // rvalue → 移动出来，避免拷贝
```

---

## 3. CRTP 模式与 I/O 操作

### 3.1 什么是 CRTP？

CRTP (Curiously Recurring Template Pattern) 是一种静态多态技术：

```cpp
template <typename Derived>
class Base {
    void do_something() {
        static_cast<Derived*>(this)->impl();  // 编译期分派
    }
};

class MyClass : public Base<MyClass> {
    void impl() { /* 具体实现 */ }
};
```

与虚函数对比：
- **虚函数**：运行时通过 vtable 查找 → 有间接调用开销
- **CRTP**：编译期确定调用目标 → 零开销抽象

### 3.2 IoOperation<Derived> — CRTP 案例

`io_awaitable.hpp` 中的 `IoOperation<Derived>` 是 ultra-net 最核心的 CRTP 应用：

```cpp
template <typename Derived>
class IoOperation : public IoOperationBase {
    // ...
    auto await_resume() {
        // CRTP 调用：编译期确定调用哪个 Derived::resume()
        return static_cast<Derived*>(this)->resume();
    }
};
```

每个 I/O 操作类继承 `IoOperation<自身>`：

```cpp
class Read : public IoOperation<Read> {    // Derived = Read
    IoResult<size_t> resume() noexcept { ... } // 由基类的 await_resume 调用
};

class Write : public IoOperation<Write> {  // Derived = Write
    IoResult<size_t> resume() noexcept { ... }
};
```

**为什么用 CRTP 而不是虚函数？**

I/O 操作在热路径上（每次 read/write/accept 都要经过），虚函数的间接跳转开销不可忽略。
CRTP 在编译期确定调用目标，生成的代码与直接调用无异。

**同时保留了虚函数用于 `resubmit`/`cancel`**：

`IoOperationBase` 有纯虚函数 `resubmit()` 和 `cancel()`（`io_callback.hpp:77-78`），
这些是低频操作（仅在 `EAGAIN` 重试或主动取消时调用），虚函数开销可以接受。
`IoOperation<Derived>::resubmit()` 实现了通用逻辑，派生类不再覆盖。

### 3.3 Read 操作的完整流程

```cpp
// read.hpp — 派生类定义
class Read : public IoOperation<Read> {
public:
    Read(int fd, void* buf, size_t count) noexcept
        : IoOperation<Read>(
            // 传给基类的第一个参数：SQE 准备函数
            [](io_uring_sqe* sqe, int f, void* b, size_t c) {
                io_uring_prep_recv(sqe, f, b, c, 0);
            },
            fd, buf, count  // 传给 lambda 的参数
        )
    {}

    IoResult<size_t> resume() noexcept {
        if (m_callback.m_result < 0) {
            return std::unexpected(make_io_error(-m_callback.m_result));
        }
        return static_cast<size_t>(m_callback.m_result);
    }
};
```

流程：
1. 构造时，基类把 SQE 准备 lambda + 参数捕获进 `std::function`
2. `co_await reader` 触发 `await_suspend`
3. `await_suspend` 获取 SQE，调用 `m_setup_fn(sqe)`，设置 `IOSQE_IO_LINK` + link_timeout
4. CQE 返回后，`await_resume` → CRTP 调用 `resume()` → 返回 `IoResult<size_t>`

### 3.4 同步 Awaitable：Bind 和 Listen

`Bind` 和 `Listen` 不需要 io_uring（这些系统调用是非阻塞的）：

```cpp
// bind.hpp
class Bind {
    int m_fd;
    sockaddr_in m_addr;
    int m_result{-1};
    int m_errno{0};
public:
    Bind(int fd, const sockaddr* addr, socklen_t len) : m_fd(fd) {
        memcpy(&m_addr, addr, len);
    }

    bool await_ready() const noexcept {
        // 直接执行 bind，不挂起
        const_cast<Bind*>(this)->m_result = ::bind(m_fd, (sockaddr*)&m_addr, sizeof(m_addr));
        if (m_result < 0) const_cast<Bind*>(this)->m_errno = errno;
        return true;  // 总是 ready — 永不挂起
    }

    void await_suspend(std::coroutine_handle<>) noexcept {} // 不会调用

    IoResult<int> await_resume() const noexcept {
        if (m_result < 0) return std::unexpected(make_io_error(m_errno));
        return m_result;
    }
};
```

这种模式的关键洞察：`await_ready` 返回 `true` 意味着"不需要暂停"。
C++ 协程运行时看到 `true` 就直接调用 `await_resume`，跳过 `await_suspend`。
所以 `co_await Bind(...)` 虽然写起来像异步操作，实际上是同步执行的。

---

## 4. 模板元编程

本章覆盖 ultra-net 中使用的模板技巧。

### 4.1 Variadic Template + Fold Expression

`when_all` 和 `when_any` 需要接受任意数量、任意类型的 `Task<T>`：

```cpp
template <typename... Ts>
    requires (sizeof...(Ts) >= 1)  // C++20 requires 子句：至少一个参数
WhenAllAwaiter<Ts...> when_all(Task<Ts>... tasks) {
    return WhenAllAwaiter<Ts...>(std::move(tasks)...);
}
```

**C++11 等价写法**：
```cpp
template <typename... Ts>
typename std::enable_if<(sizeof...(Ts) >= 1), WhenAllAwaiter<Ts...>>::type
when_all(Task<Ts>... tasks) { ... }
```

`requires` 子句比 `enable_if` 更可读，错误消息也更清晰。

### 4.2 `if constexpr` — 编译期分支

`when_all_wrapper` 需要处理 `Task<void>` 和 `Task<T>` 两种情况：

```cpp
template <typename T, size_t I, typename State>
Task<void> when_all_wrapper(std::shared_ptr<State> state, Task<T> task) {
    try {
        if constexpr (!std::is_void_v<T>) {
            // Task<int> 等非 void 类型 → 获取返回值并存入 tuple
            auto value = co_await task;
            std::get<I>(state->results) = std::move(value);
        } else {
            // Task<void> → 只等待完成，不取值
            co_await task;
        }
    } catch (...) {
        if (!state->exception) state->exception = std::current_exception();
    }
    // ... 通知等待者 ...
}
```

**C++11 等价写法**：需要两个重载或 tag dispatch：
```cpp
template <typename T> void handle_result(TagNonVoid) { /* 取值 */ }
template <typename T> void handle_result(TagVoid) { /* 不取值 */ }
```

`if constexpr` 的优势：条件在**编译期**求值，不匹配的分支**不会被编译**。
在上面的代码中，如果 `T = void`，`auto value = co_await task;` 这一行根本不会编译，
所以不会产生"无法对 void 类型赋值"的编译错误。

### 4.3 参数包展开的多种方式

**方式一：Fold expression (C++17)**

```cpp
// when_all.hpp:89
(launch_one<Is>(sched), ...);
// 展开为: launch_one<0>(sched), launch_one<1>(sched), ..., launch_one<N-1>(sched);
```

逗号 fold expression `(expr, ...)` 对参数包的每个索引执行表达式并丢弃结果。
这里用于批量提交协程 wrapper 到调度器。

**方式二：Lambda 捕获展开 (C++20)**

```cpp
// io_awaitable.hpp:31 — 将异构参数捕获进闭包
[f = std::decay_t<F>(std::forward<F>(f)),
 ...args = std::decay_t<Args>(std::forward<Args>(args))]
(io_uring_sqe* sqe) mutable {
    std::invoke(f, sqe, args...);
}
```

`...args = ...` 是 C++20 的包展开捕获，按值捕获每个参数。
C++17 以下需要分别写 `arg1 = std::move(arg1), arg2 = std::move(arg2), ...`。

### 4.4 `result_slot_t` — 条件类型转换

```cpp
// when_all.hpp:17-21
template <typename T>
using result_slot_t = std::conditional_t<
    std::is_void_v<T>,
    std::monostate,  // void → monostate（占位类型）
    T                // 其他类型不变
>;
```

`std::tuple<void, int>` 是不合法的（`void` 不能作为 tuple 元素）。
`result_slot_t` 将 `void` 映射为 `std::monostate`（一个空类型，类似 `std::nullopt_t`），
所以 `std::tuple<result_slot_t<void>, result_slot_t<int>>` = `std::tuple<std::monostate, int>`。

### 4.5 `when_all` 的结果收集机制

```cpp
// when_all.hpp:22-32
template <typename... Ts>
struct WhenAllState {
    static constexpr size_t N = sizeof...(Ts);
    std::atomic<size_t> remaining{N};            // 倒计数
    std::tuple<result_slot_t<Ts>...> results{};  // 存储各任务结果
    std::exception_ptr exception{nullptr};       // 首个异常
    std::coroutine_handle<> caller{nullptr};     // 父协程

    bool decrement_and_check_last() {
        return remaining.fetch_sub(1, std::memory_order_acq_rel) == 1;
    }
};
```

每个 wrapper 协程完成后：
1. 如果是最后一个（`decrement_and_check_last()` 返回 true），恢复父协程
2. 父协程从 `results` tuple 中提取各任务结果

`when_any` 额外使用 CAS 保证只有一个"胜者"：

```cpp
// when_all.hpp:118-126
bool claimed = false;
if (state->done.compare_exchange_strong(claimed, true,
        std::memory_order_acq_rel)) {
    // 我是第一个完成的！存储结果
    std::get<I>(state->results) = std::move(value);
    state->winner_index = I;
}
```

---

## 5. std::variant 与 std::visit

### 5.1 传统方式：虚函数 + 堆分配

C++11 中实现"存储两种不同类型之一"通常需要：

```cpp
class Schedulable {
    virtual void execute() = 0;
    virtual ~Schedulable() = default;
};
class CoroutineTask : public Schedulable {
    coroutine_handle<> h;
    void execute() override { h.resume(); }
};
class FunctionTask : public Schedulable {
    std::function<void()> f;
    void execute() override { f(); }
};

// 使用时需要 new：
std::unique_ptr<Schedulable> task = std::make_unique<CoroutineTask>(handle);
```

缺点：每次调度都需要堆分配 + 虚函数调用。

### 5.2 ultra-net 的方式：variant + visit

```cpp
// unified_task.hpp
class UnifiedTask {
    using TaskVariant = std::variant<
        std::coroutine_handle<>,   // 协程句柄
        std::function<void()>      // 普通函数（如 packaged_task 包装器）
    >;
    TaskVariant m_task;

    void operator()() {
        std::visit([](auto& task) {
            using T = std::decay_t<decltype(task)>;
            if constexpr (std::is_same_v<T, std::coroutine_handle<>>) {
                // 恢复协程
                if (task && !task.done()) task.resume();
            } else {
                // 调用函数
                if (task) task();
            }
        }, m_task);
    }
};
```

`std::variant` 将两个类型**内联存储**（不堆分配），`std::visit` + 泛型 lambda + `if constexpr` 在**编译期**生成两个版本的代码（一个处理 `coroutine_handle`，一个处理 `function`）。

**内存布局**（简化）：

```
UnifiedTask 对象 (栈或容器内):
┌────────────────────────────────────────────┐
│ index: 0 → coroutine_handle (8 bytes)      │
│ data:  0x7f... (协程帧地址)                 │
│        或                                   │
│ index: 1 → function<void()> (32 bytes)     │
│ data:  {vtable_ptr, fn_ptr, ...}           │
└────────────────────────────────────────────┘
max(sizeof(coroutine_handle), sizeof(function)) + sizeof(index)
```

### 5.3 std::visit 的工作原理

`std::visit(visitor, variant)` 在运行时检查 variant 的 `index()`，
然后**编译期生成的跳转表**分发到对应类型的 visitor 调用。

泛型 lambda `[](auto& task) { ... }` 的 `auto` 会被实例化为具体类型，
结合 `if constexpr` 对不同类型执行不同逻辑。这相当于编译器帮你写了：

```cpp
switch (m_task.index()) {
    case 0: {
        auto& task = std::get<0>(m_task);
        if (task && !task.done()) task.resume();
        break;
    }
    case 1: {
        auto& task = std::get<1>(m_task);
        if (task) task();
        break;
    }
}
```

但 `std::visit` 更安全：如果你添加了第三种类型到 variant，编译器会强制你处理。

---

## 6. std::expected 与错误处理

### 6.1 传统错误处理方式

C++11 中处理 I/O 错误通常有三种方式：

```cpp
// 方式1：返回值 + 错误码（C 风格）
int fd = socket(...);
if (fd < 0) { perror("socket"); return; }

// 方式2：异常
try {
    socket s = Socket::create();
} catch (const socket_error& e) { ... }

// 方式3：输出参数
error_code ec;
auto result = read(fd, buf, size, ec);
if (ec) { ... }
```

各有缺点：方式1 返回值语义不清，方式2 异常开销大，方式3 需要额外参数。

### 6.2 std::expected 方案

```cpp
// io_awaitable.hpp:21
template <typename T = std::size_t>
using IoResult = std::expected<T, std::error_code>;
```

`std::expected<T, E>` 是一个"可能有值，可能有错误"的类型。类似于 Rust 的 `Result<T, E>`。

```cpp
// 返回错误
if (m_callback.m_result < 0) {
    return std::unexpected(make_io_error(EAGAIN));  // 错误分支
}
return static_cast<size_t>(m_callback.m_result);     // 成功分支

// 使用方
auto n = co_await Read(fd, buf, size);
if (!n) {
    // n.error() → std::error_code
    if (is_timeout(n.error())) { /* 超时 */ }
} else {
    // *n → size_t 读取字节数
    process(buf, *n);
}
```

**std::expected 优于传统方式的点**：
1. **语义清晰**：返回类型明确表达了"可能失败"
2. **强制检查**：虽然 C++ 没有 Rust 的 `#[must_use]` 那么严格，但 `expected` 的设计鼓励检查
3. **零开销**：在成功路径上没有异常的开销
4. **可组合**：C++23 提供 `and_then`/`or_else`/`transform` 等 monadic 操作

### 6.3 错误分类工具函数

```cpp
// error.hpp
inline bool is_timeout(const std::error_code& ec) noexcept {
    return ec.value() == ETIMEDOUT;
}
inline bool is_retryable(const std::error_code& ec) noexcept {
    return ec.value() == EAGAIN || ec.value() == EINTR;
}
inline bool is_closed(const std::error_code& ec) noexcept {
    return ec.value() == ECONNRESET || ec.value() == EPIPE
        || ec.value() == ENOTCONN || ec.value() == EBADF;
}
inline bool is_refused(const std::error_code& ec) noexcept {
    return ec.value() == ECONNREFUSED;
}
```

这些是薄封装，但将错误分类逻辑集中到一处，避免在业务代码中散布 `errno` 判断。

---

## 7. 移动语义与 RAII

### 7.1 Noncopyable 基础类

```cpp
// utils/noncopyable.h
class Noncopyable {
public:
    Noncopyable(const Noncopyable&) = delete;
    Noncopyable& operator=(const Noncopyable&) = delete;
protected:
    Noncopyable() = default;   // 允许派生类默认构造
    ~Noncopyable() = default;  // 允许派生类析构
};
```

项目大量使用私有继承 `Noncopyable` 来禁用拷贝，改用移动语义：

```cpp
class TcpSocket : ynet::utils::Noncopyable { ... };
class Task : Noncopyable { ... };
class CircuitBreaker : Noncopyable { ... };
class MpscQueue : Noncopyable { ... };
class Channel : Noncopyable { ... };
```

继承 `Noncopyable` 的效果：
- 删除拷贝构造和拷贝赋值 → 防止意外拷贝（如双 close fd）
- 不影响移动构造和移动赋值（需手动定义）

### 7.2 TcpSocket — RAII socket 管理

```cpp
class TcpSocket : Noncopyable {
    int m_fd = -1;

    explicit TcpSocket(int fd) noexcept : m_fd(fd) {}
    
    ~TcpSocket() {
        if (m_fd >= 0) ::close(m_fd);  // RAII：析构时关闭 fd
    }
    
    // 移动构造：转移所有权
    TcpSocket(TcpSocket&& other) noexcept : m_fd(other.m_fd) {
        other.m_fd = -1;  // 关键！源对象变为空
    }
    
    // 移动赋值：先关闭现有 fd
    TcpSocket& operator=(TcpSocket&& other) noexcept {
        if (this != &other) {
            if (m_fd >= 0) ::close(m_fd);
            m_fd = other.m_fd;
            other.m_fd = -1;
        }
        return *this;
    }
    
    // 释放所有权：调用者负责关闭
    int release() noexcept {
        int fd = m_fd;
        m_fd = -1;
        return fd;
    }
};
```

**移动语义的核心约定**：
1. 移动后，源对象的 `m_fd = -1`
2. 析构时只 close `>= 0` 的 fd
3. 因此移动后的"空"对象在析构时不会错误地关闭 fd

**`release()` 的用途**：当需要把 fd 交给另一个包装器时（如 `TcpSocket` → `WebSocket`），
`release()` 转移所有权而不关闭。

### 7.3 WebSocket 的 move 构造

```cpp
WebSocket(WebSocket&& other) noexcept
    : m_socket(std::move(other.m_socket))  // 移动 TcpSocket
    , m_masked(other.m_masked)
    , m_closed(other.m_closed) {}
```

`WebSocket` 拥有一个 `TcpSocket` 成员。move 构造时：
- `std::move(other.m_socket)` 调用 `TcpSocket` 的移动构造 → fd 所有权转移
- `m_masked` 和 `m_closed` 是标量值，直接拷贝
- `other.m_socket` 变为空（fd = -1），`other.m_closed` 不变

**为什么不能用 `= default`**：`TcpSocket` 有自定义移动构造，编译器无法正确生成 `WebSocket` 的默认移动构造（GCC 13 存在相关 bug）。

### 7.4 Task<T> 的 move + 生命周期

```cpp
~Task() {
    if (m_handle && m_handle.done()) {
        m_handle.destroy();  // 只在协程完成时才销毁
    }
}
```

**注意事项**：如果 `Task` 析构时协程还没完成（`!done()`），协程帧**不会被销毁**。
这就是为什么 `[[nodiscard]]` 在 `Task` 上很重要：忘记使用 Task 会导致协程帧泄漏。

正确的生命周期管理：
```cpp
// 方式1：await 到底
auto result = co_await my_task();  // co_await 后 task 已 done

// 方式2：提交给调度器
pool.submit(my_task().release());  // release() 转移所有权给调度器

// 错误方式：
{ auto t = my_task(); }  // 析构时可能未 done → 协程帧泄漏！
```

---

## 8. 无锁并发编程

### 8.1 为什么需要无锁？

ultra-net 的线程池大量使用无锁数据结构，原因是：

1. **性能**：mutex 在低竞争下很快，但在高并发下会引发系统调用（futex）
2. **与 io_uring 兼容**：`pthread_mutex` 在 io_uring worker 线程上触发了 glibc 优先级变更断言失败（实测过的 bug）
3. **确定性**：无锁结构不会引起优先级反转

### 8.2 SpinLock — 最简单的无锁工具

```cpp
// channel.hpp
class SpinLock {
    std::atomic_flag m_flag = ATOMIC_FLAG_INIT;
public:
    void lock() noexcept {
        while (m_flag.test_and_set(std::memory_order_acquire)) {
            // 自旋等待
        }
    }
    void unlock() noexcept {
        m_flag.clear(std::memory_order_release);
    }
};

// RAII 包装器
class SpinLockGuard {
    SpinLock& m_lock;
public:
    explicit SpinLockGuard(SpinLock& lock) noexcept : m_lock(lock) {
        m_lock.lock();
    }
    ~SpinLockGuard() noexcept { m_lock.unlock(); }
};
```

**工作原理**：
- `test_and_set(acquire)`：原子地设置为 true 并返回旧值。如果旧值是 true（已锁），继续自旋。
- `clear(release)`：原子地设置为 false，释放锁。
- `acquire`/`release` 内存顺序确保临界区内的修改对其他线程可见。

**适用场景**：临界区极短（几条指令）。Channel 的临界区只是 push/pop 队列，耗时 < 10ns。
不适合：临界区需要等待 I/O 或持锁时间长的情况。

### 8.3 MPSC 队列 — 多生产者单消费者

`mpsc_queue.hpp` 实现了无锁多生产者单消费者队列。

**数据结构**：

```cpp
template <typename T, size_t Capacity = 256>
class MpscQueue {
    struct Slot {
        std::atomic<size_t> sequence;  // 协调生产者和消费者的序列号
        T data;                         // 实际数据
    };
    
    std::array<Slot, Capacity> m_buffer;
    alignas(64) std::atomic<size_t> m_enqueue_pos{0};  // 生产者竞争位置
    size_t m_dequeue_pos{0};                              // 仅消费者访问（非原子）
};
```

**算法核心：序列号协议**

每个 slot 有一个 `sequence` 原子变量，它的值决定了 slot 的状态：

```
sequence == pos       → slot 空闲，生产者可以尝试写入
sequence == pos + 1   → 生产者已写入数据，消费者可以读取
sequence <  pos       → slot 已被消费者读完（sequence = dequeue_pos + Capacity）
```

**生产者（多线程安全）**：

```cpp
bool try_push(T item) {
    size_t pos = m_enqueue_pos.load(std::memory_order_relaxed);
    for (;;) {
        Slot& slot = m_buffer[pos & kMask];               // 取模（Capacity 是 2 的幂）
        size_t seq = slot.sequence.load(std::memory_order_acquire);
        
        if (seq < pos) return false;                       // 队列满！
        if (seq == pos && m_enqueue_pos.compare_exchange_weak(  // CAS 抢占
                pos, pos + 1, std::memory_order_relaxed)) {
            slot.data = std::move(item);                   // 写入数据
            slot.sequence.store(pos + 1, std::memory_order_release); // 通知消费者
            return true;
        }
        // CAS 失败 → 另一个生产者抢先了，重试
    }
}
```

**消费者（单线程）**：

```cpp
std::optional<T> try_pop() {
    Slot& slot = m_buffer[m_dequeue_pos & kMask];
    size_t seq = slot.sequence.load(std::memory_order_acquire);
    
    if (seq != m_dequeue_pos + 1) return std::nullopt;  // 无数据
    
    T item = std::move(slot.data);                              // 取出数据
    slot.sequence.store(m_dequeue_pos + Capacity,               // 标记为已读
                        std::memory_order_release);
    m_dequeue_pos++;
    return item;
}
```

**内存顺序关键点**：

| 操作 | 内存顺序 | 原因 |
|------|---------|------|
| 生产者读 sequence | acquire | 必须看到消费者已释放 slot |
| 生产者 CAS enqueue_pos | relaxed | CAS 本身提供原子性，不需要额外屏障 |
| 生产者写 sequence = pos+1 | release | 确保 data 写入在 sequence 更新前对其他线程可见 |
| 消费者读 sequence | acquire | 必须看到生产者已写入 data |
| 消费者写 sequence = pos+Capacity | release | 确保 data 被复制在 sequence 更新前完成 |

### 8.4 Chase-Lev 工作窃取队列

`WorkStealingQueue` (`queue.hpp`) 实现了 Chase-Lev deque，用于工作窃取线程池。

**为什么需要工作窃取？** 某个 worker 线程可能先处理完所有任务，而其他 worker 还有积压。
work-stealing 允许空闲线程从忙线程"窃取"任务，实现负载均衡。

**数据结构**：

```cpp
alignas(64) std::atomic<int64_t> m_top;     // 窃取端（读/写）
alignas(64) std::atomic<int64_t> m_bottom;  // 本线程端（读/写）
```

- `push`/`pop` 在 bottom 端操作（owner 线程）
- `steal` 在 top 端操作（other 线程）

**push (owner)**：

```cpp
void push(T item) {
    int64_t b = m_bottom.load(relaxed);
    int64_t t = m_top.load(acquire);
    
    // 如果需要，扩容环形缓冲区
    if ((b - t) >= capacity - 1) grow();
    
    m_buffer->put(b, std::move(item));
    std::atomic_thread_fence(std::memory_order_release);
    m_bottom.store(b + 1, std::memory_order_relaxed);
}
```

**pop (owner)**：

```cpp
std::optional<T> pop() {
    int64_t b = m_bottom.load(relaxed) - 1;
    m_bottom.store(b, std::memory_order_seq_cst);  // 必须 seq_cst！
    
    int64_t t = m_top.load(std::memory_order_seq_cst);
    
    if (t <= b) {
        T item = m_buffer->get(b);
        if (t == b) {
            // 只剩下一个元素，可能和 steal 竞争
            if (!m_top.compare_exchange_strong(t, t + 1, seq_cst)) {
                // steal 抢先了，放弃
                m_bottom.store(b + 1, relaxed);
                return std::nullopt;
            }
        }
        m_bottom.store(b + 1, relaxed);
        return item;
    }
    // 队列空
    m_bottom.store(b + 1, relaxed);
    return std::nullopt;
}
```

**为什么 pop 需要 seq_cst？**

当只剩一个元素时（`t == b`），pop 和 steal 可能同时尝试获取。
seq_cst 确保 pop 和 steal 之间有一个**全局顺序**，
保证只有一个能成功获取那个元素。

### 8.5 CircuitBreaker — 原子状态机

`CircuitBreaker` (`circuit_breaker.hpp`) 使用原子操作实现线程安全的状态转换。

**状态机**：

```
        连续失败 >= threshold            超时到期
  Closed ─────────────────────> Open ────────────────> HalfOpen
    ↑                            │                        │
    └──────── 探测成功 ──────────┘     └── 探测失败 ──────┘
```

**try_acquire — HalfOpen 的 CAS 转换**：

```cpp
bool try_acquire() noexcept {
    auto st = m_state.load(std::memory_order_acquire);
    
    if (st == CircuitState::Closed) {
        return true;  // 闭路，直接放行
    }
    
    if (st == CircuitState::Open) {
        // 检查是否到了尝试恢复的时间
        auto now = steady_clock::now().time_since_epoch().count();
        if (now - m_last_state_change.load(acquire) >= open_timeout_ns) {
            // CAS：只有第一个成功的线程进入 HalfOpen
            if (m_state.compare_exchange_strong(st, CircuitState::HalfOpen,
                    std::memory_order_acq_rel)) {
                m_last_state_change.store(now, release);
                m_failure_count.store(0, release);
                return true;  // 这个线程充当"探测者"
            }
        }
        return false;  // 开路，拒绝
    }
    
    if (st == CircuitState::HalfOpen) {
        // 只允许一个探测请求
        return m_half_open_count.fetch_add(1, acquire) == 0;
    }
    
    return false;
}
```

**关键设计**：
1. 最多一个线程能从 Open 转换到 HalfOpen（CAS 保证互斥）
2. HalfOpen 时最多允许一个请求（通过 `m_half_open_count` 原子计数器限制）
3. 成功 → Closed，失败 → Open（回到开路状态，等待下一个超时周期）

---

## 9. 类型擦除

### 9.1 什么是类型擦除？

类型擦除是指隐藏具体类型，只暴露统一接口的技术。C++ 中最常见的类型擦除是虚函数和 `std::function`。

### 9.2 Scheduler — 虚函数类型擦除

```cpp
// scheduler.h
class Scheduler {
public:
    virtual ~Scheduler() = default;
    virtual void submit(std::coroutine_handle<> handle) = 0;
    virtual void resubmit(std::coroutine_handle<> handle) = 0;
};
```

`ExecutionContext` 持有一个 `thread_local Scheduler*`。任何代码都可以通过
`ExecutionContext::current()` 获取调度器，而不需要知道具体是哪个线程池实现。

### 9.3 std::function 在 I/O 操作中

`IoOperation` 构造函数接受一个可调用对象 + 异构参数：

```cpp
template <typename F, typename... Args>
    requires std::is_invocable_v<F, io_uring_sqe*, Args...>
IoOperation(F&& f, Args... args)
    : m_setup_fn([f = std::decay_t<F>(std::forward<F>(f)),
                   ...args = std::decay_t<Args>(std::forward<Args>(args))]
                  (io_uring_sqe* sqe) mutable {
        std::invoke(f, sqe, args...);
    })
{
```

每个 I/O 操作的类型参数不同（`Read(int,void*,size_t)`, `Accept(int)`, `SendTo(...)`），
但都被擦除为 `std::function<void(io_uring_sqe*)>`。这是在热路径上使用 `std::function` 的折中 — 有一个小的堆分配开销。

### 9.4 ILogger — 策略模式 + 类型擦除

```cpp
// logger.hpp
class ILogger {
    virtual void log(Level level, const std::string& msg) = 0;
    virtual void flush() = 0;
};

inline std::shared_ptr<ILogger>& get_logger() {
    static std::shared_ptr<ILogger> s_logger = std::make_shared<StdoutLogger>();
    return s_logger;
}
```

`shared_ptr<ILogger>` 实现了运行时多态日志策略。`StdoutLogger` 输出到 stderr，
`SpdlogLogger`（条件编译）提供异步日志。切换日志后端不需要修改任何业务代码。

---

## 10. io_uring 集成

### 10.1 io_uring 基本模型

io_uring 是 Linux 5.1+ 的异步 I/O 接口，包含两个环形缓冲区：

```
应用程序                        内核
┌───────────┐               ┌───────────┐
│  SQ (提交) │  ───写入───>  │   内核    │
│  CQ (完成) │  <───读取───  │   I/O 栈  │
└───────────┘               └───────────┘
```

- **SQ (Submission Queue)**：应用程序向 SQ 写入 SQE（Submission Queue Entry），告诉内核要做什么
- **CQ (Completion Queue)**：内核完成后向 CQ 写入 CQE（Completion Queue Entry），包含结果

### 10.2 超时机制：link_timeout

普通 timeout 的问题是：
- 如果 `accept` + `timeout` 顺序提交，timeout SQE 先到达内核，先完成，
  然后"取消"一个还没开始的 accept → accept 被永久阻塞

ultra-net 使用 `IORING_OP_LINK_TIMEOUT`（opcode 15）：

```cpp
// io_awaitable.hpp:77-87
m_sqe->flags |= IOSQE_IO_LINK;     // 标记主 SQE 为链式
auto* timeout_sqe = ctx->get_sqe();
io_uring_prep_link_timeout(timeout_sqe, &ts, 0);  // 链接的超时
```

**工作原理**：
1. 主 SQE + `IOSQE_IO_LINK` 标志
2. 紧随其后的 SQE 是 link_timeout
3. 主 SQE 先开始执行
4. 如果主 SQE 先完成 → link_timeout 被自动取消
5. 如果 link_timeout 先到期 → 主 SQE 被取消（返回 -ECANCELED）

这就是为什么 `ResolveAcceptTimeout` 测试能够通过：accept 立即开始等待连接，同时有一个
500ms 的超时取消它。

### 10.3 背压机制

```cpp
// io_awaitable.hpp:65-70
auto* ctx = IoUringEngine::current();
if (ctx && ctx->over_watermark()) {
    m_callback.m_result = -ENOBUFS;
    m_callback.m_completed = true;
    return;  // 直接完成，不提交 SQE
}
```

当 `m_pending_ops >= max_pending_ops`（默认 256）时，新的 I/O 操作立即返回 `-ENOBUFS`。
协程不会被挂起，而是在 `await_resume` 中得到错误，可以稍后重试。

这防止了内存无限增长：如果 I/O 完成速度低于提交速度，pending_ops 会无限累积，
最终导致 OOM。背压机制在达到水位线时"拒绝"新操作，让系统自然降速。

### 10.4 批量提交优化

```cpp
// io_engine.hpp
void increment_pending() noexcept { m_pending_sqes.fetch_add(1, ...); }
bool should_submit() const noexcept {
    return m_pending_sqes.load(...) >= m_config.batch_threshold;
}
```

默认每积累 64 个 SQE 才调用一次 `io_uring_submit`。这减少了系统调用次数：
- 不批量：每个 `co_await` 一次 `io_uring_enter` → 高系统调用开销
- 批量 64：每 64 个操作一次 `io_uring_enter` → 开销分摊

**特殊情况**：带超时的操作必须立即提交（`submit_now()`），
否则超时计时器在 SQE 仍在本地缓冲区时就开始了，导致超时计算不准确。

---

## 11. 实用语法糖汇总

### 11.1 std::string_view — 零拷贝字符串

```cpp
// HTTP header 查找（http.hpp）
std::string_view header(std::string_view name) const {
    for (const auto& h : headers) {
        // 大小写不敏感比较
        if (iequals(h.name, name)) return h.value;
    }
    return {};  // 空 string_view
}
```

`string_view` 不拥有数据，只是一个指针+长度的视图。优势：
- 传递给函数不需要拷贝（不像 `std::string`）
- 可以指向已有数据的子串（如 HTTP body 的一部分）
- 需要注意：**原数据的生命周期必须长于 string_view**

### 11.2 std::optional — 表达"可能为空"

```cpp
// Channel::read() 返回 optional<T>
auto value = co_await ch.read();
if (value) {
    // *value 是 T 类型
    process(*value);
} else {
    // channel 已关闭且为空
    return;
}
```

`optional<T>` 明确表达了"可能没有值"的语义。与指针相比：不涉及堆分配，不涉及空指针。

### 11.3 [[nodiscard]] — 防止遗漏返回值

```cpp
class [[nodiscard]] Task { ... };
```

如果编译如下代码：
```cpp
my_coroutine();  // 忘记 co_await 或 release
```

编译器会发出警告：`ignoring return value of 'Task<void>' declared with 'nodiscard'`

### 11.4 std::format — 类型安全的格式化

```cpp
// logger.hpp
ULTRA_LOG_INFO("Server started on port {}", port);
// 等价于：
std::format("Server started on port {}", port);
```

与 `printf` 相比：类型安全（编译期检查格式字符串），不依赖 varargs，支持自定义类型。

### 11.5 Lambda 表达式的多种用法

```cpp
// 1. 泛型 lambda (C++14) + if constexpr (C++17)
std::visit([](auto& task) {
    if constexpr (std::is_same_v<decltype(task), coroutine_handle<>>) {
        task.resume();
    } else {
        task();
    }
}, m_task);

// 2. 初始化捕获 (C++14)
[f = std::decay_t<F>(std::forward<F>(f)),  // 捕获外部对象
 ...args = std::decay_t<Args>(std::forward<Args>(args))]  // 参数包展开

// 3. mutable lambda — 允许修改捕获的变量
(io_uring_sqe* sqe) mutable {
    std::invoke(f, sqe, args...);  // f 和 args 是按值捕获的副本
}
```

---

## 12. 调试与排错指南

### 12.1 协程帧泄漏

**症状**：程序运行一段时间后 RSS 持续增长，`pool.wait_all()` 永不返回。

**原因**：`Task` 析构时协程未完成（`!m_handle.done()`），协程帧和资源永不释放。

**检查**：
```cpp
// 在 Task::~Task() 添加日志
~Task() {
    if (m_handle) {
        if (!m_handle.done()) {
            ULTRA_LOG_WARN("Task destroyed before completion: {}", m_handle.address());
        } else {
            m_handle.destroy();
        }
    }
}
```

**常见原因**：
- 忘记 `co_await` 一个 Task
- 提交给调度器时忘记 `release()`：`pool.submit(task.release())` ← 正确，`pool.submit(task)` ← 错误
- 异常导致协程提前退出但未调用 `Close(fd)`

### 12.2 io_uring 超时不符合预期

**症状**：设置了 `with_timeout(100ms)`，但操作在 500ms 后才完成。

**排查**：
1. 确认 `submit_now()` 被调用（带超时的操作必须在 await_suspend 中立即提交）
2. 检查 `-ECANCELED` → `-ETIMEDOUT` 的映射（`await_resume` line 106-108）
3. 检查 link_timeout SQE 是否紧跟在主 SQE 后面（`IOSQE_IO_LINK` 标志）

### 12.3 FD 泄漏

**症状**：`lsof -p <pid>` 显示越来越多的 fd 处于 CLOSE_WAIT 状态。

**排查**：
```bash
# 查看进程的 fd 情况
ls -la /proc/<pid>/fd | wc -l
# 查看 CLOSE_WAIT
ss -tnp | grep CLOSE_WAIT
```

**常见原因**：
- 错误路径未 `co_await Close(fd)`
- `TcpSocket` 对象在 `release()` 后被丢弃（fd 未被 close）
- 异常导致协程跳过 Close

### 12.4 线程池 worker 挂起

**症状**：程序无响应，所有 worker 线程阻塞。

**原因**：Worker 在 `io_uring_wait_cqe_timeout(5s)` 阻塞，但该被唤醒的（eventfd write 丢失）。

**检查**：
```bash
# 查看线程栈
gdb -p <pid> -batch -ex "thread apply all bt"
# 查看 io_uring 状态
cat /proc/<pid>/fdinfo/<ring_fd>
```

### 12.5 理解编译错误

模板错误可能很冗长。以下技巧可以帮助：

1. **先看第一个 error**：后面的错误通常是级联的
2. **查找 "in instantiation of"**：这告诉你哪个模板实例化触发了错误
3. **检查 requires 子句**：如果错误说 "constraints not satisfied"，检查模板的 `requires` 条件
4. **协程相关错误**：如果错误涉及 `promise_type`，检查返回类型是否满足协程要求（需要 `promise_type` 内部类型 + 正确的 `return_value`/`return_void` 签名）

### 12.6 常见编译错误速查

| 错误信息 | 可能原因 | 解决 |
|---------|---------|------|
| `no member named 'promise_type'` | 返回类型不是协程兼容的 | 返回 `Task<T>` 而不是裸 `T` |
| `call to deleted constructor` | 尝试拷贝 Noncopyable 对象 | 使用 `std::move()` 或传递引用 |
| `constraints not satisfied` | requires 子句不满足 | 检查模板参数是否匹配要求 |
| `no matching co_await` | 类型不是 awaitable | 实现 `await_ready/suspend/resume` 或 `operator co_await` |
| `cannot convert Task<T> to T` | 忘记 `co_await` | 在 Task 前面加 `co_await` |
| `ignoring return value of Task` | [[nodiscard]] 警告 | 使用 `co_await`、`release()` 或赋值给变量 |

---

## 附录 A：关键文件索引

| 文件 | 内容 | 重要程度 |
|------|------|---------|
| `coroutine/task.hpp` | Task<T>, 协程 promise | 必读 |
| `io/io_awaitable.hpp` | IoOperation CRTP, IoResult<T> | 必读 |
| `io/io_engine.hpp` | IoUringEngine, 背压 | 必读 |
| `coroutine/thread_pool.hpp` | WorkStealingThreadPool | 必读 |
| `coroutine/when_all.hpp` | when_all/when_any 实现 | 推荐 |
| `coroutine/channel.hpp` | 协程 Channel + SpinLock | 推荐 |
| `coroutine/mpsc_queue.hpp` | 无锁 MPSC 队列 | 推荐 |
| `coroutine/queue.hpp` | 工作窃取队列 | 选读 |
| `coroutine/circuit_breaker.hpp` | 断路器状态机 | 推荐 |
| `coroutine/retry.hpp` | with_retry + 指数退避 | 推荐 |
| `coroutine/unified_task.hpp` | variant + visit 类型擦除 | 推荐 |
| `net/tcp_socket.hpp` | RAII socket 封装 | 必读 |
| `net/websocket.hpp` | WebSocket 完整实现 | 推荐 |
| `net/http.hpp` | HTTP 解析/序列化 | 推荐 |
| `lifecycle/shutdown.hpp` | 优雅关闭协调器 | 推荐 |

## 附录 B：内存顺序速查

| 顺序 | 保证 | 使用场景 | 相当于 |
|------|------|---------|--------|
| `relaxed` | 原子性，无顺序保证 | 计数器递增 | — |
| `acquire` | 后续读写不会重排到此之前 | 读锁、读共享数据 | lock 的读屏障 |
| `release` | 之前的读写不会重排到此之后 | 写锁、发布共享数据 | unlock 的写屏障 |
| `acq_rel` | acquire + release | CAS 操作 | read-modify-write |
| `seq_cst` | 全局统一顺序 | Chase-Lev deque 的 pop/steal 竞争 | 最严，最慢 |

## 附录 C：项目常用代码模式

### 服务器主循环

```cpp
int main() {
    ShutdownCoordinator shutdown;
    shutdown.install_signal_handlers();

    scheduling::WorkStealingThreadPool pool(2);
    ExecutionContext::Scope scope(&pool);
    pool.submit(my_server(port, shutdown).release());
    pool.wait_all();
}
```

### 协程函数

```cpp
Task<void> my_coroutine(int arg) {
    // 使用 co_await 调用 I/O 操作
    auto result = co_await Read(fd, buf, size);
    if (!result) { /* 错误处理 */ co_return; }
    // ... 处理数据 ...
    co_await Close(fd);
}
```

### 错误检查

```cpp
auto n = co_await SomeIoOp(...);
if (!n) {
    if (is_timeout(n.error()))     { /* 超时 */ }
    else if (is_closed(n.error())) { /* 对端关闭 */ }
    co_return;
}
// 正常处理 *n
```

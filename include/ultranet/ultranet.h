// include/ultranet/ultranet.h - Public header for ultranet library
#pragma once

/**
 * ultranet - High-performance async I/O library based on io_uring + C++20 coroutines
 *
 * Features:
 * - io_uring for Linux async I/O
 * - C++20 coroutine-based API with co_await
 * - Zero-copy buffer management with buffer groups
 * - Batch submission for high throughput
 * - Multishot accept/read operations
 * - Event-driven waiting via eventfd
 * - Operation timeouts via IORING_OP_TIMEOUT
 *
 * Example usage:
 *
 *   WorkStealingThreadPool pool(4);
 *   pool.submit(my_coroutine().task());
 *   pool.wait_all();
 */

#include "ultranet/coroutine/task.hpp"
#include "ultranet/coroutine/thread_pool.hpp"
#include "scheduler.h"
#include "execution_context.hpp"
#include "ultranet/io/io_engine.hpp"
#include "ultranet/io/io_callback.hpp"
#include "ultranet/io/io_awaitable.hpp"
#include "ultranet/io/timer.hpp"
#include "ultranet/buffer/buffer.h"
#include "ultranet/net/socket.hpp"
#include "ultranet/net/bind.hpp"
#include "ultranet/net/listen.hpp"
#include "ultranet/net/accept.hpp"
#include "ultranet/net/read.hpp"
#include "ultranet/net/write.hpp"
#include "ultranet/net/close.hpp"
#include "ultranet/net/connect.hpp"
#include "ultranet/net/sendto.hpp"
#include "ultranet/net/recvfrom.hpp"
#include "ultranet/net/shutdown.hpp"
#include "ultranet/net/dns.hpp"

// Version information
#define ULTRANET_VERSION_MAJOR 0
#define ULTRANET_VERSION_MINOR 1
#define ULTRANET_VERSION_PATCH 0

#define ULTRANET_VERSION_STRING "0.1.0"
#define ULTRANET_VERSION (ULTRANET_VERSION_MAJOR * 10000 + ULTRANET_VERSION_MINOR * 100 + ULTRANET_VERSION_PATCH)

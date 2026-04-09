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
 *
 * Example usage:
 *
 *   // Create io_uring context
 *   IoUringContext::Scope scope;
 *
 *   // Register buffer group
 *   auto& bg = IoUringContext::current()->register_buffer_group(1, 1024, 4096);
 *
 *   // Read data with co_await
 *   Read reader(fd, 1);
 *   auto data = co_await reader;
 *
 *   // Write data with co_await
 *   co_await Write(fd, data->data(), data->size());
 */

#include <ultranet/core/task.hpp>
#include <ultranet/core/scheduler.hpp>
#include <ultranet/core/execution_context.hpp>
#include <ultranet/io/io_context.hpp>
#include <ultranet/io/buffer.hpp>
#include <ultranet/io/io_callback.hpp>
#include <ultranet/io/io_awaitable.hpp>
#include <ultranet/net/socket.hpp>
#include <ultranet/net/listen.hpp>
#include <ultranet/net/accept.hpp>
#include <ultranet/net/read.hpp>
#include <ultranet/net/write.hpp>
#include <ultranet/net/close.hpp>
#include <ultranet/net/connect.hpp>

// Version information
#define ULTRANET_VERSION_MAJOR 0
#define ULTRANET_VERSION_MINOR 1
#define ULTRANET_VERSION_PATCH 0

#define ULTRANET_VERSION_STRING "0.1.0"
#define ULTRANET_VERSION (ULTRANET_VERSION_MAJOR * 10000 + ULTRANET_VERSION_MINOR * 100 + ULTRANET_VERSION_PATCH)

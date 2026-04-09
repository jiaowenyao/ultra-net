# ultranet

High-performance async I/O library for Linux based on io_uring and C++20 coroutines.

## Features

- **io_uring**: Native Linux async I/O via io_uring interface
- **C++20 Coroutines**: Zero-cost async/await API design
- **Batch Submission**: Efficient batch submission for high throughput
- **Multishot Operations**: Support for multishot accept/read operations
- **Buffer Groups**: Zero-copy buffer management with registered buffer groups

## Requirements

- Linux kernel 5.6+ (io_uring support)
- GCC 12+ or Clang 16+ with C++23 support
- liburing 2.5+

## Building

```bash
# Clone the repository
git clone https://github.com/yourusername/ultranet.git
cd ultranet

# Create build directory
mkdir build && cd build

# Configure with CMake
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build
make -j$(nproc)

# Run tests
ctest --output-on-failure

# Install (optional)
sudo make install
```

## Quick Start

```cpp
#include <ultranet/ultranet.h>

using namespace ultranet;
using namespace ynet::async;

async::Task<void> echo_session(int fd) {
    auto& bg = IoUringContext::current()->register_buffer_group(1, 1024, 4096);

    Read reader(fd, 1);
    while (true) {
        auto data = co_await reader;
        if (!data) break;

        Write writer(fd, data->data(), data->size());
        co_await writer;
    }
    co_await Close(fd);
}
```

## Project Structure

```
ultranet/
├── include/ultranet/     # Public headers
│   ├── ultranet.h        # Main include
│   ├── core/             # Core components (Task, Scheduler)
│   ├── io/               # I/O context and buffers
│   └── net/              # Network operations
├── async/                # Implementation
│   ├── io/               # I/O implementation
│   ├── scheduling/       # Task scheduling
│   └── net/              # Network operations
├── tests/                # Unit tests
├── examples/             # Example programs
└── cmake/               # CMake modules
```

## Examples

### Echo Server

```bash
# Terminal 1: Start server
./bin/echo_server 8080

# Terminal 2: Connect with netcat
nc localhost 8080
Hello
Hello
```

### HTTP Server

```bash
./bin/http_server 8080
curl http://localhost:8080/
```

## Configuration

CMake options:

- `BUILD_SHARED_LIBS`: Build shared libraries (default: OFF)
- `ULTRANET_BUILD_TESTS`: Build tests (default: ON)
- `ULTRANET_BUILD_EXAMPLES`: Build examples (default: ON)

## Documentation

For detailed API documentation, see the [docs/](docs/) directory.

## License

MIT License - see [LICENSE](LICENSE) file.

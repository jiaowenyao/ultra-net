#include "ultranet/ultranet.h"
#include <iostream>

using namespace ynet::async;
using namespace ynet::async::lifecycle;
using namespace ynet::async::io;

static int passed = 0, failed = 0;
#define T(name) do { std::cout << "  " << name << "... " << std::flush; } while(0)
#define PASS() do { std::cout << "PASSED" << std::endl; ++passed; } while(0)
#define FAIL(m) do { std::cout << "FAILED: " << m << std::endl; ++failed; } while(0)
#define CHECK(c, m) do { if (!(c)) { FAIL(m); return; } } while(0)
#define CO_CHECK(c, m) do { if (!(c)) { FAIL(m); co_return; } } while(0)

void test_shutdown_sets_flag() {
    T("shutdown() sets is_shutdown");
    ShutdownCoordinator coord;
    CHECK(!coord.is_shutdown(), "initially running");
    coord.shutdown();
    CHECK(coord.is_shutdown(), "after shutdown()");
    PASS();
}

void test_shutdown_idempotent() {
    T("shutdown() is idempotent");
    ShutdownCoordinator coord;
    coord.shutdown();
    coord.shutdown();
    CHECK(coord.is_shutdown(), "still shutdown");
    PASS();
}

void test_shutdown_flag() {
    T("shutdown flag transitions");
    ShutdownCoordinator coord;
    CHECK(!coord.is_shutdown(), "initially false");
    coord.shutdown();
    CHECK(coord.is_shutdown(), "true after shutdown");
    CHECK(coord.is_shutdown(), "stays true");
    PASS();
}

int main() {
    std::cout << "=== ShutdownCoordinator Tests ===" << std::endl;
    test_shutdown_sets_flag();
    test_shutdown_idempotent();
    test_shutdown_flag();

    std::cout << "\n=== Shutdown Test Summary ===" << std::endl;
    std::cout << "Passed: " << passed << std::endl;
    std::cout << "Failed: " << failed << std::endl;
    return failed > 0 ? 1 : 0;
}

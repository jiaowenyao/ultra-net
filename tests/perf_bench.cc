// perf 可分析的基准测试 —— 纯 actor 消息路径，无网络开销
#include <iostream>
#include <chrono>
#include <atomic>
#include "ultranet/actor.hpp"
using namespace ynet::actor;
using namespace std::chrono;

struct tick { static constexpr const char* actor_type = "tick"; uint64_t seq; uint64_t ts; };
class Bench : public actor<Bench> {
public:
    std::atomic<uint64_t> count{0}, lat_sum{0};
    Bench() { register_handler<tick>([this](const tick& m){
        count.fetch_add(1,std::memory_order_relaxed);
        lat_sum.fetch_add(duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count()-m.ts,std::memory_order_relaxed);
    });}
};

int main() {
    constexpr uint64_t N = 50'000;
    system_config cfg{.num_threads=4,.max_per_activation=1024};
    actor_system sys(cfg);
    auto ref=sys.spawn<Bench>("b");
    auto* a=static_cast<Bench*>(ref.proxy()->local_actor());

    uint64_t t0=duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
    for(uint64_t i=0;i<N;++i){
        uint64_t ts=duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
        ref.send(tick{i,ts});
    }
    while(a->count.load()<N) std::this_thread::sleep_for(microseconds(10));
    uint64_t t1=duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();

    std::cout<<"N="<<N<<" elapsed="<<(t1-t0)/1000<<"ms "<<(N*1000000ULL/(t1-t0))<<" msg/s avg_lat="<<(a->lat_sum/N)<<"us\n";
}

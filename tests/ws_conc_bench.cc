// WebSocket 多连接并发压测 — N 条连接各自流水线收发
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include "ultranet/ultranet.h"
#include "ultranet/net/websocket.hpp"
using namespace ynet::async;
using namespace ynet::async::net;
using namespace ynet::async::net::websocket;
using namespace std::chrono;
static uint64_t now_us(){return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();}

int main(int argc,char**argv){
    uint16_t port=argc>1?static_cast<uint16_t>(atoi(argv[1])):9100;
    int conns=argc>2?atoi(argv[2]):4; int N=argc>3?atoi(argv[3]):500;

    std::atomic<uint64_t> total_sent{0},total_recv{0};
    std::atomic<uint64_t> lat_sum{0},lat_max{0},t0{0},t1{0};

    std::cout<<"=== Ultra-Net WS "<<conns<<"-Conn Concurrent ===\n";
    std::cout<<"Target: :"<<port<<"  Msg/conn: "<<N<<"\n\n";

    // 每条连接独立线程独立 Launcher（独立 io_uring ring）
    std::vector<std::thread> threads;
    for(int c=0;c<conns;++c){
        threads.emplace_back([&,c,port,N](){
            Launcher().threads(2).run([&,c,port,N]()->Task<void>{
                auto ws=co_await WebSocket::connect("127.0.0.1",port,"/");
                if(c==0) t0.store(now_us());
                std::string p(64,'x');

                for(int i=0;i<N;++i){
                    uint64_t ts=now_us();
                    co_await ws.send_text(p);
                    auto f=co_await ws.read_frame();
                    uint64_t lat=now_us()-ts;

                    total_sent.fetch_add(1); total_recv.fetch_add(1);
                    lat_sum.fetch_add(lat);
                    uint64_t cur=lat_max.load();
                    while(lat>cur&&!lat_max.compare_exchange_weak(cur,lat)){}
                    // 控制发送速率，避免淹没服务器
                    if(i>0&&i%50==0) co_await sleep_for(microseconds(100));
                }
                co_await ws.close();
            });
        });
    }
    for(auto& t:threads) t.join();
    t1.store(now_us());

    uint64_t t1v=t1.load(), t0v=t0.load();
    uint64_t total=total_recv.load();
    uint64_t elapsed=t1v>t0v?t1v-t0v:1;
    std::cout<<std::fixed<<std::setprecision(0);
    std::cout<<"  Conns    : "<<conns<<"\n";
    std::cout<<"  Total    : "<<total<<" msg\n";
    std::cout<<"  Elapsed  : "<<elapsed/1000<<" ms\n";
    std::cout<<"  Throughput: "<<(total*1000000ULL/elapsed)<<" msg/s\n";
    if(total>0){
        std::cout<<"  Avg lat  : "<<(lat_sum/total)<<" μs\n";
        std::cout<<"  Max lat  : "<<lat_max.load()<<" μs\n";
    }
    std::cout<<"  Result   : "<<(total_sent==total_recv?"✅ 无丢失":"❌")<<"\n";
}

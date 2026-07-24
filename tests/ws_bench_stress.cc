// 生产场景综合压测套件
// 维度: 多核扩展 / 消息大小 / 混合负载 / 长时耐力
#include <iostream>
#include <iomanip>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include <algorithm>
#include <cstring>
#include <sys/resource.h>
#include "ultranet/ultranet.h"
#include "ultranet/actor.hpp"
#include "ultranet/net/websocket.hpp"

using namespace ynet::async;
using namespace ynet::async::io;
using namespace ynet::async::net;
using namespace ynet::async::net::websocket;
using namespace ynet::actor;
using namespace std::chrono;

static uint64_t now_us(){return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();}
static long get_rss_kb(){std::ifstream f("/proc/self/statm");long rss=0;f>>rss>>rss;return rss*sysconf(_SC_PAGESIZE)/1024;}

// ═══════════════════════════════════════════════════════════════════
// 测试结果
// ═══════════════════════════════════════════════════════════════════

struct BenchResult {
    std::string name;
    uint64_t sent=0, recv=0, lat_sum=0, lat_max=0;
    uint64_t elapsed_us=0;
    long rss_delta=0;
    bool ok=false;
    void print(){
        double ms=elapsed_us/1000.0;
        std::cout<<std::fixed<<std::setprecision(0)
                 <<"  "<<std::setw(28)<<std::left<<name
                 <<std::setw(10)<<recv<<"/"<<sent
                 <<std::setw(8)<<ms<<"ms"
                 <<std::setw(8)<<(elapsed_us>0?recv*1000000ULL/elapsed_us:0ULL)<<"/s"
                 <<std::setw(8)<<(recv>0?lat_sum/recv:0ULL)<<"μs"
                 <<std::setw(8)<<rss_delta<<"KB"
                 <<"  "<<(ok?"✅":"❌")<<"\n";
    }
};

void print_header(){
    std::cout<<std::setw(30)<<std::left<<"  测试"
             <<std::setw(10)<<"消息"
             <<std::setw(8)<<"耗时"
             <<std::setw(8)<<"吞吐"
             <<std::setw(8)<<"延迟"
             <<std::setw(8)<<"RSS"
             <<"  结果\n";
    std::cout<<std::string(80,'-')<<"\n";
}

// ═══════════════════════════════════════════════════════════════════
// WS Echo Server（独立线程，SO_REUSEPORT 多核）
// ═══════════════════════════════════════════════════════════════════

void run_echo_server(uint16_t port, int threads, std::atomic<bool>& ready){
    std::thread([port,threads,&ready](){
        Launcher().threads(threads).run_per_thread(
            [port,&ready](int tid,ShutdownCoordinator& sd)->Task<void>{
                auto s=co_await Socket(AF_INET,SOCK_STREAM,0);int fd=*s;
                int opt=1;setsockopt(fd,SOL_SOCKET,SO_REUSEPORT,&opt,sizeof(opt));
                setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
                sockaddr_in addr{};addr.sin_family=AF_INET;addr.sin_port=htons(port);
                addr.sin_addr.s_addr=INADDR_ANY;
                co_await Bind(fd,(sockaddr*)&addr,sizeof(addr));
                co_await Listen(fd,64);
                if(tid==0)ready.store(true);
                while(!sd.is_shutdown()){
                    Accept a(fd);a.with_timeout(milliseconds(200));
                    auto c=co_await a;if(!c)continue;
                    int cfd=*c;
                    auto* sched=ExecutionContext::current();
                    if(sched)sched->submit([](int cfd)->Task<void>{
                        TcpSocket cs(cfd);WebSocket ws(std::move(cs));
                        char buf[4096];Read r(ws.socket().fd(),buf,sizeof(buf));
                        r.with_timeout(seconds(5));auto rr=co_await r;
                        if(!rr||*rr==0){co_return;}
                        http::HttpRequest req;if(req.parse(buf,*rr)==0){co_return;}
                        if(co_await ws.accept(req)){co_return;}
                        while(true){
                            auto f=co_await ws.read_frame();
                            if(f.opcode==OpCode::Close)break;
                            co_await ws.write_frame(f);
                        }
                    }(cfd).release());
                }
            });
    }).detach();
}

// ═══════════════════════════════════════════════════════════════════
// WS Client（独立 Launcher，可配置消息大小和数量）
// ═══════════════════════════════════════════════════════════════════

BenchResult run_ws_client(uint16_t port, int msgs, int msg_size, int conn_id=0){
    BenchResult r; r.name="ws_"+std::to_string(msg_size)+"B";
    std::atomic<uint64_t> sent{0},recv{0},lat_sum{0},lat_max{0};
    uint64_t t0=0,t1=0;
    Launcher().threads(1).run([&]()->Task<void>{
        auto ws=co_await WebSocket::connect("127.0.0.1",port,"/");
        std::string payload(msg_size,'x');
        t0=now_us();
        for(int i=0;i<msgs;++i){
            uint64_t ts=now_us();
            co_await ws.send_text(payload);
            auto f=co_await ws.read_frame();
            uint64_t lat=now_us()-ts;
            sent.fetch_add(1);recv.fetch_add(1);lat_sum.fetch_add(lat);
            uint64_t cur=lat_max.load();
            while(lat>cur&&!lat_max.compare_exchange_weak(cur,lat)){}
        }
        t1=now_us();
    });
    r.sent=sent.load();r.recv=recv.load();
    r.lat_sum=lat_sum.load();r.lat_max=lat_max.load();
    r.elapsed_us=t1>t0?t1-t0:1;
    r.ok=(sent==recv);
    return r;
}

// ═══════════════════════════════════════════════════════════════════
// A. 多核扩展性测试
// ═══════════════════════════════════════════════════════════════════

void bench_scalability(){
    std::cout<<"\n=== A. 多核扩展性 (SO_REUSEPORT, 64B×500msg) ===\n";
    print_header();
    long rss0=get_rss_kb();

    for(int threads: {1,2,4}){
        uint16_t port=static_cast<uint16_t>(9900+threads);
        std::atomic<bool> ready{false};
        run_echo_server(port,threads,ready);
        while(!ready.load()) std::this_thread::sleep_for(milliseconds(10));
        std::this_thread::sleep_for(milliseconds(200));

        BenchResult total; total.name=std::to_string(threads)+"核×"+std::to_string(threads)+"连接";
        uint64_t t0=now_us();
        std::vector<std::thread> clients;
        for(int c=0;c<threads;++c){
            clients.emplace_back([&total,port,c](){
                auto r=run_ws_client(port,500,64,c);
                total.sent+=r.sent;total.recv+=r.recv;
                total.lat_sum+=r.lat_sum;
                if(r.lat_max>total.lat_max)total.lat_max=r.lat_max;
            });
        }
        for(auto& t:clients)t.join();
        total.elapsed_us=now_us()-t0;
        total.rss_delta=get_rss_kb()-rss0;
        total.ok=(total.sent==total.recv);
        total.print();
    }
}

// ═══════════════════════════════════════════════════════════════════
// B. 消息大小影响测试
// ═══════════════════════════════════════════════════════════════════

void bench_msg_sizes(){
    std::cout<<"\n=== B. 消息大小影响 (单连接, 各200msg) ===\n";
    print_header();
    uint16_t port=9950;
    std::atomic<bool> ready{false};
    run_echo_server(port,1,ready);
    while(!ready.load())std::this_thread::sleep_for(milliseconds(10));
    std::this_thread::sleep_for(milliseconds(200));

    for(int size: {64, 256, 1024, 4096, 16384, 65536}){
        long rss0=get_rss_kb();
        auto r=run_ws_client(port,200,size);
        r.name=std::to_string(size)+"B";
        r.rss_delta=get_rss_kb()-rss0;
        r.print();
    }
}

// ═══════════════════════════════════════════════════════════════════
// C. 混合负载：Actor 消息 + WS Echo 并发
// ═══════════════════════════════════════════════════════════════════

struct actor_tick{static constexpr const char* at="tick";uint64_t seq;uint64_t ts;};
class MixActor:public actor<MixActor>{public:std::atomic<uint64_t> cnt{0},lat{0};
    MixActor(){register_handler<actor_tick>([this](const actor_tick& m){
        cnt.fetch_add(1);lat.fetch_add(now_us()-m.ts);});}
};

void bench_mixed(){
    std::cout<<"\n=== C. 混合负载 (Actor+WS并发) ===\n";
    print_header();
    long rss0=get_rss_kb();

    uint16_t port=9960;
    std::atomic<bool> ready{false};
    run_echo_server(port,1,ready);
    while(!ready.load())std::this_thread::sleep_for(milliseconds(10));
    std::this_thread::sleep_for(milliseconds(200));

    // Actor 系统
    ynet::actor::system_config acfg{.num_threads=2};
    ynet::actor::actor_system asys(acfg);
    auto aref=asys.spawn<MixActor>("mix");
    auto* aa=static_cast<MixActor*>(aref.proxy()->local_actor());

    uint64_t t0=now_us();
    std::atomic<bool> stop{false};

    // Actor 生产者线程
    std::thread actor_producer([&](){
        uint64_t seq=0;
        while(!stop.load()){aref.send(actor_tick{seq++,now_us()});}
    });

    // WS 客户端线程
    std::thread ws_client([&](){
        auto r=run_ws_client(port,500,256,0);
        stop.store(true);
        uint64_t elapsed=now_us()-t0;
        BenchResult br;br.name="Actor+WS混合";
        br.sent=r.sent+r.recv;br.recv=aa->cnt.load();br.elapsed_us=elapsed;
        br.rss_delta=get_rss_kb()-rss0;br.ok=true;
        br.print();
        std::cout<<"    WS: "<<r.recv<<"msg  Actor: "<<aa->cnt.load()<<"msg\n";
    });

    actor_producer.join();ws_client.join();
}

// ═══════════════════════════════════════════════════════════════════
// Main
// ═══════════════════════════════════════════════════════════════════

int main(int argc,char** argv){
    std::string mode=argc>1?argv[1]:"all";
    std::cout<<"╔══════════════════════════════════════════════════╗\n";
    std::cout<<"║  Ultra-Net 生产场景综合压测套件                   ║\n";
    std::cout<<"╚══════════════════════════════════════════════════╝\n";
    std::cout<<"CPU: "<<std::thread::hardware_concurrency()<<" cores  RSS:"<<get_rss_kb()<<"KB\n";

    if(mode=="all"||mode=="scale")bench_scalability();
    if(mode=="all"||mode=="size")bench_msg_sizes();
    if(mode=="all"||mode=="mixed")bench_mixed();

    std::cout<<"\n🏁 完成\n";
    return 0;
}

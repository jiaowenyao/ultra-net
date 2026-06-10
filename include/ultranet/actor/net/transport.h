// io_uring TCP transport — completely hidden from users.
// Handles connections, framing, and message dispatch.
#pragma once
#include <unordered_map>
#include <string>
#include <functional>
#include "ultranet/ultranet.h"
#include "ultranet/lifecycle/shutdown.hpp"

namespace ynet::actor::net {

using namespace ynet::async; using namespace ynet::async::io;
using namespace ynet::async::net; using namespace ynet::async::lifecycle;
using msg_handler_t = std::function<Task<void>(std::vector<uint8_t>)>;

struct outbound_conn {
    TcpSocket sock; bool valid=true;
    outbound_conn(TcpSocket s):sock(std::move(s)){}
    Task<void> send(const std::vector<uint8_t>& d){
        uint32_t l=(uint32_t)d.size(); auto w=co_await sock.write(&l,4);
        if(!w){valid=false;co_return;}
        size_t n=0; while(n<d.size()){auto w2=co_await sock.write(d.data()+n,d.size()-n);if(!w2){valid=false;co_return;}n+=*w2;}
    }
};

class tcp_transport {
public:
    tcp_transport(uint16_t port, msg_handler_t handler)
        : m_port(port), m_handler(std::move(handler)) {}

    uint16_t port() const { return m_port; }

    Task<void> serve(ShutdownCoordinator& sd) {
        auto sock = co_await Socket(AF_INET, SOCK_STREAM, 0); int fd = *sock;
        int opt=1; setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));
        sockaddr_in addr{}; addr.sin_family=AF_INET; addr.sin_port=htons(m_port);
        addr.sin_addr.s_addr=INADDR_ANY;
        co_await Bind(fd,(sockaddr*)&addr,sizeof(addr)); co_await Listen(fd,64);
        std::cout << "[transport] :" << m_port << std::endl;
        while(!sd.is_shutdown()){
            Accept a(fd); a.with_timeout(std::chrono::milliseconds(100));
            auto c=co_await a; if(!c)continue;
            int cfd=*c;
            auto* s=ExecutionContext::current();
            if(s)s->submit(handle_conn(cfd).release());
        }
        co_await Close(fd);
    }

    Task<std::shared_ptr<outbound_conn>> connect(const std::string& host, uint16_t port) {
        auto sock = co_await TcpSocket::connect(host,port,std::chrono::seconds(2));
        if(!sock.is_valid()) co_return nullptr;
        int fd=sock.fd(); int o=1; setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&o,sizeof(o));
        co_return std::make_shared<outbound_conn>(std::move(sock));
    }

private:
    uint16_t m_port; msg_handler_t m_handler;

    Task<void> handle_conn(int cfd) {
        uint32_t len;
        while(true){
            Read r(cfd,&len,4); r.with_timeout(std::chrono::seconds(30));
            auto rr=co_await r; if(!rr||*rr<4)break;
            std::vector<uint8_t> buf(len);
            size_t n=0;
            while(n<len){ Read r2(cfd,buf.data()+n,len-n); r2.with_timeout(std::chrono::seconds(10));
                auto rr2=co_await r2; if(!rr2||*rr2==0){co_await Close(cfd);co_return;} n+=*rr2; }
            co_await m_handler(std::move(buf));
        }
        co_await Close(cfd);
    }
};

} // namespace ynet::actor::net

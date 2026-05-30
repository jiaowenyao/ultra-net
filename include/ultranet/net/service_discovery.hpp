#pragma once

#include "ultranet/net/tcp_socket.hpp"
#include "ultranet/net/dns.hpp"
#include "ultranet/coroutine/channel.hpp"
#include "ultranet/coroutine/circuit_breaker.hpp"
#include "ultranet/coroutine/when_all.hpp"
#include "ultranet/io/timer.hpp"
#include "ultranet/config/config.hpp"
#include <atomic>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

namespace ynet::async::discovery {

struct Endpoint {
    std::string host;
    uint16_t port = 0;
    uint16_t priority = 0;
    uint16_t weight = 0;
    bool healthy = true;

    std::string id() const { return host + ":" + std::to_string(port); }
    bool operator==(const Endpoint& o) const { return host == o.host && port == o.port; }
};

enum class EndpointChange : uint8_t { Added, Removed, StateChanged, Refresh };

struct EndpointEvent {
    EndpointChange change = EndpointChange::Refresh;
    Endpoint endpoint;
    std::vector<Endpoint> all_endpoints;
};

class ServiceDiscovery : ynet::utils::Noncopyable {
public:
    using Config = config::ServiceDiscoveryConfig;

    // DNS SRV backend
    ServiceDiscovery(std::string service, std::string protocol, std::string domain,
                     Config cfg = {})
        : m_backend(Backend::DnsSrv), m_service(std::move(service))
        , m_protocol(std::move(protocol)), m_domain(std::move(domain)), m_config(cfg) {}

    // Static list backend
    ServiceDiscovery(std::vector<Endpoint> endpoints, Config cfg = {})
        : m_backend(Backend::Static), m_static_endpoints(std::move(endpoints)), m_config(cfg) {
        m_endpoints = m_static_endpoints;
        for (auto& ep : m_endpoints) get_or_create_cb(ep.id());
    }

    ~ServiceDiscovery() { shutdown(); }

    Task<void> run() {
        if (m_backend == Backend::Static) {
            EndpointEvent evt;
            evt.change = EndpointChange::Refresh;
            evt.all_endpoints = m_static_endpoints;
            for (auto& ep : m_static_endpoints) {
                m_endpoints.push_back(ep);
                get_or_create_cb(ep.id());
            }
            m_events.try_write(std::move(evt));
        }

        auto next_refresh = std::chrono::steady_clock::now();
        auto next_health = std::chrono::steady_clock::now();

        while (!m_shutdown.load(std::memory_order_acquire)) {
            auto now = std::chrono::steady_clock::now();
            auto wait = std::min(next_refresh, next_health);
            if (now < wait) co_await io::sleep_for(wait - now);
            if (m_shutdown.load(std::memory_order_acquire)) break;
            now = std::chrono::steady_clock::now();

            if (now >= next_refresh && m_backend == Backend::DnsSrv) {
                auto eps = co_await query_srv();
                if (!eps.empty()) update_endpoints(std::move(eps));
                next_refresh = now + m_config.refresh_interval;
            }
            if (now >= next_health && m_config.enable_health_checks) {
                co_await health_check_all();
                next_health = now + m_config.health_check_interval;
            }
        }
    }

    void shutdown() {
        m_shutdown.store(true, std::memory_order_release);
        m_events.close();
    }

    std::vector<Endpoint> endpoints() const {
        Lock lock(m_mutex);
        std::vector<Endpoint> result;
        for (auto& ep : m_endpoints) {
            auto it = m_circuit_breakers.find(ep.id());
            auto* cb = it != m_circuit_breakers.end() ? it->second.get() : nullptr;
            if (!cb || cb->state() != CircuitState::Open) result.push_back(ep);
        }
        return result;
    }

    std::vector<Endpoint> all_endpoints() const {
        Lock lock(m_mutex);
        return m_endpoints;
    }

    Channel<EndpointEvent, 64>& events() { return m_events; }

    CircuitBreaker* circuit_breaker(const Endpoint& ep) {
        return get_or_create_cb(ep.id());
    }

    struct Stats { size_t dns_queries{0}, dns_failures{0}, health_checks{0}; };
    Stats snapshot() const { return {m_dns_queries.load(), m_dns_failures.load(), m_health_checks.load()}; }

private:
    enum class Backend : uint8_t { DnsSrv, Static };
    Backend m_backend;
    std::string m_service, m_protocol, m_domain;
    std::vector<Endpoint> m_static_endpoints;

    mutable detail::SpinLock m_mutex;
    std::vector<Endpoint> m_endpoints;
    std::unordered_map<std::string, std::unique_ptr<CircuitBreaker>> m_circuit_breakers;

    Channel<EndpointEvent, 64> m_events;
    Config m_config;
    std::atomic<bool> m_shutdown{false};
    std::atomic<size_t> m_dns_queries{0}, m_dns_failures{0}, m_health_checks{0};

    using Lock = detail::SpinLockGuard;

    CircuitBreaker* get_or_create_cb(const std::string& id) {
        auto it = m_circuit_breakers.find(id);
        if (it != m_circuit_breakers.end()) return it->second.get();
        auto cb = std::make_unique<CircuitBreaker>(config::CircuitBreakerConfig{
            .failure_threshold = m_config.circuit_breaker_failures,
            .open_timeout = m_config.circuit_breaker_timeout
        });
        auto* ptr = cb.get();
        m_circuit_breakers.emplace(id, std::move(cb));
        return ptr;
    }

    Task<std::vector<Endpoint>> query_srv() {
        m_dns_queries.fetch_add(1, std::memory_order_relaxed);
        auto records = co_await io::resolve_srv(m_service, m_protocol, m_domain, m_config.connect_timeout);
        if (records.empty()) {
            m_dns_failures.fetch_add(1, std::memory_order_relaxed);
            co_return std::vector<Endpoint>{};
        }
        std::stable_sort(records.begin(), records.end(),
            [](const io::SrvRecord& a, const io::SrvRecord& b) { return a.priority < b.priority; });
        std::vector<Endpoint> result;
        for (auto& r : records)
            result.push_back({r.target, r.port, r.priority, r.weight, true});
        co_return result;
    }

    void update_endpoints(std::vector<Endpoint> incoming) {
        Lock lock(m_mutex);
        std::unordered_set<std::string> old_ids, new_ids;
        for (auto& e : m_endpoints) old_ids.insert(e.id());
        for (auto& e : incoming) new_ids.insert(e.id());

        for (auto& ep : incoming) {
            if (!old_ids.count(ep.id())) {
                get_or_create_cb(ep.id());
                EndpointEvent evt{EndpointChange::Added, ep};
                m_events.try_write(std::move(evt));
            }
        }
        for (auto& ep : m_endpoints) {
            if (!new_ids.count(ep.id())) {
                EndpointEvent evt{EndpointChange::Removed, ep};
                m_events.try_write(std::move(evt));
            }
        }
        m_endpoints = std::move(incoming);
    }

    Task<void> health_check_all() {
        m_health_checks.fetch_add(1, std::memory_order_relaxed);
        std::vector<Endpoint> current;
        { Lock lock(m_mutex); current = m_endpoints; }
        if (current.empty()) co_return;

        std::vector<Task<void>> checks;
        for (size_t i = 0; i < current.size(); ++i) {
            checks.push_back(check_one(current[i]));
        }
        // Sequential: simpler, no nested when_all allocation
        for (auto& c : checks) co_await c;
    }

    Task<void> check_one(Endpoint ep) {
        auto cb = get_or_create_cb(ep.id());
        bool reachable = false;
        try {
            auto sock = co_await ynet::async::net::TcpSocket::connect(ep.host, ep.port, m_config.connect_timeout);
            reachable = sock.is_valid();
        } catch (...) { reachable = false; }

        if (reachable) cb->on_success();
        else cb->on_failure();

        { Lock lock(m_mutex);
            for (auto& e : m_endpoints) {
                if (e.id() == ep.id()) { e.healthy = reachable; break; }
            }
        }
    }
};

} // namespace ynet::async::discovery

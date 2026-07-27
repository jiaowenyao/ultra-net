// native_ws_bench — 原生 C WebSocket 压测客户端（无框架依赖，纯 POSIX + epoll）
//
// 这是一个完全中立的第三方压测工具：
//   - 不依赖 ultra-net、liburing、或任何 C++ 框架
//   - 仅使用 POSIX socket + epoll + 手写 WebSocket 帧编解码
//   - 同时压测 uWS 和 ultra-net，用完全相同的客户端代码，确保公平对比
//
// 用法:
//   ./native_ws_bench <host> <port> <duration_sec> <connections> [payload_size]
//
// 编译:
//   gcc -O3 -Wall -o native_ws_bench native_ws_bench.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>
#include <pthread.h>

#define MAX_EVENTS 256
#define BUF_SIZE 65536
#define MAX_LATENCIES (1 << 20)  // 1M samples per connection

// ── WebSocket 帧编解码（最小实现，仅支持客户端→服务端 mask）─────────

static void ws_generate_mask(unsigned char mask[4]) {
    unsigned int r = (unsigned int)time(NULL) ^ (unsigned int)pthread_self();
    mask[0] = (r >> 24) & 0xFF; mask[1] = (r >> 16) & 0xFF;
    mask[2] = (r >> 8) & 0xFF;  mask[3] = r & 0xFF;
}

// 编码客户端→服务端帧（含 mask）
static int ws_encode_frame(unsigned char *out, const unsigned char *payload,
                           int payload_len, int opcode) {
    int pos = 0;
    out[pos++] = 0x80 | (opcode & 0x0F);  // FIN + opcode

    unsigned char mask[4];
    ws_generate_mask(mask);

    if (payload_len < 126) {
        out[pos++] = 0x80 | payload_len;
    } else if (payload_len <= 65535) {
        out[pos++] = 0x80 | 126;
        out[pos++] = (payload_len >> 8) & 0xFF;
        out[pos++] = payload_len & 0xFF;
    } else {
        out[pos++] = 0x80 | 127;
        for (int i = 7; i >= 0; i--) out[pos++] = (payload_len >> (i * 8)) & 0xFF;
    }
    for (int i = 0; i < 4; i++) out[pos++] = mask[i];
    for (int i = 0; i < payload_len; i++) out[pos++] = payload[i] ^ mask[i % 4];
    return pos;
}

// 解码服务端→客户端帧（无 mask），返回 payload 长度，-1 表示不完整，-2 表示 error
static int ws_decode_frame(const unsigned char *data, int len,
                           unsigned char *payload, int payload_cap) {
    if (len < 2) return -1;
    (void)(data[0] & 0x80);  // fin flag, unused in echo client
    (void)(data[0] & 0x0F);  // opcode, unused in echo client
    int masked = (data[1] & 0x80) != 0;  // 服务端不应 mask
    unsigned long long plen = data[1] & 0x7F;
    int pos = 2;

    if (plen == 126) {
        if (len < 4) return -1;
        plen = (data[2] << 8) | data[3];
        pos = 4;
    } else if (plen == 127) {
        if (len < 10) return -1;
        plen = 0;
        for (int i = 0; i < 8; i++) plen = (plen << 8) | data[2 + i];
        pos = 10;
    }
    if (masked) {
        if (len < pos + 4) return -1;
        pos += 4;  // 跳过 mask key（服务端不应该有，但兼容处理）
    }
    if (pos + plen > (unsigned long long)len) return -1;
    if (plen > (unsigned long long)payload_cap) return -2;

    memcpy(payload, data + pos, plen);
    return (int)plen;
}

// ── 非阻塞 fd 设置 ─────────────────────────────────────────────────

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

// ── WebSocket 连接 + 握手 ──────────────────────────────────────────

static int ws_connect(const char *host, int port, const char *path) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd); return -1;
    }

    // 发送 HTTP upgrade 请求
    char req[512];
    int req_len = snprintf(req, sizeof(req),
        "GET %s HTTP/1.1\r\n"
        "Host: %s:%d\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n",
        path, host, port);
    send(fd, req, req_len, 0);

    // 读取 HTTP 101 响应
    char buf[4096];
    int n = recv(fd, buf, sizeof(buf) - 1, 0);
    if (n <= 0) { close(fd); return -1; }
    buf[n] = '\0';
    if (!strstr(buf, "101")) { close(fd); return -1; }

    set_nonblocking(fd);
    return fd;
}

// ── 压测线程 ───────────────────────────────────────────────────────

typedef struct {
    int thread_id;
    const char *host;
    int port;
    int duration_sec;
    int payload_size;
    volatile int *stop;
    long long total_requests;
    long long total_success;
    unsigned long long *latencies;  // us
    int latency_count;
    int latency_cap;
} worker_args_t;

static void *worker_thread(void *arg) {
    worker_args_t *a = (worker_args_t *)arg;
    int epfd = epoll_create1(0);
    int fd = ws_connect(a->host, a->port, "/");
    if (fd < 0) {
        fprintf(stderr, "[worker %d] connect failed\n", a->thread_id);
        return NULL;
    }

    struct epoll_event ev = {0};
    ev.events = EPOLLIN | EPOLLOUT;
    ev.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

    // 构造一个 WebSocket 帧并持久复用（只变 mask）
    unsigned char *payload = malloc(a->payload_size);
    memset(payload, 'x', a->payload_size);
    unsigned char frame_buf[BUF_SIZE];

    unsigned char recv_buf[BUF_SIZE];
    int recv_total = 0;

    struct timespec ts;
    int state = 0;  // 0=等待发送, 1=等待接收

    while (!*a->stop) {
        struct epoll_event events[4];
        int nfds = epoll_wait(epfd, events, 4, 100);  // 100ms 超时

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd != fd) continue;

            if (events[i].events & EPOLLOUT && state == 0) {
                int frame_len = ws_encode_frame(frame_buf, payload,
                                                a->payload_size, 0x01);
                clock_gettime(CLOCK_MONOTONIC, &ts);
                unsigned long long t0 = (unsigned long long)ts.tv_sec * 1000000ULL
                                      + ts.tv_nsec / 1000;

                ssize_t n = send(fd, frame_buf, frame_len, MSG_NOSIGNAL);
                if (n > 0) {
                    state = 1;
                    recv_total = 0;
                    // 记录发送时间（存在 recv_buf 的前 8 字节）
                    memcpy(recv_buf, &t0, 8);
                }
            }

            if (events[i].events & EPOLLIN && state == 1) {
                ssize_t n = recv(fd, recv_buf + 8 + recv_total,
                                 BUF_SIZE - 8 - recv_total, 0);
                if (n <= 0) {
                    if (n == 0 || errno != EAGAIN) {
                        *a->stop = 1;  // 连接关闭
                    }
                    continue;
                }
                recv_total += n;

                unsigned char rsp_payload[BUF_SIZE];
                int plen = ws_decode_frame(recv_buf + 8, recv_total,
                                           rsp_payload, sizeof(rsp_payload));
                if (plen > 0) {
                    // 计算延迟
                    clock_gettime(CLOCK_MONOTONIC, &ts);
                    unsigned long long t1 = (unsigned long long)ts.tv_sec * 1000000ULL
                                          + ts.tv_nsec / 1000;
                    unsigned long long t0;
                    memcpy(&t0, recv_buf, 8);
                    unsigned long long lat = (t1 > t0) ? (t1 - t0) : 0;

                    __atomic_add_fetch(&a->total_requests, 1, __ATOMIC_RELAXED);
                    if (plen == a->payload_size) {
                        __atomic_add_fetch(&a->total_success, 1, __ATOMIC_RELAXED);
                    }

                    // 存储延迟样本
                    if (a->latency_count < a->latency_cap) {
                        a->latencies[a->latency_count++] = lat;
                    }

                    // 简化：直接重置接收缓冲区
                    recv_total = 0;
                    state = 0;  // 准备发送下一帧
                } else if (plen == -1 && recv_total >= BUF_SIZE - 8) {
                    // 缓冲区满但仍无法解码 → 丢弃
                    recv_total = 0;
                    state = 0;
                }
                // plen == -1: 数据不完整，继续接收
            }
        }
    }

    free(payload);
    close(fd);
    close(epfd);
    return NULL;
}

// ── 比较函数（qsort）───────────────────────────────────────────────

static int cmp_ull(const void *a, const void *b) {
    if (*(unsigned long long *)a < *(unsigned long long *)b) return -1;
    if (*(unsigned long long *)a > *(unsigned long long *)b) return 1;
    return 0;
}

// ── 主函数 ─────────────────────────────────────────────────────────

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "用法: %s <host> <port> <duration_sec> <connections> [payload_size]\n",
                argv[0]);
        return 1;
    }

    const char *host = argv[1];
    int port = atoi(argv[2]);
    int duration = atoi(argv[3]);
    int conns = atoi(argv[4]);
    int payload_size = argc > 5 ? atoi(argv[5]) : 64;

    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  native_ws_bench — 原生C压测客户端  ║\n");
    printf("║  %s:%-5d  conns=%-4d  dur=%-4ds   ║\n",
           host, port, conns, duration);
    printf("╚══════════════════════════════════════╝\n\n");

    volatile int stop = 0;
    worker_args_t workers[conns];
    pthread_t threads[conns];

    for (int i = 0; i < conns; i++) {
        workers[i] = (worker_args_t){
            .thread_id = i,
            .host = host,
            .port = port,
            .duration_sec = duration,
            .payload_size = payload_size,
            .stop = &stop,
            .total_requests = 0,
            .total_success = 0,
            .latencies = calloc(MAX_LATENCIES, sizeof(unsigned long long)),
            .latency_count = 0,
            .latency_cap = MAX_LATENCIES,
        };
        pthread_create(&threads[i], NULL, worker_thread, &workers[i]);
    }

    sleep(duration);
    stop = 1;

    long long total_reqs = 0, total_ok = 0;
    int total_samples = 0;
    for (int i = 0; i < conns; i++) {
        pthread_join(threads[i], NULL);
        total_reqs += workers[i].total_requests;
        total_ok += workers[i].total_success;
        total_samples += workers[i].latency_count;
    }

    // 合并所有延迟样本
    unsigned long long *all_lats = malloc(
        total_samples * sizeof(unsigned long long));
    int idx = 0;
    for (int i = 0; i < conns; i++) {
        memcpy(all_lats + idx, workers[i].latencies,
               workers[i].latency_count * sizeof(unsigned long long));
        idx += workers[i].latency_count;
        free(workers[i].latencies);
    }

    qsort(all_lats, total_samples, sizeof(unsigned long long), cmp_ull);

    unsigned long long sum = 0;
    for (int i = 0; i < total_samples; i++) sum += all_lats[i];

    printf("  请求总数 : %lld\n", total_reqs);
    printf("  成功率   : %.2f%%\n",
           total_reqs > 0 ? 100.0 * total_ok / total_reqs : 0);
    printf("  吞吐量   : %.0f msg/s\n",
           duration > 0 ? (double)total_ok / duration : 0);

    if (total_samples > 0) {
        printf("\n  延迟 (μs):\n");
        printf("    Avg    : %llu\n", sum / total_samples);
        printf("    P50    : %llu\n", all_lats[total_samples * 50 / 100]);
        printf("    P90    : %llu\n", all_lats[total_samples * 90 / 100]);
        printf("    P99    : %llu\n", all_lats[total_samples * 99 / 100]);
        printf("    P999   : %llu\n", all_lats[total_samples * 999 / 1000]);
        printf("    Max    : %llu\n", all_lats[total_samples - 1]);
    }
    printf("\n");

    free(all_lats);
    return (total_reqs > 0 && total_ok == total_reqs) ? 0 : 1;
}

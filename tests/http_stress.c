// http_stress — wrk 级 HTTP 压测工具（纯 C + epoll，零依赖）
// 用法: ./http_stress <host> <port> <duration_sec> <connections> [path]
// 编译: gcc -O3 -Wall -o http_stress http_stress.c -lpthread

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
#define BUF_SIZE 262144
#define MAX_LATENCIES (1 << 22)

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

typedef struct {
    int thread_id;
    const char *host;
    int port;
    const char *path;
    int duration_sec;
    volatile int *stop;
    long long total_requests;
    long long total_success;
    unsigned long long *latencies;
    int latency_count;
    int latency_cap;
} worker_args_t;

static void *worker_thread(void *arg) {
    worker_args_t *a = (worker_args_t *)arg;
    int epfd = epoll_create1(0);

    // 连接
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)a->port);
    inet_pton(AF_INET, a->host, &addr.sin_addr);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "[worker %d] connect failed\n", a->thread_id);
        close(fd); close(epfd); return NULL;
    }
    set_nonblocking(fd);

    struct epoll_event ev = {0};
    ev.events = EPOLLIN | EPOLLOUT;
    ev.data.fd = fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);

    // 构造 HTTP 请求
    char req[512];
    int req_len = snprintf(req, sizeof(req),
        "GET %s HTTP/1.1\r\nHost: %s:%d\r\nConnection: keep-alive\r\n\r\n",
        a->path, a->host, a->port);

    char recv_buf[BUF_SIZE];
    int recv_total = 0;
    struct timespec ts;
    int state = 0; // 0=发送, 1=接收

    while (!*a->stop) {
        struct epoll_event events[4];
        int nfds = epoll_wait(epfd, events, 4, 100);

        for (int i = 0; i < nfds; i++) {
            if (events[i].data.fd != fd) continue;

            if (events[i].events & EPOLLOUT && state == 0) {
                clock_gettime(CLOCK_MONOTONIC, &ts);
                unsigned long long t0 = (unsigned long long)ts.tv_sec * 1000000ULL
                                      + ts.tv_nsec / 1000;
                memcpy(recv_buf, &t0, 8);
                ssize_t n = send(fd, req, req_len, MSG_NOSIGNAL);
                if (n > 0) { state = 1; recv_total = 0; }
            }

            if (events[i].events & EPOLLIN && state == 1) {
                ssize_t n = recv(fd, recv_buf + 8 + recv_total,
                                 BUF_SIZE - 8 - recv_total, 0);
                if (n <= 0) {
                    if (n == 0 || errno != EAGAIN) { *a->stop = 1; }
                    continue;
                }
                recv_total += n;

                // 检查 HTTP 响应是否完整：\r\n\r\n 头部结束 + 等 Content-Length body
                char *body_start = strstr(recv_buf + 8, "\r\n\r\n");
                if (body_start) {
                    body_start += 4;
                    int header_len = (int)(body_start - (recv_buf + 8));
                    // 解析 Content-Length
                    char *cl = strstr(recv_buf + 8, "Content-Length: ");
                    int content_len = 0;
                    if (cl) content_len = atoi(cl + 16);
                    int total_needed = header_len + content_len;

                    if (recv_total >= total_needed) {
                        clock_gettime(CLOCK_MONOTONIC, &ts);
                        unsigned long long t1 = (unsigned long long)ts.tv_sec * 1000000ULL
                                              + ts.tv_nsec / 1000;
                        unsigned long long t0;
                        memcpy(&t0, recv_buf, 8);
                        unsigned long long lat = (t1 > t0) ? (t1 - t0) : 0;

                        __atomic_add_fetch(&a->total_requests, 1, __ATOMIC_RELAXED);
                        __atomic_add_fetch(&a->total_success, 1, __ATOMIC_RELAXED);
                        if (a->latency_count < a->latency_cap)
                            a->latencies[a->latency_count++] = lat;

                        recv_total = 0;
                        state = 0;
                    }
                }
            }
        }
    }
    close(fd);
    close(epfd);
    return NULL;
}

static int cmp_ull(const void *a, const void *b) {
    if (*(unsigned long long *)a < *(unsigned long long *)b) return -1;
    if (*(unsigned long long *)a > *(unsigned long long *)b) return 1;
    return 0;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "用法: %s <host> <port> <duration_sec> <connections> [path]\n", argv[0]);
        return 1;
    }
    const char *host = argv[1];
    int port = atoi(argv[2]);
    int duration = atoi(argv[3]);
    int conns = atoi(argv[4]);
    const char *path = argc > 5 ? argv[5] : "/";

    printf("\n╔══════════════════════════════════════╗\n");
    printf("║  http_stress — HTTP 压测工具 (wrk级) ║\n");
    printf("║  %s:%-5d  conns=%-4d  dur=%-4ds   ║\n", host, port, conns, duration);
    printf("╚══════════════════════════════════════╝\n\n");

    volatile int stop = 0;
    worker_args_t workers[conns];
    pthread_t threads[conns];

    for (int i = 0; i < conns; i++) {
        workers[i] = (worker_args_t){
            .thread_id = i, .host = host, .port = port, .path = path,
            .duration_sec = duration, .stop = &stop,
            .total_requests = 0, .total_success = 0,
            .latencies = calloc(MAX_LATENCIES, sizeof(unsigned long long)),
            .latency_count = 0, .latency_cap = MAX_LATENCIES,
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

    unsigned long long *all_lats = malloc(total_samples * sizeof(unsigned long long));
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
    printf("  成功率   : %.2f%%\n", total_reqs > 0 ? 100.0 * total_ok / total_reqs : 0);
    printf("  吞吐量   : %.0f req/s\n", duration > 0 ? (double)total_ok / duration : 0);
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

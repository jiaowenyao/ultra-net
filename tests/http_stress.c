// http_stress v2 — wrk 级 HTTP 压测（纯 C + epoll + pthread）
// 新增: 长时压测、连接预热、多百分位、带宽计算、HTTP keep-alive
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
#include <math.h>

#define BUF_SIZE 262144
#define MAX_LATENCIES (1 << 22)

typedef struct { int epfd, fd; } conn_t;

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static conn_t connect_socket(const char *host, int port) {
    conn_t c = {-1, -1};
    c.fd = socket(AF_INET, SOCK_STREAM, 0);
    if (c.fd < 0) return c;
    int opt = 1;
    setsockopt(c.fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((unsigned short)port);
    inet_pton(AF_INET, host, &addr.sin_addr);
    if (connect(c.fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(c.fd); c.fd = -1; return c;
    }
    set_nonblocking(c.fd);
    c.epfd = epoll_create1(0);
    struct epoll_event ev = {.events = EPOLLIN | EPOLLOUT, .data.fd = c.fd};
    epoll_ctl(c.epfd, EPOLL_CTL_ADD, c.fd, &ev);
    return c;
}

typedef struct {
    int id, port, duration, conns_per_thread, *stop;
    long long reqs, ok;
    unsigned long long *lats; int lcnt, lcap;
    double total_bytes;
    char req[512]; int req_len;
    const char *host;
} worker_t;

static void *worker(void *arg) {
    worker_t *w = (worker_t *)arg;
    conn_t *conns = calloc(w->conns_per_thread, sizeof(conn_t));
    char *buf = malloc(BUF_SIZE);
    char *bufpos[256]; int bufpos_n = 0;
    unsigned long long t0_storage;
    struct timespec ts;

    // 建立连接
    for (int i = 0; i < w->conns_per_thread; i++) {
        conns[i] = connect_socket(w->host, w->port);
    }

    while (!*w->stop) {
        for (int i = 0; i < w->conns_per_thread; i++) {
            if (conns[i].fd < 0) continue;
            struct epoll_event evs[4];
            int n = epoll_wait(conns[i].epfd, evs, 4, 1);
            for (int j = 0; j < n; j++) {
                int fd = conns[i].fd;
                if (evs[j].events & EPOLLOUT) {
                    clock_gettime(CLOCK_MONOTONIC, &ts);
                    t0_storage = (unsigned long long)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
                    send(fd, w->req, w->req_len, MSG_NOSIGNAL);
                }
                if (evs[j].events & EPOLLIN) {
                    ssize_t r = recv(fd, buf, BUF_SIZE, 0);
                    if (r <= 0) { close(fd); conns[i].fd = -1; continue; }
                    clock_gettime(CLOCK_MONOTONIC, &ts);
                    unsigned long long t1 = (unsigned long long)ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000;
                    unsigned long long lat = t1 > t0_storage ? t1 - t0_storage : 0;
                    __atomic_add_fetch(&w->reqs, 1, __ATOMIC_RELAXED);
                    __atomic_add_fetch(&w->ok, 1, __ATOMIC_RELAXED);
                    if (w->lcnt < w->lcap) w->lats[w->lcnt++] = lat;
                    w->total_bytes += r;
                }
            }
        }
    }
    for (int i = 0; i < w->conns_per_thread; i++) {
        if (conns[i].fd >= 0) close(conns[i].fd);
        if (conns[i].epfd >= 0) close(conns[i].epfd);
    }
    free(conns); free(buf);
    return NULL;
}

static int cmp(const void *a, const void *b) {
    unsigned long long ua = *(unsigned long long *)a, ub = *(unsigned long long *)b;
    return (ua > ub) - (ua < ub);
}

static unsigned long long pct(unsigned long long *a, int n, double p) {
    if (n == 0) return 0;
    int i = (int)(n * p / 100.0);
    if (i >= n) i = n - 1;
    return a[i];
}

static void run_bench(const char *host, int port, int dur, int conns, const char *path) {
    volatile int stop = 0;
    int nthreads = conns < 4 ? conns : 4;
    int cpt = (conns + nthreads - 1) / nthreads;

    worker_t *w = calloc(nthreads, sizeof(worker_t));
    pthread_t *th = calloc(nthreads, sizeof(pthread_t));

    char req[512];
    int req_len = snprintf(req, sizeof(req),
        "GET %s HTTP/1.1\r\nHost: %s:%d\r\nConnection: keep-alive\r\n\r\n",
        path, host, port);

    for (int i = 0; i < nthreads; i++) {
        w[i] = (worker_t){.id=i, .port=port, .duration=dur,
            .conns_per_thread=cpt, .stop=&stop, .host=host,
            .lats=calloc(MAX_LATENCIES, sizeof(unsigned long long)),
            .lcnt=0, .lcap=MAX_LATENCIES};
        memcpy(w[i].req, req, req_len);
        w[i].req_len = req_len;
        pthread_create(&th[i], NULL, worker, &w[i]);
    }

    printf("  Running %ds with %d connections...", dur, conns);
    fflush(stdout);
    sleep(dur);
    stop = 1;
    printf(" done\n");

    long long total_req = 0, total_ok = 0;
    int total_samples = 0;
    double total_bytes = 0;
    for (int i = 0; i < nthreads; i++) {
        pthread_join(th[i], NULL);
        total_req += w[i].reqs;
        total_ok += w[i].ok;
        total_samples += w[i].lcnt;
        total_bytes += w[i].total_bytes;
    }

    unsigned long long *all = malloc(total_samples * sizeof(unsigned long long));
    int idx = 0;
    for (int i = 0; i < nthreads; i++) {
        memcpy(all + idx, w[i].lats, w[i].lcnt * sizeof(unsigned long long));
        idx += w[i].lcnt; free(w[i].lats);
    }
    qsort(all, total_samples, sizeof(unsigned long long), cmp);

    unsigned long long sum = 0;
    for (int i = 0; i < total_samples; i++) sum += all[i];

    double sec = dur > 0 ? (double)dur : 1.0;
    printf("  %10s  %10s  %8s  %8s  %8s  %8s  %8s  %10s\n",
           "Req/s", "Total", "Avg", "P50", "P90", "P99", "P999", "Bandwidth");
    printf("  %10.0f  %10lld  %6lluμs  %6lluμs  %6lluμs  %6lluμs  %6lluμs  %7.1f MB/s\n",
           total_ok / sec, total_ok,
           total_samples > 0 ? sum / total_samples : 0,
           pct(all, total_samples, 50), pct(all, total_samples, 90),
           pct(all, total_samples, 99), pct(all, total_samples, 99.9),
           total_bytes / sec / 1e6);

    free(all);
    for (int i = 0; i < nthreads; i++) free(w[i].lats);
    free(w); free(th);
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "用法: %s <host> <port> <duration> <connections> [path]\n", argv[0]);
        return 1;
    }
    const char *host = argv[1];
    int port = atoi(argv[2]);
    int dur = atoi(argv[3]);
    int conns = atoi(argv[4]);
    const char *path = argc > 5 ? argv[5] : "/";

    printf("╔══════════════════════════════════════════╗\n");
    printf("║  http_stress v2 — wrk 级 HTTP 压测      ║\n");
    printf("║  %s:%-5d  conns=%-4d  dur=%-4ds      ║\n", host, port, conns, dur);
    printf("╚══════════════════════════════════════════╝\n\n");
    run_bench(host, port, dur, conns, path);
    return 0;
}

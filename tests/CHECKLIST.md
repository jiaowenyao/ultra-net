# 必测清单

每次重大改动后必须通过的测试项。

## 1. 正确性验证
- [ ] `actor_gtest` 120 测试全部通过
- [ ] `actor_v2_test` 10/10, `actor_mailbox_test` 12/12
- [ ] `actor_robustness_test` 8/8, `shutdown_test` 3/3
- [ ] ASAN + LeakSanitizer 零错误

## 2. 单连接 WS Echo（基础性能）
- [ ] 5000 msg 零丢失、延迟 < 300μs
- [ ] checksum 校验通过

## 3. 多核扩展性
- [ ] 1核×1连接 → 吞吐基线
- [ ] 2核×2连接 → 吞吐 ≥ 2x 基线
- [ ] 4核×4连接 → 吞吐 ≥ 3x 基线

## 4. 消息大小影响
- [ ] 64B / 256B / 1KB / 4KB / 16KB / 64KB 各通过
- [ ] 大消息延迟增长合理（< 10x for 1000x size increase）

## 5. 混合负载（Actor + Network）
- [ ] Actor 消息 + WS Echo 并发不互相阻塞
- [ ] 无 crash、无数据丢失

## 6. 长时间稳定性
- [ ] 60s 持续压测，零消息丢失
- [ ] RSS 增长 < 10MB（无内存泄漏）

## 7. 异常安全
- [ ] Handler 异常被隔离，actor 继续运行
- [ ] 优雅关闭无 crash（消息未处理完时 sys.reset()）
- [ ] 无效 ref 操作安全（不崩溃）

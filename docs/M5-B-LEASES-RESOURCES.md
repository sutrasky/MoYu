# M5-B 租约、资源边界与故障恢复

日期：2026-11-06。基线：`main@af35caf9315b22d8c421584409bea329372862b8`。
范围：Issue #27 的 M5-B。本文记录设计、状态不变量、迁移与回滚、可核验矩阵的当前状态；
逐项实测命令与结果见 [`M5-B-VERIFICATION.md`](M5-B-VERIFICATION.md)。

## 1. 统一仓库租约

### 1.1 一条代次

`runtime_leases` 仍是每仓库至多一条 `active` 行；`fencing_token` 是由 PostgreSQL `bigserial`
分配的持久代次。新增字段：

| 字段 | 作用 |
| --- | --- |
| `recovery_state` | `none` / `required` / `verifying` / `blocked` / `recovered` |
| `recovery_reason`, `waiting_for` | 恢复原因与等待对象（界面只显示已有事实） |
| `stop_proof`, `stop_proven_at` | 旧执行及受管后代已停止的证据 |
| `stale_receipt_count`, `last_error` | 旧代次晚到回执的审计计数 |

`task_operations` 与 `deliveries` 记录签发时的 `runtime_lease_id`/`fencing_token`；
`runtime_lease_events` 追加记录 acquired / renewed / expired / stop_proof_required /
recovered / adopted / released / stale_receipt_rejected。回执写入前必须比对任务当前绑定的
租约与 token；旧代次的回执只记账，不改任务、投递或操作状态。

### 1.2 入口

| 入口 | 门控位置 |
| --- | --- |
| 网页新任务（launch） | `deliverOnce` 发送 Trigger 前取得租约，写入 `deliveries` |
| 网页回复/人工输入 | 同一 `deliverOnce` 路径；任务已占租约时续租同一代次 |
| 网页 start / resume / retry | `sendOperation` 在发送前取得租约；Bridge 支持 admission 时随请求发送，runtime 校验缺失/旧代次 |
| 在线 GitHub @ | M5-B runtime 先把 Ticket 置 `pending_admission` 且不创建 Assignment；`ticket.imported` 后平台取得租约并调用 `/admit`，失败/无租约时没有 worker 和副作用 |
| 显式唤醒 / 重启恢复 | `maintainLeases` 续租活跃任务、释放等待/终态任务；`reconcileLeases` 处理过期代次 |
| 人工 pause | 不需要新租约；结果落库后释放任务占用的执行名额 |

`pending_admission`、`queued`、`running`、`discussing`、`stopping` 占执行名额；
`waiting_input`、`awaiting_review`、`stopped`、`blocked`、`failed`、`completed`、
`closed_unmerged` 释放名额。实例在线名额与执行名额仍然是两个独立约束。

### 1.3 过期与停止证明

- 租约过期不会直接创建新代次。第一次发现过期时把旧行标为 `required`，等待
  `execution_stop_proof`；此时任何入口都得到 `stop_proof_required`/`recovery_blocked`。
- `reconcileLeases` 对过期代次读取 Bridge Ticket 快照：
  - 仍在运行的 Ticket 被**沿用原代次接管**（adopt），不启动第二个执行，也不伪造停止；
  - `waiting_input`/`awaiting_review`/`stopped`/`completed`/`closed_unmerged` 快照证明没有活跃
    worker，写 `recovered` 并释放任务绑定；
  - 已 `paused` 且 `stoppedAt` + 空 job/subagent 的完整证据写 `recovered`；
  - Bridge 不可达、无 Ticket、或快照状态未知时写 `blocked`，保留名额与错误，不杀进程。
- 只有 `recovered` 的旧行才会在下一次获取时转 `expired`，新代次由 `bigserial` 单调递增。

## 2. 资源边界

### 2.1 单一可强制边界

生产/隔离测试服务使用 systemd slice `moyu-m5b.slice`，`MemoryHigh=6G`、
`MemoryMax=8G`。平台 API、dispatcher、supervisor、隔离 PostgreSQL 与全部实例 cgroup 都在
该 slice 内；supervisor 的 `MOYU_CGROUP_ROOT` 指向 slice 内的
`moyu-m5b.slice/moyu-supervisor`，因此实例是其子 cgroup，而不是宿主根下的旁路树。
详见 [`deploy/systemd/README.md`](../deploy/systemd/README.md)。

supervisor 同时读取该 slice 的 `memory.max`：若 slice 上限高于
`MEMORY_HARD_LIMIT_MB`（默认 8192），拒绝新准入而不是假装 8 GB 边界成立；若 slice 当前
`memory.current` 大于各分项之和，采用 slice 的实测值。

### 2.2 准入与计量

- `MEMORY_TARGET_MB=4096`、`MEMORY_BUDGET_MB=6144`、`MEMORY_HARD_LIMIT_MB=8192`、
  `MEMORY_RESERVE_MB=1024`、`MEMORY_HYSTERESIS_MB=256` 均可配置，作用域是**整个系统**，
  不是每实例。
- 每实例 cgroup 设置 `memory.max`/`memory.high`；采样 `memory.current`、`memory.peak`、
  `memory.events`、`memory.pressure` 与宿主 `MemAvailable`，写入
  `runtime_resource_samples` 与 `repository_instances.resource_*`。
- 低于预算直接准入；高于预算只在实测宿主余量、压力门槛允许时弹性扩至硬上限；高于硬上限、
  未知 cgroup、slice 上限配置不符时拒绝。
- 被拒仓库进入 `resource_wait`（占排队位置，不启动进程），低于
  `budget - hysteresis` 后才再次准入，避免抖动。
- OOM：进程退出时读取 cgroup `memory.events.oom_kill`，故障码为 `oom_killed`；连续
  `MAX_START_ATTEMPTS` 次启动/OOM/失联失败后进入 `retry_exhausted`，需要显式重试；
  恢复成功后清除旧 `error_code`/`reason`。

## 3. 界面投影

`RepositoryRuntime` 增加 `resource` 与 `lease`：

```jsonc
{
  "resource": {
    "state": "ok|resource_wait|over_budget|oom|unknown",
    "detail": "…", "memoryCurrentBytes": 123, "memoryPeakBytes": 456,
    "memoryLimitBytes": 789, "oomKillCount": 0, "checkedAt": "…"
  },
  "lease": {
    "fencingToken": "42", "recoveryState": "blocked",
    "waitingFor": "execution_stop_proof", "detail": "…", "stopProvenAt": null
  }
}
```

侧栏显示内存占用、等待内存余量、执行代次与停止证明等待。`resource_wait`、
`retry_exhausted`、`stop_failed` 与租约恢复阻塞都有独立原因，不显示猜测的完成时间。

## 4. 迁移与回滚

- 新增 `0013_loving_quentin_quire.sql` 与
  `packages/db/migrations/rollback/0013_loving_quentin_quire.down.sql`。
- 回滚要求 0013 是最后一条迁移，并传入 0013 SQL 的 sha256；回滚删除两个新表、新增列与
  约束，不触碰旧表数据。
- 实测：独立 PostgreSQL 16 上 0013 rollback → reapply 通过，见验证记录。
- 生产 `moyu` 库、3082 bootstrap、4300 线上实例与 Nginx 未改动。

## 5. 已知限制（未完成即如实标记）

1. **上游 admission 已实现但未部署到 live 实例**。`0023-execution-admission.patch` 在固定
   upstream commit 上通过 typecheck、构建与 1299/1300 上游测试（唯一失败是既有可选项 Feishu
   测试）。核心 retry/start/admit 现在必须携带十进制 admission，并在 KV 持久化仓库级最高
   fence；延迟 Trigger 低于 fence 时只记录为 `pending_admission`，不创建 Assignment；pause
   先于 admit 时核心拒绝启动。main Session 的 retry 工具因此返回 admission 拒绝而不是绕过。
   supervisor 先让子进程在 fd 3 上阻塞，写入实例 cgroup 后才释放 exec，消除执行前逃逸。
   受管 profile 的 env 现在固定写入 `MOYU_REQUIRE_ADMISSION=1`；capability 缺失时 dispatcher 拒绝
   不安全执行（`runtime_admission_unsupported`）而不是无 token 发送。仍未把构建切换进
   supervisor 管理的 live profile，也没有三仓库/重启/pause 交错的 live 证据。
2. **非 root supervisor 生命周期已验证，但全栈 8 GB 计量未完成**。独立 user/DB/profile/
   4 GiB slice 下已跑通 start→online→stop→no descendants→supervisor restart→no auto-start→
   wake；尚未把平台、隔离 DB、runtime 与构建测试放进同一 8 GiB slice 压到边界。
3. **双入口/竞态的 live 执行次数尚未取得**：隔离 runtime 已用 loopback 合成 provider
   实证 route→Ticket/Assignment，但 prepare 失败（非 root known_hosts/deploy key）后未启动
   worker；修复 clone 后同一 state 不再交付新 candidate。上游 Bridge 单测覆盖缺失/旧代次
   拒绝，但没有真实子进程/双入口执行计数。
   三仓库排队、恢复清错与完整六类矩阵仍未完成。
4. 因此本单不能宣称完整六类矩阵通过；PR 必须保持未完成状态，由所有者决定是否继续补齐。

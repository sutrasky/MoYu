# M5-B 执行生命周期诊断（2026-11-06）

## 责任方与持久状态

| 阶段 | 责任方 | 持久状态 | 关键 await / 重启边界 |
| --- | --- | --- | --- |
| 准入授予 | platform dispatcher `leases.ts` + upstream `Tickets.acceptAdmission` | `runtime_leases`、`admission-fence` KV（token/lease/ticket） | 租约到期、Bridge 请求前、`admit` 事务 |
| 实时输入 | upstream `Tickets.addInput` + `DshExecutor.deliver` | Ticket `undelivered`、`delivery` effect KV | `addInput` 事务提交后 enqueue |
| 持久重放 | `Effects` 消费 `launch`/`delivery` | effect KV、Ticket admission | 进程重启、effect 重试、session 恢复 |
| 最终会话副作用 | `DshExecutor.launch`/`deliverInput`/`retry` | Assignment `pending/running/ended`、Session inbox | prepare、`sessions.ensure`、`stopped`、`prompt` 授权回调 |
| 停止/替代 | upstream `Tickets.admit` + `DshExecutor.stop`/`stopped` | 进程内 `stopping`/`stopErrors`；Ticket ended | stop 后台 promise、重启 |
| 平台侧替代 | dispatcher `admissions.ts`、`operations.ts` | `task_links`、`runtime_leases`、`task_operations` | lease 等待、admit/retry、restart reconcile |

## 当前判定

- 旧代次实时投递：`Tickets.addInput` 和 `DshExecutor.deliverQueued`/`deliverInput` 已校验
  fence；旧代次保留 Input，不 prompt/不唤醒。
- 旧代次 launch：`launchQueued` 入口曾校验，但 `launch()` 的 `current()` 与 prompt 最终授权
  只查 Ticket/Assignment/暂停；在 prepare/ensure/stopped 的 await 中切到 B 新代次时 A 仍可能
  prompt/`assignmentStarted`。已把 `admissionCurrent` 加入 `current()`，因此每个 await 后的
  `current()` 和 prompt 授权回调都会拒绝 A。
- 持久重放：`launch`/`delivery` effect 消费时重新读取 Ticket 与 fence；旧 effect 不能唤醒。
- 停止证明：已新增 `src/executor/stop-proof.ts`，在 `stop()` 启动前持久记录旧
  lease/token、Assignment 和 **本 Ticket 的 job ID** 为 `unproven`；不再把整个 harness 的
  processTree 记入 A，避免 A 的证明污染 B。只有本次 stop 回调成功且本 Session 无运行 job 时才
  `completeStopProof`；unproven 记录不会被自动 settle，重启后空 job registry 也不能证明旧 job
  已退出。`track` 只结算发起该次 stop 的 Assignment，重复 record 保留既有证据。真实
  stop 拒绝分支已加入 DshExecutor 级测试（通用 `Error('kill refused')`，同一 host/内存 Map/
  Jobs 服务，直接调用私有 `assertStops`）。它只证明拒绝分支，不是真实 EPERM、真实受管
  runtime、持久存储重开或注册入口的 recovery replay；这些仍缺。
- 启动器：release 前登记 fd3 error/early exit，保存新 PID/cgroup ownership；同代事件已收敛
  一次失败。generation 比对与 DB 写失败交错仍缺独立探针。
- 第二候选：fresh-2 在 provider 前停住；现有证据只到 trigger 被存储、无新 assessment effect /
  provider 请求。尚未定位 trigger→eval→effect→session 的最早断点。

## 已有可失败/通过回归

- `tests/admission.spec.ts`：缺失/非数字/旧代次、同 token 第二 Ticket、wake 后再准入、
  旧代次 deliver 被挡、新代次替代一次、pause 优先。
- `apps/supervisor/test/process.test.ts`：EOF/错误信号/SIGKILL 父进程不执行、release EPIPE、
  cgroup 探针。
- `scripts/m5b/launcher-cgroup-check.sh`：实际 256 MiB probe、父子同组、marker、kill 后空。
- 未覆盖：真实 Jobs stop EPERM 持久证明、launch await 中切代次的真实 DshExecutor barrier、
  第二候选最早断点、双入口/pause-start/restart 现场计数。

## 安全行为

- 有 admission 但读不到 fence：受管实现拒绝执行并保留输入；只有无 admission 的旧 Ticket 走兼容路径。
- 无 admission 的旧 Ticket：保持 legacy 行为；受管新 Ticket 在 `requireAdmission` 下必须有 token。

## 恢复红证据（2026-11-06）

- 记录一个真实 `sleep` 的 PID/starttime/cgroup 到持久 proof，终止并回收该进程后
  `identityAlive=false`，但 `replacementBlocked` 仍为 true、proof 仍为 `unproven`，因为没有
  原 stop 成功回调。这是当前恢复闭环的失败断言，不是通过项。
- 缺口根因：DSH Jobs 只暴露 job id，不暴露进程归属；共享 harness 进程树不能作为 A 的边界。
  需要每 Ticket/Assignment 的受管进程边界（cgroup/pid namespace）或可持久对账的 job 终止
  事件，才能把“已清空”写成权威证明。否则只能保守占用名额。

## 跨存储准入断点证据（2026-11-07）

外部隔离回归（jd-mc，`tests/moyu-http-replay.spec.ts`，SHA256 `3da58493f6d142c65127208a9dacb51ad4afef21da1b195abb35f408941393a4`）：
真实 Bridge HTTP + SQLite，首次 admit 已提交后断响应并重开 Store。旧实现对错误代次也返回 200；050b84d 返回 422。一个 Assignment、一个持久 launch effect，重放不改变 generation。Ticket save 后、DshExecutor 入队前注入异常时，pending Ticket/fence/launch effect 一起回滚。

平台侧 PostgreSQL 预留行复用（本仓库 `apps/dispatcher/test/integration/reservations.integration.test.ts`，直接调用 helper，不是完整 dispatcher 断点）：
- `keeps one reserved identity across a lost Bridge response before claim`
- 断言同一 requestKey 的 reservation 保持 `reserved`、行数 1、身份/代次不变，重试返回原行，claim 恰好一次。
- 事件同步路径新增 `reconcileExecutionClaimOn`：只有 queued/running 事件且当前 lease 匹配时才把仍 reserved 的行 claim；lease 已释放时拒绝。
- 命令：`TEST_DATABASE_ADMIN_URL=postgres://moyu@127.0.0.1:55432/postgres pnpm --filter @moyu/dispatcher test:integration -- reservations`
- 结果：dispatcher 集成 55/55。

边界：两次存储不在同一事务；平台保留租约与 reservation 作为恢复依据，Bridge 的 SQLite 负责其本地 Assignment/launch effect。event-sync 先消费的整链重启仍未跑真实 dispatcher。

## 历史事件恢复（2026-11-07）

- event sync 在消费事件页前执行一次 `recoverReservedTasks`：对 queued/running 且 reservation 仍 reserved 的旧任务，读取 Bridge `/bridge/v1/tickets/:id` 权威快照，核对 repository/Ticket/Assignment/state，再由 `claimExecution` 复核 lease/fence。
- 快照缺失/不匹配时保留 reserved，并在 task stop_evidence 记录 `snapshot_mismatch`/`generation_mismatch`，不无证据 claim。
- 仍待：真实旧 producer→新 producer 升级 + 丢响应 + dispatcher 重启整链与真实启动计数。

## 恢复并发与预算（2026-11-07）

- mismatch、catch、成功三条路径都只更新仍 queued/running 的任务；stop_evidence 一律合并，成功只删除本次恢复的键。
- 单轮最多 32 条，按仓库隔离的 task-id cursor 轮转；失败/不匹配不重复写同值（含 catch 的 generation_mismatch）。
- 回归已收录：`recovery-positive`、`recovery-race`、`recovery-review-{catch,clear,idempotent,multi-repository,ordered-fairness,repeat-catch}`。
- 命令：`TEST_DATABASE_ADMIN_URL=postgres://moyu@127.0.0.1:55432/postgres pnpm --filter @moyu/dispatcher test:integration`；6 个恢复文件 12/12。
- 仍待：完整 Bridge admission lease/fence 权威证明、真实两库/dispatcher 重启、真实子进程启动数。cursor 为进程内状态，重启后从头部开始但会轮转。

## 晚到错代事件投影（2026-11-07）

- ticket.state_changed 身份与 reservation（无论 reserved/claimed）不一致时，只推进 last_event_seq 并合并 `admissionProjection: stale_generation`，不修改 task.state/不覆盖停止证明。
- 即使身份一致，stopped/paused 任务也不会被 queued/running 状态事件重新激活；只记 `stale_state` 并推进游标。
- 外部真实 HTTP/SQLite+PG 回归 `tests/moyu-review-b79a13b-stale-projection.spec.ts` 由红转绿（1/1）。
- 仍待：真实 pause/子进程、旧 producer 升级、两库 dispatcher 重启与启动计数。

## 新代次与暂停投影分支（2026-11-07）

- 事件身份先与当前 active lease/fence 比对；当前代次即使历史 reservation 不同也允许投影。
- 投影 UPDATE 使用 `CASE WHEN state IN ('stopped','stopping') THEN state ELSE $2 END`，在暂停事务提交后仍保持停止态；SQL 谓词不得出现 `'paused'`（不是 task_state 枚举值）。
- `projectionBranches` 记录每事件分支：`return-no-ticket-id`、`return-task-missing`、`return-stale-generation`、`return-stale-cursor`、`projected`。
- 回归：`apps/dispatcher/test/integration/recovery-projection-branches.integration.test.ts`（2/2）。
- 外部真实 HTTP/PG：`tests/moyu-review-cedcb82-current-projection.spec.ts` 与 `...pid-barrier-1039.spec.ts` 4/4。

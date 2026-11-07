# M5-B 验证记录

日期：2026-11-06。基线：`main@af35caf9315b22d8c421584409bea329372862b8`。
被测代码：本分支实现提交（最终完整 head 在 PR 固定评论中记录）。本文只记录实际执行过的
命令、结果与未验证项；未完成项不表述为通过。

## 1. 现场预检

| 项 | 实测 |
| --- | --- |
| 宿主 | Ubuntu 24.04.2、kernel 6.8.0-53、4 vCPU、16 373 000 kB RAM |
| cgroup | cgroup v2 unified；memory 控制器可在独立子树启用 |
| 服务 | systemd 可用；Docker 未安装；Node 24.22.2（`/opt/moyu-node24`）、pnpm 11.7.0 |
| 数据库 | 隔离 PostgreSQL 16 `127.0.0.1:55432`；生产 `moyu` 未触碰 |
| 基线 | `origin/main` 与工作树 HEAD 均为 `af35caf…`；工作树开始时干净 |
| 既有服务 | 3082 bootstrap、4300 线上 API、3401–3404 M5-A 实例未改动 |

## 2. 静态检查与单元测试

```bash
corepack pnpm check
# eslint: 通过
# prettier --check: 通过
# typecheck: 8 个项目通过
# unit: contracts 13、runtime-client 13、api 20、dispatcher 3、supervisor 27
#       以及 guard 15 / verify-lock 15
corepack pnpm build
# API/dispatcher/supervisor/web 全部构建成功
```

构建制品 sha256（实现构建，非发布制品）：

```text
61543347b7e4e9932d519210699000526d60dcb11621230e445e82657c4c470c  apps/api/dist/server.js
cd5b5ccf7aa86eb26d9e54de9f76ea9041c21084c5a7f098d6f42b7b63bcc2e5  apps/dispatcher/dist/main.js
fe2c47c8add5e8c6391b5cbf91c3ef2dc9e871d27a6d8a5a650b558c757b6a36  apps/supervisor/dist/main.js
b55236342ec8251d9890787cf6e0b45829a322b51cf99c5daaa36c1f74fe9746  apps/web/dist/assets/index-Cng83waZ.js
39a2cbfc47d44d272bc18fad5772346b891e06713d07009e4357f7af51cb5424  apps/web/dist/assets/index-DnIo0nRx.css
b06e32aebd8dfa96845f74dc916613de7f4d775935c7ce27f5875fcef13f8196  packages/db/migrations/0013_loving_quentin_quire.sql
```

## 3. 真实 PostgreSQL 集成测试

```bash
TEST_DATABASE_ADMIN_URL=postgres://moyu@127.0.0.1:55432/postgres corepack pnpm test:integration
```

| 套件 | 结果 |
| --- | --- |
| `@moyu/db`（含 0013 rollback → reapply） | 10 文件 / 36 测试通过 |
| `@moyu/api`（M4 暂停继续、双入口、分页、仓库生命周期） | 11 文件 / 66 测试通过 |
| `@moyu/dispatcher`（多仓库、operations、租约、复审回归、admission） | 4 文件 / 49 测试通过 |

租约测试覆盖：

1. 两个任务并发竞争同一仓库，只产生一条 `active` 租约，另一方得到 `held`。
2. 过期租约没有停止证明时拒绝替代（`stop_proof_required`），且不插入新代次。
3. 运行中的 Ticket 被原代次接管；停止证明到达后旧行 `recovered`，新代次 token 严格增大。
4. 旧代次的回执被拒绝，`runtime_lease_events` 记录 `stale_receipt_rejected`，任务状态不变。
5. 空闲/等待人工状态释放名额，任务与租约绑定清除。

资源策略单元测试覆盖预算内准入、硬上限拒绝、宿主余量/压力弹性、迟滞、未知 cgroup 按估算
计入、slice 上限低于/高于策略等 7 个用例。

### 复审回归（M5B-PR28-REVIEW-590524F）

- `releaseTaskLease` 现在锁定 task+lease 并接受 `expectedLeaseId`；`blocked`/`stopping` 没有停止证明时不释放，
  `waiting_input`/`awaiting_review`/`completed`/`closed_unmerged` 与已记录 `stopped_at` 才是确认安全状态。
- `maintainLeases` 只释放确认安全状态，且跳过 queued/running 的 owner 命令，避免释放 resume/retry 新租约。
- `applyOperationResult` 在同一个事务中锁定 lease+task、校验 fencing 并写入任务/操作，旧 token 不再可能先检查后写入。
- `delivery` 的 delivered/failed/filtered/retry 写入全部带 `runtime_lease_id`+`fencing_token` 条件；旧回执不改状态。
- 未证停止的 pause 会记录任务当前代次并要求 `execution_stop_proof`；新增真实 PostgreSQL 回归证明第二执行者无法取得租约。
- `assessAdmission` 在 slice/受管 cgroup 无法测量时直接拒绝；cgroup 创建失败或 `memory.max/high` 写入失败时不再 spawn。
- 估算值不再写入实测字段；`runtime_resource_samples` 的 aggregate `memoryCurrentBytes` 在边界未知时为 null，估算单独记录。
- systemd `Slice=` 修正到 `[Service]`，`systemd-analyze verify` 不再报告该键被忽略；OOM 探针改为独占 slice/unit，断言
  anchor 归属、8 GiB/256 MiB 限额、`oom_kill=1` 与宿主余量，失败返回非零。
- 仍未闭合：start/retry/在线 GitHub 的上游 admission 契约（见 §8）。

### 非 root supervisor 生命周期

- 独立 user `moyu-m5b`（uid 995）、独立 DB `moyu_m5b_sup`、独立 profile
  `probe1`（panel 3702 / Bridge 3701 / control 3790），supervisor 运行在
  `/m5bsupprobe.slice/m5b-sup.service/supervisor`，实例运行在同一 slice 的
  `/instances/...` 子组，slice `MemoryMax=4G`、实例 `memory.max=512M`。
- 启动：wake 后 DSH PID 758912 以 uid 995 运行在实例 cgroup，`memory.current=228519936`，
  宿主 `MemAvailable=14027196 kB`；Bridge/panel 就绪后状态 `online`。
- 停止：sleep 返回 accepted，进程消失、3701/3702 关闭、实例 cgroup `cgroup.procs` 为空。
- 重启：停止 supervisor 后控制/运行端口关闭且无 DSH；重启后实例保持 sleeping、不自动启动，
  再次 wake 可重新 online；随后再次 clean stop。
- 本步修复了非 root cgroup 路径：子进程在 `exec` 前阻塞于 fd 3，supervisor 先把它写入
  实例 cgroup 再释放；启动/停止中断都不会有用户代码在边界外运行。另把可读且空的 cgroup
  作为边界证明（无 cgroup 的 unknown 仍然阻止释放）。supervisor 单测 33 通过，含 EOF/错误信号/SIGKILL 父进程不执行及
  “释放前不执行命令”回归；独立 slice 探针实测第一代与提前 fork 子进程同属
  `m5b-launcher-*.slice/probe`，kill 后为空（`docs/evidence/m5b/launcher-cgroup.txt`）。
- 证据 `docs/evidence/m5b/nonroot-supervisor-lifecycle.txt`。

### 本地测试 provider 尝试（未通过）

- 在隔离 0023 profile 中把 `llm-pi-ai` 的 `baseURL` 指向 loopback
  `http://127.0.0.1:3801/v1`，模型 `local-test`，使用 dummy key；provider 只返回合成响应。
- DSH 接受了该 provider 和两条 workspace candidate（`workspace:dual-20/30`，explicit；
  实际 provider 触发 ID 为 `workspace:dual-30`），
  主 Session 工具列表含 `orchestrator_route`。第一次合成路由参数形状错误，工具返回
  `expected array at decisions`；修正为 `{decisions:[{kind,trigger_id,...}]}` 后仍未提交
  ticket/decision，Bridge events 为空，没有 Assignment 或子进程。
- 因此只证明了 provider 接口可加载和 candidate 可送达，未取得真实执行/双入口/竞态计数；
  证据 `docs/evidence/m5b/local-provider-attempt.txt`，测试脚本
  `scripts/m5b/local-provider.mjs`。共享 3082 和真实凭据未介入。

### 上游 admission 补丁（0023）

- 在固定 upstream `6337fc823ce20251c8de8085948165c81aaa701e` + 0001–0022 上生成
  `integrations/superagent/patches/0023-execution-admission.patch`；`build-patched.sh` 全量
  `git apply --check`、typecheck、构建通过。
- 上游测试：1301/1302 通过；唯一失败是 `tests/sources.spec.ts` 的既有可选 Feishu 用例
  （README 已记录，不是本补丁引入）。
- 定向 admission 测试：`tests/admission.spec.ts` 7、`tests/bridge.spec.ts` 24、`tests/pause.spec.ts` 16、
  `tests/bridge-control.spec.ts` 25 全通过，覆盖：
  - 无 admission 的 workspace trigger 返回 422 且不落 Trigger、不产生执行；
  - start/retry 无 admission 返回 422 且不调用启动；带当前 admission 才入队；
  - GitHub Ticket 创建后停在 `pending_admission`、无 Assignment；`/admit` 后才启动；
  - 旧代次 token 被 `admissionStale` 拒绝；跨 Ticket 的仓库 fence 拒绝延迟 Trigger；
  - 同一 token 的第二个 Ticket 被 fence 挡住；pause 先于 admit 时不创建 Assignment；wake 无 admission 时不创建 Assignment。
- 构建制品：`/root/src/upstream/m5b-admission-build2`；hash 见
  `docs/evidence/m5b/upstream-admission-build.txt`。
- 平台侧按 `capabilities.admission` 发送 admission；`pending_admission` 任务由
  `admitImportedOnce` 取得租约后调用 `/admit`，失败/无租约不创建 worker。新增真实
  PostgreSQL 回归见 dispatcher 集成测试。
- **隔离 runtime live 拒绝证据**：用 `0023` 构建启动独立 profile（panel 3602 / Bridge 3601），
  在 `m5bprobeadm.slice/m5b-adm-runtime.service` 的 512 MiB cgroup 内设置
  `MOYU_REQUIRE_ADMISSION=1`。capabilities 返回 `admission:true`；无 admission 的 trigger、
  start、retry 均返回 422，Bridge 事件 0 行，没有 Trigger/Ticket/Assignment 或副作用；
  带 admission 但未配置的实例返回 503。证据 `docs/evidence/m5b/live-admission-runtime.txt`。
- **未部署到运行中的 M5-B 实例**：该 profile 是独立验证实例，没有切换到 supervisor 管理的
  live profile，也没有真实三仓库/重启/pause 交错证据；`MOYU_REQUIRE_ADMISSION=1` 才启用。

## 4. 迁移回滚

```bash
# 独立 PostgreSQL 16
psql -v hash=<0013 sha256> -f packages/db/migrations/rollback/0013_loving_quentin_quire.down.sql
DATABASE_URL=postgres://moyu@127.0.0.1:55432/<isolated> \
  corepack pnpm --filter @moyu/db db:migrate
```

结果：0013 是最后一条迁移时回滚成功（删除两个新表、9 个新增列/约束），错误/缺失 hash 与
非最后一条时拒绝；reapply 后表、列、索引恢复。`rollback-0013` 测试通过。

## 5. 非 root 隔离 cgroup 与 8 GiB slice

```bash
bash scripts/m5b/nonroot-oom-check.sh
```

实测（`docs/evidence/m5b/nonroot-oom.log`）：

| 检查 | 结果 |
| --- | --- |
| 宿主启动前 MemAvailable | 14 327 384 kB（高于 8 GiB 安全下限） |
| slice | 独占 `m5bprobe700863.slice`（运行前不存在，结束后删除） |
| slice 控制组 | `/m5bprobe700863.slice` |
| slice `memory.max` / `memory.high` | 8 589 934 592 / 6 442 450 944 字节（8 / 6 GiB） |
| anchor 归属 | `/m5bprobe700863.slice/m5bprobe700863-anchor.service`，已核对在 slice 内 |
| 压测进程身份 | `nobody`（uid 65534），运行在 slice 内 256 MiB scope |
| scope `memory.max` / peak | 268 435 456 / 268 435 456 字节 |
| `memory.events` | `oom 1 oom_kill 1`；systemd `Result=oom-kill`，swap 0 |
| 宿主结束后 MemAvailable | 14 080 328 kB（无宿主级 OOM） |

该 fixture 先读取自身 `memory.max`，只有 ≤256 MiB 才分配；使用 `MemorySwapMax=0` 强制在
cgroup 内 OOM，而不是让宿主回收/换出。脚本使用独占 slice/unit 名称，只清理本次创建的对象，
拒绝或未发生 OOM 时返回非零。此证据证明非 root 子组、单一 slice 上限与受控 OOM 可用；
**不**证明完整非 root supervisor 生命周期或平台/DB/runtime/三仓库同 slice 压力。

## 6. 浏览器 before/after

两个真实 API 实例读取同一个隔离数据库：before 为 `main@af35caf` 的 Web/API 构建，
after 为本分支构建；同一仓库、视口与主题。运行行是固定的运行时投影 fixture（用于视觉状态），
后端行为由上面的集成/单元测试验证，不把 fixture 当作 live supervisor 证据。

```bash
M5B_SESSION_TOKEN=<isolated session> node scripts/m5b/screenshots.mjs
M5B_SESSION_TOKEN=<isolated session> node scripts/m5b/ui-evidence.mjs
```

产物：`docs/evidence/m5b/` 12 张截图（beta 资源等待：桌面/手机、浅/深；gamma 重试/OOM；
alpha 在线并持有租约）、`ui-checks.json`、`records.json`。

DOM 断言结果：

| 状态 | before | after |
| --- | --- | --- |
| 资源等待仓库显示“等待内存余量 / 内存不足，等待实测余量” | 否 | 是 |
| 在线仓库显示执行代次 | 否 | 是 |
| 故障仓库显示“自动重试已用尽 / 已发生 OOM / 等待运行时” | 否 | 是 |

## 7. 故障事件（必须记录）

2026-11-06 约 01:53 CST，在一次非 root cgroup 探针中，内存压测进程未能写入预期的委托
cgroup（`cgroup.procs` 写入失败），实际运行在
`/system.slice/dsh-superagent.service`，触发内核全局 OOM，杀死了约 14.7 GB anon-rss 的
node 进程，主 3082 服务随之中断。未修改生产数据、Nginx、3082 配置或 systemd unit；
空的测试 cgroup 已清理，主服务由所有者恢复。此后没有再次执行宿主级内存压力测试。

该事件说明：没有先验证进程所在 cgroup 就分配内存是不安全的；仓库中的压测 fixture
`scripts/m5b/memory-hog.mjs` 因此先读取自身 `memory.max`，超过 256 MiB 或缺失即拒绝分配，
并且只应通过 `systemd-run` 放入独立 slice 后使用。

## 8. 未验证项

1. **非 root supervisor 的真实生命周期**：已用 `nobody` 在 256 MiB scope 中验证 cgroup
   OOM 与 8 GiB slice 上限，但未以非 root 运行 supervisor 本身，也未跑启动/停止/失联/重试
   的完整非 root 生命周期。
2. **平台+DB+runtime+构建测试同 slice 的完整压力**：slice 上限与子组 OOM 已验证；未在真实
   平台、隔离 DB、runtime 与三仓库同时运行时压到边界，不把 256 MiB 子组证据外推为完整
   8 GiB 聚合验收。
3. **三仓库资源压力、恢复后清错**：未完成 live 复现；OOM 子组证据不等于三仓库调度证据。
4. **上游 runtime admission**：start/retry/GitHub Trigger 尚未带上 admission；当前 fencing
   在平台控制面执行，在线 GitHub 入口存在 runtime 先执行的窗口。
5. 因此六类矩阵未完整通过，PR 不能按“全阶段完成”交付；需要继续补齐上述 live 证据与
   上游契约改动。

## 9. 回滚与清理

- 代码回滚：revert 本 PR；迁移回滚见 §4（只在 0013 为最后一条时执行）。
- 测试 UI：隔离 API 已停止，`moyu_m5b_ui` 已删除；截图与 JSON 保留在
  `docs/evidence/m5b/`。
- 测试 cgroup：`/sys/fs/cgroup/moyu-m5b-*` 已无残留；未改动线上/共享 cgroup。


### supervisor cgroup 停止证明切片（2026-11-06）

- 新增 `apps/supervisor/src/stop-proof.ts`：持久绑定 runtimeId/generation/cgroup inode/
  Ticket/Assignment/lease/token，`bindStopProof` 在停止前写 `unproven`；`verifyStopProof`
  仅在路径 inode 未重用、绑定字段齐全且 `cgroup.events` 为 `populated 0` 时写 `proven`。
- 路径缺失、generation 不符、inode 重用、events 不可读、仍有后代均拒绝出证。
- 真实 cgroup 测试：sleep 移入专属 cgroup 时 refused(`populated`)，SIGKILL/空后 proven；
  覆盖 missing、generation mismatch。supervisor 35 通过。
- 尚未接入 dispatcher 的 launch/delivery 消费和旧入口封存；这是下一步集成缺口。

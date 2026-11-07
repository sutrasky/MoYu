# superagent 集成基线

本目录固定本项目依赖的上游 superagent 与 Harness 制品，并记录后续补丁的边界。M0 只做取证与锁定，没有修改上游或 bootstrap 实例。

## 文件

| 文件 | 用途 |
| --- | --- |
| `upstream.lock.json` | 可追溯版本、commit/tarball 摘要、安装清单摘要、构建结果与已知失败 |
| `compatibility-findings.md` | Source / Session / stop-retry / 模型 / 执行准入的真实差异与补丁落点 |
| `verify-lock.sh` | 只读核对现场安装制品是否仍与锁定值一致 |
| `probes/` | 可复现的两实例 GitHub 通知探针步骤 |
| `evidence/` | 本次真实通知实验的原始值快照（测试仓库可能不可访问） |
| `patches/`（后续） | 按顺序应用的补丁；`git apply` 失败即停止构建，不模糊套用 |

环境、隔离 profile、两实例通知实验与清理步骤见 [`docs/environment-baseline.md`](../../docs/environment-baseline.md)。

## 固定组合

- superagent：`deepseek-ai/dsh-superagent-prototype` commit `6337fc823ce20251c8de8085948165c81aaa701e`
- Harness：`@deepseek-ai/dsh@0.2.1-rc.2`（tag `dsh-v0.2.1-rc.2`）
- Node 22.23.3；构建用 `pnpm@11.7.0`

## 校验

```bash
integrations/superagent/verify-lock.sh
```

可选传入 tarball 做网络侧复核：

```bash
SUPERAGENT_TARBALL=/path/to/6337fc8.tar.gz \
DSH_TARBALL=/path/to/dsh-0.2.1-rc.2.tgz \
integrations/superagent/verify-lock.sh
```

回归检查用临时制品覆盖普通、含空格和含单引号的目录，并验证修改制品后返回非零：

```bash
node --test integrations/superagent/tests/verify-lock.test.mjs
```

## M4 补丁

在 M3 的 `0001`–`0004` 之后追加有序补丁：

| 补丁 | 作用 |
| --- | --- |
| `0005-discussion-mode.patch` | 只读讨论工具/权限、`discussing` 投影 |
| `0006-recoverable-pause.patch` | 可恢复暂停、停止证据、resume admission、start |
| `0007-bridge-execution-control.patch` | pause/resume/start/retry、operation 日志、`targetTicketId` |
| `0008-github-import.patch` | `ticket.imported` 投影 |
| `0022-link-and-ignore-events.patch` | 既有 workspace Ticket 关联 GitHub 线程的 import 事件、ignore 决策事件 |
| `0009-discussion-ready-stays-open.patch` | 讨论 ready 不结束 Ticket，owner 仍可 start |
| `0010-projection-current-assignment.patch` | 新 Assignment 后不再用旧 ready report 挡住运行态 |
| `0011-resume-does-not-reopen-closed-pr.patch` | resume/start 指示不重开已关闭未合并的 PR |
| `0023-execution-admission.patch` | M5-B 执行租约 admission：trigger/start/retry 必须携带当前代次；GitHub Ticket 先 `pending_admission`，平台 `/admit` 后才创建 Assignment；`MOYU_REQUIRE_ADMISSION=1` 启用 |

构建与验证见 [`docs/M4-VERIFICATION.md`](../../docs/M4-VERIFICATION.md)。

## 约束

- 不把 `/root/dsh-superagent` 当作可编辑发布物；补丁在固定 commit 的 checkout 上生成，并作为有序 patch 提交。
- 运行 `pnpm check` 必须用干净环境，避免 worker shell 的 `GIT_CONFIG_GLOBAL`/`PATH` 污染（见 environment-baseline §4）。
- 当前上游 `pnpm check` 有 1 条可选 Feishu 测试失败；不得把它描述成全绿。
- 不提交 token、私钥、Cookie、真实绑定码或数据库内容；测试仓库只放探针文件。
- 不修改 bootstrap profile/state/3082；被测实例必须使用独立 `DSH_HOME`、state、端口和测试仓库。

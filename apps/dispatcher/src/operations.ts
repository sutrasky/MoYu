import type pg from 'pg';

import type { TaskOperationKind, TaskOperationState } from '@moyu/contracts';
import type { BridgeOperation, RuntimeBridgeClient } from '@moyu/runtime-client';

import type { DispatcherConfig } from './config';
import { appendEventSql } from './events-sql';
import {
  acquireExecutionLease,
  ACTIVE_EXECUTION_STATES,
  receiptIsCurrentOn,
  reconcileLeases,
  releaseTaskLease,
  requireLeaseStopProof,
  type ExecutionLease,
} from './leases';
import { pauseEvidenceProblem, pauseEvidenceProven } from './pause-evidence';
import type { DispatcherLogger } from './logger';
import type { RepositoryBinding } from './runtime';
import { bridgeErrorMessage, isRetryableBridgeError } from './runtime';
import { stateFromSnapshot, TASK_COLUMNS, taskSummary, type TaskRow } from './tasks';

export const OPERATION_RUN_INSTANCE = `moyu-operations-${process.pid}`;

interface OperationRow {
  id: string;
  taskId: string;
  kind: TaskOperationKind;
  state: TaskOperationState;
  bridgeOperationId: string | null;
  attemptCount: number;
  evidence: Record<string, unknown>;
  runtimeLeaseId: string | null;
  fencingToken: string | null;
}

interface ClaimedOperation extends OperationRow {
  repositoryId: string;
}

const OPERATION_COLUMNS = `
  op.id, op.task_id as "taskId", op.repository_id as "repositoryId", op.kind, op.state,
  op.bridge_operation_id as "bridgeOperationId", op.attempt_count as "attemptCount", op.evidence,
  op.runtime_lease_id as "runtimeLeaseId", op.fencing_token as "fencingToken"
`;

async function appendTaskEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  task: TaskRow,
  sourceEventId?: string,
): Promise<void> {
  await appendEventSql(client, {
    type: 'task.state_changed',
    source: 'dispatcher',
    sourceEventId: sourceEventId ?? null,
    repositoryId: repository.id,
    conversationId: task.conversationId,
    taskId: task.id,
    payload: { task: taskSummary(task) },
  });
}

async function taskById(
  client: pg.PoolClient,
  taskId: string,
  forUpdate = false,
): Promise<TaskRow | null> {
  const result = await client.query<TaskRow>(
    `select ${TASK_COLUMNS} from task_links t where t.id = $1 limit 1${forUpdate ? ' for update' : ''}`,
    [taskId],
  );
  return result.rows[0] ?? null;
}

/**
 * Claims one queued operation for this repository. A lease prevents two
 * dispatchers from sending the same command; the runtime operation id is the
 * second idempotency boundary when a lease expires mid-send.
 */
async function claimOperation(
  pool: pg.Pool,
  config: DispatcherConfig,
  repositoryId: string,
): Promise<ClaimedOperation | null> {
  const result = await pool.query<ClaimedOperation>(
    `with candidate as (
       select op.id
         from task_operations op
        where op.repository_id = $1
          and op.state = 'queued'
          and op.next_attempt_at <= now()
          and (op.lease_expires_at is null or op.lease_expires_at <= now())
        order by op.created_at
        limit 1
        for update of op skip locked
     )
     update task_operations op
        set lease_owner = $2,
            lease_expires_at = now() + make_interval(secs => $3),
            attempt_count = op.attempt_count + 1,
            updated_at = now()
       from candidate cand
      where op.id = cand.id
      returning ${OPERATION_COLUMNS}`,
    [repositoryId, OPERATION_RUN_INSTANCE, config.operationLeaseMs / 1000],
  );
  return result.rows[0] ?? null;
}

async function loadTaskForOperation(pool: pg.Pool, taskId: string): Promise<TaskRow | null> {
  const result = await pool.query<TaskRow>(
    `select ${TASK_COLUMNS} from task_links t where t.id = $1 limit 1`,
    [taskId],
  );
  return result.rows[0] ?? null;
}

interface Queryable {
  query(text: string, values?: unknown[]): Promise<{ rowCount: number | null }>;
}

async function storeOperationEvidence(
  database: Queryable,
  operationId: string,
  state: TaskOperationState,
  fields: {
    bridgeOperationId?: string | null;
    evidence?: Record<string, unknown>;
    error?: string | null;
    retryInSeconds?: number;
  },
): Promise<void> {
  await database.query(
    `update task_operations
        set state = $2,
            bridge_operation_id = coalesce($3, bridge_operation_id),
            evidence = $4::jsonb,
            error = $5,
            next_attempt_at = case when $6::double precision is null then now()
                                   else now() + make_interval(secs => $6::double precision) end,
            lease_owner = null,
            lease_expires_at = null,
            updated_at = now()
      where id = $1`,
    [
      operationId,
      state,
      fields.bridgeOperationId ?? null,
      JSON.stringify(fields.evidence ?? {}),
      fields.error ?? null,
      fields.retryInSeconds ?? null,
    ],
  );
}

/** Applies a terminal operation result to the task; returns the updated task. */
async function applyOperationResult(
  pool: pg.Pool,
  repository: RepositoryBinding,
  operation: OperationRow,
  bridgeOperation: BridgeOperation,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    const task = await taskById(client, operation.taskId, true);
    if (!task) {
      await storeOperationEvidence(client, operation.id, 'failed', {
        error: 'The platform task no longer exists',
      });
      await client.query('commit');
      return;
    }
    // A result that belongs to a superseded lease generation must not rewrite
    // the task. It is audited on the operation and lease, then dropped.
    if (operation.runtimeLeaseId) {
      const receipt = await receiptIsCurrentOn(client, {
        repositoryId: repository.id,
        leaseId: operation.runtimeLeaseId,
        fencingToken: operation.fencingToken,
        taskId: task.id,
        operationId: operation.id,
        kind: operation.kind,
      });
      if (!receipt.current) {
        await storeOperationEvidence(client, operation.id, 'failed', {
          error: receipt.reason ?? 'stale execution generation',
          evidence: { stale: true, runtimeState: bridgeOperation.state },
        });
        await client.query('commit');
        logger.warn(
          {
            event: 'operation.stale_receipt',
            operationId: operation.id,
            taskId: task.id,
            leaseId: operation.runtimeLeaseId,
            reason: receipt.reason,
          },
          'refused a runtime result from a superseded execution generation',
        );
        return;
      }
    }
    let evidence = bridgeOperation.evidence ?? {};
    // The runtime settles a pause operation and the ticket's stop evidence
    // together, but a poll can observe the operation a moment before the evidence
    // reaches it. Re-read the ticket's authoritative paused evidence before
    // treating a successful pause as unproven.
    if (
      operation.kind === 'pause' &&
      bridgeOperation.state === 'succeeded' &&
      !pauseEvidenceProven(evidence) &&
      task.upstreamTicketId
    ) {
      try {
        const snapshot = await bridge.ticket(task.upstreamTicketId);
        if (snapshot.paused?.stoppedAt) {
          evidence = { ...snapshot.paused.evidence, stoppedAt: snapshot.paused.stoppedAt };
        }
      } catch {
        // keep the operation evidence; the pause stays unproven and is not shown as stopped
      }
    }
    // A pause is only proven with a stop timestamp, no loaded worker or an idle
    // one, and an empty job/subagent set. `absent` means the runtime restarted and
    // the worker Session is not loaded, so nothing of it can still run.
    const pauseProven = pauseEvidenceProven(evidence);
    const succeeded =
      bridgeOperation.state === 'succeeded' && (operation.kind !== 'pause' || pauseProven);
    let nextState: string | null = null;
    let stoppedAt: Date | null | undefined;
    let storeEvidence = false;
    let executionMode = task.executionMode;
    let clearStop = false;

    if (succeeded && operation.kind === 'pause') {
      if (task.state === 'stopping') {
        nextState = 'stopped';
        const stopped =
          typeof evidence.stoppedAt === 'string' ? new Date(evidence.stoppedAt) : null;
        stoppedAt = stopped && !Number.isNaN(stopped.getTime()) ? stopped : new Date();
      }
    } else if (succeeded && operation.kind === 'start') {
      nextState = 'queued';
      executionMode = 'execute';
    } else if (succeeded && operation.kind === 'resume') {
      if (task.state === 'stopped') {
        nextState = 'queued';
        clearStop = true;
      }
    } else if (succeeded && operation.kind === 'retry') {
      if (task.state === 'failed' || task.state === 'blocked') {
        nextState = 'queued';
      }
    } else if (!succeeded && operation.kind === 'pause') {
      // A stop that could not be proven is not shown as stopped.
      if (task.state === 'stopping') {
        nextState = 'blocked';
        storeEvidence = true;
      }
    }

    let updated = task;
    if (
      nextState !== null ||
      stoppedAt !== undefined ||
      storeEvidence ||
      executionMode !== task.executionMode ||
      clearStop
    ) {
      const result = await client.query<TaskRow>(
        `update task_links
            set state = coalesce($2, state),
                stopped_at = case when $3::boolean then $4::timestamptz
                                  when $5::boolean then null else stopped_at end,
                stop_evidence = case when $5::boolean then '{}'::jsonb
                                     when $8::boolean then $6::jsonb
                                     when $3::boolean then $6::jsonb else stop_evidence end,
                execution_mode = $7,
                version = version + 1,
                updated_at = now()
          where id = $1
          returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
        [
          task.id,
          nextState,
          stoppedAt !== undefined,
          stoppedAt ?? null,
          clearStop,
          JSON.stringify(
            pauseProven ? evidence : { ...evidence, problem: pauseEvidenceProblem(evidence) },
          ),
          executionMode,
          storeEvidence,
        ],
      );
      updated = result.rows[0] ?? task;
      if (executionMode !== task.executionMode || operation.kind === 'start') {
        await client.query(
          `update conversations
              set execution_mode = $2,
                  plan_revision = coalesce(plan_revision, $3),
                  updated_at = now()
            where id = $1`,
          [task.conversationId, executionMode, task.planRevision],
        );
      }
      await appendTaskEvent(client, repository, updated, `operation:${operation.id}`);
    }

    await storeOperationEvidence(client, operation.id, succeeded ? 'succeeded' : 'failed', {
      bridgeOperationId: bridgeOperation.operationId,
      evidence,
      error: succeeded
        ? null
        : (bridgeOperation.error ??
          (operation.kind === 'pause'
            ? 'The runtime did not prove the worker and its background work stopped'
            : 'The runtime operation failed')),
    });
    await client.query('commit');
    const safeToRelease =
      (operation.kind === 'pause' && succeeded && pauseProven) ||
      ['completed', 'closed_unmerged'].includes(updated.state);
    if (safeToRelease) {
      await releaseTaskLease(
        pool,
        repository.id,
        updated.id,
        `operation ${operation.kind} settled safely as ${updated.state}`,
        {
          ...(operation.runtimeLeaseId ? { expectedLeaseId: operation.runtimeLeaseId } : {}),
          ...(operation.kind === 'pause' && pauseProven ? { proof: evidence } : {}),
        },
      ).catch(() => undefined);
    } else if (['blocked', 'stopping'].includes(updated.state)) {
      // Unknown stop state: keep the generation and let reconciliation obtain
      // the proof. Never free the slot on a failed/unproven pause. A pause
      // command records the task's current generation for this check.
      const generation = operation.runtimeLeaseId ?? updated.runtimeLeaseId;
      if (generation) {
        await requireLeaseStopProof(
          pool,
          repository.id,
          generation,
          `operation ${operation.kind} settled as ${updated.state} without a stop proof`,
        ).catch(() => undefined);
      }
    }
    logger.info(
      {
        event: 'operation.settled',
        operationId: operation.id,
        taskId: operation.taskId,
        kind: operation.kind,
        state: bridgeOperation.state,
        taskState: updated.state,
      },
      'task operation settled',
    );
  } catch (error) {
    await client.query('rollback').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

async function failOperation(
  pool: pg.Pool,
  repository: RepositoryBinding,
  operation: OperationRow,
  error: string,
  code?: string,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    const task = await taskById(client, operation.taskId, true);
    if (
      task &&
      ((operation.kind === 'pause' && task.state === 'stopping') ||
        (operation.kind === 'resume' && task.state === 'stopped'))
    ) {
      // A resume the runtime cannot satisfy is blocked; a closed PR is terminal for
      // this task and is shown as closed_unmerged without auto-reopening.
      const state =
        operation.kind === 'resume' && code === 'prClosedUnmerged' ? 'closed_unmerged' : 'blocked';
      const updated = await client.query<TaskRow>(
        `update task_links set state = $3, stop_evidence = $2::jsonb,
                version = version + 1, updated_at = now()
          where id = $1 returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
        [task.id, JSON.stringify({ error, ...(code ? { code } : {}) }), state],
      );
      const next = updated.rows[0];
      if (next) {
        await appendTaskEvent(client, repository, next, `operation:${operation.id}`);
      }
    }
    await storeOperationEvidence(client, operation.id, 'failed', { error });
    await client.query('commit');
  } catch (caught) {
    await client.query('rollback').catch(() => undefined);
    throw caught;
  } finally {
    client.release();
  }
}

/** Applies the runtime's current ticket state, used when an operation is refused because the ticket moved on. */
async function applySnapshot(
  pool: pg.Pool,
  repository: RepositoryBinding,
  taskId: string,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
): Promise<void> {
  let snapshot;
  try {
    const task = await loadTaskForOperation(pool, taskId);
    if (!task?.upstreamTicketId) {
      return;
    }
    snapshot = await bridge.ticket(task.upstreamTicketId);
  } catch (error) {
    logger.warn(
      { event: 'operation.snapshot_failed', taskId, error: bridgeErrorMessage(error) },
      'could not read the runtime ticket after an operation was refused',
    );
    return;
  }
  const state = stateFromSnapshot(snapshot.state);
  if (!state) {
    return;
  }
  const client = await pool.connect();
  try {
    await client.query('begin');
    const task = await taskById(client, taskId);
    const paused = snapshot.paused;
    const proven = pauseEvidenceProven(
      paused?.stoppedAt ? { ...paused.evidence, stoppedAt: paused.stoppedAt } : undefined,
    );
    const problem =
      paused?.stoppedAt && !proven
        ? pauseEvidenceProblem({ ...paused.evidence, stoppedAt: paused.stoppedAt })
        : undefined;
    // The snapshot path is the same stop-evidence boundary as the event, poll and
    // restart paths: a timestamp without an idle worker/no jobs/no subagents is not stopped.
    const effectiveState = state === 'stopped' && !proven ? 'blocked' : state;
    if (!task || task.state === effectiveState) {
      await client.query('commit');
      return;
    }
    const stoppedAt = proven ? new Date(paused!.stoppedAt!) : null;
    const result = await client.query<TaskRow>(
      `update task_links
          set state = $2, stopped_at = $3::timestamptz, stop_evidence = $4::jsonb,
              execution_mode = $5, version = version + 1, updated_at = now()
        where id = $1 returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
      [
        taskId,
        effectiveState,
        stoppedAt,
        JSON.stringify(
          proven
            ? (paused?.evidence ?? {})
            : { ...paused?.evidence, ...(problem ? { problem } : {}) },
        ),
        snapshot.mode === 'discuss' ? 'discuss' : 'execute',
      ],
    );
    const next = result.rows[0];
    if (next) {
      await appendTaskEvent(client, repository, next, `snapshot:${taskId}:${Date.now()}`);
    }
    await client.query('commit');
  } catch (error) {
    await client.query('rollback').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

async function sendOperation(
  pool: pg.Pool,
  config: DispatcherConfig,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  operation: ClaimedOperation,
  task: TaskRow,
  logger: DispatcherLogger,
  admissionSupported = true,
): Promise<void> {
  if (!task.upstreamTicketId) {
    await failOperation(pool, repository, operation, 'The runtime has not accepted this task');
    return;
  }
  const ticketId = task.upstreamTicketId;
  const planRevision = task.planRevision ?? 0;
  let lease: ExecutionLease | null = null;
  const failWithLease = async (message: string, code?: string) => {
    await failOperation(pool, repository, operation, message, code);
    if (lease) {
      // A refused/lost request may still have started something. Keep the
      // generation and require a stop proof before any replacement.
      await requireLeaseStopProof(
        pool,
        repository.id,
        lease.id,
        `operation ${operation.kind} failed without a stop proof: ${message.slice(0, 200)}`,
      ).catch(() => undefined);
    }
  };
  try {
    let accepted;
    if (!admissionSupported && operation.kind !== 'pause') {
      await failOperation(
        pool,
        repository,
        operation,
        'The runtime does not support execution admission; refusing to start',
        'runtime_admission_unsupported',
      );
      return;
    }
    if (operation.kind === 'pause') {
      // A pause does not acquire a new generation; it records the generation
      // it observed so a late/unproven result can be fenced and reconciled.
      const bound = await pool.query<{ id: string; fencing_token: string }>(
        `select l.id, l.fencing_token
           from task_links t join runtime_leases l on l.id = t.runtime_lease_id
          where t.id = $1 and l.state = 'active'`,
        [task.id],
      );
      const observed = bound.rows[0];
      if (observed) {
        await pool.query(
          `update task_operations
              set runtime_lease_id = $2, fencing_token = $3, updated_at = now()
            where id = $1`,
          [operation.id, observed.id, observed.fencing_token],
        );
        operation.runtimeLeaseId = observed.id;
        operation.fencingToken = observed.fencing_token;
      }
      accepted = await bridge.pause(ticketId, { operationId: operation.id, reason: 'owner stop' });
    } else {
      if (operation.kind === 'start' && !planRevision) {
        await failWithLease('No approved plan revision is recorded');
        return;
      }
      // start, resume and retry all need the repository's single execution
      // lease. A refused admission stays queued; the task is not run beside an
      // unproven or still-running generation.
      const account = await acquireExecutionLease(
        pool,
        repository.id,
        { taskId: task.id, reason: `operation ${operation.kind}` },
        config.operationLeaseTtlMs,
      );
      if (!account.lease) {
        await storeOperationEvidence(pool, operation.id, 'queued', {
          retryInSeconds: 5,
          evidence: {
            waitingFor: account.waitingFor,
            leaseRefused: account.refused,
            detail: account.detail,
          },
        });
        logger.info(
          {
            event: 'operation.lease_wait',
            operationId: operation.id,
            taskId: task.id,
            kind: operation.kind,
            refused: account.refused,
            waitingFor: account.waitingFor,
          },
          'operation waits for the repository execution lease',
        );
        return;
      }
      lease = account.lease;
      await pool.query(
        `update task_operations
            set runtime_lease_id = $2, fencing_token = $3, updated_at = now()
          where id = $1`,
        [operation.id, lease.id, lease.fencingToken],
      );

      if (operation.kind === 'start') {
        accepted = await bridge.start(ticketId, {
          operationId: operation.id,
          planRevision,
          ...(admissionSupported
            ? { admission: { leaseId: lease.id, fencingToken: lease.fencingToken } }
            : {}),
        });
      } else if (operation.kind === 'resume') {
        const snapshot = await bridge.ticket(ticketId);
        if (!snapshot.paused) {
          await applySnapshot(pool, repository, task.id, bridge, logger);
          await failWithLease('The runtime ticket is not paused');
          return;
        }
        if (!snapshot.paused.stoppedAt) {
          await storeOperationEvidence(pool, operation.id, 'queued', {
            retryInSeconds: 3,
            evidence: { waitingFor: 'stop evidence' },
          });
          return;
        }
        if (!snapshot.execution?.directory) {
          await failWithLease('The runtime cannot confirm the preserved worktree');
          return;
        }
        accepted = await bridge.resume(ticketId, {
          operationId: operation.id,
          admission: { leaseId: lease.id, fencingToken: lease.fencingToken },
        });
      } else {
        accepted = await bridge.retry(ticketId, {
          operationId: operation.id,
          ...(admissionSupported
            ? { admission: { leaseId: lease.id, fencingToken: lease.fencingToken } }
            : {}),
        });
      }
    }

    await storeOperationEvidence(pool, operation.id, 'running', {
      bridgeOperationId: accepted.operationId,
      evidence: { runtimeState: accepted.state },
      retryInSeconds: 1,
    });
    logger.info(
      {
        event: 'operation.sent',
        operationId: operation.id,
        taskId: task.id,
        kind: operation.kind,
        runtimeState: accepted.state,
        leaseId: lease?.id ?? null,
        fencingToken: lease?.fencingToken ?? null,
      },
      'runtime accepted the task operation',
    );
  } catch (error) {
    if (isRetryableBridgeError(error)) {
      const delayMs = Math.min(
        config.operationRetryCapMs,
        1000 * 2 ** Math.max(0, operation.attemptCount - 1),
      );
      // Keep the lease: a lost response may still have started execution, so the
      // same operation id is retried under the same generation.
      await storeOperationEvidence(pool, operation.id, 'queued', {
        retryInSeconds: delayMs / 1000,
        error: bridgeErrorMessage(error),
      });
      logger.warn(
        {
          event: 'operation.retry',
          operationId: operation.id,
          attempt: operation.attemptCount,
          error: bridgeErrorMessage(error),
        },
        'task operation failed transiently; the same operation id will be retried',
      );
      return;
    }
    await applySnapshot(pool, repository, task.id, bridge, logger).catch(() => undefined);
    const code =
      typeof (error as { code?: unknown }).code === 'string'
        ? (error as { code: string }).code
        : undefined;
    await failWithLease(bridgeErrorMessage(error), code);
    logger.error(
      { event: 'operation.failed', operationId: operation.id, error: bridgeErrorMessage(error) },
      'task operation was refused permanently',
    );
  }
}

/** Polls operations the runtime accepted; settles them from the runtime's persisted result. */
async function pollOperations(
  pool: pg.Pool,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
): Promise<boolean> {
  const result = await pool.query<OperationRow>(
    `select ${OPERATION_COLUMNS}
       from task_operations op
      where op.repository_id = $1
        and op.state = 'running'
        and op.bridge_operation_id is not null
        and op.next_attempt_at <= now()
      order by op.next_attempt_at
      limit 20`,
    [repository.id],
  );
  let worked = false;
  for (const operation of result.rows) {
    try {
      const bridgeOperation = await bridge.operation(operation.bridgeOperationId ?? '');
      worked = true;
      if (bridgeOperation.state === 'succeeded' || bridgeOperation.state === 'failed') {
        await applyOperationResult(pool, repository, operation, bridgeOperation, bridge, logger);
      } else {
        await pool.query(
          `update task_operations set next_attempt_at = now() + interval '1 second',
                  evidence = $2::jsonb, updated_at = now()
            where id = $1`,
          [operation.id, JSON.stringify({ runtimeState: bridgeOperation.state })],
        );
      }
    } catch (error) {
      await pool.query(
        `update task_operations set next_attempt_at = now() + interval '3 seconds',
                error = $2, updated_at = now() where id = $1`,
        [operation.id, bridgeErrorMessage(error)],
      );
      logger.warn(
        {
          event: 'operation.poll_failed',
          operationId: operation.id,
          error: bridgeErrorMessage(error),
        },
        'could not poll the runtime operation',
      );
    }
  }
  return worked;
}

/**
 * One operations pass: settle accepted runtime operations, then send at most one
 * queued owner command. The runtime operation id and the per-task active index
 * keep retries idempotent.
 */
export async function operationsOnce(
  pool: pg.Pool,
  config: DispatcherConfig,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
  admissionSupported = true,
): Promise<boolean> {
  if (await pollOperations(pool, repository, bridge, logger)) {
    return true;
  }
  const operation = await claimOperation(pool, config, repository.id);
  if (!operation) {
    return false;
  }
  const task = await loadTaskForOperation(pool, operation.taskId);
  if (!task) {
    await failOperation(pool, repository, operation, 'The platform task no longer exists');
    return true;
  }
  await sendOperation(
    pool,
    config,
    repository,
    bridge,
    operation,
    task,
    logger,
    admissionSupported,
  );
  return true;
}

/** Extends leases held by active tasks and releases those whose task stopped or ended. */
export async function maintainLeases(
  pool: pg.Pool,
  config: DispatcherConfig,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
): Promise<void> {
  // Release only confirmed-safe states. `blocked`/`stopping`/`failed` keep the
  // generation until a stop proof exists; a queued/running owner command also
  // keeps the slot so a maintenance pass cannot free a resume/retry admission.
  await pool.query(
    `update runtime_leases l
        set state = 'released', released_at = now(), heartbeat_at = now(),
            waiting_for = null, recovery_reason = null
      from task_links t
     where t.runtime_lease_id = l.id and l.state = 'active'
       and l.repository_id = $1
       and (t.state::text in ('waiting_input', 'awaiting_review', 'completed', 'closed_unmerged')
            or (t.state::text = 'stopped' and t.stopped_at is not null))
       and not exists (
         select 1 from task_operations op
          where op.task_id = t.id and op.state in ('queued', 'running')
       )`,
    [repository.id],
  );
  await pool.query(
    `update task_links t
        set runtime_lease_id = null, updated_at = now()
       from runtime_leases l
      where t.runtime_lease_id = l.id and l.state <> 'active'`,
  );
  // Live admissions are renewed; expiry alone never frees the slot.
  await pool.query(
    `update runtime_leases l
        set heartbeat_at = now(), expires_at = now() + make_interval(secs => $2)
      from task_links t
     where t.runtime_lease_id = l.id and l.state = 'active'
       and l.repository_id = $1
       and t.state::text in (${ACTIVE_EXECUTION_STATES.map((_, index) => `$${index + 3}`).join(', ')})`,
    [repository.id, config.operationLeaseTtlMs / 1000, ...ACTIVE_EXECUTION_STATES],
  );
  // Launch leases carry no task yet. Keep them alive while their delivery is
  // still in flight, and leave the rest to expiry + recovery.
  await pool.query(
    `update runtime_leases l
        set heartbeat_at = now(), expires_at = now() + make_interval(secs => $2)
       from deliveries d
      where d.runtime_lease_id = l.id and l.state = 'active'
        and l.repository_id = $1 and d.state in ('pending', 'delivering')`,
    [repository.id, config.operationLeaseTtlMs / 1000],
  );
  await reconcileLeases(pool, repository, bridge, logger);
}

/**
 * After a platform restart, reconciles active tasks with the runtime's
 * authoritative ticket snapshot. A task the runtime cannot confirm is marked
 * blocked instead of being assumed healthy; a task already stopped stays
 * stopped.
 */
export async function reconcileTasks(
  pool: pg.Pool,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
): Promise<void> {
  const active = await pool.query<TaskRow>(
    `select ${TASK_COLUMNS} from task_links t
      where t.repository_id = $1
        and t.upstream_ticket_id is not null
        and t.state::text in ('queued', 'running', 'discussing', 'waiting_input', 'awaiting_review', 'stopping')
      order by t.updated_at
      limit 50`,
    [repository.id],
  );
  if (active.rows.length === 0) {
    return;
  }
  const unconfirmed = new Map<string, string>();
  for (const task of active.rows) {
    try {
      const snapshot = await bridge.ticket(task.upstreamTicketId ?? '');
      const state = stateFromSnapshot(snapshot.state);
      if (!state) {
        unconfirmed.set(task.id, `unrecognised runtime state: ${snapshot.state}`);
        continue;
      }
      const paused = snapshot.paused;
      const proven = pauseEvidenceProven(
        paused?.stoppedAt ? { ...paused.evidence, stoppedAt: paused.stoppedAt } : undefined,
      );
      const stoppedAt = proven ? new Date(paused!.stoppedAt!) : null;
      const problem =
        paused?.stoppedAt && !proven
          ? pauseEvidenceProblem({ ...paused.evidence, stoppedAt: paused.stoppedAt })
          : undefined;
      const contradictory =
        task.state === 'stopped' && state !== 'stopped' && state !== 'closed_unmerged';
      // A restart snapshot with a timestamp but incomplete evidence is not a stopped task.
      const nextState = contradictory || (state === 'stopped' && !proven) ? 'blocked' : state;
      const writeEvidence = !!paused && (proven || nextState === 'blocked');
      const client = await pool.connect();
      try {
        await client.query('begin');
        if (nextState !== task.state || stoppedAt || snapshot.pr || writeEvidence) {
          const result = await client.query<TaskRow>(
            `update task_links
                set state = $2,
                    stopped_at = coalesce($3::timestamptz, stopped_at),
                    stop_evidence = case when $3::timestamptz is null and not $9::boolean then stop_evidence else $4::jsonb end,
                    execution_mode = $5,
                    plan_revision = coalesce($6, plan_revision),
                    pr_url = coalesce($7, pr_url),
                    pr_number = coalesce($8, pr_number),
                    version = version + 1,
                    updated_at = now()
              where id = $1
              returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
            [
              task.id,
              nextState,
              stoppedAt,
              JSON.stringify(
                proven
                  ? (paused?.evidence ?? {})
                  : { ...paused?.evidence, ...(problem ? { problem } : {}) },
              ),
              snapshot.mode === 'discuss' ? 'discuss' : 'execute',
              snapshot.planRevision ?? null,
              snapshot.pr?.url ?? null,
              snapshot.pr?.number ?? null,
              writeEvidence,
            ],
          );
          const next = result.rows[0];
          if (next) {
            await appendTaskEvent(client, repository, next, `reconcile:${task.id}:${Date.now()}`);
          }
        }
        await client.query('commit');
      } catch (error) {
        await client.query('rollback').catch(() => undefined);
        throw error;
      } finally {
        client.release();
      }
      logger.info(
        {
          event: 'reconcile.task',
          taskId: task.id,
          previous: task.state,
          state: nextState,
          contradictory,
        },
        'reconciled a task with its runtime ticket',
      );
    } catch (error) {
      unconfirmed.set(task.id, bridgeErrorMessage(error));
    }
  }
  for (const task of active.rows) {
    const error = unconfirmed.get(task.id);
    if (!error) {
      continue;
    }
    const client = await pool.connect();
    try {
      await client.query('begin');
      const result = await client.query<TaskRow>(
        `update task_links
            set state = 'blocked', stop_evidence = $2::jsonb,
                version = version + 1, updated_at = now()
          where id = $1 and state::text in ('running', 'stopping')
          returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
        [task.id, JSON.stringify({ reconciliationError: error })],
      );
      const next = result.rows[0];
      if (next) {
        await appendTaskEvent(client, repository, next, `reconcile:blocked:${task.id}`);
      }
      await client.query('commit');
      logger.warn(
        { event: 'reconcile.unconfirmed', taskId: task.id, error },
        'the runtime could not confirm this task; it is blocked instead of guessed',
      );
    } catch (caught) {
      await client.query('rollback').catch(() => undefined);
      throw caught;
    } finally {
      client.release();
    }
  }
}

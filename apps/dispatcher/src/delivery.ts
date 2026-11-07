import type pg from 'pg';

import { RuntimeBridgeError, type RuntimeBridgeClient } from '@moyu/runtime-client';

import type { DispatcherConfig } from './config';
import type { DispatcherLogger } from './logger';
import type { RepositoryBinding } from './runtime';
import { bridgeErrorMessage, isRetryableBridgeError } from './runtime';
import { appendEventSql } from './events-sql';
import { acquireExecutionLease, releaseExecutionLease } from './leases';
import { TASK_COLUMNS, taskModel, type TaskRow } from './tasks';

export const RUN_INSTANCE = `dispatcher-${process.pid}`;

interface ClaimedRow {
  id: string;
  message_id: string;
  attempt_count: number;
  upstream_trigger_id: string | null;
}

interface DeliveryDetailRow {
  messageId: string;
  body: string;
  createdAt: Date;
  replyToMessageId: string | null;
  conversationId: string;
  repositoryId: string;
  baseBranch: string;
  baseCommit: string | null;
  modelId: string | null;
  modelReasoningEffort: string | null;
  executionMode: 'execute' | 'discuss';
  ownerName: string;
  ownerId: string;
  fullName: string;
  conversationSource: 'web' | 'github';
  taskId: string | null;
  taskState: string | null;
  taskUpstreamTicketId: string | null;
  taskPlanRevision: number | null;
  taskStoppedAt: Date | null;
  taskStopEvidence: Record<string, unknown>;
  taskRuntimeLeaseId: string | null;
  taskBaseBranch: string | null;
  taskBaseCommit: string | null;
  taskModelId: string | null;
  taskModelEffort: string | null;
  taskExecutionMode: 'execute' | 'discuss' | null;
}

/** Claims one due delivery for the repository, honouring per-conversation order and a lease. */
async function claimDelivery(
  pool: pg.Pool,
  config: DispatcherConfig,
  repositoryId: string,
): Promise<{ id: string; messageId: string; attemptCount: number; triggerId: string } | null> {
  const result = await pool.query<ClaimedRow>(
    `with candidate as (
       select d.id
         from deliveries d
         join messages m on m.id = d.message_id
         join conversations c on c.id = m.conversation_id
         join task_links t on t.conversation_id = c.id
        where c.repository_id = $1
          and d.state in ('pending', 'delivering')
          and d.next_attempt_at <= now()
          and (d.state = 'pending' or d.lease_expires_at is null or d.lease_expires_at <= now())
          -- A stopped task keeps its new messages stored until resume; a closed or
          -- completed task never auto-executes another message.
          and t.state::text not in ('stopping', 'stopped', 'completed', 'closed_unmerged', 'not_accepted')
          -- A pending start/pause/resume/retry holds delivery so the frozen mode and
          -- base cannot change under a message that is being sent.
          and not exists (
            select 1 from task_operations op
             where op.task_id = t.id and op.state in ('queued', 'running')
          )
          and not exists (
            select 1
              from deliveries d2
              join messages m2 on m2.id = d2.message_id
             where m2.conversation_id = m.conversation_id
               and m2.created_at < m.created_at
               and d2.state in ('pending', 'delivering')
          )
        order by d.next_attempt_at, d.created_at
        limit 1
        for update of d skip locked
     )
     update deliveries d
        set state = 'delivering',
            lease_owner = $2,
            lease_expires_at = now() + make_interval(secs => $3),
            last_attempt_at = now(),
            attempt_count = d.attempt_count + 1,
            updated_at = now()
       from candidate cand
      where d.id = cand.id
      returning d.id, d.message_id, d.attempt_count, d.upstream_trigger_id`,
    [repositoryId, RUN_INSTANCE, config.deliveryLeaseMs / 1000],
  );
  const row = result.rows[0];
  if (!row) {
    return null;
  }
  return {
    id: row.id,
    messageId: row.message_id,
    attemptCount: row.attempt_count,
    triggerId: row.upstream_trigger_id ?? `workspace:${row.message_id}`,
  };
}

async function loadDetail(pool: pg.Pool, messageId: string): Promise<DeliveryDetailRow | null> {
  const result = await pool.query<DeliveryDetailRow>(
    `select m.id as "messageId", m.body, m.created_at as "createdAt", m.reply_to_message_id as "replyToMessageId",
            c.id as "conversationId", c.repository_id as "repositoryId", c.base_branch as "baseBranch",
            c.base_commit as "baseCommit", c.model_id as "modelId", c.model_reasoning_effort as "modelReasoningEffort",
            c.execution_mode as "executionMode", o.display_name as "ownerName", o.id as "ownerId", r.full_name as "fullName",
            c.source as "conversationSource",
            t.id as "taskId", t.state as "taskState", t.upstream_ticket_id as "taskUpstreamTicketId",
            t.plan_revision as "taskPlanRevision", t.stopped_at as "taskStoppedAt",
            t.stop_evidence as "taskStopEvidence", t.runtime_lease_id as "taskRuntimeLeaseId",
            t.base_branch as "taskBaseBranch", t.base_commit as "taskBaseCommit",
            t.model_id as "taskModelId", t.model_reasoning_effort as "taskModelEffort", t.execution_mode as "taskExecutionMode"
       from messages m
       join conversations c on c.id = m.conversation_id
       join repositories r on r.id = c.repository_id
       join owners o on o.id = c.owner_id
       left join task_links t on t.conversation_id = c.id
      where m.id = $1`,
    [messageId],
  );
  return result.rows[0] ?? null;
}

async function recordDeliveryEvent(
  pool: pg.Pool,
  input: {
    repositoryId: string;
    conversationId: string;
    messageId: string;
    state: 'delivered' | 'read' | 'failed';
  },
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    await appendEventSql(client, {
      type: 'message.delivery_changed',
      source: 'dispatcher',
      repositoryId: input.repositoryId,
      conversationId: input.conversationId,
      payload: { messageId: input.messageId, deliveryState: input.state },
    });
    await client.query('commit');
  } catch (error) {
    await client.query('rollback').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

function retryDelayMs(config: DispatcherConfig, attemptCount: number): number {
  return Math.min(config.deliveryRetryCapMs, 1000 * 2 ** Math.max(0, attemptCount - 1));
}

/**
 * Attempts one delivery. A retryable failure returns the message to `pending`
 * with a capped backoff; the stable Trigger id means a retry after a lost
 * response is deduplicated by the Bridge and cannot create a second ticket.
 */
export async function deliverOnce(
  pool: pg.Pool,
  config: DispatcherConfig,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
  admissionSupported = true,
): Promise<boolean> {
  const claimed = await claimDelivery(pool, config, repository.id);
  if (!claimed) {
    return false;
  }
  const detail = await loadDetail(pool, claimed.messageId);
  if (!detail) {
    await pool.query(
      `update deliveries set state='pending', last_error='message row missing', lease_owner=null,
              lease_expires_at=null, next_attempt_at=now() + interval '1 minute', updated_at=now()
        where id=$1`,
      [claimed.id],
    );
    return true;
  }

  const taskLike: TaskRow | null = detail.taskId
    ? {
        id: detail.taskId,
        conversationId: detail.conversationId,
        repositoryId: detail.repositoryId,
        state: detail.taskState ?? 'pending_admission',
        upstreamTicketId: detail.taskUpstreamTicketId,
        upstreamThreadId: null,
        planRevision: detail.taskPlanRevision,
        stopRequestedAt: null,
        stoppedAt: detail.taskStoppedAt,
        stopEvidence: detail.taskStopEvidence,
        runtimeLeaseId: detail.taskRuntimeLeaseId,
        baseBranch: detail.taskBaseBranch ?? detail.baseBranch,
        baseCommit: detail.taskBaseCommit,
        modelId: detail.taskModelId,
        modelReasoningEffort: detail.taskModelEffort,
        executionMode: detail.taskExecutionMode ?? detail.executionMode,
        question: null,
        questionMessageId: null,
        prUrl: null,
        prNumber: null,
        version: 0,
        createdAt: detail.createdAt,
        updatedAt: detail.createdAt,
      }
    : null;

  const model = taskLike
    ? taskModel(taskLike)
    : detail.modelId
      ? taskModel({
          id: '',
          conversationId: detail.conversationId,
          repositoryId: detail.repositoryId,
          state: 'pending_admission',
          upstreamTicketId: null,
          upstreamThreadId: null,
          planRevision: null,
          stopRequestedAt: null,
          stoppedAt: null,
          stopEvidence: {},
          runtimeLeaseId: null,
          baseBranch: detail.baseBranch,
          baseCommit: detail.baseCommit,
          modelId: detail.modelId,
          modelReasoningEffort: detail.modelReasoningEffort,
          executionMode: detail.executionMode,
          question: null,
          questionMessageId: null,
          prUrl: null,
          prNumber: null,
          version: 0,
          createdAt: detail.createdAt,
          updatedAt: detail.createdAt,
        })
      : null;

  let cleanupLease: { id: string; fencingToken: string } | null = null;
  try {
    // Every launch and reply goes through the same repository execution lease.
    // A refused lease leaves the message stored and waits; it does not deliver
    // execution work beside an unproven or still-running admission.
    const account = await acquireExecutionLease(
      pool,
      repository.id,
      {
        taskId: detail.taskId,
        deliveryId: claimed.id,
        reason: detail.taskId ? 'message reply' : 'task launch',
      },
      config.operationLeaseTtlMs,
    );
    if (!account.lease) {
      await pool.query(
        `update deliveries
            set state='pending', next_attempt_at = now() + interval '5 seconds',
                last_error=$2, evidence=$3, lease_owner=null, lease_expires_at=null, updated_at=now()
          where id=$1`,
        [
          claimed.id,
          account.detail,
          JSON.stringify({
            waitingFor: account.waitingFor,
            leaseRefused: account.refused,
          }),
        ],
      );
      logger.warn(
        {
          event: 'delivery.lease_wait',
          messageId: detail.messageId,
          refused: account.refused,
          waitingFor: account.waitingFor,
        },
        'delivery waits for the repository execution lease',
      );
      return true;
    }
    cleanupLease = account.lease;
    await pool.query(
      `update deliveries set runtime_lease_id=$2, fencing_token=$3, updated_at=now()
        where id=$1`,
      [claimed.id, account.lease.id, account.lease.fencingToken],
    );
    if (!admissionSupported) {
      throw new RuntimeBridgeError(
        'runtime_admission_unsupported: refusing to execute on a runtime without admission support',
        { code: 'runtime_admission_unsupported', retryable: false },
      );
    }

    const result = await bridge.trigger({
      triggerId: claimed.triggerId,
      conversationId: detail.conversationId,
      messageId: detail.messageId,
      replyToMessageId: detail.replyToMessageId,
      actor: { id: detail.ownerId, name: detail.ownerName },
      text: detail.body,
      at: detail.createdAt.toISOString(),
      // An imported GitHub conversation continues its existing ticket; every
      // other conversation opens its own workspace ticket.
      ...(detail.conversationSource === 'github' && detail.taskUpstreamTicketId
        ? { targetTicketId: detail.taskUpstreamTicketId }
        : {}),
      ...(admissionSupported
        ? { admission: { leaseId: account.lease.id, fencingToken: account.lease.fencingToken } }
        : {}),
      executionSpec: {
        repository: detail.fullName,
        baseBranch: taskLike?.baseBranch ?? detail.baseBranch,
        baseCommit: taskLike?.baseCommit ?? detail.baseCommit,
        model,
        mode: taskLike?.executionMode ?? detail.executionMode,
      },
    });

    if (result.result === 'filtered') {
      const updated = await pool.query(
        `update deliveries
            set state='failed', last_error=$2, evidence=$3, lease_owner=null, lease_expires_at=null, updated_at=now()
          where id=$1 and runtime_lease_id=$4 and fencing_token=$5`,
        [
          claimed.id,
          'The runtime filtered this trigger (Source scope)',
          JSON.stringify({ result: result.result, httpStatus: 200 }),
          account.lease.id,
          account.lease.fencingToken,
        ],
      );
      if ((updated.rowCount ?? 0) === 0) {
        logger.warn(
          {
            event: 'delivery.stale_receipt',
            messageId: detail.messageId,
            reason: 'filtered result',
          },
          'refused a filtered result from a superseded delivery generation',
        );
        return true;
      }
      await releaseExecutionLease(
        pool,
        repository.id,
        account.lease.id,
        'the runtime filtered the trigger; no execution started',
        { source: 'trigger_filtered', at: new Date().toISOString() },
      );
      await recordDeliveryEvent(pool, {
        repositoryId: repository.id,
        conversationId: detail.conversationId,
        messageId: detail.messageId,
        state: 'failed',
      });
      logger.warn(
        {
          event: 'delivery.filtered',
          messageId: detail.messageId,
          conversationId: detail.conversationId,
        },
        'runtime filtered the trigger',
      );
      return true;
    }

    const resolvedBase = result.baseCommit;
    const client = await pool.connect();
    try {
      await client.query('begin');
      const delivered = await client.query(
        `update deliveries
            set state='delivered', delivered_at=now(), upstream_trigger_id=$2, evidence=$3,
                last_error=null, lease_owner=null, lease_expires_at=null, updated_at=now()
          where id=$1 and runtime_lease_id=$4 and fencing_token=$5`,
        [
          claimed.id,
          claimed.triggerId,
          JSON.stringify({ result: result.result, httpStatus: 200, baseCommit: resolvedBase }),
          account.lease.id,
          account.lease.fencingToken,
        ],
      );
      if ((delivered.rowCount ?? 0) === 0) {
        await client.query('rollback');
        logger.warn(
          {
            event: 'delivery.stale_receipt',
            messageId: detail.messageId,
            leaseId: account.lease.id,
          },
          'refused a delivered receipt from a superseded delivery generation',
        );
        return true;
      }
      if (resolvedBase) {
        await client.query(
          `update conversations set base_commit=coalesce(base_commit,$2), updated_at=now() where id=$1`,
          [detail.conversationId, resolvedBase],
        );
        await client.query(
          `update task_links set base_commit=coalesce(base_commit,$2), updated_at=now()
            where conversation_id=$1`,
          [detail.conversationId, resolvedBase],
        );
      }
      await appendEventSql(client, {
        type: 'message.delivery_changed',
        source: 'dispatcher',
        repositoryId: repository.id,
        conversationId: detail.conversationId,
        payload: { messageId: detail.messageId, deliveryState: 'delivered' },
      });
      await client.query('commit');
    } catch (error) {
      await client.query('rollback').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }

    logger.info(
      {
        event: 'delivery.delivered',
        messageId: detail.messageId,
        conversationId: detail.conversationId,
        result: result.result,
        attempt: claimed.attemptCount,
      },
      'runtime accepted the trigger',
    );
    return true;
  } catch (error) {
    if (isRetryableBridgeError(error)) {
      const delay = retryDelayMs(config, claimed.attemptCount);
      const retried = await pool.query(
        `update deliveries
            set state='pending', next_attempt_at = now() + make_interval(secs => $2),
                last_error=$3, lease_owner=null, lease_expires_at=null, updated_at=now()
          where id=$1 and runtime_lease_id=$4 and fencing_token=$5`,
        [
          claimed.id,
          delay / 1000,
          bridgeErrorMessage(error),
          cleanupLease?.id ?? '',
          cleanupLease?.fencingToken ?? '',
        ],
      );
      if ((retried.rowCount ?? 0) === 0) {
        logger.warn(
          { event: 'delivery.stale_retry', messageId: detail.messageId },
          'left a superseded delivery generation untouched',
        );
        return true;
      }
      logger.warn(
        {
          event: 'delivery.retry',
          messageId: detail.messageId,
          attempt: claimed.attemptCount,
          delayMs: delay,
          error: bridgeErrorMessage(error),
        },
        'delivery failed; the same trigger will be retried',
      );
      return true;
    }

    const failed = await pool.query(
      `update deliveries
          set state='failed', last_error=$2, lease_owner=null, lease_expires_at=null, updated_at=now()
        where id=$1 and runtime_lease_id=$3 and fencing_token=$4`,
      [
        claimed.id,
        bridgeErrorMessage(error),
        cleanupLease?.id ?? '',
        cleanupLease?.fencingToken ?? '',
      ],
    );
    if ((failed.rowCount ?? 0) === 0) {
      logger.warn(
        { event: 'delivery.stale_failure', messageId: detail.messageId },
        'refused a failure receipt from a superseded delivery generation',
      );
      return true;
    }
    // A permanently refused trigger cannot have produced a worker execution.
    if (cleanupLease) {
      await releaseExecutionLease(
        pool,
        repository.id,
        cleanupLease.id,
        'the runtime refused the trigger permanently',
        { source: 'trigger_refused', at: new Date().toISOString() },
      ).catch(() => undefined);
    }
    await recordDeliveryEvent(pool, {
      repositoryId: repository.id,
      conversationId: detail.conversationId,
      messageId: detail.messageId,
      state: 'failed',
    });
    logger.error(
      {
        event: 'delivery.failed',
        messageId: detail.messageId,
        error: bridgeErrorMessage(error),
      },
      'delivery was rejected permanently',
    );
    return true;
  }
}

export { TASK_COLUMNS };

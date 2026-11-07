import type pg from 'pg';
import { githubRequirementUrl } from '@moyu/contracts';

import { taskStateSchema, type ArtifactSummary } from '@moyu/contracts';
import type { RuntimeBridgeClient, BridgeEvent } from '@moyu/runtime-client';

import type { DispatcherLogger } from './logger';
import { bridgeErrorMessage, type RepositoryBinding } from './runtime';
import { claimExecution, reconcileExecutionClaimOn } from './reservations';
import { appendEventSql } from './events-sql';
import { endedState, stateFromSnapshot, TASK_COLUMNS, taskSummary, type TaskRow } from './tasks';
import { pauseEvidenceProblem, pauseEvidenceProven } from './pause-evidence';
import { acquireExecutionLeaseOn } from './leases';

/** Last projection branch per Bridge event, for lifecycle diagnostics/tests. */
export const projectionBranches = new Map<string, string>();

interface ProjectedTask {
  task: TaskRow;
}

function asRecord(value: unknown): Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function asString(value: unknown): string | null {
  return typeof value === 'string' && value.length > 0 ? value : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function stateOf(value: unknown): string {
  const parsed = taskStateSchema.safeParse(value);
  return parsed.success ? parsed.data : 'queued';
}

async function taskByConversation(
  client: pg.PoolClient,
  conversationId: string,
  repositoryId: string,
): Promise<TaskRow | null> {
  const result = await client.query<TaskRow>(
    `select ${TASK_COLUMNS} from task_links t
      where t.conversation_id = $1 and t.repository_id = $2 limit 1`,
    [conversationId, repositoryId],
  );
  return result.rows[0] ?? null;
}

async function taskByTicket(
  client: pg.PoolClient,
  ticketId: string,
  repositoryId: string,
): Promise<TaskRow | null> {
  const result = await client.query<TaskRow>(
    `select ${TASK_COLUMNS} from task_links t
      where t.upstream_ticket_id = $1 and t.repository_id = $2 limit 1`,
    [ticketId, repositoryId],
  );
  return result.rows[0] ?? null;
}

async function emit(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
  type: string,
  conversationId: string | null,
  taskId: string | null,
  payload: Record<string, unknown>,
): Promise<void> {
  await appendEventSql(client, {
    type,
    source: 'bridge',
    sourceEventId: event.id,
    repositoryId: repository.id,
    conversationId,
    taskId,
    payload,
    occurredAt: new Date(event.at),
  });
}

function traceBranch(eventId: string, branch: string): void {
  projectionBranches.set(eventId, branch);
}

async function applyTaskEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
  logger: DispatcherLogger,
): Promise<void> {
  const data = asRecord(event.data);
  const ticketId = asString(data.ticketId);
  if (!ticketId) {
    traceBranch(event.id, 'return-no-ticket-id');
    logger.warn(
      { event: 'bridge.event_invalid', bridgeEvent: event.id },
      'ticket event had no ticketId',
    );
    return;
  }
  const conversationId = asString(data.conversationId);
  const existing = conversationId
    ? await taskByConversation(client, conversationId, repository.id)
    : await taskByTicket(client, ticketId, repository.id);
  if (!existing) {
    traceBranch(event.id, 'return-task-missing');
    // The run may not have linked the ticket yet; a later ticket.linked event
    // carries the conversation. Dropping the projection is safer than
    // inventing a task row.
    logger.warn(
      { event: 'bridge.task_unmapped', bridgeEvent: event.id, ticketId },
      'ticket event arrived before the platform task was linked',
    );
    return;
  }

  let next: TaskRow | null = existing;
  if (event.type === 'ticket.linked') {
    const base = asRecord(data.base);
    const baseBranch = asString(base.branch);
    const baseCommit = asString(base.commit);
    const model = asRecord(base.model);
    const provider = asString(model.provider);
    const modelName = asString(model.model);
    const effort = asString(model.reasoningEffort);
    const updated = await client.query(
      `update task_links
          set upstream_ticket_id = $3,
              upstream_thread_id = coalesce(upstream_thread_id, $4),
              state = $5,
              base_branch = coalesce($6, base_branch),
              base_commit = coalesce($7, base_commit),
              model_id = coalesce($8, model_id),
              model_reasoning_effort = coalesce($9, model_reasoning_effort),
              last_event_seq = greatest(coalesce(last_event_seq, 0), $10),
              version = version + 1,
              updated_at = now()
        where id = $1 and repository_id = $2
          and (last_event_seq is null or last_event_seq < $10)`,
      [
        existing.id,
        repository.id,
        ticketId,
        `workspace:${existing.conversationId}`,
        stateOf(data.state),
        baseBranch,
        baseCommit,
        provider && modelName ? `${provider}/${modelName}` : null,
        effort,
        event.seq,
      ],
    );
    if (baseCommit) {
      await client.query(
        `update conversations set base_commit = coalesce(base_commit, $2), updated_at = now() where id = $1`,
        [existing.conversationId, baseCommit],
      );
    }
    next = await taskByConversation(client, existing.conversationId, repository.id);
    if (next && (updated.rowCount ?? 0) > 0) {
      await emit(client, repository, event, 'task.linked', next.conversationId, next.id, {
        task: taskSummary(next),
      });
    }
    // A progress event may have arrived before this link; project it now, in
    // the same transaction that advances the page cursor.
    await flushDeferredEvents(client, repository, ticketId);
    return;
  }

  if (event.type === 'ticket.state_changed') {
    const question = asString(data.question);
    const assignmentId = asString(data.assignmentId);
    const leaseId = asString(data.leaseId);
    const fencingToken = asString(data.fencingToken);
    if (assignmentId && leaseId && fencingToken) {
      const active = await client.query<{ id: string; fencing_token: string }>(
        `select id, fencing_token from runtime_leases
          where repository_id = $1 and state = 'active'
          order by fencing_token desc limit 1`,
        [repository.id],
      );
      const activeLease = active.rows[0];
      const currentGeneration =
        activeLease !== undefined &&
        activeLease.id === leaseId &&
        String(activeLease.fencing_token) === fencingToken;
      const reservation = await client.query<{
        assignment_id: string;
        lease_id: string;
        fencing_token: string;
        state: string;
      }>(
        `select assignment_id, lease_id, fencing_token, state
           from execution_reservations
          where repository_id = $1 and request_key = $2`,
        [repository.id, `admit:${existing.id}`],
      );
      const reserved = reservation.rows[0];
      if (
        !currentGeneration &&
        reserved &&
        (reserved.assignment_id !== assignmentId ||
          reserved.lease_id !== leaseId ||
          String(reserved.fencing_token) !== fencingToken)
      ) {
        // A late event from another generation must not project queued/running
        // over a stopped task or advance its state; keep the projection and
        // record the mismatch, while still consuming the event cursor.
        await client.query(
          `update task_links
              set stop_evidence = coalesce(stop_evidence, '{}'::jsonb) || $2::jsonb,
                  last_event_seq = greatest(coalesce(last_event_seq, 0), $3),
                  version = version + 1,
                  updated_at = now()
            where id = $1`,
          [existing.id, JSON.stringify({ admissionProjection: 'stale_generation' }), event.seq],
        );
        traceBranch(event.id, 'return-stale-generation');
        return;
      }
    }
    const target = stateOf(data.state);
    if (
      (existing.state === 'stopped' || existing.state === 'paused') &&
      (target === 'queued' || target === 'running')
    ) {
      // A projected state event cannot reactivate a stopped/paused task, even
      // when its generation matches or it carries no identity at all.
      await client.query(
        `update task_links
            set stop_evidence = coalesce(stop_evidence, '{}'::jsonb) || $2::jsonb,
                last_event_seq = greatest(coalesce(last_event_seq, 0), $3),
                version = version + 1,
                updated_at = now()
          where id = $1`,
        [existing.id, JSON.stringify({ admissionProjection: 'stale_state' }), event.seq],
      );
      return;
    }
    const updated = await client.query(
      `update task_links
          set state = case when state in ('stopped', 'stopping') then state else $2 end,
              question = $3::text,
              question_message_id = case when $3::text is null then null else question_message_id end,
              last_event_seq = greatest(coalesce(last_event_seq, 0), $4),
              version = version + 1,
              updated_at = now()
        where id = $1 and (last_event_seq is null or last_event_seq < $4)`,
      [existing.id, target, question, event.seq],
    );
    if ((updated.rowCount ?? 0) === 0) {
      traceBranch(event.id, 'return-stale-cursor');
      return;
    }
    traceBranch(event.id, 'projected');
    next = await taskByTicket(client, ticketId, repository.id);
    if (next) {
      if (next.state === 'queued' || next.state === 'running') {
        await reconcileExecutionClaimOn(
          client,
          repository.id,
          `admit:${next.id}`,
          ticketId,
          asString(data.assignmentId),
          asString(data.leaseId),
          asString(data.fencingToken),
        );
      }
      await emit(client, repository, event, 'task.state_changed', next.conversationId, next.id, {
        task: taskSummary(next),
      });
    }
    return;
  }

  if (event.type === 'ticket.question') {
    const question = asString(data.question);
    if (!question) {
      return;
    }
    const updated = await client.query(
      `update task_links
          set question = $2,
              last_event_seq = greatest(coalesce(last_event_seq, 0), $3),
              version = version + 1,
              updated_at = now()
        where id = $1 and (last_event_seq is null or last_event_seq < $3)`,
      [existing.id, question, event.seq],
    );
    if ((updated.rowCount ?? 0) === 0) {
      return;
    }
    next = await taskByTicket(client, ticketId, repository.id);
    if (next) {
      await emit(client, repository, event, 'task.question', next.conversationId, next.id, {
        task: taskSummary(next),
      });
    }
    return;
  }

  if (event.type === 'ticket.ended') {
    const updated = await client.query(
      `update task_links
          set state = $2,
              question = null,
              question_message_id = null,
              last_event_seq = greatest(coalesce(last_event_seq, 0), $3),
              version = version + 1,
              updated_at = now()
        where id = $1 and (last_event_seq is null or last_event_seq < $3)`,
      [existing.id, endedState(data.outcome), event.seq],
    );
    if ((updated.rowCount ?? 0) === 0) {
      return;
    }
    next = await taskByTicket(client, ticketId, repository.id);
    if (next) {
      await emit(client, repository, event, 'task.state_changed', next.conversationId, next.id, {
        task: taskSummary(next),
      });
    }
  }
}

async function applyReadEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
  logger: DispatcherLogger,
): Promise<void> {
  const data = asRecord(event.data);
  const messageId = asString(data.messageId);
  if (!messageId) {
    return;
  }
  const at = asString(data.at);
  const updated = await client.query<{ conversation_id: string }>(
    `update deliveries d
        set read_at = coalesce(d.read_at, $2::timestamptz), updated_at = now()
       from messages m
      where d.message_id = m.id and m.id = $1 and d.read_at is null
      returning m.conversation_id`,
    [messageId, at ?? new Date().toISOString()],
  );
  const conversationId = updated.rows[0]?.conversation_id;
  if (!conversationId) {
    logger.warn(
      { event: 'bridge.read_unmapped', bridgeEvent: event.id, messageId },
      'read evidence for a message without a delivery',
    );
    return;
  }
  await emit(client, repository, event, 'message.delivery_changed', conversationId, null, {
    messageId,
    deliveryState: 'read',
  });
}

async function applyDeliveredEvent(client: pg.PoolClient, event: BridgeEvent): Promise<void> {
  const data = asRecord(event.data);
  const messageId = asString(data.messageId);
  if (!messageId) {
    return;
  }
  const at = asString(data.at);
  await client.query(
    `update deliveries
        set handed_to_worker_at = coalesce(handed_to_worker_at, $2::timestamptz), updated_at = now()
      where message_id = $1 and handed_to_worker_at is null`,
    [messageId, at ?? new Date().toISOString()],
  );
}

/** Stores a Bridge event whose Ticket is not linked yet, so the cursor can advance safely. */
async function deferEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
): Promise<void> {
  await client.query(
    `insert into deferred_bridge_events (repository_id, source_event_id, type, payload, occurred_at)
     values ($1, $2, $3, $4::jsonb, $5)
     on conflict (repository_id, source_event_id) do nothing`,
    [repository.id, event.id, event.type, JSON.stringify(event.data), event.at],
  );
}

/** Projects and removes deferred events once their Ticket has a platform task. */
async function flushDeferredEvents(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  ticketId: string,
): Promise<void> {
  const deferred = await client.query<{
    id: string;
    source_event_id: string;
    type: string;
    payload: Record<string, unknown>;
    occurred_at: Date;
  }>(
    `select id, source_event_id, type, payload, occurred_at
       from deferred_bridge_events
      where repository_id = $1 and payload->>'ticketId' = $2
      order by occurred_at, id`,
    [repository.id, ticketId],
  );
  for (const row of deferred.rows) {
    const task = await taskByTicket(client, ticketId, repository.id);
    if (!task) {
      return;
    }
    const event: BridgeEvent = {
      seq: 0,
      id: row.source_event_id,
      type: row.type,
      at: row.occurred_at.toISOString(),
      data: row.payload,
    };
    if (row.type === 'task.progress') {
      const text = asString(row.payload.text);
      if (text) {
        await emit(client, repository, event, 'task.progress', task.conversationId, task.id, {
          ticketId,
          text: text.slice(0, 4000),
          at: asString(row.payload.at) ?? event.at,
        });
      }
    }
    await client.query(`delete from deferred_bridge_events where id = $1`, [row.id]);
  }
}

async function applyProgressEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
): Promise<void> {
  const data = asRecord(event.data);
  const ticketId = asString(data.ticketId);
  const text = asString(data.text);
  if (!ticketId || !text) {
    return;
  }
  const task = await taskByTicket(client, ticketId, repository.id);
  if (!task) {
    await deferEvent(client, repository, event);
    return;
  }
  await emit(client, repository, event, 'task.progress', task.conversationId, task.id, {
    ticketId,
    text: text.slice(0, 4000),
    at: asString(data.at) ?? event.at,
  });
}

async function applyArtifactEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
): Promise<void> {
  const data = asRecord(event.data);
  const ticketId = asString(data.ticketId);
  const url = asString(data.url);
  const number = asNumber(data.number);
  if (!ticketId || !url || number === null) {
    return;
  }
  const task = await taskByTicket(client, ticketId, repository.id);
  if (!task) {
    return undefined;
  }
  const inserted = await client.query<{
    id: string;
    task_id: string | null;
    conversation_id: string | null;
    type: string;
    mime_type: string | null;
    size_bytes: number | null;
    sha256: string | null;
    metadata: Record<string, unknown>;
    created_at: Date;
  }>(
    `insert into artifacts (repository_id, conversation_id, task_id, type, metadata)
     values ($1, $2, $3, 'pull_request', $4::jsonb)
     returning id, task_id, conversation_id, type, mime_type, size_bytes, sha256, metadata, created_at`,
    [
      repository.id,
      task.conversationId,
      task.id,
      JSON.stringify({ url, number, title: asString(data.title) }),
    ],
  );
  const artifactRow = inserted.rows[0];
  if (!artifactRow) {
    return;
  }
  await client.query(
    `update task_links set pr_url = $2, pr_number = $3, updated_at = now() where id = $1`,
    [task.id, url, number],
  );
  const artifact: ArtifactSummary = {
    id: artifactRow.id,
    taskId: artifactRow.task_id,
    conversationId: artifactRow.conversation_id,
    type: 'pull_request',
    mimeType: artifactRow.mime_type,
    sizeBytes: artifactRow.size_bytes,
    sha256: artifactRow.sha256,
    metadata: artifactRow.metadata,
    createdAt: artifactRow.created_at.toISOString(),
  };
  await emit(client, repository, event, 'artifact.created', task.conversationId, task.id, {
    artifact,
  });
}

/**
 * A recoverable human pause. The runtime emits this when the intent is recorded
 * and again (with `stoppedAt` and evidence) when the worker and its background
 * work actually ended. The platform only shows `stopped` for the second form.
 */
async function applyPausedEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
  logger: DispatcherLogger,
): Promise<void> {
  const data = asRecord(event.data);
  const ticketId = asString(data.ticketId);
  if (!ticketId) {
    return;
  }
  const task = await taskByTicket(client, ticketId, repository.id);
  if (!task) {
    logger.warn(
      { event: 'bridge.pause_unmapped', bridgeEvent: event.id, ticketId },
      'pause evidence arrived before the platform task was linked',
    );
    return;
  }
  const evidence = asRecord(data.evidence);
  const pauseError = asString(data.error);
  const proven = pauseEvidenceProven({ ...evidence, stoppedAt: data.stoppedAt });
  const problem = proven
    ? undefined
    : pauseEvidenceProblem({ ...evidence, stoppedAt: data.stoppedAt });
  const stoppedAt = proven ? asString(data.stoppedAt) : undefined;
  const state = proven
    ? 'stopped'
    : pauseError || data.stoppedAt !== undefined
      ? 'blocked'
      : 'stopping';
  const storedEvidence = proven
    ? evidence
    : {
        ...evidence,
        ...(problem ? { problem } : {}),
        ...(pauseError ? { error: pauseError } : {}),
      };
  const updated = await client.query(
    `update task_links
        set state = $2,
            stopped_at = $3::timestamptz,
            stop_evidence = $4::jsonb,
            last_event_seq = greatest(coalesce(last_event_seq, 0), $5),
            version = version + 1,
            updated_at = now()
      where id = $1 and (last_event_seq is null or last_event_seq < $5)`,
    [task.id, state, stoppedAt, JSON.stringify(storedEvidence), event.seq],
  );
  if ((updated.rowCount ?? 0) === 0) {
    return;
  }
  // The paused event and the Bridge operation settle independently. Settle the
  // platform operation from this evidence too, so a stop followed immediately by
  // resume is not refused as "another command is still being processed".
  // Only the terminal pause event settles the Bridge operation. The first
  // event records the intent and has no `stoppedAt`; treating its expected
  // "did not record when the worker stopped" as a failure would fail the
  // operation before the settled event with evidence arrives.
  if (data.stoppedAt !== undefined || pauseError) {
    await client.query(
      `update task_operations
          set state = $2,
              bridge_operation_id = coalesce($3, bridge_operation_id),
              evidence = $4::jsonb,
              error = $5,
              updated_at = now()
        where task_id = $1 and kind = 'pause' and state in ('queued', 'running')`,
      [
        task.id,
        proven ? 'succeeded' : 'failed',
        asString(data.operationId),
        JSON.stringify(storedEvidence),
        proven ? null : (pauseError ?? problem),
      ],
    );
  }
  const next = await taskByTicket(client, ticketId, repository.id);
  if (next) {
    await emit(client, repository, event, 'task.state_changed', next.conversationId, next.id, {
      task: taskSummary(next),
    });
  }
}

/**
 * Imports one GitHub-origin runtime ticket into Work. `(repository, source,
 * external_key)` keeps one conversation and `(repository, upstream_ticket_id)`
 * keeps one task, so a repeated Bridge event or a second entry point reuses the
 * same work instead of executing it twice.
 */
async function lockConversationForWrite(
  client: pg.PoolClient,
  conversationId: string,
): Promise<string | null> {
  const [source] = (
    await client.query<{ id: string; mergedInto: string | null }>(
      `select id, merged_into_conversation_id as "mergedInto"
         from conversations where id = $1 for update`,
      [conversationId],
    )
  ).rows;
  if (!source) return null;
  if (!source.mergedInto) return source.id;
  // Same alias -> target order as the API message transaction.
  const [target] = (
    await client.query<{ id: string }>(`select id from conversations where id = $1 for update`, [
      source.mergedInto,
    ])
  ).rows;
  return target?.id ?? null;
}

async function applyIgnoredDecision(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
  logger: DispatcherLogger,
): Promise<void> {
  const data = asRecord(event.data);
  const conversationId = asString(data.conversationId);
  const decisionId = asString(data.decisionId);
  const messageId = asString(data.messageId);
  const messageIds = Array.isArray(data.messageIds)
    ? data.messageIds.filter(
        (value): value is string => typeof value === 'string' && value.length > 0,
      )
    : messageId
      ? [messageId]
      : [];
  if (!conversationId || !decisionId || messageIds.length === 0) return;
  // Serialize with API message acceptance on the same conversation row. The
  // conditional UPDATE alone is not enough: an API transaction can read the
  // pending task before committing its new message.
  const targetId = await lockConversationForWrite(client, conversationId);
  if (!targetId) return;
  const task = await taskByConversation(client, targetId, repository.id);
  if (!task || task.state !== 'pending_admission') return;
  const members = await client.query<{ count: number }>(
    `select count(*)::int as count from messages
      where conversation_id = $1 and id = any($2::uuid[])`,
    [targetId, messageIds],
  );
  if (members.rows[0]?.count !== messageIds.length) return;
  // Close only the admission generation the decision names. A later message or
  // an accepted-but-not-yet-delivered message must keep the task open, or the
  // ignore would strand that delivery forever.
  const updated = await client.query(
    `update task_links t
        set state = 'not_accepted', version = version + 1, updated_at = now()
      where t.id = $1 and t.repository_id = $2 and t.state = 'pending_admission'
        and not exists (
          select 1 from messages m
           where m.conversation_id = t.conversation_id
             and m.kind = 'user'
             and not (m.id = any($3::uuid[]))
             and m.created_at > (
               select max(decided.created_at) from messages decided
                where decided.id = any($3::uuid[])
             )
        )
        and not exists (
          select 1 from deliveries d
            join messages m on m.id = d.message_id
           where m.conversation_id = t.conversation_id
             and m.kind = 'user'
             and not (m.id = any($3::uuid[]))
             and d.state in ('pending', 'delivering')
        )`,
    [task.id, repository.id, messageIds],
  );
  if ((updated.rowCount ?? 0) === 0) return;
  const next = await taskByConversation(client, targetId, repository.id);
  if (next) {
    await emit(client, repository, event, 'task.state_changed', targetId, next.id, {
      task: taskSummary(next),
    });
  }
  logger.info(
    {
      event: 'bridge.decision_ignored',
      decisionId,
      taskId: task.id,
      conversationId: targetId,
      messageId,
    },
    'closed the pending admission named by the ignored decision',
  );
}

async function applyImportedEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
  logger: DispatcherLogger,
): Promise<string | undefined> {
  const data = asRecord(event.data);
  const ticketId = asString(data.ticketId);
  const externalId = asString(data.externalId);
  if (!ticketId || !externalId) {
    logger.warn(
      { event: 'bridge.import_invalid', bridgeEvent: event.id },
      'ticket.imported had no ticketId or externalId',
    );
    return;
  }
  const state = stateFromSnapshot(data.state) ?? 'queued';
  const base = asRecord(data.base);
  const baseBranch = asString(base.branch) ?? repository.defaultBranch;
  const baseCommit = asString(base.commit);
  const modelValue = base.model;
  const modelId =
    typeof modelValue === 'string' && modelValue.length > 0
      ? modelValue
      : (() => {
          const model = asRecord(modelValue);
          const provider = asString(model.provider);
          const name = asString(model.model);
          return provider && name ? `${provider}/${name}` : null;
        })();
  const title = asString(data.title) ?? `GitHub ${externalId}`;
  const url = asString(data.url);
  const number = asNumber(data.number);
  const isPr = data.isPr === true;

  const existingTask = await taskByTicket(client, ticketId, repository.id);
  if (existingTask) {
    // A Web-first requirement already has a task before the GitHub thread is
    // linked. Claim that exact conversation only when one of its messages names
    // this repository and number; never guess from recent activity.
    if (number != null) {
      const bodies = await client.query<{ body: string }>(
        `select body from messages where conversation_id = $1`,
        [existingTask.conversationId],
      );
      if (
        bodies.rows.some((row) => githubRequirementUrl(row.body, repository.fullName, isPr, number))
      ) {
        await client.query(
          `update conversations
              set source = 'github', external_key = $2, external_url = $3, updated_at = now()
            where id = $1 and source = 'web' and external_key is null`,
          [existingTask.conversationId, externalId, url ?? null],
        );
      }
    }
    const updated = await client.query(
      `update task_links
          set state = $2,
              base_branch = coalesce($3, base_branch),
              base_commit = coalesce($4, base_commit),
              model_id = coalesce($5, model_id),
              last_event_seq = greatest(coalesce(last_event_seq, 0), $6),
              version = case when state is distinct from $2 then version + 1 else version end,
              updated_at = now()
        where id = $1 and (last_event_seq is null or last_event_seq < $6)`,
      [existingTask.id, state, baseBranch, baseCommit, modelId, event.seq],
    );
    if ((updated.rowCount ?? 0) > 0) {
      const next = await taskByTicket(client, ticketId, repository.id);
      if (next) {
        await emit(client, repository, event, 'task.state_changed', next.conversationId, next.id, {
          task: taskSummary(next),
        });
      }
    }
    return undefined;
  }

  const ownerResult = await client.query<{ owner_id: string }>(
    `select owner_id from repositories where id = $1`,
    [repository.id],
  );
  const ownerId = ownerResult.rows[0]?.owner_id;
  if (!ownerId) {
    return undefined;
  }

  let conversation = (
    await client.query<{ id: string; title: string | null }>(
      `select id, title from conversations
        where repository_id = $1 and source = 'github' and external_key = $2 limit 1`,
      [repository.id, externalId],
    )
  ).rows[0];
  // A requirement first started in Work can be linked from a GitHub mention: claim
  // the Web conversation that already contains this Issue/PR URL instead of opening
  // a second one and executing the same requirement twice.
  if (!conversation && number != null) {
    const candidates = await client.query<{ id: string; title: string | null; body: string }>(
      `select c.id, c.title, m.body from conversations c
         join messages m on m.conversation_id = c.id
        where c.repository_id = $1 and c.source = 'web' and c.external_key is null
          and m.body like '%github.com/%'
        order by c.updated_at desc, m.created_at desc
        limit 50`,
      [repository.id],
    );
    const web = candidates.rows.find((row) =>
      githubRequirementUrl(row.body, repository.fullName, isPr, number),
    );
    if (web) {
      await client.query(
        `update conversations
            set source = 'github', external_key = $2, external_url = $3, updated_at = now()
          where id = $1`,
        [web.id, externalId, url ?? null],
      );
      conversation = { id: web.id, title: web.title };
    }
  }
  if (!conversation) {
    conversation = (
      await client.query<{ id: string; title: string | null }>(
        `insert into conversations
           (owner_id, repository_id, mode, execution_mode, source, external_key, external_url,
            base_branch, base_commit, model_id, title)
         values ($1, $2, 'work', 'execute', 'github', $3, $4, $5, $6, $7, $8)
         returning id, title`,
        [ownerId, repository.id, externalId, url, baseBranch, baseCommit, modelId, title],
      )
    ).rows[0];
  }
  if (!conversation) {
    return undefined;
  }

  // The conversation can already hold a task for a different runtime Ticket for the
  // same requirement (a replay or a second GitHub entry). Reuse that task instead of
  // inserting a second one, which would violate the conversation unique key and
  // abort the whole event page, leaving the cursor stuck.
  const existingConversationTask = (
    await client.query<TaskRow>(
      `select ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}
         from task_links where conversation_id = $1 limit 1`,
      [conversation.id],
    )
  ).rows[0];
  if (existingConversationTask && existingConversationTask.upstreamTicketId !== ticketId) {
    const terminal = new Set(['completed', 'closed_unmerged', 'not_accepted', 'failed', 'dropped']);
    if (!terminal.has(existingConversationTask.state)) {
      await client.query(
        `insert into messages
           (conversation_id, direction, kind, body, source, source_event_id, created_at)
         values ($1, 'inbound', 'notice', $2, 'bridge', $3, now())
         on conflict (conversation_id, source_event_id) where source_event_id is not null
         do nothing`,
        [
          conversation.id,
          `A second GitHub ticket (${externalId}) for this requirement was linked to the task that is already running; it is not executed separately.`,
          `import-duplicate:${ticketId}`,
        ],
      );
      logger.warn(
        {
          event: 'bridge.import_duplicate',
          bridgeEvent: event.id,
          ticketId,
          existingTicketId: existingConversationTask.upstreamTicketId,
          taskId: existingConversationTask.id,
        },
        'ignored a duplicate GitHub ticket for an active Work task',
      );
      // The runtime already has this second Ticket. Queue a durable pause so a
      // failed call or a restart retries it; the platform task remains the only
      // execution.
      await client.query(
        `insert into duplicate_tickets (repository_id, task_id, ticket_id)
         values ($1, $2, $3)
         on conflict (repository_id, ticket_id) do nothing`,
        [repository.id, existingConversationTask.id, ticketId],
      );
      return ticketId;
    }
    // The previous ticket ended: the same conversation starts a new attempt on the
    // newer ticket, clearing the old PR/stop fields but preserving message history.
    const updated = await client.query<TaskRow>(
      `update task_links
          set upstream_ticket_id = $2,
              upstream_thread_id = $3,
              state = $4,
              base_branch = coalesce($5, base_branch),
              base_commit = coalesce($6, base_commit),
              model_id = coalesce($7, model_id),
              runtime_lease_id = null,
              stopped_at = null,
              stop_evidence = '{}'::jsonb,
              pr_url = null,
              pr_number = null,
              plan_revision = null,
              last_event_seq = greatest(coalesce(last_event_seq, 0), $8),
              version = version + 1,
              updated_at = now()
        where id = $1
        returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
      [
        existingConversationTask.id,
        ticketId,
        externalId,
        state,
        baseBranch,
        baseCommit,
        modelId,
        event.seq,
      ],
    );
    const next = updated.rows[0];
    if (next) {
      await emit(client, repository, event, 'task.state_changed', conversation.id, next.id, {
        task: taskSummary(next),
      });
    }
    return undefined;
  }

  const insertedTask = (
    await client.query<{ id: string }>(
      `insert into task_links
         (conversation_id, repository_id, upstream_ticket_id, upstream_thread_id, state,
          base_branch, base_commit, model_id, execution_mode, last_event_seq)
       values ($1, $2, $3, $4, $5, $6, $7, $8, 'execute', $9)
       on conflict (repository_id, upstream_ticket_id) where upstream_ticket_id is not null
       do nothing
       returning id`,
      [
        conversation.id,
        repository.id,
        ticketId,
        externalId,
        state,
        baseBranch,
        baseCommit,
        modelId,
        event.seq,
      ],
    )
  ).rows[0];
  const task = await taskByTicket(client, ticketId, repository.id);
  if (!task) {
    return undefined;
  }

  // A GitHub @ in an M5-B runtime is held as pending_admission until the
  // platform admits it; an older runtime has already started a worker, so a
  // refused lease there still needs the durable pause path. Bind the task to
  // the repository execution lease and never create execution beside another
  // admitted generation.
  const admissionRequired = data.admissionRequired === true;
  const admission = await acquireExecutionLeaseOn(
    client,
    repository.id,
    { taskId: task.id, reason: 'github import' },
    3_600_000,
  );
  let admittedTask = task;
  if (!admission.lease) {
    if (admissionRequired) {
      const pending = await client.query<TaskRow>(
        `update task_links
            set state = case when state = 'pending_admission' then state else 'pending_admission' end,
                stop_evidence = $2::jsonb, version = version + 1, updated_at = now()
          where id = $1
          returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
        [
          task.id,
          JSON.stringify({
            admissionRefused: admission.refused,
            waitingFor: admission.waitingFor,
            reason: admission.detail,
          }),
        ],
      );
      admittedTask = pending.rows[0] ?? task;
    } else if (!['blocked', 'stopped', 'completed', 'closed_unmerged'].includes(task.state)) {
      const blocked = await client.query<TaskRow>(
        `update task_links
            set state = 'blocked', stop_evidence = $2::jsonb,
                version = version + 1, updated_at = now()
          where id = $1
          returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
        [
          task.id,
          JSON.stringify({
            admissionRefused: admission.refused,
            waitingFor: admission.waitingFor,
            reason: admission.detail,
          }),
        ],
      );
      admittedTask = blocked.rows[0] ?? task;
      await client.query(
        `insert into task_operations
           (task_id, repository_id, kind, client_command_id, request_fingerprint, evidence)
         values ($1, $2, 'pause', $3, 'import-admission', $4::jsonb)
         on conflict do nothing`,
        [
          task.id,
          repository.id,
          `import-admission:${task.id}`,
          JSON.stringify({
            reason: 'github import was refused an execution lease',
            leaseRefused: admission.refused,
          }),
        ],
      );
    }
  }

  if (insertedTask) {
    await emit(client, repository, event, 'task.linked', conversation.id, task.id, {
      task: taskSummary(admittedTask),
    });
  }

  const text = asString(data.text);
  const triggerId = asString(data.triggerId);
  if (text && triggerId) {
    const inserted = await client.query<{ id: string }>(
      `insert into messages
         (conversation_id, direction, kind, body, source, source_event_id, created_at)
       values ($1, 'inbound', 'user', $2, 'github', $3, $4)
       on conflict (conversation_id, source_event_id) where source_event_id is not null
       do nothing
       returning id`,
      [conversation.id, text, `import:${triggerId}`, event.at],
    );
    const messageId = inserted.rows[0]?.id;
    if (messageId) {
      await emit(
        client,
        repository,
        { ...event, id: `${event.id}:message` },
        'message.created',
        conversation.id,
        task.id,
        { messageId, direction: 'inbound' },
      );
    }
  }

  logger.info(
    {
      event: 'bridge.ticket_imported',
      bridgeEvent: event.id,
      ticketId,
      externalId,
      isPr,
      number,
    },
    'imported a GitHub-origin ticket into Work',
  );
  return undefined;
}

export async function applyEvent(
  client: pg.PoolClient,
  repository: RepositoryBinding,
  event: BridgeEvent,
  logger: DispatcherLogger,
): Promise<string | undefined> {
  const seen = await client.query(
    `select 1 from events where source = 'bridge' and source_event_id = $1 limit 1`,
    [event.id],
  );
  if ((seen.rowCount ?? 0) > 0) {
    return undefined;
  }
  switch (event.type) {
    case 'ticket.linked':
    case 'ticket.state_changed':
    case 'ticket.question':
    case 'ticket.ended':
      await applyTaskEvent(client, repository, event, logger);
      return;
    case 'ticket.paused':
      await applyPausedEvent(client, repository, event, logger);
      return;
    case 'ticket.imported':
      return applyImportedEvent(client, repository, event, logger);
    case 'decision.ignored':
      await applyIgnoredDecision(client, repository, event, logger);
      return;
    case 'message.read':
      await applyReadEvent(client, repository, event, logger);
      return;
    case 'message.delivered':
      await applyDeliveredEvent(client, event);
      return;
    case 'task.progress':
      await applyProgressEvent(client, repository, event);
      return;
    case 'artifact.created':
      await applyArtifactEvent(client, repository, event);
      return;
    default:
      logger.warn(
        { event: 'bridge.event_unknown', bridgeEvent: event.id, type: event.type },
        'skipped an unknown Bridge event type',
      );
  }
}

/** Rotates the recovery pass per repository; in-process and wrapped on restart. */
const recoveryCursors = new Map<string, string>();

/**
 * Historical identity recovery: a queued/running task whose reservation is
 * still reserved may predate identity fields on state events. Confirm the
 * Bridge's authoritative snapshot (repository, Ticket, Assignment, state)
 * before claiming; lease/fence are re-checked by claimExecution. An unclear
 * snapshot leaves the reservation reserved and records why.
 */
export async function recoverReservedTasks(
  pool: pg.Pool,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
): Promise<void> {
  const load = async (cursor: string | undefined) =>
    pool.query<{
      id: string;
      upstream_ticket_id: string | null;
      assignment_id: string;
      lease_id: string;
      fencing_token: string;
    }>(
      `select t.id, t.upstream_ticket_id, r.assignment_id, r.lease_id, r.fencing_token
       from task_links t
       join execution_reservations r
         on r.repository_id = t.repository_id and r.request_key = 'admit:' || t.id
      where t.repository_id = $1 and t.state in ('queued', 'running') and r.state = 'reserved'
        and ($2::uuid is null or t.id > $2::uuid)
      order by t.id
      limit 32`,
      [repository.id, cursor ?? null],
    );
  const cursor = recoveryCursors.get(repository.id);
  let rows = await load(cursor);
  if (rows.rows.length === 0 && cursor) {
    recoveryCursors.delete(repository.id);
    rows = await load(undefined);
  }
  const last = rows.rows.at(-1);
  if (last) recoveryCursors.set(repository.id, last.id);
  for (const row of rows.rows) {
    if (!row.upstream_ticket_id) continue;
    let snapshot: Awaited<ReturnType<RuntimeBridgeClient['ticket']>>;
    try {
      snapshot = await bridge.ticket(row.upstream_ticket_id);
    } catch (error) {
      logger.warn(
        {
          event: 'admission.recovery_unavailable',
          taskId: row.id,
          error: bridgeErrorMessage(error),
        },
        'could not read the authoritative Bridge snapshot; keeping the reservation',
      );
      continue;
    }
    const repositoryName = snapshot.base?.repository;
    if (
      snapshot.ticketId !== row.upstream_ticket_id ||
      snapshot.assignmentId !== row.assignment_id ||
      snapshot.admission?.leaseId !== row.lease_id ||
      String(snapshot.admission?.fencingToken) !== String(row.fencing_token) ||
      // GitHub-imported tickets have no frozen workspace spec, so the snapshot
      // base (and repository) is null; the connection already binds the repo.
      (repositoryName !== undefined &&
        repositoryName !== null &&
        repositoryName !== repository.fullName) ||
      (snapshot.state !== 'queued' && snapshot.state !== 'running')
    ) {
      await pool.query(
        `update task_links
            set stop_evidence = coalesce(stop_evidence, '{}'::jsonb) || $2::jsonb,
                version = version + 1, updated_at = now()
          where id = $1 and state in ('queued', 'running')
            and coalesce(stop_evidence->>'admissionRecovery', '') is distinct from 'snapshot_mismatch'`,
        [row.id, JSON.stringify({ admissionRecovery: 'snapshot_mismatch' })],
      );
      continue;
    }
    try {
      const claimed = await claimExecution(pool, {
        repositoryId: repository.id,
        requestKey: `admit:${row.id}`,
        taskId: row.id,
        ticketId: row.upstream_ticket_id,
        assignmentId: row.assignment_id,
        leaseId: row.lease_id,
        fencingToken: String(row.fencing_token),
      });
      if (!claimed.claimed) {
        logger.warn(
          { event: 'admission.recovery_not_claimable', taskId: row.id },
          'snapshot matched but the reservation was already claimed',
        );
      } else {
        await pool.query(
          `update task_links
              set stop_evidence = stop_evidence - 'admissionRecovery' - 'admissionRecoveryError',
                  version = version + 1, updated_at = now()
            where id = $1 and state in ('queued', 'running')`,
          [row.id],
        );
      }
    } catch (error) {
      await pool.query(
        `update task_links
            set stop_evidence = coalesce(stop_evidence, '{}'::jsonb) || $2::jsonb,
                version = version + 1, updated_at = now()
          where id = $1 and state in ('queued', 'running')
            and (
              coalesce(stop_evidence->>'admissionRecovery', '') is distinct from 'generation_mismatch'
              or coalesce(stop_evidence->>'admissionRecoveryError', '') is distinct from $3
            )`,
        [
          row.id,
          JSON.stringify({
            admissionRecovery: 'generation_mismatch',
            admissionRecoveryError: bridgeErrorMessage(error),
          }),
          bridgeErrorMessage(error),
        ],
      );
      logger.warn(
        { event: 'admission.recovery_refused', taskId: row.id, error: bridgeErrorMessage(error) },
        'kept the reservation because the current generation did not match',
      );
    }
  }
}

/** Pulls one page of Bridge events and applies them with the cursor in one transaction. */
export async function syncOnce(
  pool: pg.Pool,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  logger: DispatcherLogger,
): Promise<boolean> {
  await pool.query(
    `insert into runtime_cursors (repository_id) values ($1) on conflict do nothing`,
    [repository.id],
  );
  const cursorResult = await pool.query<{ cursor: string }>(
    `select cursor from runtime_cursors where repository_id = $1`,
    [repository.id],
  );
  const cursor = BigInt(cursorResult.rows[0]?.cursor ?? '0');
  const page = await bridge.events(cursor, 100);
  await recoverReservedTasks(pool, repository, bridge, logger);
  if (page.events.length === 0) {
    return false;
  }

  const client = await pool.connect();
  try {
    await client.query('begin');
    for (const event of page.events) {
      // A failed projection rolls back the whole page and keeps the cursor
      // before it, so the next sync retries the event instead of silently
      // losing a task-state or message-read update.
      await client.query('savepoint bridge_event');
      try {
        const duplicate = await applyEvent(client, repository, event, logger);
        if (duplicate) {
          logger.info(
            { event: 'bridge.import_duplicate_queued', bridgeEvent: event.id, ticketId: duplicate },
            'queued a duplicate runtime ticket for a durable pause',
          );
        }
        await client.query('release savepoint bridge_event');
      } catch (error) {
        if (!isUniqueViolation(error)) {
          throw error;
        }
        // An unexpected duplicate is one bad event, not a reason to keep the
        // whole page (and its cursor) stuck forever. Keep the projection that
        // already exists and advance past this event.
        await client.query('rollback to savepoint bridge_event');
        logger.warn(
          { event: 'bridge.event_duplicate', bridgeEvent: event.id, type: event.type },
          'skipped an event that conflicted with an existing row',
        );
      }
    }
    const last = page.events[page.events.length - 1]?.seq ?? cursor;
    await client.query(
      `update runtime_cursors set cursor = $2, updated_at = now() where repository_id = $1`,
      [repository.id, String(last)],
    );
    await client.query('commit');
  } catch (error) {
    await client.query('rollback').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
  return true;
}

export { githubRequirementUrl } from '@moyu/contracts';

function isUniqueViolation(error: unknown): boolean {
  return (
    typeof error === 'object' &&
    error !== null &&
    'code' in error &&
    (error as { code?: unknown }).code === '23505'
  );
}

export type { ProjectedTask };

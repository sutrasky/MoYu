import { randomUUID } from 'node:crypto';

import type pg from 'pg';

import type { RuntimeBridgeClient } from '@moyu/runtime-client';

import type { DispatcherConfig } from './config';
import { appendEventSql } from './events-sql';
import { acquireExecutionLease } from './leases';
import { claimExecution, readReservation, reserveExecution } from './reservations';
import type { DispatcherLogger } from './logger';
import { bridgeErrorMessage, isRetryableBridgeError, type RepositoryBinding } from './runtime';
import { TASK_COLUMNS, taskSummary, type TaskRow } from './tasks';

async function emitTaskState(
  pool: pg.Pool,
  repository: RepositoryBinding,
  task: TaskRow,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query('begin');
    await appendEventSql(client, {
      type: 'task.state_changed',
      source: 'dispatcher',
      sourceEventId: `admission:${task.id}:${task.version}`,
      repositoryId: repository.id,
      conversationId: task.conversationId,
      taskId: task.id,
      payload: { task: taskSummary(task) },
    });
    await client.query('commit');
  } catch (error) {
    await client.query('rollback').catch(() => undefined);
    throw error;
  } finally {
    client.release();
  }
}

/**
 * Admits GitHub-origin tickets that an M5-B runtime created as
 * `pending_admission`. No worker exists before this call, so a refused or
 * missing lease has no execution side effect; a 4xx refusal releases the slot,
 * while a retryable transport error keeps the generation and retries.
 */
export async function admitImportedOnce(
  pool: pg.Pool,
  config: DispatcherConfig,
  repository: RepositoryBinding,
  bridge: RuntimeBridgeClient,
  supported: boolean,
  logger: DispatcherLogger,
  stopProof?: (task: TaskRow) => Promise<{ allowed: boolean; reason?: string }>,
): Promise<boolean> {
  if (!supported) return false;
  const rows = await pool.query<TaskRow>(
    `select ${TASK_COLUMNS} from task_links t
      where t.repository_id = $1
        and t.state = 'pending_admission'
        and t.upstream_ticket_id is not null
        and not exists (
          select 1 from task_operations op
           where op.task_id = t.id and op.state in ('queued', 'running')
        )
      order by t.updated_at
      limit 5`,
    [repository.id],
  );
  let worked = false;
  for (const task of rows.rows) {
    worked = true;
    if (stopProof) {
      const proof = await stopProof(task);
      if (!proof.allowed) {
        await pool.query(
          `update task_links
              set stop_evidence = $2::jsonb, updated_at = now()
            where id = $1 and state = 'pending_admission'`,
          [task.id, JSON.stringify({ stopProof: proof.reason ?? 'unproven' })],
        );
        continue;
      }
    }
    const account = await acquireExecutionLease(
      pool,
      repository.id,
      { taskId: task.id, reason: 'github admission' },
      config.operationLeaseTtlMs,
    );
    if (!account.lease) {
      await pool.query(
        `update task_links
            set stop_evidence = $2::jsonb, updated_at = now()
          where id = $1 and state = 'pending_admission'`,
        [
          task.id,
          JSON.stringify({
            admissionRefused: account.refused,
            waitingFor: account.waitingFor,
            reason: account.detail,
          }),
        ],
      );
      continue;
    }
    const requestKey = `admit:${task.id}`;
    const existing = await readReservation(pool, repository.id, requestKey);
    if (
      existing &&
      (existing.leaseId !== account.lease.id ||
        existing.fencingToken !== account.lease.fencingToken)
    ) {
      // The Bridge may already own this identity under another generation.
      // Keep the slot and ask a person/recovery path to resolve it.
      await pool.query(
        `update task_links set state = 'blocked', stop_evidence = $2::jsonb,
                version = version + 1, updated_at = now()
          where id = $1 and state = 'pending_admission'`,
        [task.id, JSON.stringify({ admissionReservation: 'generation_conflict', requestKey })],
      );
      logger.error(
        { event: 'admission.reservation_generation_conflict', taskId: task.id },
        'reservation belongs to another lease generation; keeping the execution slot',
      );
      continue;
    }
    const assignmentId = existing?.assignmentId ?? randomUUID().replaceAll('-', '').slice(0, 32);
    const reservation = {
      repositoryId: repository.id,
      requestKey,
      taskId: task.id,
      ticketId: task.upstreamTicketId ?? '',
      assignmentId,
      leaseId: account.lease.id,
      fencingToken: account.lease.fencingToken,
    };
    try {
      await reserveExecution(pool, reservation);
      await bridge.admit(task.upstreamTicketId ?? '', {
        admission: { leaseId: account.lease.id, fencingToken: account.lease.fencingToken },
        assignmentId,
      });
      await claimExecution(pool, reservation);
      const updated = await pool.query<TaskRow>(
        `update task_links
            set state = 'queued', version = version + 1, updated_at = now()
          where id = $1 and state = 'pending_admission'
          returning ${TASK_COLUMNS.replaceAll('t.', 'task_links.')}`,
        [task.id],
      );
      const next = updated.rows[0];
      if (next) {
        await emitTaskState(pool, repository, next);
        logger.info(
          {
            event: 'admission.started',
            taskId: task.id,
            leaseId: account.lease.id,
            fencingToken: account.lease.fencingToken,
          },
          'admitted a pending GitHub ticket under the repository execution lease',
        );
      }
    } catch (error) {
      if (isRetryableBridgeError(error)) {
        // The request may or may not have started; keep the generation and
        // reuse the persisted reservation on the next pass.
        logger.warn(
          { event: 'admission.retry', taskId: task.id, error: bridgeErrorMessage(error) },
          'admission request failed transiently; the same grant will be retried',
        );
        continue;
      }
      // After the reservation was sent, a 4xx/conflict may still have persisted
      // the identity and started work. Keep the lease and the reservation for a
      // same-identity check; never release a generation that may have landed.
      await pool.query(
        `update task_links
            set state = 'blocked', stop_evidence = $2::jsonb,
                version = version + 1, updated_at = now()
          where id = $1 and state = 'pending_admission'`,
        [
          task.id,
          JSON.stringify({
            admissionRefused: 'runtime',
            reason: bridgeErrorMessage(error),
          }),
        ],
      );
      logger.error(
        { event: 'admission.refused', taskId: task.id, error: bridgeErrorMessage(error) },
        'runtime refused the execution admission',
      );
    }
  }
  return worked;
}

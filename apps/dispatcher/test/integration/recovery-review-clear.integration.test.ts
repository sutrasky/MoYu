import { recoverReservedTasks } from '../../src/sync';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { acquireExecutionLease } from '../../src/leases';
import { readReservation, reserveExecution } from '../../src/reservations';
import type { RepositoryBinding } from '../../src/runtime';
import { createTestDatabase, type TestDatabase } from './helpers';

describe('execution reservation on real PostgreSQL', () => {
  let db: TestDatabase;
  let repository: RepositoryBinding;
  let leaseId = '';
  let token = '';
  let taskId = '';
  const key = `reservation-${process.pid}`;

  beforeAll(async () => {
    db = await createTestDatabase();
    const owner = await db.pool.query<{ id: string }>(
      `insert into owners (display_name) values ('reservation') returning id`,
    );
    const repo = await db.pool.query<{ id: string }>(
      `insert into repositories (owner_id, full_name, default_branch, status) values ($1, $2, 'main', 'active') returning id`,
      [owner.rows[0]!.id, `Bouyer12/m5b-reservation-${process.pid}`],
    );
    repository = {
      id: repo.rows[0]!.id,
      fullName: `Bouyer12/m5b-reservation-${process.pid}`,
      defaultBranch: 'main',
      bridgeUrl: 'http://127.0.0.1:1',
      bridgeCredentialRef: 'MOYU_BRIDGE_TOKEN',
    };
    const conversation = await db.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'reservation') returning id`,
      [owner.rows[0]!.id, repository.id],
    );
    const message = await db.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'reservation-1', 'x') returning id`,
      [conversation.rows[0]!.id],
    );
    await db.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [message.rows[0]!.id, `workspace:${message.rows[0]!.id}`],
    );
    const task = await db.pool.query<{ id: string }>(
      `insert into task_links (conversation_id, repository_id, state, base_branch)
       values ($1, $2, 'queued', 'main') returning id`,
      [conversation.rows[0]!.id, repository.id],
    );
    taskId = task.rows[0]!.id;
    const lease = await acquireExecutionLease(db.pool, repository.id, { taskId }, 60_000);
    leaseId = lease.lease!.id;
    token = lease.lease!.fencingToken;
  }, 60_000);
  afterAll(async () => {
    await db.drop();
  });

  const input = (assignmentId = 'assignment-1', fencingToken = token) => ({
    repositoryId: repository.id,
    requestKey: key,
    taskId,
    ticketId: 'ticket-1',
    assignmentId,
    leaseId,
    fencingToken,
  });

  it('review recovery clears its stale error after a successful claim', async () => {
    const evidence = {
      status: 'proven',
      marker: 'keep-proof',
      admissionRecovery: 'snapshot_mismatch',
    };
    await db.pool.query(
      "update task_links set upstream_ticket_id='ticket-1',stop_evidence=$2::jsonb where id=$1",
      [taskId, JSON.stringify(evidence)],
    );
    await reserveExecution(db.pool, { ...input(), requestKey: 'admit:' + taskId });
    const bridge = {
      ticket: async () => ({
        ticketId: 'ticket-1',
        assignmentId: 'assignment-1',
        state: 'queued',
        base: null,
        admission: { leaseId, fencingToken: token },
      }),
    };
    const logger = { info: () => {}, warn: () => {}, error: () => {} };
    await recoverReservedTasks(db.pool, repository, bridge as never, logger);
    expect((await readReservation(db.pool, repository.id, 'admit:' + taskId))?.state).toBe(
      'claimed',
    );
    const result = await db.pool.query('select stop_evidence from task_links where id=$1', [
      taskId,
    ]);
    console.log('REVIEW_SUCCESS_EVIDENCE', JSON.stringify(result.rows[0]));
    expect(result.rows[0].stop_evidence).toEqual({ status: 'proven', marker: 'keep-proof' });
  });
});

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

  it('review recovery does not let another repository reset the scan', async () => {
    const newRepo = await db.pool.query(
      "insert into repositories(owner_id,full_name,default_branch,status) select owner_id,'Bouyer12/recovery-other','main','active' from repositories where id=$1 returning id",
      [repository.id],
    );
    const other = { ...repository, id: newRepo.rows[0].id, fullName: 'Bouyer12/recovery-other' };
    const add = async (repoId: string, id: string, ticket: string) => {
      const c = await db.pool.query(
        "insert into conversations(owner_id,repository_id,mode,execution_mode,base_branch,title) select owner_id,id,'work','execute','main','deterministic recovery' from repositories where id=$1 returning id",
        [repoId],
      );
      await db.pool.query(
        "insert into task_links(id,conversation_id,repository_id,state,base_branch,upstream_ticket_id) values($1,$2,$3,'queued','main',$4)",
        [id, c.rows[0].id, repoId, ticket],
      );
    };
    let target = '';
    for (let i = 0; i < 33; i++) {
      const id = '00000000-0000-4000-8000-' + String(i + 1).padStart(12, '0');
      await add(repository.id, id, 'recovery-' + i);
      await reserveExecution(db.pool, {
        ...input(),
        taskId: id,
        ticketId: 'recovery-' + i,
        assignmentId: 'assignment-' + i,
        requestKey: 'admit:' + id,
      });
      if (i === 32) target = id;
    }
    const b = 'ffffffff-ffff-4fff-8fff-ffffffffffff';
    await add(other.id, b, 'other-ticket');
    const otherLease = (await acquireExecutionLease(db.pool, other.id, { taskId: b }, 60_000))
      .lease!;
    await reserveExecution(db.pool, {
      ...input(),
      repositoryId: other.id,
      taskId: b,
      ticketId: 'other-ticket',
      assignmentId: 'other-assignment',
      requestKey: 'admit:' + b,
      leaseId: otherLease.id,
      fencingToken: otherLease.fencingToken,
    });
    const seen: string[] = [];
    const bridge = {
      ticket: async (id: string) => {
        seen.push(id);
        if (id !== 'recovery-32') throw new Error('controlled unavailable snapshot');
        return {
          ticketId: id,
          assignmentId: 'assignment-32',
          state: 'queued',
          base: null,
          admission: { leaseId, fencingToken: token },
        };
      },
    };
    const logger = { info: () => {}, warn: () => {}, error: () => {} };
    await recoverReservedTasks(db.pool, repository, bridge as never, logger);
    expect(seen).not.toContain('recovery-32');
    await recoverReservedTasks(db.pool, other, bridge as never, logger);
    await recoverReservedTasks(db.pool, repository, bridge as never, logger);
    const result = await readReservation(db.pool, repository.id, 'admit:' + target);
    console.log(
      'REVIEW_SHARED_CURSOR',
      JSON.stringify({
        calls: seen.length,
        otherSeen: seen.includes('other-ticket'),
        targetSeen: seen.includes('recovery-32'),
        state: result?.state,
      }),
    );
    expect(result?.state).toBe('claimed');
  });
});

import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { BridgeEvent } from '@moyu/runtime-client';
import { acquireExecutionLease } from '../../src/leases';
import { applyEvent, projectionBranches } from '../../src/sync';
import type { RepositoryBinding } from '../../src/runtime';
import { createTestDatabase, type TestDatabase } from './helpers';

describe('event projection generation branch on real PostgreSQL', () => {
  let db: TestDatabase;
  let repository: RepositoryBinding;
  let taskId = '';
  let conversationId = '';
  let leaseId = '';
  let token = '';

  beforeAll(async () => {
    db = await createTestDatabase();
    const owner = await db.pool.query<{ id: string }>(
      `insert into owners (display_name) values ('projection') returning id`,
    );
    const repo = await db.pool.query<{ id: string }>(
      `insert into repositories (owner_id, full_name, default_branch, status) values ($1, $2, 'main', 'active') returning id`,
      [owner.rows[0]!.id, `Bouyer12/m5b-projection-${process.pid}`],
    );
    repository = {
      id: repo.rows[0]!.id,
      fullName: `Bouyer12/m5b-projection-${process.pid}`,
      defaultBranch: 'main',
      bridgeUrl: 'http://127.0.0.1:1',
      bridgeCredentialRef: 'MOYU_BRIDGE_TOKEN',
    };
    const conversation = await db.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'projection') returning id`,
      [owner.rows[0]!.id, repository.id],
    );
    conversationId = conversation.rows[0]!.id;
    const task = await db.pool.query<{ id: string }>(
      `insert into task_links (conversation_id, repository_id, state, base_branch, upstream_ticket_id)
       values ($1, $2, 'queued', 'main', 'ticket-1') returning id`,
      [conversationId, repository.id],
    );
    taskId = task.rows[0]!.id;
    const lease = await acquireExecutionLease(db.pool, repository.id, { taskId }, 60_000);
    leaseId = lease.lease!.id;
    token = lease.lease!.fencingToken;
  }, 60_000);
  afterAll(async () => {
    await db.drop();
  });

  const event = (id: string, state: string, seq: number): BridgeEvent =>
    ({
      id,
      type: 'ticket.state_changed',
      seq,
      at: new Date().toISOString(),
      data: {
        ticketId: 'ticket-1',
        state,
        assignmentId: 'assignment-1',
        leaseId,
        fencingToken: token,
      },
    }) as BridgeEvent;

  it('projects a current-generation running event and records the branch', async () => {
    const client = await db.pool.connect();
    try {
      await client.query('begin');
      await applyEvent(client, repository, event('evt-current', 'running', 10), {
        info() {},
        warn() {},
        error() {},
      });
      await client.query('commit');
    } finally {
      client.release();
    }
    const task = await db.pool.query<{ state: string }>(
      `select state from task_links where id = $1`,
      [taskId],
    );
    expect(task.rows[0]?.state).toBe('running');
    expect(projectionBranches.get('evt-current')).toBe('projected');
  });

  it('keeps a stopped task and its evidence when a queued event arrives', async () => {
    const evidence = { marker: 'pause-proof' };
    await db.pool.query(
      `update task_links set state = 'stopped', stop_evidence = $2::jsonb where id = $1`,
      [taskId, JSON.stringify(evidence)],
    );
    const client = await db.pool.connect();
    try {
      await client.query('begin');
      await applyEvent(client, repository, event('evt-stopped', 'queued', 11), {
        info() {},
        warn() {},
        error() {},
      });
      await client.query('commit');
    } finally {
      client.release();
    }
    const task = await db.pool.query<{ state: string; stop_evidence: Record<string, unknown> }>(
      `select state, stop_evidence from task_links where id = $1`,
      [taskId],
    );
    expect(task.rows[0]?.state).toBe('stopped');
    expect(task.rows[0]?.stop_evidence).toMatchObject(evidence);
  });
});

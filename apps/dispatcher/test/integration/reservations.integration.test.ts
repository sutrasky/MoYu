import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { acquireExecutionLease } from '../../src/leases';
import {
  claimExecution,
  readReservation,
  reconcileExecutionClaimOn,
  reserveExecution,
} from '../../src/reservations';
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

  it('claims a reserved identity from a queued state event only for the current lease', async () => {
    const key3 = `event-claim-${process.pid}`;
    await reserveExecution(db.pool, { ...input(), requestKey: key3 });
    const key4 = `event-wrong-${process.pid}`;
    await reserveExecution(db.pool, { ...input(), requestKey: key4 });
    const wrongEarly = await db.pool.connect();
    try {
      expect(
        await reconcileExecutionClaimOn(
          wrongEarly,
          repository.id,
          key4,
          'ticket-1',
          'old-assignment',
          leaseId,
          token,
        ),
      ).toBe(false);
    } finally {
      wrongEarly.release();
    }
    const wrongFence = await db.pool.connect();
    try {
      expect(
        await reconcileExecutionClaimOn(
          wrongFence,
          repository.id,
          key4,
          'ticket-1',
          'assignment-1',
          leaseId,
          String(Number(token) + 1),
        ),
      ).toBe(false);
    } finally {
      wrongFence.release();
    }
    expect((await readReservation(db.pool, repository.id, key4))?.state).toBe('reserved');
    const rightEarly = await db.pool.connect();
    try {
      expect(
        await reconcileExecutionClaimOn(
          rightEarly,
          repository.id,
          key4,
          'ticket-1',
          'assignment-1',
          leaseId,
          token,
        ),
      ).toBe(true);
    } finally {
      rightEarly.release();
    }
    const client = await db.pool.connect();
    try {
      expect(
        await reconcileExecutionClaimOn(
          client,
          repository.id,
          key3,
          'ticket-1',
          'assignment-1',
          leaseId,
          token,
        ),
      ).toBe(true);
    } finally {
      client.release();
    }
    expect((await readReservation(db.pool, repository.id, key3))?.state).toBe('claimed');
    const wrongClient = await db.pool.connect();
    try {
      expect(
        await reconcileExecutionClaimOn(
          wrongClient,
          repository.id,
          key3,
          'ticket-1',
          'old-assignment',
          leaseId,
          token,
        ),
      ).toBe(false);
    } finally {
      wrongClient.release();
    }
    const stale = await reserveExecution(db.pool, {
      ...input('assignment-stale'),
      requestKey: `event-stale-${process.pid}`,
    });
    await db.pool.query(`update runtime_leases set state = 'released' where id = $1`, [leaseId]);
    const staleClient = await db.pool.connect();
    try {
      expect(
        await reconcileExecutionClaimOn(
          staleClient,
          repository.id,
          `event-stale-${process.pid}`,
          'ticket-1',
          'assignment-stale',
          leaseId,
          token,
        ),
      ).toBe(false);
    } finally {
      staleClient.release();
    }
    expect(stale.id).toBeDefined();
    await db.pool.query(
      `update runtime_leases set state = 'active', released_at = null where id = $1`,
      [leaseId],
    );
  });

  it('keeps one reserved identity across a lost Bridge response before claim', async () => {
    const key2 = `lost-response-${process.pid}`;
    const first = await reserveExecution(db.pool, { ...input(), requestKey: key2 });
    // The Bridge has persisted/started, but the platform never reaches claim.
    const reread = await readReservation(db.pool, repository.id, key2);
    expect(reread).toMatchObject({
      assignmentId: 'assignment-1',
      leaseId,
      fencingToken: token,
      state: 'reserved',
    });
    const retry = await reserveExecution(db.pool, { ...input(), requestKey: key2 });
    expect(retry.id).toBe(first.id);
    const rows = await db.pool.query<{ count: number }>(
      `select count(*)::int as count from execution_reservations where repository_id = $1 and request_key = $2`,
      [repository.id, key2],
    );
    expect(rows.rows[0]?.count).toBe(1);
    const claim = await claimExecution(db.pool, { ...input(), requestKey: key2 });
    expect(claim).toMatchObject({ claimed: true, id: first.id });
  });

  it('replays the same reservation, rejects identity and stale-fence changes, and claims once', async () => {
    const first = await reserveExecution(db.pool, input());
    const replay = await reserveExecution(db.pool, input());
    expect(replay).toEqual(first);
    expect(await readReservation(db.pool, repository.id, key)).toMatchObject({
      assignmentId: 'assignment-1',
      leaseId,
      fencingToken: token,
    });
    await expect(reserveExecution(db.pool, input('assignment-2'))).rejects.toThrow(
      /different identity/,
    );
    await expect(
      reserveExecution(db.pool, input('assignment-1', String(Number(token) + 1))),
    ).rejects.toThrow(/current active lease/);
    const claims = await Promise.all([
      claimExecution(db.pool, input()),
      claimExecution(db.pool, input()),
    ]);
    expect(claims.filter((item) => item.claimed)).toHaveLength(1);
    expect(claims[0]!.id).toBe(first.id);
    expect(claims[1]!.id).toBe(first.id);
  });
});

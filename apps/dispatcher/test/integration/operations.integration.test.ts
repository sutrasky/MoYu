import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { once } from 'node:events';

import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import type { BridgeEvent, BridgeTicketSnapshot } from '@moyu/runtime-client';
import { RuntimeBridgeClient } from '@moyu/runtime-client';

import { loadDispatcherConfig, type DispatcherConfig } from '../../src/config';
import { deliverOnce } from '../../src/delivery';
import { createConsoleLogger } from '../../src/logger';
import { acquireExecutionLease } from '../../src/leases';
import { maintainLeases, operationsOnce, reconcileTasks } from '../../src/operations';
import { captureReportsOnce } from '../../src/reports';
import type { RepositoryBinding } from '../../src/runtime';
import { applyEvent, githubRequirementUrl, syncOnce } from '../../src/sync';
import { duplicatesOnce } from '../../src/duplicates';
import { createTestDatabase, type TestDatabase } from './helpers';

const TOKEN = 'm4-test-bridge-token-0123456789';

interface OperationCall {
  kind: 'pause' | 'resume' | 'start' | 'retry';
  ticketId: string;
  body: Record<string, unknown>;
}

interface FakeBridge {
  url: string;
  calls: OperationCall[];
  operationState: 'queued' | 'running' | 'succeeded' | 'failed';
  operationEvidence: Record<string, unknown>;
  operationError: string | null;
  snapshots: Map<string, BridgeTicketSnapshot>;
  events: BridgeEvent[];
  refuse: { kind: OperationCall['kind']; code: string; message: string } | null;
  close: () => Promise<void>;
}

function readBody(request: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(Buffer.from(chunk)));
    request.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (error) {
        reject(error);
      }
    });
    request.on('error', reject);
  });
}

async function startFakeBridge(): Promise<FakeBridge> {
  const state: Omit<FakeBridge, 'url' | 'close'> & { server?: Server } = {
    calls: [],
    operationState: 'succeeded',
    operationEvidence: {},
    operationError: null,
    snapshots: new Map(),
    events: [],
    refuse: null,
  };
  const server = createServer(async (request: IncomingMessage, response: ServerResponse) => {
    const send = (status: number, value: unknown) => {
      response.writeHead(status, { 'content-type': 'application/json' });
      response.end(JSON.stringify(value));
    };
    if (request.headers.authorization !== `Bearer ${TOKEN}`) {
      send(401, { error: { code: 'unauthorized', message: 'machine token required' } });
      return;
    }
    const url = new URL(request.url ?? '/', 'http://127.0.0.1');
    const parts = url.pathname.split('/').filter(Boolean);
    if (
      request.method === 'POST' &&
      parts[0] === 'bridge' &&
      parts[1] === 'v1' &&
      parts[2] === 'tickets'
    ) {
      const ticketId = decodeURIComponent(parts[3] ?? '');
      const kind = parts[4] as OperationCall['kind'] | undefined;
      if (!kind || !['pause', 'resume', 'start', 'retry'].includes(kind)) {
        send(404, { error: { code: 'not_found', message: 'route not found' } });
        return;
      }
      const body = (await readBody(request)) as Record<string, unknown>;
      state.calls.push({ kind, ticketId, body });
      if (state.refuse?.kind === kind) {
        send(409, { error: { code: state.refuse.code, message: state.refuse.message } });
        return;
      }
      send(202, {
        operationId: String(body.operationId ?? ''),
        ticketId,
        kind,
        state: 'queued',
      });
      return;
    }
    if (
      request.method === 'GET' &&
      parts[0] === 'bridge' &&
      parts[1] === 'v1' &&
      parts[2] === 'operations'
    ) {
      const operationId = decodeURIComponent(parts[3] ?? '');
      const ticket = state.calls.at(-1)?.ticketId ?? 'ticket';
      send(200, {
        operationId,
        ticketId: ticket,
        kind: state.calls.at(-1)?.kind ?? 'pause',
        state: state.operationState,
        ...(state.operationError ? { error: state.operationError } : {}),
        evidence: state.operationEvidence,
        updatedAt: new Date().toISOString(),
      });
      return;
    }
    if (
      request.method === 'GET' &&
      parts[0] === 'bridge' &&
      parts[1] === 'v1' &&
      parts[2] === 'tickets'
    ) {
      const ticketId = decodeURIComponent(parts[3] ?? '');
      const snapshot = state.snapshots.get(ticketId);
      if (!snapshot) {
        send(404, { error: { code: 'not_found', message: 'ticket not found' } });
        return;
      }
      send(200, snapshot);
      return;
    }
    if (request.method === 'GET' && url.pathname === '/bridge/v1/events') {
      const after = Number(url.searchParams.get('after') ?? '0');
      const limit = Number(url.searchParams.get('limit') ?? '100');
      const page = state.events.filter((event) => event.seq > after).slice(0, limit);
      send(200, {
        events: page,
        next: page.at(-1)?.seq ?? after,
        hasMore: state.events.some((event) => event.seq > (page.at(-1)?.seq ?? after)),
      });
      return;
    }
    send(404, { error: { code: 'not_found', message: 'route not found' } });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') {
    throw new Error('fake Bridge failed to listen');
  }
  return {
    url: `http://127.0.0.1:${address.port}`,
    get calls() {
      return state.calls;
    },
    get operationState() {
      return state.operationState;
    },
    set operationState(value) {
      state.operationState = value;
    },
    get operationEvidence() {
      return state.operationEvidence;
    },
    set operationEvidence(value) {
      state.operationEvidence = value;
    },
    get operationError() {
      return state.operationError;
    },
    get refuse() {
      return state.refuse;
    },
    set refuse(value) {
      state.refuse = value;
    },
    set operationError(value) {
      state.operationError = value;
    },
    get snapshots() {
      return state.snapshots;
    },
    get events() {
      return state.events;
    },
    set events(value) {
      state.events = value;
    },
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  };
}

interface Seed {
  repository: RepositoryBinding;
  ownerId: string;
  conversationId: string;
  taskId: string;
  ticketId: string;
}

describe('M4 task operations, reconciliation and GitHub import on real PostgreSQL', () => {
  let testDatabase: TestDatabase;
  let fakeBridge: FakeBridge;
  let config: DispatcherConfig;
  let client: RuntimeBridgeClient;
  let seedCounter = 0;

  beforeEach(() => {
    fakeBridge.refuse = null;
  });

  async function seed(options: {
    state: string;
    executionMode?: 'execute' | 'discuss';
    stoppedAt?: Date | null;
    stopEvidence?: Record<string, unknown>;
    planRevision?: number | null;
    source?: 'web' | 'github';
    externalKey?: string | null;
    repository?: RepositoryBinding;
  }): Promise<Seed> {
    seedCounter += 1;
    const suffix = `${process.pid}-${seedCounter}-${Date.now()}`;
    const owner = await testDatabase.pool.query<{ id: string }>(
      `insert into owners (display_name) values ($1) returning id`,
      [`M4 Test Owner ${suffix}`],
    );
    const ownerId = owner.rows[0]!.id;
    const repo = options.repository
      ? { rows: [{ id: options.repository.id, full_name: options.repository.fullName }] }
      : await testDatabase.pool.query<{ id: string; full_name: string }>(
          `insert into repositories (owner_id, full_name, default_branch, status, bridge_url)
           values ($1, $2, 'main', 'active', $3) returning id, full_name`,
          [ownerId, `Bouyer12/m4-ops-${suffix}`, fakeBridge.url],
        );
    const repositoryId = repo.rows[0]!.id;
    const fullName = repo.rows[0]!.full_name;
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations
         (owner_id, repository_id, mode, execution_mode, source, external_key, base_branch, base_commit, model_id, title)
       values ($1, $2, 'work', $3, $4, $5, 'main', $6, 'commandcode/deepseek/deepseek-v4.1-flash', 'M4 task')
       returning id`,
      [
        ownerId,
        repositoryId,
        options.executionMode ?? 'execute',
        options.source ?? 'web',
        options.externalKey ?? null,
        'a'.repeat(40),
      ],
    );
    const conversationId = conversation.rows[0]!.id;
    const ticketId = `ticket-${suffix}`;
    const task = await testDatabase.pool.query<{ id: string }>(
      `insert into task_links
         (conversation_id, repository_id, upstream_ticket_id, upstream_thread_id, state,
          base_branch, base_commit, model_id, execution_mode, plan_revision,
          stopped_at, stop_evidence)
       values ($1, $2, $3, $4, $5, 'main', $6, 'commandcode/deepseek/deepseek-v4.1-flash',
               $7, $8, $9, $10::jsonb)
       returning id`,
      [
        conversationId,
        repositoryId,
        ticketId,
        `workspace:${conversationId}`,
        options.state,
        'a'.repeat(40),
        options.executionMode ?? 'execute',
        options.planRevision ?? null,
        options.stoppedAt ?? null,
        JSON.stringify(options.stopEvidence ?? {}),
      ],
    );
    return {
      ownerId,
      conversationId,
      taskId: task.rows[0]!.id,
      ticketId,
      repository: {
        id: repositoryId,
        fullName,
        defaultBranch: 'main',
        bridgeUrl: fakeBridge.url,
        bridgeCredentialRef: null,
      },
    };
  }

  async function insertOperation(
    seed: Seed,
    kind: 'start' | 'pause' | 'resume' | 'retry',
    clientCommandId: string,
  ): Promise<string> {
    const result = await testDatabase.pool.query<{ id: string }>(
      `insert into task_operations (task_id, repository_id, kind, client_command_id, request_fingerprint)
       values ($1, $2, $3, $4, 'fingerprint') returning id`,
      [seed.taskId, seed.repository.id, kind, clientCommandId],
    );
    return result.rows[0]!.id;
  }

  async function makeOperationsDue(): Promise<void> {
    await testDatabase.pool.query('update task_operations set next_attempt_at = now()');
  }

  async function readTask(taskId: string) {
    const result = await testDatabase.pool.query(
      `select state, execution_mode as "executionMode", stopped_at as "stoppedAt",
              stop_evidence as "stopEvidence", runtime_lease_id as "runtimeLeaseId",
              plan_revision as "planRevision"
         from task_links where id = $1`,
      [taskId],
    );
    return result.rows[0] as Record<string, unknown>;
  }

  beforeAll(async () => {
    testDatabase = await createTestDatabase();
    fakeBridge = await startFakeBridge();
    config = loadDispatcherConfig({
      NODE_ENV: 'test',
      DATABASE_URL: testDatabase.url,
      BRIDGE_TOKEN: TOKEN,
      OPERATION_RETRY_CAP_MS: '1000',
    });
    client = new RuntimeBridgeClient({ baseUrl: fakeBridge.url, token: TOKEN });
  }, 60_000);

  afterAll(async () => {
    await fakeBridge?.close();
    await testDatabase?.drop();
  });

  it('sends a pause, then shows stopped only after the runtime supplies stop evidence', async () => {
    const fixture = await seed({ state: 'stopping' });
    const operationId = await insertOperation(fixture, 'pause', `cmd-${Date.now()}`);
    const logger = createConsoleLogger('test');
    fakeBridge.operationEvidence = {
      ticketId: fixture.ticketId,
      assignmentId: 'assignment-1',
      sessionId: `worker-${fixture.taskId}`,
      harnessPid: 4321,
      cgroup: '0::/test.slice',
      agentStatus: 'idle',
      jobsBefore: [{ id: 'bash-1', name: 'npm test' }],
      jobsAfter: [],
      subagentsAfter: 0,
      cancelledTurn: true,
      stoppedAt: new Date().toISOString(),
    };

    expect(
      await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger),
    ).toBe(true);
    expect(fakeBridge.calls.at(-1)).toMatchObject({ kind: 'pause', ticketId: fixture.ticketId });
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'stopping' });

    await makeOperationsDue();
    expect(
      await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger),
    ).toBe(true);
    const task = await readTask(fixture.taskId);
    expect(task.state).toBe('stopped');
    expect(task.stoppedAt).toBeInstanceOf(Date);
    expect(task.stopEvidence).toMatchObject({ harnessPid: 4321, agentStatus: 'idle' });
    const operation = await testDatabase.pool.query(
      `select state, bridge_operation_id from task_operations where id = $1`,
      [operationId],
    );
    expect(operation.rows[0]).toMatchObject({
      state: 'succeeded',
      bridge_operation_id: operationId,
    });
  }, 30_000);

  it('does not report stopped when the runtime refuses or fails the pause', async () => {
    const fixture = await seed({ state: 'stopping' });
    await insertOperation(fixture, 'pause', `cmd-fail-${Date.now()}`);
    fakeBridge.operationState = 'failed';
    fakeBridge.operationError = 'the worker did not become idle';
    const logger = createConsoleLogger('test');

    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    await makeOperationsDue();
    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    const task = await readTask(fixture.taskId);
    expect(task.state).toBe('blocked');
    expect(task.stoppedAt).toBeNull();
    const operation = await testDatabase.pool.query(
      `select state, error from task_operations where task_id = $1`,
      [fixture.taskId],
    );
    expect(operation.rows[0]).toMatchObject({ state: 'failed' });
    expect(String(operation.rows[0]?.error)).toContain('idle');
    fakeBridge.operationState = 'succeeded';
    fakeBridge.operationError = null;
  }, 30_000);

  it('keeps the execution slot when a pause has no proven stop and blocks a second executor', async () => {
    const fixture = await seed({ state: 'stopping' });
    const lease = await testDatabase.pool.query<{ id: string }>(
      `insert into runtime_leases (repository_id, holder_id, expires_at)
       values ($1, 'dispatcher:test', now() + interval '10 minutes') returning id`,
      [fixture.repository.id],
    );
    const leaseId = lease.rows[0]!.id;
    await testDatabase.pool.query(`update task_links set runtime_lease_id = $2 where id = $1`, [
      fixture.taskId,
      leaseId,
    ]);
    await insertOperation(fixture, 'pause', `cmd-unproven-${Date.now()}`);
    fakeBridge.operationState = 'succeeded';
    fakeBridge.operationEvidence = {
      ticketId: fixture.ticketId,
      agentStatus: 'running',
      jobsAfter: [{ id: 'bash-1', name: 'sleep 300' }],
      subagentsAfter: 0,
      stoppedAt: new Date().toISOString(),
    };
    const logger = createConsoleLogger('test');

    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    await makeOperationsDue();
    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);

    const task = await readTask(fixture.taskId);
    expect(task.state).toBe('blocked');
    expect(task.runtimeLeaseId).toBe(leaseId);
    const held = await testDatabase.pool.query<{ state: string; recovery_state: string }>(
      `select state, recovery_state from runtime_leases where id = $1`,
      [leaseId],
    );
    expect(held.rows[0]).toMatchObject({ state: 'active', recovery_state: 'required' });

    // A surviving old execution (no proof) must keep the second executor out.
    const second = await seed({ state: 'waiting_input', repository: fixture.repository });
    const refused = await acquireExecutionLease(
      testDatabase.pool,
      fixture.repository.id,
      { taskId: second.taskId },
      60_000,
    );
    expect(refused.lease).toBeNull();
    expect(refused.refused).toBe('stop_proof_required');

    fakeBridge.operationState = 'succeeded';
    fakeBridge.operationEvidence = {};
  }, 30_000);

  it('settles the pause operation from the paused event before the operation poll', async () => {
    const fixture = await seed({ state: 'stopping' });
    const operationId = await insertOperation(fixture, 'pause', `cmd-event-${Date.now()}`);
    await testDatabase.pool.query(
      `update task_operations set state = 'running', bridge_operation_id = $2 where id = $1`,
      [operationId, operationId],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `ticket-paused:${fixture.ticketId}:settled`,
        type: 'ticket.paused',
        at: '2026-11-04T03:00:00.000Z',
        data: {
          ticketId: fixture.ticketId,
          operationId,
          at: '2026-11-04T02:59:59.000Z',
          stoppedAt: '2026-11-04T03:00:00.000Z',
          evidence: { agentStatus: 'idle', harnessPid: 1, jobsAfter: [], subagentsAfter: 0 },
        },
      },
    ];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    const operation = await testDatabase.pool.query(
      `select state from task_operations where id = $1`,
      [operationId],
    );
    expect(operation.rows[0]?.state).toBe('succeeded');
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'stopped' });
    // No active operation remains, so a resume command can be inserted.
    await testDatabase.pool.query(
      `insert into task_operations (task_id, repository_id, kind, client_command_id, request_fingerprint)
       values ($1, $2, 'resume', $3, 'fingerprint')`,
      [fixture.taskId, fixture.repository.id, `cmd-resume-${Date.now()}`],
    );
  }, 30_000);

  it('does not fail a pause operation on the first stopping event before its settled evidence', async () => {
    const fixture = await seed({ state: 'stopping' });
    const operationId = await insertOperation(fixture, 'pause', `cmd-stopping-${Date.now()}`);
    await testDatabase.pool.query(
      `update task_operations set state = 'running', bridge_operation_id = $2 where id = $1`,
      [operationId, operationId],
    );
    const stopping: BridgeEvent = {
      seq: 1,
      id: `ticket-paused:${fixture.ticketId}:stopping`,
      type: 'ticket.paused',
      at: '2026-11-04T03:00:10.000Z',
      data: { ticketId: fixture.ticketId, operationId, at: '2026-11-04T03:00:09.000Z' },
    };
    fakeBridge.events = [stopping];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    const during = await testDatabase.pool.query(
      `select state from task_operations where id = $1`,
      [operationId],
    );
    expect(during.rows[0]?.state).toBe('running');
    fakeBridge.events = [
      stopping,
      {
        seq: 2,
        id: `ticket-paused:${fixture.ticketId}:settled2`,
        type: 'ticket.paused',
        at: '2026-11-04T03:00:11.000Z',
        data: {
          ticketId: fixture.ticketId,
          operationId,
          at: '2026-11-04T03:00:09.000Z',
          stoppedAt: '2026-11-04T03:00:10.500Z',
          evidence: {
            agentStatus: 'idle',
            harnessPid: 1,
            jobsAfter: [],
            subagentsAfter: 0,
            lingeringPids: [],
          },
        },
      },
    ];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    const operation = await testDatabase.pool.query(
      `select state from task_operations where id = $1`,
      [operationId],
    );
    expect(operation.rows[0]?.state).toBe('succeeded');
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'stopped' });
  }, 30_000);

  it('blocks a paused event whose evidence still shows a background job', async () => {
    const fixture = await seed({ state: 'stopping' });
    const operationId = await insertOperation(fixture, 'pause', `cmd-busy-${Date.now()}`);
    await testDatabase.pool.query(
      `update task_operations set state = 'running', bridge_operation_id = $2 where id = $1`,
      [operationId, operationId],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `ticket-paused:${fixture.ticketId}:busy`,
        type: 'ticket.paused',
        at: '2026-11-04T03:01:00.000Z',
        data: {
          ticketId: fixture.ticketId,
          operationId,
          stoppedAt: '2026-11-04T03:01:00.000Z',
          evidence: {
            agentStatus: 'idle',
            harnessPid: 1,
            jobsAfter: [{ id: 'bash-1', name: 'sleep 300' }],
            subagentsAfter: 0,
            lingeringPids: [4242],
          },
        },
      },
    ];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    const task = await readTask(fixture.taskId);
    expect(task.state).toBe('blocked');
    expect(task.stoppedAt).toBeNull();
    expect(task.stopEvidence).toMatchObject({ problem: expect.stringContaining('background job') });
    const operation = await testDatabase.pool.query(
      `select state, error from task_operations where id = $1`,
      [operationId],
    );
    expect(operation.rows[0]?.state).toBe('failed');
    expect(String(operation.rows[0]?.error)).toContain('background job');
  }, 30_000);

  it('does not show stopped when a succeeded pause has no stop evidence', async () => {
    const fixture = await seed({ state: 'stopping' });
    await insertOperation(fixture, 'pause', `cmd-no-evidence-${Date.now()}`);
    fakeBridge.operationState = 'succeeded';
    fakeBridge.operationError = null;
    fakeBridge.operationEvidence = {};
    const logger = createConsoleLogger('test');

    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    await makeOperationsDue();
    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    const task = await readTask(fixture.taskId);
    expect(task.state).toBe('blocked');
    expect(task.stoppedAt).toBeNull();
    const operation = await testDatabase.pool.query(
      `select state, error from task_operations where task_id = $1`,
      [fixture.taskId],
    );
    expect(operation.rows[0]?.state).toBe('failed');
    expect(String(operation.rows[0]?.error)).toContain('did not prove');
  }, 30_000);

  it('starts a discussion into execution without creating a new task', async () => {
    const fixture = await seed({
      state: 'awaiting_review',
      executionMode: 'discuss',
      planRevision: 2,
    });
    await insertOperation(fixture, 'start', `cmd-start-${Date.now()}`);
    const logger = createConsoleLogger('test');

    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    expect(fakeBridge.calls.at(-1)).toMatchObject({
      kind: 'start',
      ticketId: fixture.ticketId,
      body: { planRevision: 2 },
    });
    await makeOperationsDue();
    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    const task = await readTask(fixture.taskId);
    expect(task).toMatchObject({ state: 'queued', executionMode: 'execute', planRevision: 2 });
    const conversation = await testDatabase.pool.query(
      `select execution_mode as "executionMode", plan_revision as "planRevision"
         from conversations where id = $1`,
      [fixture.conversationId],
    );
    expect(conversation.rows[0]).toMatchObject({ executionMode: 'execute', planRevision: 2 });
    const count = await testDatabase.pool.query(
      `select count(*)::int as count from task_links where conversation_id = $1`,
      [fixture.conversationId],
    );
    expect(count.rows[0]?.count).toBe(1);
  }, 30_000);

  it('blocks a resume the runtime refuses because the worktree is missing', async () => {
    const fixture = await seed({
      state: 'stopped',
      stoppedAt: new Date(),
      stopEvidence: { agentStatus: 'idle', jobsAfter: [], subagentsAfter: 0 },
    });
    fakeBridge.snapshots.set(fixture.ticketId, {
      ticketId: fixture.ticketId,
      title: 'M4 task',
      state: 'stopped',
      mode: 'execute',
      paused: {
        at: new Date().toISOString(),
        by: 'human',
        stoppedAt: new Date().toISOString(),
        evidence: { agentStatus: 'idle', jobsAfter: [], subagentsAfter: 0 },
      },
      execution: { name: 'gone', directory: '/nonexistent/m4-worktree', assignment: 'Fix it' },
    });
    fakeBridge.refuse = {
      kind: 'resume',
      code: 'worktreeMissing',
      message: 'The preserved worktree is missing',
    };
    await insertOperation(fixture, 'resume', `cmd-refuse-worktree-${Date.now()}`);
    const logger = createConsoleLogger('test');
    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    const task = await readTask(fixture.taskId);
    expect(task.state).toBe('blocked');
    expect(task.stopEvidence).toMatchObject({ error: expect.stringContaining('worktree') });
  }, 30_000);

  it('does not accept a snapshot stopped state when its pause evidence is incomplete', async () => {
    const fixture = await seed({
      state: 'stopped',
      stoppedAt: new Date(),
      stopEvidence: { agentStatus: 'idle', jobsAfter: [] },
    });
    fakeBridge.snapshots.set(fixture.ticketId, {
      ticketId: fixture.ticketId,
      title: 'M4 task',
      state: 'stopped',
      mode: 'execute',
      paused: {
        at: new Date().toISOString(),
        by: 'human',
        stoppedAt: new Date().toISOString(),
        evidence: {
          agentStatus: 'running',
          jobsAfter: [{ id: 'bash-1', name: 'sleep 300' }],
          subagentsAfter: 0,
        },
      },
      execution: { name: 'fix', directory: '/tmp/m4-worktree', assignment: 'Fix it' },
    });
    fakeBridge.refuse = { kind: 'resume', code: 'bridge_error', message: 'refused' };
    await insertOperation(fixture, 'resume', `cmd-snapshot-evidence-${Date.now()}`);
    await operationsOnce(
      testDatabase.pool,
      config,
      fixture.repository,
      client,
      createConsoleLogger('test'),
    );
    const task = await readTask(fixture.taskId);
    expect(task.state).toBe('blocked');
    expect(task.stoppedAt).toBeNull();
  }, 30_000);

  it('marks a task closed_unmerged when the runtime refuses to resume a closed PR', async () => {
    const fixture = await seed({
      state: 'stopped',
      stoppedAt: new Date(),
      stopEvidence: { agentStatus: 'idle', jobsAfter: [], subagentsAfter: 0 },
    });
    fakeBridge.snapshots.set(fixture.ticketId, {
      ticketId: fixture.ticketId,
      title: 'M4 task',
      state: 'stopped',
      mode: 'execute',
      paused: {
        at: new Date().toISOString(),
        by: 'human',
        stoppedAt: new Date().toISOString(),
        evidence: { agentStatus: 'idle', jobsAfter: [], subagentsAfter: 0 },
      },
      execution: { name: 'fix', directory: '/tmp/m4-worktree', assignment: 'Fix it' },
    });
    fakeBridge.refuse = {
      kind: 'resume',
      code: 'prClosedUnmerged',
      message: 'The pull request was closed unmerged',
    };
    await insertOperation(fixture, 'resume', `cmd-refuse-pr-${Date.now()}`);
    const logger = createConsoleLogger('test');
    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    expect((await readTask(fixture.taskId)).state).toBe('closed_unmerged');
  }, 30_000);

  it('re-acquires a runtime lease and verifies the preserved worktree before resume', async () => {
    const fixture = await seed({
      state: 'stopped',
      stoppedAt: new Date(),
      stopEvidence: { agentStatus: 'idle', harnessPid: 99 },
    });
    fakeBridge.snapshots.set(fixture.ticketId, {
      ticketId: fixture.ticketId,
      title: 'M4 task',
      state: 'stopped',
      mode: 'execute',
      paused: {
        at: new Date().toISOString(),
        by: 'human',
        stoppedAt: new Date().toISOString(),
        evidence: { agentStatus: 'idle', harnessPid: 99 },
      },
      execution: { name: 'm4-task', directory: '/tmp/m4-task', assignment: '' },
    });
    await insertOperation(fixture, 'resume', `cmd-resume-${Date.now()}`);
    const logger = createConsoleLogger('test');

    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    const resumeCall = fakeBridge.calls.at(-1);
    expect(resumeCall).toMatchObject({ kind: 'resume', ticketId: fixture.ticketId });
    expect(resumeCall?.body).toMatchObject({
      admission: { leaseId: expect.any(String), fencingToken: expect.any(String) },
    });
    await makeOperationsDue();
    await operationsOnce(testDatabase.pool, config, fixture.repository, client, logger);
    const task = await readTask(fixture.taskId);
    expect(task).toMatchObject({ state: 'queued' });
    expect(task.stoppedAt).toBeNull();
    expect(task.runtimeLeaseId).toEqual(expect.any(String));

    await maintainLeases(testDatabase.pool, config, fixture.repository, client, logger);
    const lease = await testDatabase.pool.query(
      `select state, expires_at from runtime_leases where id = $1`,
      [task.runtimeLeaseId],
    );
    expect(lease.rows[0]?.state).toBe('active');
  }, 30_000);

  it('releases the execution slot for human-waiting and review states but not a running task', async () => {
    const waiting = await seed({ state: 'waiting_input' });
    const running = await seed({ state: 'running' });
    const leaseFor = async (seed: Seed, taskId: string) => {
      const lease = await testDatabase.pool.query<{ id: string }>(
        `insert into runtime_leases (repository_id, holder_id, expires_at)
         values ($1, $2, now() + interval '10 minutes') returning id`,
        [seed.repository.id, `holder-${taskId}`],
      );
      const leaseId = lease.rows[0]!.id;
      await testDatabase.pool.query('update task_links set runtime_lease_id = $2 where id = $1', [
        taskId,
        leaseId,
      ]);
      return leaseId;
    };
    const waitingLease = await leaseFor(waiting, waiting.taskId);
    const runningLease = await leaseFor(running, running.taskId);
    const logger = createConsoleLogger('test');

    await maintainLeases(testDatabase.pool, config, waiting.repository, client, logger);

    const states = await testDatabase.pool.query<{ id: string; state: string }>(
      'select id, state from runtime_leases where id = any($1::uuid[])',
      [[waitingLease, runningLease]],
    );
    const byId = new Map(states.rows.map((row) => [row.id, row.state]));
    expect(byId.get(waitingLease)).toBe('released');
    expect(byId.get(runningLease)).toBe('active');
  }, 30_000);

  it('keeps a resume queued when a different task holds the repository lease', async () => {
    const first = await seed({ state: 'running' });
    await insertOperation(first, 'pause', `cmd-hold-${Date.now()}`);
    await testDatabase.pool.query(
      `insert into runtime_leases (repository_id, holder_id, expires_at)
       values ($1, 'another-task', now() + interval '10 minutes')`,
      [first.repository.id],
    );
    const second = await seed({
      state: 'stopped',
      stoppedAt: new Date(),
      repository: first.repository,
    });
    await insertOperation(second, 'resume', `cmd-wait-${Date.now()}`);
    const logger = createConsoleLogger('test');

    await operationsOnce(testDatabase.pool, config, first.repository, client, logger);
    const resumeCalls = fakeBridge.calls.filter(
      (call) => call.kind === 'resume' && call.ticketId === second.ticketId,
    );
    expect(resumeCalls).toHaveLength(0);
    const operation = await testDatabase.pool.query(
      `select state, evidence from task_operations
        where task_id = $1 and kind = 'resume'`,
      [second.taskId],
    );
    expect(operation.rows[0]?.state).toBe('queued');
  }, 30_000);

  it('imports a GitHub ticket into one Work conversation and does not duplicate a replay', async () => {
    const owner = await testDatabase.pool.query<{ id: string }>(
      `insert into owners (display_name) values ('M4 GitHub Owner') returning id`,
    );
    const ownerId = owner.rows[0]!.id;
    const repo = await testDatabase.pool.query<{ id: string; full_name: string }>(
      `insert into repositories (owner_id, full_name, default_branch, status, bridge_url)
       values ($1, $2, 'main', 'active', $3) returning id, full_name`,
      [ownerId, `Bouyer12/m4-import-${Date.now()}`, fakeBridge.url],
    );
    const repository: RepositoryBinding = {
      id: repo.rows[0]!.id,
      fullName: repo.rows[0]!.full_name,
      defaultBranch: 'main',
      bridgeUrl: fakeBridge.url,
      bridgeCredentialRef: null,
    };
    const importedEvent = (seq: number, id: string, state = 'running'): BridgeEvent => ({
      seq,
      id,
      type: 'ticket.imported',
      at: '2026-11-04T00:00:00.000Z',
      data: {
        ticketId: 'github-ticket-42',
        source: 'github',
        externalId: 'github:42',
        number: 42,
        isPr: true,
        url: 'https://github.com/Bouyer12/m4-import/pull/42',
        title: 'Fix the GitHub issue',
        state,
        triggerId: 'github-comment:42:1',
        text: 'Please fix this margin',
        base: {
          branch: 'main',
          commit: 'b'.repeat(40),
          model: 'commandcode/deepseek/deepseek-v4.1-flash',
        },
        at: '2026-11-04T00:00:00.000Z',
      },
    });
    fakeBridge.events = [importedEvent(1, 'ticket-imported:github-ticket-42')];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, repository, client, logger)).toBe(true);

    const conversation = await testDatabase.pool.query(
      `select id, source, external_key as "externalKey", external_url as "externalUrl"
         from conversations where repository_id = $1`,
      [repository.id],
    );
    expect(conversation.rows).toHaveLength(1);
    expect(conversation.rows[0]).toMatchObject({
      source: 'github',
      externalKey: 'github:42',
      externalUrl: 'https://github.com/Bouyer12/m4-import/pull/42',
    });
    const task = await testDatabase.pool.query(
      `select upstream_ticket_id as "ticketId", upstream_thread_id as "thread", state
         from task_links where repository_id = $1`,
      [repository.id],
    );
    expect(task.rows[0]).toMatchObject({
      ticketId: 'github-ticket-42',
      thread: 'github:42',
      state: 'running',
    });
    const messages = await testDatabase.pool.query(
      `select body, source from messages where conversation_id = $1`,
      [conversation.rows[0]!.id],
    );
    expect(messages.rows[0]).toMatchObject({ body: 'Please fix this margin', source: 'github' });

    // A second import with a new event id but the same external identity must
    // reuse the conversation and task.
    fakeBridge.events = [
      importedEvent(1, 'ticket-imported:github-ticket-42'),
      importedEvent(2, 'ticket-imported:github-ticket-42:again', 'awaiting_review'),
    ];
    expect(await syncOnce(testDatabase.pool, repository, client, logger)).toBe(true);
    const again = await testDatabase.pool.query(
      `select count(*)::int as conversations from conversations where repository_id = $1`,
      [repository.id],
    );
    expect(again.rows[0]?.conversations).toBe(1);
    const tasks = await testDatabase.pool.query(
      `select count(*)::int as tasks from task_links where repository_id = $1`,
      [repository.id],
    );
    expect(tasks.rows[0]?.tasks).toBe(1);
    const state = await testDatabase.pool.query(
      `select state from task_links where repository_id = $1`,
      [repository.id],
    );
    expect(state.rows[0]?.state).toBe('awaiting_review');
  }, 30_000);

  it('claims the Web conversation for an import event on an existing workspace task', async () => {
    const fixture = await seed({ state: 'running', source: 'web' });
    const url = `https://github.com/${fixture.repository.fullName}/issues/42`;
    await testDatabase.pool.query(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', $2)`,
      [fixture.conversationId, `Web-first target ${url}`],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `ticket-imported:${fixture.ticketId}:github:42`,
        type: 'ticket.imported',
        at: '2026-11-04T00:00:00.000Z',
        data: {
          ticketId: fixture.ticketId,
          source: 'github',
          externalId: 'github:42',
          number: 42,
          isPr: false,
          url,
          title: 'Web-first target',
          state: 'running',
          triggerId: 'github-comment:42:1',
          text: 'Web-first target',
          base: {
            branch: 'main',
            commit: 'b'.repeat(40),
            model: 'commandcode/deepseek/deepseek-v4.1-flash',
          },
          at: '2026-11-04T00:00:00.000Z',
        },
      },
    ];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    const conversation = await testDatabase.pool.query(
      `select source, external_key as "externalKey", external_url as "externalUrl"
         from conversations where id = $1`,
      [fixture.conversationId],
    );
    expect(conversation.rows[0]).toMatchObject({
      source: 'github',
      externalKey: 'github:42',
      externalUrl: url,
    });
    const tasks = await testDatabase.pool.query(
      `select upstream_ticket_id as "ticketId", state from task_links where repository_id = $1`,
      [fixture.repository.id],
    );
    expect(tasks.rows).toHaveLength(1);
    expect(tasks.rows[0]).toMatchObject({ ticketId: fixture.ticketId, state: 'running' });
  }, 30_000);

  it('closes only the pending admission tied to an ignored decision', async () => {
    const pending = await seed({ state: 'pending_admission' });
    const running = await seed({ state: 'running' });
    const pendingMessage = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'probe') returning id`,
      [pending.conversationId],
    );
    const runningMessage = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'probe') returning id`,
      [running.conversationId],
    );
    const event = (seq: number, fixture: typeof pending, messageId: string): BridgeEvent => ({
      seq,
      id: `decision-ignored:${fixture.ticketId}`,
      type: 'decision.ignored',
      at: '2026-11-04T00:00:00.000Z',
      data: {
        decisionId: `decision-${fixture.ticketId}`,
        triggerIds: [`workspace:${messageId}`],
        conversationId: fixture.conversationId,
        messageId,
        at: '2026-11-04T00:00:00.000Z',
      },
    });
    fakeBridge.events = [
      event(1, pending, pendingMessage.rows[0]!.id),
      event(2, running, runningMessage.rows[0]!.id),
    ];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, pending.repository, client, logger)).toBe(true);
    expect(await readTask(pending.taskId)).toMatchObject({ state: 'not_accepted' });
    expect(await readTask(running.taskId)).toMatchObject({ state: 'running' });
    const version = (await readTask(pending.taskId)).version;
    fakeBridge.events = [event(1, pending, pendingMessage.rows[0]!.id)];
    await syncOnce(testDatabase.pool, pending.repository, client, logger);
    expect((await readTask(pending.taskId)).version).toBe(version);
  }, 30_000);

  it('resolves a merged conversation alias before closing the ignored admission', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'old') returning id`,
      [fixture.conversationId],
    );
    const alias = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations
         (owner_id, repository_id, mode, execution_mode, source, base_branch, base_commit, model_id, title, merged_into_conversation_id)
       values ($1, $2, 'work', 'execute', 'web', 'main', $3, 'commandcode/deepseek/deepseek-v4.1-flash', 'alias', $4)
       returning id`,
      [fixture.ownerId, fixture.repository.id, 'a'.repeat(40), fixture.conversationId],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `decision-ignored:${fixture.ticketId}:alias`,
        type: 'decision.ignored',
        at: '2026-11-04T00:00:00.000Z',
        data: {
          decisionId: `decision-${fixture.ticketId}`,
          conversationId: alias.rows[0]!.id,
          messageId: message.rows[0]!.id,
          at: '2026-11-04T00:00:00.000Z',
        },
      },
    ];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'not_accepted' });
  }, 30_000);

  it('does not let an import notice block the ignored admission', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const url = `https://github.com/${fixture.repository.fullName}/issues/42`;
    const user = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body, source)
       values ($1, 'inbound', 'user', $2, 'web') returning id`,
      [fixture.conversationId, `target ${url}`],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `ticket-imported:${fixture.ticketId}:duplicate`,
        type: 'ticket.imported',
        at: '2026-11-04T00:00:00.000Z',
        data: {
          ticketId: `second-ticket-${fixture.ticketId}`,
          source: 'github',
          externalId: 'github:42',
          number: 42,
          isPr: false,
          url,
          title: 'second ticket',
          state: 'running',
          triggerId: 'github-comment:42:1',
          text: 'second entry',
          base: { branch: 'main', commit: 'b'.repeat(40) },
          at: '2026-11-04T00:00:00.000Z',
        },
      },
      {
        seq: 2,
        id: `decision-ignored:${fixture.ticketId}:notice`,
        type: 'decision.ignored',
        at: '2026-11-04T00:00:00.000Z',
        data: {
          decisionId: `decision-${fixture.ticketId}`,
          conversationId: fixture.conversationId,
          messageId: user.rows[0]!.id,
          messageIds: [user.rows[0]!.id],
          at: '2026-11-04T00:00:00.000Z',
        },
      },
    ];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'not_accepted' });
    const notices = await testDatabase.pool.query(
      `select count(*)::int as count from messages where conversation_id = $1 and kind = 'notice'`,
      [fixture.conversationId],
    );
    expect(notices.rows[0]?.count).toBe(1);
  }, 30_000);

  it('closes a batched ignore decision covering every pending message', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const first = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body, created_at)
       values ($1, 'inbound', 'user', 'batch one', now() - interval '1 minute') returning id`,
      [fixture.conversationId],
    );
    const second = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'batch two') returning id`,
      [fixture.conversationId],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `decision-ignored:${fixture.ticketId}:batch`,
        type: 'decision.ignored',
        at: '2026-11-04T00:00:00.000Z',
        data: {
          decisionId: `decision-${fixture.ticketId}`,
          conversationId: fixture.conversationId,
          messageId: first.rows[0]!.id,
          messageIds: [first.rows[0]!.id, second.rows[0]!.id],
          triggerIds: [`workspace:${first.rows[0]!.id}`, `workspace:${second.rows[0]!.id}`],
          at: '2026-11-04T00:00:00.000Z',
        },
      },
    ];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'not_accepted' });
  }, 30_000);

  it('keeps a batch pending when an uncovered newer input exists', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const first = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body, created_at)
       values ($1, 'inbound', 'user', 'batch one', now() - interval '1 minute') returning id`,
      [fixture.conversationId],
    );
    const second = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body, created_at)
       values ($1, 'inbound', 'user', 'batch two', now() - interval '30 seconds') returning id`,
      [fixture.conversationId],
    );
    const third = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'new input') returning id`,
      [fixture.conversationId],
    );
    const event = (seq: number, messageIds: string[], id: string): BridgeEvent => ({
      seq,
      id,
      type: 'decision.ignored',
      at: '2026-11-04T00:00:00.000Z',
      data: {
        decisionId: `decision-${fixture.ticketId}`,
        conversationId: fixture.conversationId,
        messageId: messageIds[0]!,
        messageIds,
        triggerIds: messageIds.map((messageId) => `workspace:${messageId}`),
        at: '2026-11-04T00:00:00.000Z',
      },
    });
    fakeBridge.events = [
      event(
        1,
        [first.rows[0]!.id, second.rows[0]!.id],
        `decision-ignored:${fixture.ticketId}:batch-1`,
      ),
    ];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'pending_admission' });
    fakeBridge.events = [
      event(
        2,
        [first.rows[0]!.id, second.rows[0]!.id, third.rows[0]!.id],
        `decision-ignored:${fixture.ticketId}:batch-2`,
      ),
    ];
    await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test'));
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'not_accepted' });
  }, 30_000);

  it('closes its own admission while that message delivery is still delivering', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'late ack probe') returning id`,
      [fixture.conversationId],
    );
    await testDatabase.pool.query(
      `insert into deliveries (message_id, state) values ($1, 'delivering')`,
      [message.rows[0]!.id],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `decision-ignored:${fixture.ticketId}:late-ack`,
        type: 'decision.ignored',
        at: '2026-11-04T00:00:00.000Z',
        data: {
          decisionId: `decision-${fixture.ticketId}`,
          conversationId: fixture.conversationId,
          messageId: message.rows[0]!.id,
          at: '2026-11-04T00:00:00.000Z',
        },
      },
    ];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'not_accepted' });
    await testDatabase.pool.query(`update deliveries set state='delivered' where message_id=$1`, [
      message.rows[0]!.id,
    ]);
    const version = (await readTask(fixture.taskId)).version;
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(false);
    expect((await readTask(fixture.taskId)).version).toBe(version);
  }, 30_000);

  it('serializes an API message acceptance before an ignored decision', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const old = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'old') returning id`,
      [fixture.conversationId],
    );
    const event: BridgeEvent = {
      seq: 1,
      id: `decision-ignored:${fixture.ticketId}:race`,
      type: 'decision.ignored',
      at: '2026-11-04T00:00:00.000Z',
      data: {
        decisionId: `decision-${fixture.ticketId}`,
        conversationId: fixture.conversationId,
        messageId: old.rows[0]!.id,
        at: '2026-11-04T00:00:00.000Z',
      },
    };
    const api = await testDatabase.pool.connect();
    const sync = await testDatabase.pool.connect();
    try {
      await api.query('begin');
      await api.query('select id from conversations where id = $1 for update', [
        fixture.conversationId,
      ]);
      const task = await api.query('select state from task_links where id = $1', [fixture.taskId]);
      expect(task.rows[0]).toMatchObject({ state: 'pending_admission' });
      const created = await api.query<{ id: string }>(
        `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'accepted while ignore waits') returning id`,
        [fixture.conversationId],
      );
      await api.query(`insert into deliveries (message_id, state) values ($1, 'pending')`, [
        created.rows[0]!.id,
      ]);
      const applying = (async () => {
        await sync.query('begin');
        await applyEvent(sync, fixture.repository, event, createConsoleLogger('test'));
        await sync.query('commit');
      })();
      await new Promise((resolve) => setTimeout(resolve, 100));
      const blocked = await testDatabase.pool.query('select state from task_links where id = $1', [
        fixture.taskId,
      ]);
      expect(blocked.rows[0]).toMatchObject({ state: 'pending_admission' });
      await api.query('commit');
      await applying;
      expect(await readTask(fixture.taskId)).toMatchObject({ state: 'pending_admission' });
      const delivery = await testDatabase.pool.query(
        'select state from deliveries where message_id = $1',
        [created.rows[0]!.id],
      );
      expect(delivery.rows[0]).toMatchObject({ state: 'pending' });
    } finally {
      await api.query('rollback').catch(() => undefined);
      await sync.query('rollback').catch(() => undefined);
      api.release();
      sync.release();
    }
  }, 30_000);

  it('rejects a later API acceptance after an ignored decision commits first', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const old = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'old') returning id`,
      [fixture.conversationId],
    );
    const event: BridgeEvent = {
      seq: 1,
      id: `decision-ignored:${fixture.ticketId}:race-reverse`,
      type: 'decision.ignored',
      at: '2026-11-04T00:00:00.000Z',
      data: {
        decisionId: `decision-${fixture.ticketId}`,
        conversationId: fixture.conversationId,
        messageId: old.rows[0]!.id,
        at: '2026-11-04T00:00:00.000Z',
      },
    };
    const api = await testDatabase.pool.connect();
    const sync = await testDatabase.pool.connect();
    try {
      await sync.query('begin');
      await applyEvent(sync, fixture.repository, event, createConsoleLogger('test'));
      let locked = false;
      const waiting = (async () => {
        await api.query('begin');
        await api.query('select id from conversations where id = $1 for update', [
          fixture.conversationId,
        ]);
        locked = true;
        const task = await api.query('select state from task_links where id = $1', [
          fixture.taskId,
        ]);
        expect(task.rows[0]).toMatchObject({ state: 'not_accepted' });
        await api.query('rollback');
      })();
      await new Promise((resolve) => setTimeout(resolve, 100));
      expect(locked).toBe(false);
      await sync.query('commit');
      await waiting;
      expect(await readTask(fixture.taskId)).toMatchObject({ state: 'not_accepted' });
      const messages = await testDatabase.pool.query(
        'select count(*)::int as count from messages where conversation_id = $1',
        [fixture.conversationId],
      );
      expect(messages.rows[0]?.count).toBe(1);
    } finally {
      await api.query('rollback').catch(() => undefined);
      await sync.query('rollback').catch(() => undefined);
      api.release();
      sync.release();
    }
  }, 30_000);

  it('does not close a newer accepted admission when an older message is ignored', async () => {
    const fixture = await seed({ state: 'pending_admission' });
    const older = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body, created_at)
       values ($1, 'inbound', 'user', 'older probe', now() - interval '1 minute') returning id`,
      [fixture.conversationId],
    );
    const newer = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, body) values ($1, 'inbound', 'user', 'newer probe') returning id`,
      [fixture.conversationId],
    );
    await testDatabase.pool.query(
      `insert into deliveries (message_id, state) values ($1, 'pending')`,
      [newer.rows[0]!.id],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: `decision-ignored:${fixture.ticketId}:older`,
        type: 'decision.ignored',
        at: '2026-11-04T00:00:00.000Z',
        data: {
          decisionId: `decision-${fixture.ticketId}`,
          triggerIds: [`workspace:${older.rows[0]!.id}`],
          conversationId: fixture.conversationId,
          messageId: older.rows[0]!.id,
          at: '2026-11-04T00:00:00.000Z',
        },
      },
    ];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'pending_admission' });
    const delivery = await testDatabase.pool.query(
      `select state from deliveries where message_id = $1`,
      [newer.rows[0]!.id],
    );
    expect(delivery.rows[0]).toMatchObject({ state: 'pending' });
  }, 30_000);

  it('keeps one task when the same requirement arrives with a different GitHub ticket', async () => {
    const fixture = await seed({
      state: 'running',
      source: 'github',
      externalKey: 'github:42',
    });
    const otherTicket = (seq: number, ticketId: string, state: string): BridgeEvent => ({
      seq,
      id: `ticket-imported:${ticketId}`,
      type: 'ticket.imported',
      at: '2026-11-04T03:10:00.000Z',
      data: {
        ticketId,
        source: 'github',
        externalId: 'github:42',
        number: 42,
        isPr: true,
        url: 'https://github.com/Bouyer12/m4-import/pull/42',
        title: 'Fix the GitHub issue',
        state,
        triggerId: `github-comment:42:${seq}`,
        text: 'Another entry for the same requirement',
        base: { branch: 'main', commit: 'b'.repeat(40) },
        at: '2026-11-04T03:10:00.000Z',
      },
    });
    fakeBridge.events = [otherTicket(1, 'github-ticket-99', 'running')];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    // The active task keeps its original ticket; the second ticket is only a notice.
    expect(await readTask(fixture.taskId)).toMatchObject({
      state: 'running',
    });
    const tasks = await testDatabase.pool.query(
      `select upstream_ticket_id as "ticketId" from task_links where conversation_id = $1`,
      [fixture.conversationId],
    );
    expect(tasks.rows).toEqual([{ ticketId: fixture.ticketId }]);
    const notice = await testDatabase.pool.query(
      `select body from messages where conversation_id = $1 and kind = 'notice'`,
      [fixture.conversationId],
    );
    expect(notice.rows[0]?.body).toContain('not executed separately');

    // Once the first attempt ended, the same conversation moves to the newer ticket.
    await testDatabase.pool.query(`update task_links set state = 'completed' where id = $1`, [
      fixture.taskId,
    ]);
    fakeBridge.events = [otherTicket(2, 'github-ticket-100', 'running')];
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    const resumed = await testDatabase.pool.query(
      `select upstream_ticket_id as "ticketId", state, pr_url as "prUrl"
         from task_links where conversation_id = $1`,
      [fixture.conversationId],
    );
    expect(resumed.rows).toEqual([
      { ticketId: 'github-ticket-100', state: 'running', prUrl: null },
    ]);
  }, 30_000);

  it('does not claim a Web conversation for a different issue number', async () => {
    const fixture = await seed({ state: 'running' });
    const fullName = fixture.repository.fullName;
    await testDatabase.pool.query(
      `insert into messages (conversation_id, direction, kind, body)
       values ($1, 'inbound', 'user', $2)`,
      [fixture.conversationId, `Different task https://github.com/${fullName}/issues/10`],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: 'ticket-imported:github-ticket-issue-1',
        type: 'ticket.imported',
        at: '2026-11-04T04:20:00.000Z',
        data: {
          ticketId: 'github-ticket-issue-1',
          source: 'github',
          externalId: 'github:1',
          number: 1,
          isPr: false,
          url: `https://github.com/${fullName}/issues/1`,
          title: 'Issue one',
          state: 'running',
          triggerId: 'github-comment:1:web',
          text: 'GitHub entry',
          base: { branch: 'main', commit: 'b'.repeat(40) },
          at: '2026-11-04T04:20:00.000Z',
        },
      },
    ];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    const conversations = await testDatabase.pool.query(
      `select id, source, external_key as "externalKey" from conversations where repository_id = $1 order by created_at`,
      [fixture.repository.id],
    );
    expect(conversations.rows).toHaveLength(2);
    expect(conversations.rows[0]).toMatchObject({
      id: fixture.conversationId,
      source: 'web',
      externalKey: null,
    });
    expect(conversations.rows[1]).toMatchObject({ source: 'github', externalKey: 'github:1' });
  }, 30_000);

  it('claims a Web conversation that already contains the GitHub requirement URL', async () => {
    const fixture = await seed({ state: 'running' });
    const fullName = fixture.repository.fullName;
    await testDatabase.pool.query(
      `insert into messages (conversation_id, direction, kind, body)
       values ($1, 'inbound', 'user', $2)`,
      [fixture.conversationId, `Please fix https://github.com/${fullName}/issues/42`],
    );
    fakeBridge.events = [
      {
        seq: 1,
        id: 'ticket-imported:github-ticket-web-42',
        type: 'ticket.imported',
        at: '2026-11-04T03:20:00.000Z',
        data: {
          ticketId: 'github-ticket-web-42',
          source: 'github',
          externalId: 'github:42',
          number: 42,
          isPr: false,
          url: `https://github.com/${fullName}/issues/42`,
          title: 'Fix the GitHub issue',
          state: 'running',
          triggerId: 'github-comment:42:web',
          text: 'GitHub entry',
          base: { branch: 'main', commit: 'b'.repeat(40) },
          at: '2026-11-04T03:20:00.000Z',
        },
      },
    ];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    const conversation = await testDatabase.pool.query(
      `select source, external_key as "externalKey" from conversations where id = $1`,
      [fixture.conversationId],
    );
    expect(conversation.rows[0]).toMatchObject({ source: 'github', externalKey: 'github:42' });
    const tasks = await testDatabase.pool.query(
      `select count(*)::int as count from task_links where conversation_id = $1`,
      [fixture.conversationId],
    );
    expect(tasks.rows[0]?.count).toBe(1);
  }, 30_000);

  it('returns the duplicate ticket id and persists a pause even when the first pause fails', async () => {
    const fixture = await seed({ state: 'running', source: 'github', externalKey: 'github:42' });
    const event: BridgeEvent = {
      seq: 1,
      id: 'ticket-imported:github-ticket-dup',
      type: 'ticket.imported',
      at: '2026-11-04T04:10:00.000Z',
      data: {
        ticketId: 'github-ticket-dup',
        source: 'github',
        externalId: 'github:42',
        number: 42,
        isPr: true,
        url: 'https://github.com/Bouyer12/m4-import/pull/42',
        title: 'Fix the GitHub issue',
        state: 'running',
        triggerId: 'github-comment:42:dup',
        text: 'Duplicate entry',
        base: { branch: 'main', commit: 'b'.repeat(40) },
        at: '2026-11-04T04:10:00.000Z',
      },
    };
    const dbClient = await testDatabase.pool.connect();
    let returned: string | undefined;
    try {
      await dbClient.query('begin');
      returned = await applyEvent(dbClient, fixture.repository, event, createConsoleLogger('test'));
      await dbClient.query('rollback');
    } finally {
      dbClient.release();
    }
    expect(returned).toBe('github-ticket-dup');
    fakeBridge.events = [event];
    expect(
      await syncOnce(testDatabase.pool, fixture.repository, client, createConsoleLogger('test')),
    ).toBe(true);
    const queued = await testDatabase.pool.query(
      `select ticket_id as "ticketId", state from duplicate_tickets where repository_id = $1`,
      [fixture.repository.id],
    );
    expect(queued.rows[0]).toMatchObject({ ticketId: 'github-ticket-dup', state: 'pending' });

    // A failed pause is persisted and retried, not only logged.
    const callsBefore = fakeBridge.calls.length;
    fakeBridge.refuse = { kind: 'pause', code: 'bridge_error', message: 'temporary failure' };
    await duplicatesOnce(
      testDatabase.pool,
      config,
      fixture.repository,
      client,
      createConsoleLogger('test'),
    );
    const failed = await testDatabase.pool.query(
      `select state, last_error as "lastError" from duplicate_tickets where repository_id = $1`,
      [fixture.repository.id],
    );
    expect(failed.rows[0]).toMatchObject({
      state: 'failed',
      lastError: expect.stringContaining('temporary'),
    });
    fakeBridge.refuse = null;
    await testDatabase.pool.query(
      `update duplicate_tickets set next_attempt_at = now() where repository_id = $1`,
      [fixture.repository.id],
    );
    await duplicatesOnce(
      testDatabase.pool,
      config,
      fixture.repository,
      client,
      createConsoleLogger('test'),
    );
    const paused = await testDatabase.pool.query(
      `select state from duplicate_tickets where repository_id = $1`,
      [fixture.repository.id],
    );
    expect(paused.rows[0]?.state).toBe('paused');
    expect(
      fakeBridge.calls.slice(callsBefore).filter((call) => call.kind === 'pause'),
    ).toHaveLength(2);
  }, 30_000);

  it('recovers a duplicate pause left in flight by a restart', async () => {
    const fixture = await seed({ state: 'running', source: 'github', externalKey: 'github:42' });
    await testDatabase.pool.query(
      `insert into duplicate_tickets (repository_id, task_id, ticket_id, state, attempts, operation_id, updated_at)
       values ($1, $2, 'github-ticket-restart', 'pausing', 1, '00000000-0000-4000-8000-000000000001', now() - interval '2 minutes')`,
      [fixture.repository.id, fixture.taskId],
    );
    await duplicatesOnce(
      testDatabase.pool,
      config,
      fixture.repository,
      client,
      createConsoleLogger('test'),
    );
    const row = await testDatabase.pool.query(
      `select state from duplicate_tickets where ticket_id = 'github-ticket-restart'`,
    );
    expect(['paused', 'failed']).toContain(row.rows[0]?.state);
  }, 30_000);

  it('matches only the exact GitHub requirement URL, not #10/#11 or another repository', () => {
    const repo = 'Bouyer12/m4-import';
    expect(
      githubRequirementUrl('see https://github.com/Bouyer12/m4-import/issues/1', repo, false, 1),
    ).toBe(true);
    expect(
      githubRequirementUrl('see https://github.com/Bouyer12/m4-import/issues/10', repo, false, 1),
    ).toBe(false);
    expect(
      githubRequirementUrl('see https://github.com/Bouyer12/m4-import/issues/11', repo, false, 1),
    ).toBe(false);
    expect(
      githubRequirementUrl(
        'https://github.com/Bouyer12/m4-import/issues/1#issuecomment-9',
        repo,
        false,
        1,
      ),
    ).toBe(true);
    expect(
      githubRequirementUrl(
        'https://github.com/Bouyer12/m4-import/issues/1?foo=bar',
        repo,
        false,
        1,
      ),
    ).toBe(true);
    expect(
      githubRequirementUrl('https://github.com/Bouyer12/m4-import/pull/1', repo, true, 1),
    ).toBe(true);
    expect(
      githubRequirementUrl('https://github.com/Bouyer12/m4-import/pull/1', repo, false, 1),
    ).toBe(false);
    expect(githubRequirementUrl('https://github.com/Bouyer12/other/issues/1', repo, false, 1)).toBe(
      false,
    );
  });

  it('copies a ready runtime report into the conversation exactly once', async () => {
    const fixture = await seed({ state: 'awaiting_review', executionMode: 'discuss' });
    fakeBridge.snapshots.set(fixture.ticketId, {
      ticketId: fixture.ticketId,
      title: 'M4 task',
      state: 'awaiting_review',
      mode: 'discuss',
      report: {
        state: 'ready',
        summary: 'Plan: fix README.md line 5 (teh -> the).',
        at: '2026-11-04T02:00:00.000Z',
      },
    });
    const logger = createConsoleLogger('test');
    expect(await captureReportsOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(
      true,
    );
    expect(await captureReportsOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(
      false,
    );
    const messages = await testDatabase.pool.query(
      `select kind, body, source_event_id as "sourceEventId" from messages where conversation_id = $1`,
      [fixture.conversationId],
    );
    expect(messages.rows).toHaveLength(1);
    expect(messages.rows[0]).toMatchObject({
      kind: 'result',
      body: 'Plan: fix README.md line 5 (teh -> the).',
      sourceEventId: `report:${fixture.ticketId}:2026-11-04T02:00:00.000Z`,
    });
  }, 30_000);

  it('maps a closed PR event to closed_unmerged and holds later deliveries', async () => {
    const fixture = await seed({ state: 'awaiting_review' });
    fakeBridge.events = [
      {
        seq: 1,
        id: `ticket-ended:${fixture.ticketId}`,
        type: 'ticket.ended',
        at: '2026-11-04T01:00:00.000Z',
        data: { ticketId: fixture.ticketId, by: 'human', outcome: 'closed' },
      },
    ];
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, fixture.repository, client, logger)).toBe(true);
    expect(await readTask(fixture.taskId)).toMatchObject({ state: 'closed_unmerged' });

    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'closed-message', 'continue anyway') returning id`,
      [fixture.conversationId],
    );
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [message.rows[0]!.id, `workspace:${message.rows[0]!.id}`],
    );
    expect(await deliverOnce(testDatabase.pool, config, fixture.repository, client, logger)).toBe(
      false,
    );
    const delivery = await testDatabase.pool.query(
      `select state from deliveries where message_id = $1`,
      [message.rows[0]!.id],
    );
    expect(delivery.rows[0]?.state).toBe('pending');
  }, 30_000);

  it('keeps a stopped task delivery stored until resume', async () => {
    const fixture = await seed({
      state: 'stopped',
      stoppedAt: new Date(),
      stopEvidence: { agentStatus: 'idle' },
    });
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'stopped-message', 'queued until resume') returning id`,
      [fixture.conversationId],
    );
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [message.rows[0]!.id, `workspace:${message.rows[0]!.id}`],
    );
    const logger = createConsoleLogger('test');
    expect(await deliverOnce(testDatabase.pool, config, fixture.repository, client, logger)).toBe(
      false,
    );
    const delivery = await testDatabase.pool.query(
      `select state from deliveries where message_id = $1`,
      [message.rows[0]!.id],
    );
    expect(delivery.rows[0]?.state).toBe('pending');
  }, 30_000);

  it('reconciles a running task from a paused snapshot and blocks one the runtime cannot confirm', async () => {
    const paused = await seed({ state: 'running' });
    fakeBridge.snapshots.set(paused.ticketId, {
      ticketId: paused.ticketId,
      title: 'M4 task',
      state: 'stopped',
      mode: 'execute',
      paused: {
        at: new Date().toISOString(),
        by: 'human',
        stoppedAt: new Date().toISOString(),
        evidence: { agentStatus: 'idle', harnessPid: 1234, jobsAfter: [] },
      },
    });
    const unconfirmed = await seed({ state: 'running' });
    // No snapshot for this ticket: the fake Bridge answers 404.
    const logger = createConsoleLogger('test');
    await reconcileTasks(testDatabase.pool, paused.repository, client, logger);
    expect(await readTask(paused.taskId)).toMatchObject({ state: 'stopped' });
    const evidence = (await readTask(paused.taskId)).stopEvidence as Record<string, unknown>;
    expect(evidence).toMatchObject({ harnessPid: 1234, agentStatus: 'idle' });

    // A snapshot with a timestamp but a live job is incomplete, not stopped.
    const busy = await seed({ state: 'running' });
    fakeBridge.snapshots.set(busy.ticketId, {
      ticketId: busy.ticketId,
      title: 'M4 busy task',
      state: 'stopped',
      mode: 'execute',
      paused: {
        at: new Date().toISOString(),
        by: 'human',
        stoppedAt: new Date().toISOString(),
        evidence: {
          agentStatus: 'idle',
          harnessPid: 1235,
          jobsAfter: [{ id: 'bash-1', name: 'sleep 300' }],
          subagentsAfter: 0,
        },
      },
    });
    await reconcileTasks(testDatabase.pool, busy.repository, client, logger);
    const busyTask = await readTask(busy.taskId);
    expect(busyTask.state).toBe('blocked');
    expect(busyTask.stopEvidence).toMatchObject({
      problem: expect.stringContaining('background job'),
    });

    // Reconcile the second repository too; its missing snapshot must block it.
    await reconcileTasks(testDatabase.pool, unconfirmed.repository, client, logger);
    const blocked = await readTask(unconfirmed.taskId);
    expect(blocked.state).toBe('blocked');
    expect(blocked.stopEvidence).toMatchObject({
      reconciliationError: expect.stringContaining('not found'),
    });
  }, 30_000);
});

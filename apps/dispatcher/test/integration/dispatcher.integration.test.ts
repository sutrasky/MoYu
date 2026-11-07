import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { once } from 'node:events';

import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';

import type { BridgeEvent, BridgeTriggerInput } from '@moyu/runtime-client';
import { RuntimeBridgeClient } from '@moyu/runtime-client';

import { loadDispatcherConfig, type DispatcherConfig } from '../../src/config';
import { admitImportedOnce } from '../../src/admissions';
import { deliverOnce } from '../../src/delivery';
import { createConsoleLogger } from '../../src/logger';
import { resolveRepository, type RepositoryBinding } from '../../src/runtime';
import { syncOnce } from '../../src/sync';
import { createTestDatabase, type TestDatabase } from './helpers';

interface FakeBridge {
  url: string;
  triggerCalls: BridgeTriggerInput[];
  admitCalls: { ticketId: string; admission: unknown; assignmentId?: string }[];
  events: BridgeEvent[];
  failNextTrigger: boolean;
  slowNextTrigger: boolean;
  filterNextTrigger: boolean;
  server: Server;
  close: () => Promise<void>;
}

function readBody(request: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    request.on('data', (chunk: Buffer) => chunks.push(chunk));
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

async function startFakeBridge(events: BridgeEvent[]): Promise<FakeBridge> {
  const state = {
    triggerCalls: [] as BridgeTriggerInput[],
    admitCalls: [] as { ticketId: string; admission: unknown; assignmentId?: string }[],
    events,
    failNextTrigger: false,
    slowNextTrigger: false,
    filterNextTrigger: false,
  };
  const server = createServer(async (request: IncomingMessage, response: ServerResponse) => {
    const send = (status: number, value: unknown) => {
      response.writeHead(status, { 'content-type': 'application/json' });
      response.end(JSON.stringify(value));
    };
    if (request.headers.authorization !== 'Bearer m3-test-bridge-token-0123456789') {
      send(401, { error: { code: 'unauthorized', message: 'machine token required' } });
      return;
    }
    const url = new URL(request.url ?? '/', 'http://127.0.0.1');
    if (request.method === 'POST' && url.pathname === '/bridge/v1/triggers') {
      const body = (await readBody(request)) as BridgeTriggerInput;
      state.triggerCalls.push(body);
      if (state.failNextTrigger) {
        state.failNextTrigger = false;
        send(503, { error: { code: 'runtime_unavailable', message: 'retry', retryable: true } });
        return;
      }
      if (state.slowNextTrigger) {
        state.slowNextTrigger = false;
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
      if (state.filterNextTrigger) {
        state.filterNextTrigger = false;
        send(200, {
          result: 'filtered',
          triggerId: body.triggerId,
          baseCommit: null,
          ticketId: null,
        });
        return;
      }
      send(200, {
        result: state.triggerCalls.length > 1 ? 'duplicate' : 'stored',
        triggerId: body.triggerId,
        baseCommit: 'a'.repeat(40),
        ticketId: state.triggerCalls.length > 1 ? 'ticket-1' : null,
      });
      return;
    }
    if (request.method === 'GET' && url.pathname === '/bridge/v1/capabilities') {
      send(200, {
        protocolVersion: 'moyu-bridge/1',
        upstream: { package: 'dsh-superagent', version: '0.1.0' },
        capabilities: {
          trigger: true,
          events: true,
          tickets: true,
          models: true,
          pause: true,
          resume: true,
          retry: true,
          readOnlyDiscussion: true,
          start: true,
          admission: true,
        },
      });
      return;
    }
    const admitMatch = /^\/bridge\/v1\/tickets\/([^/]+)\/admit$/.exec(url.pathname);
    if (request.method === 'POST' && admitMatch) {
      const body = (await readBody(request)) as { admission: unknown; assignmentId?: string };
      state.admitCalls.push({
        ticketId: decodeURIComponent(admitMatch[1]!),
        admission: body.admission,
        assignmentId: body.assignmentId,
      });
      send(200, { ticketId: decodeURIComponent(admitMatch[1]!), admitted: true });
      return;
    }
    if (url.pathname === '/bridge/v1/events') {
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
    get triggerCalls() {
      return state.triggerCalls;
    },
    get admitCalls() {
      return state.admitCalls;
    },
    set failNextTrigger(value: boolean) {
      state.failNextTrigger = value;
    },
    get events() {
      return state.events;
    },
    set slowNextTrigger(value: boolean) {
      state.slowNextTrigger = value;
    },
    set filterNextTrigger(value: boolean) {
      state.filterNextTrigger = value;
    },
    server,
    close: () =>
      new Promise<void>((resolve) => {
        server.close(() => resolve());
      }),
  } as FakeBridge;
}

describe('dispatcher delivery and Bridge event catch-up on real PostgreSQL', () => {
  let testDatabase: TestDatabase;
  let fakeBridge: FakeBridge;
  let repository: RepositoryBinding;
  let config: DispatcherConfig;
  let client: RuntimeBridgeClient;
  let conversationId: string;
  let messageId: string;
  let ownerId: string;

  beforeAll(async () => {
    testDatabase = await createTestDatabase();
    const now = new Date('2026-11-03T12:00:00.000Z');
    const owner = await testDatabase.pool.query<{ id: string }>(
      `insert into owners (display_name) values ('M3 Dispatcher Owner') returning id`,
    );
    ownerId = owner.rows[0]!.id;
    conversationId = '';
    // The repository row needs the Bridge URL, so create it after the server listens.
    fakeBridge = await startFakeBridge([
      {
        seq: 1,
        id: 'ticket-linked:ticket-1',
        type: 'ticket.linked',
        at: now.toISOString(),
        data: {
          triggerId: 'workspace:placeholder',
          ticketId: 'ticket-1',
          conversationId: 'placeholder',
          messageId: 'placeholder',
          state: 'queued',
          title: 'M3 loop',
          base: {
            branch: 'main',
            commit: 'a'.repeat(40),
            model: { provider: 'commandcode', model: 'deepseek/deepseek-v4.1-flash' },
          },
        },
      },
      {
        seq: 2,
        id: 'message-read:trigger-1',
        type: 'message.read',
        at: now.toISOString(),
        data: {
          triggerId: 'workspace:placeholder',
          messageId: 'placeholder',
          at: now.toISOString(),
        },
      },
      {
        seq: 3,
        id: 'progress-1',
        type: 'task.progress',
        at: now.toISOString(),
        data: { ticketId: 'ticket-1', text: 'Reading the repository', at: now.toISOString() },
      },
      {
        seq: 4,
        id: 'artifact-ticket-1',
        type: 'artifact.created',
        at: now.toISOString(),
        data: {
          ticketId: 'ticket-1',
          type: 'pull_request',
          url: 'https://github.com/Bouyer12/m3-loop-test/pull/7',
          number: 7,
          title: 'M3 loop',
        },
      },
      {
        seq: 5,
        id: 'ticket-ended:ticket-1',
        type: 'ticket.ended',
        at: now.toISOString(),
        data: { ticketId: 'ticket-1', by: 'executor', outcome: 'completed' },
      },
    ]);

    const repositoryRow = await testDatabase.pool.query<{ id: string }>(
      `insert into repositories (owner_id, full_name, default_branch, status, bridge_url, bridge_credential_ref)
       values ($1, 'Bouyer12/m3-loop-test', 'main', 'active', $2, 'MOYU_BRIDGE_TOKEN')
       returning id`,
      [ownerId, fakeBridge.url],
    );
    const repositoryId = repositoryRow.rows[0]!.id;
    void ownerId;
    const conversationRow = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, model_id, model_reasoning_effort, title)
       values ($1, $2, 'work', 'execute', 'main', 'commandcode/deepseek/deepseek-v4.1-flash', 'high', 'M3 loop')
       returning id`,
      [ownerId, repositoryId],
    );
    conversationId = conversationRow.rows[0]!.id;
    const messageRow = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'client-1', 'Run the M3 loop') returning id`,
      [conversationId],
    );
    messageId = messageRow.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [messageId, `workspace:${messageId}`],
    );
    await testDatabase.pool.query(
      `insert into task_links (conversation_id, repository_id, state, base_branch, model_id, model_reasoning_effort)
       values ($1, $2, 'pending_admission', 'main', 'commandcode/deepseek/deepseek-v4.1-flash', 'high')`,
      [conversationId, repositoryId],
    );

    config = loadDispatcherConfig({
      NODE_ENV: 'test',
      DATABASE_URL: testDatabase.url,
      BRIDGE_REPOSITORY: 'Bouyer12/m3-loop-test',
      BRIDGE_TOKEN: 'm3-test-bridge-token-0123456789',
      DELIVERY_RETRY_CAP_MS: '1000',
    });
    const resolved = await resolveRepository(testDatabase.pool, config);
    if (!resolved) {
      throw new Error('repository binding missing');
    }
    repository = resolved;
    client = new RuntimeBridgeClient({
      baseUrl: fakeBridge.url,
      token: 'm3-test-bridge-token-0123456789',
    });
  }, 60_000);

  beforeEach(async () => {
    // Every test starts from a free repository slot; a lease from a previous
    // scenario is not part of the next scenario's fixture.
    await testDatabase.pool.query(
      `update runtime_leases set state = 'released', released_at = now()
        where repository_id = $1 and state = 'active'`,
      [repository.id],
    );
    await testDatabase.pool.query(
      `update task_links set runtime_lease_id = null where repository_id = $1`,
      [repository.id],
    );
  });

  afterAll(async () => {
    if (fakeBridge) {
      await fakeBridge.close();
    }
    if (testDatabase) {
      await testDatabase.drop();
    }
  });

  it('retries a transient delivery with the same trigger id', async () => {
    fakeBridge.failNextTrigger = true;
    const first = await deliverOnce(
      testDatabase.pool,
      config,
      repository,
      client,
      createConsoleLogger('test'),
    );
    expect(first).toBe(true);
    const afterFirst = await testDatabase.pool.query(
      `select state, attempt_count, last_error from deliveries where message_id = $1`,
      [messageId],
    );
    expect(afterFirst.rows[0]).toMatchObject({ state: 'pending', attempt_count: 1 });
    expect(afterFirst.rows[0]?.last_error).toBeTruthy();

    await testDatabase.pool.query(
      `update deliveries set next_attempt_at = now() where message_id = $1`,
      [messageId],
    );
    const second = await deliverOnce(
      testDatabase.pool,
      config,
      repository,
      client,
      createConsoleLogger('test'),
    );
    expect(second).toBe(true);

    const afterSecond = await testDatabase.pool.query(
      `select state, attempt_count, delivered_at, upstream_trigger_id from deliveries where message_id = $1`,
      [messageId],
    );
    expect(afterSecond.rows[0]?.state).toBe('delivered');
    expect(afterSecond.rows[0]?.attempt_count).toBe(2);
    expect(afterSecond.rows[0]?.delivered_at).toBeTruthy();

    expect(fakeBridge.triggerCalls).toHaveLength(2);
    expect(new Set(fakeBridge.triggerCalls.map((call) => call.triggerId))).toEqual(
      new Set([`workspace:${messageId}`]),
    );
    const conversation = await testDatabase.pool.query(
      `select base_commit from conversations where id = $1`,
      [conversationId],
    );
    expect(conversation.rows[0]?.base_commit).toBe('a'.repeat(40));
    expect(fakeBridge.triggerCalls[0]?.executionSpec.model).toMatchObject({
      provider: 'commandcode',
      model: 'deepseek/deepseek-v4.1-flash',
      reasoningEffort: 'high',
    });
  });

  it('projects Bridge events once, advances the cursor and survives a repeat sync', async () => {
    // The fake events reference placeholders; make them point at this fixture.
    for (const event of fakeBridge.events) {
      event.data = {
        ...event.data,
        conversationId:
          event.data.conversationId === 'placeholder' ? conversationId : event.data.conversationId,
        messageId: event.data.messageId === 'placeholder' ? messageId : event.data.messageId,
        triggerId:
          event.data.triggerId === 'workspace:placeholder'
            ? `workspace:${messageId}`
            : event.data.triggerId,
      };
    }
    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, repository, client, logger)).toBe(true);
    expect(await syncOnce(testDatabase.pool, repository, client, logger)).toBe(false);

    const task = await testDatabase.pool.query(
      `select state, upstream_ticket_id, base_commit, pr_url, pr_number, question from task_links where conversation_id = $1`,
      [conversationId],
    );
    expect(task.rows[0]).toMatchObject({
      state: 'completed',
      upstream_ticket_id: 'ticket-1',
      pr_number: 7,
    });
    expect(task.rows[0]?.pr_url).toContain('/pull/7');

    const read = await testDatabase.pool.query(
      `select read_at from deliveries where message_id = $1`,
      [messageId],
    );
    expect(read.rows[0]?.read_at).toBeTruthy();

    const platformEvents = await testDatabase.pool.query<{ type: string; source_event_id: string }>(
      `select type, source_event_id from events where source = 'bridge' order by seq`,
    );
    expect(platformEvents.rows.map((row) => row.type)).toEqual([
      'task.linked',
      'message.delivery_changed',
      'task.progress',
      'artifact.created',
      'task.state_changed',
    ]);
    expect(platformEvents.rows.map((row) => row.source_event_id)).toEqual([
      'ticket-linked:ticket-1',
      'message-read:trigger-1',
      'progress-1',
      'artifact-ticket-1',
      'ticket-ended:ticket-1',
    ]);

    const cursor = await testDatabase.pool.query(
      `select cursor from runtime_cursors where repository_id = $1`,
      [repository.id],
    );
    expect(String(cursor.rows[0]?.cursor)).toBe('5');

    const artifacts = await testDatabase.pool.query(
      `select type, metadata from artifacts where task_id = (select id from task_links where conversation_id = $1)`,
      [conversationId],
    );
    expect(artifacts.rows).toHaveLength(1);
    expect(artifacts.rows[0]?.type).toBe('pull_request');
  });

  it('keeps a progress event that arrives before its ticket link and projects it once after linking', async () => {
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'early progress') returning id`,
      [ownerId, repository.id],
    );
    const earlyConversationId = conversation.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into task_links (conversation_id, repository_id, state, base_branch)
       values ($1, $2, 'pending_admission', 'main')`,
      [earlyConversationId, repository.id],
    );
    fakeBridge.events.push(
      {
        seq: 10,
        id: 'early-progress-1',
        type: 'task.progress',
        at: '2026-11-03T12:01:00.000Z',
        data: {
          ticketId: 'ticket-early',
          text: 'Started before the link projection',
          at: '2026-11-03T12:01:00.000Z',
        },
      },
      {
        seq: 11,
        id: 'early-linked-1',
        type: 'ticket.linked',
        at: '2026-11-03T12:01:01.000Z',
        data: {
          triggerId: `workspace:${earlyConversationId}`,
          ticketId: 'ticket-early',
          conversationId: earlyConversationId,
          messageId: 'placeholder-early',
          state: 'running',
          title: 'early progress',
          base: { branch: 'main', commit: 'b'.repeat(40) },
        },
      },
    );

    const logger = createConsoleLogger('test');
    expect(await syncOnce(testDatabase.pool, repository, client, logger)).toBe(true);

    const linked = await testDatabase.pool.query(
      `select upstream_ticket_id, base_commit from task_links where conversation_id = $1`,
      [earlyConversationId],
    );
    expect(linked.rows[0]).toMatchObject({
      upstream_ticket_id: 'ticket-early',
      base_commit: 'b'.repeat(40),
    });
    const projected = await testDatabase.pool.query<{ type: string; source_event_id: string }>(
      `select type, source_event_id from events
        where conversation_id = $1 and source = 'bridge' order by seq`,
      [earlyConversationId],
    );
    expect(projected.rows.map((row) => row.type)).toEqual(['task.linked', 'task.progress']);
    expect(projected.rows[1]?.source_event_id).toBe('early-progress-1');

    const deferred = await testDatabase.pool.query(
      `select count(*)::int as count from deferred_bridge_events where repository_id = $1`,
      [repository.id],
    );
    expect(deferred.rows[0]?.count).toBe(0);

    // Replaying the same page must not project the deferred progress twice.
    await testDatabase.pool.query(
      `update runtime_cursors set cursor = 9 where repository_id = $1`,
      [repository.id],
    );
    expect(await syncOnce(testDatabase.pool, repository, client, logger)).toBe(true);
    const repeated = await testDatabase.pool.query(
      `select count(*)::int as count from events
        where conversation_id = $1 and source_event_id = 'early-progress-1'`,
      [earlyConversationId],
    );
    expect(repeated.rows[0]?.count).toBe(1);
  });

  it('marks a filtered Trigger failed instead of delivered or read and creates no ticket', async () => {
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'filtered') returning id`,
      [ownerId, repository.id],
    );
    const filteredConversationId = conversation.rows[0]!.id;
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'client-filtered', 'filtered by scope') returning id`,
      [filteredConversationId],
    );
    const filteredMessageId = message.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [filteredMessageId, `workspace:${filteredMessageId}`],
    );
    await testDatabase.pool.query(
      `insert into task_links (conversation_id, repository_id, state, base_branch)
       values ($1, $2, 'pending_admission', 'main')`,
      [filteredConversationId, repository.id],
    );

    fakeBridge.filterNextTrigger = true;
    expect(
      await deliverOnce(testDatabase.pool, config, repository, client, createConsoleLogger('test')),
    ).toBe(true);

    const delivery = await testDatabase.pool.query(
      `select state, delivered_at, read_at from deliveries where message_id = $1`,
      [filteredMessageId],
    );
    expect(delivery.rows[0]).toMatchObject({
      state: 'failed',
      delivered_at: null,
      read_at: null,
    });
    const task = await testDatabase.pool.query(
      `select upstream_ticket_id from task_links where conversation_id = $1`,
      [filteredConversationId],
    );
    expect(task.rows[0]?.upstream_ticket_id).toBeNull();
  });

  it('refuses to deliver an execution trigger when the runtime lacks admission capability', async () => {
    fakeBridge.triggerCalls.length = 0;
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'no admission capability') returning id`,
      [ownerId, repository.id],
    );
    const conversationId = conversation.rows[0]!.id;
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'client-no-admission', 'do not run this') returning id`,
      [conversationId],
    );
    const messageId = message.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into task_links (conversation_id, repository_id, state, base_branch) values ($1, $2, 'queued', 'main')`,
      [conversationId, repository.id],
    );
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [messageId, `workspace:${messageId}`],
    );
    expect(
      await deliverOnce(
        testDatabase.pool,
        config,
        repository,
        client,
        createConsoleLogger('test'),
        false,
      ),
    ).toBe(true);
    expect(fakeBridge.triggerCalls).toHaveLength(0);
    const delivery = await testDatabase.pool.query<{ state: string; last_error: string }>(
      `select state, last_error from deliveries where message_id = $1`,
      [messageId],
    );
    expect(delivery.rows[0]?.state).toBe('failed');
    expect(delivery.rows[0]?.last_error).toContain('runtime_admission_unsupported');
  }, 30_000);

  it('reuses the persisted assignment identity when admission is replayed', async () => {
    fakeBridge.admitCalls.length = 0;
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'replayed admission') returning id`,
      [ownerId, repository.id],
    );
    const conversationId = conversation.rows[0]!.id;
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'replay-admit', 'replay') returning id`,
      [conversationId],
    );
    const messageId = message.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [messageId, `github:${messageId}`],
    );
    const task = await testDatabase.pool.query<{ id: string }>(
      `insert into task_links (conversation_id, repository_id, state, base_branch, upstream_ticket_id)
       values ($1, $2, 'pending_admission', 'main', 'ticket-replay') returning id`,
      [conversationId, repository.id],
    );
    await admitImportedOnce(
      testDatabase.pool,
      config,
      repository,
      client,
      true,
      createConsoleLogger('test'),
    );
    expect(fakeBridge.admitCalls).toHaveLength(1);
    const first = fakeBridge.admitCalls[0]!.assignmentId;
    expect(first).toMatch(/^[0-9a-f]{32}$/);
    await testDatabase.pool.query(
      `update task_links set state = 'pending_admission' where id = $1`,
      [task.rows[0]!.id],
    );
    await admitImportedOnce(
      testDatabase.pool,
      config,
      repository,
      client,
      true,
      createConsoleLogger('test'),
    );
    expect(fakeBridge.admitCalls).toHaveLength(2);
    expect(fakeBridge.admitCalls[1]!.assignmentId).toBe(first);
    await testDatabase.pool.query(`update task_links set state = 'not_accepted' where id = $1`, [
      task.rows[0]!.id,
    ]);
    await testDatabase.pool.query(`delete from runtime_leases where repository_id = $1`, [
      repository.id,
    ]);
  });

  it.fails(
    'binds the complete execution identity before a cold GitHub admission starts',
    async () => {
      const conversation = await testDatabase.pool.query<{ id: string }>(
        `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'cold admission red') returning id`,
        [ownerId, repository.id],
      );
      const conversationId = conversation.rows[0]!.id;
      const message = await testDatabase.pool.query<{ id: string }>(
        `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'cold-red', 'cold start') returning id`,
        [conversationId],
      );
      const messageId = message.rows[0]!.id;
      await testDatabase.pool.query(
        `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
        [messageId, `github:${messageId}`],
      );
      await testDatabase.pool.query(
        `insert into task_links (conversation_id, repository_id, state, base_branch, upstream_ticket_id)
       values ($1, $2, 'pending_admission', 'main', 'ticket-cold-red')`,
        [conversationId, repository.id],
      );
      await testDatabase.pool.query(
        `insert into repository_instances (repository_id, state, desired_state)
       values ($1, 'sleeping', 'online')
       on conflict (repository_id) do update set state = 'sleeping', desired_state = 'online'`,
        [repository.id],
      );
      const admitted = await admitImportedOnce(
        testDatabase.pool,
        config,
        repository,
        client,
        true,
        createConsoleLogger('test'),
      );
      const ownership = await testDatabase.pool.query<{
        ownership: Record<string, unknown> | null;
      }>(`select ownership from repository_instances where repository_id = $1`, [repository.id]);
      // This is a red fixture: the current path never binds, so release anything it did start
      // before asserting, keeping later cases isolated.
      await testDatabase.pool.query(
        `update task_links set state = 'not_accepted' where conversation_id = $1`,
        [conversationId],
      );
      await testDatabase.pool.query(`delete from runtime_leases where repository_id = $1`, [
        repository.id,
      ]);
      await testDatabase.pool.query(`delete from repository_instances where repository_id = $1`, [
        repository.id,
      ]);
      // eslint-disable-next-line no-console
      console.info(
        '[cold-admission-red] ownership=',
        JSON.stringify(ownership.rows[0]?.ownership ?? null),
      );
      expect(admitted).toBe(true);
      const execution = ownership.rows[0]?.ownership?.execution;
      expect(execution).toMatchObject({
        ticketId: expect.any(String),
        assignmentId: expect.any(String),
        leaseId: expect.any(String),
        fencingToken: expect.any(String),
      });
      expect(ownership.rows[0]?.ownership?.execution).toMatchObject({
        ticketId: expect.any(String),
      });
    },
  );

  it('does not admit a pending ticket when the supervisor stop proof is unproven', async () => {
    fakeBridge.triggerCalls.length = 0;
    fakeBridge.admitCalls.length = 0;
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'unproven stop') returning id`,
      [ownerId, repository.id],
    );
    const conversationId = conversation.rows[0]!.id;
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'client-unproven', 'must not run') returning id`,
      [conversationId],
    );
    const messageId = message.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [messageId, `workspace:${messageId}`],
    );
    const task = await testDatabase.pool.query<{ id: string }>(
      `insert into task_links (conversation_id, repository_id, state, base_branch, upstream_ticket_id)
       values ($1, $2, 'pending_admission', 'main', 'ticket-unproven') returning id`,
      [conversationId, repository.id],
    );
    await admitImportedOnce(
      testDatabase.pool,
      config,
      repository,
      client,
      true,
      createConsoleLogger('test'),
      async () => ({ allowed: false, reason: 'cgroup still populated' }),
    );
    expect(fakeBridge.admitCalls).toHaveLength(0);
    const after = await testDatabase.pool.query<{ state: string; stop_evidence: unknown }>(
      `select state, stop_evidence from task_links where id = $1`,
      [task.rows[0]!.id],
    );
    expect(after.rows[0]?.state).toBe('pending_admission');
    expect(JSON.stringify(after.rows[0]?.stop_evidence)).toContain('cgroup still populated');
    await testDatabase.pool.query(`update task_links set state = 'not_accepted' where id = $1`, [
      task.rows[0]!.id,
    ]);
  });

  it('sends the lease admission with an admitted runtime and starts a pending GitHub ticket once', async () => {
    fakeBridge.triggerCalls.length = 0;
    fakeBridge.admitCalls.length = 0;
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'admitted launch') returning id`,
      [ownerId, repository.id],
    );
    const conversationId = conversation.rows[0]!.id;
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'client-admitted', 'run admitted') returning id`,
      [conversationId],
    );
    const messageId = message.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [messageId, `workspace:${messageId}`],
    );
    await testDatabase.pool.query(
      `insert into task_links (conversation_id, repository_id, state, base_branch)
       values ($1, $2, 'pending_admission', 'main')`,
      [conversationId, repository.id],
    );
    expect(
      await deliverOnce(
        testDatabase.pool,
        config,
        repository,
        client,
        createConsoleLogger('test'),
        true,
      ),
    ).toBe(true);
    const sent = fakeBridge.triggerCalls.at(-1);
    expect(sent?.admission).toMatchObject({
      leaseId: expect.any(String),
      fencingToken: expect.any(String),
    });

    const pending = await testDatabase.pool.query<{
      id: string;
      upstream_ticket_id: string | null;
    }>(`select id, upstream_ticket_id from task_links where conversation_id = $1`, [
      conversationId,
    ]);
    const taskId = pending.rows[0]!.id;
    await testDatabase.pool.query(
      `update task_links set upstream_ticket_id = 'ticket-admit', state = 'pending_admission' where id = $1`,
      [taskId],
    );
    expect(
      await admitImportedOnce(
        testDatabase.pool,
        config,
        repository,
        client,
        true,
        createConsoleLogger('test'),
      ),
    ).toBe(true);
    expect(fakeBridge.admitCalls).toHaveLength(1);
    expect(fakeBridge.admitCalls[0]?.ticketId).toBe('ticket-admit');
    expect(fakeBridge.admitCalls[0]?.assignmentId).toMatch(/^[0-9a-f]{32}$/);
    const admitted = await testDatabase.pool.query<{ state: string }>(
      `select state from task_links where id = $1`,
      [taskId],
    );
    expect(admitted.rows[0]?.state).toBe('queued');
  }, 30_000);

  it('does not admit a pending GitHub ticket while a pause operation is queued', async () => {
    fakeBridge.admitCalls.length = 0;
    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'pause before admission') returning id`,
      [ownerId, repository.id],
    );
    const conversationId = conversation.rows[0]!.id;
    const task = await testDatabase.pool.query<{ id: string }>(
      `insert into task_links (conversation_id, repository_id, state, base_branch, upstream_ticket_id)
       values ($1, $2, 'pending_admission', 'main', 'ticket-paused') returning id`,
      [conversationId, repository.id],
    );
    const taskId = task.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into task_operations (task_id, repository_id, kind, client_command_id, request_fingerprint, evidence)
       values ($1, $2, 'pause', 'pause-before-admit', 'pause-before-admit', '{}'::jsonb)`,
      [taskId, repository.id],
    );
    expect(
      await admitImportedOnce(
        testDatabase.pool,
        config,
        repository,
        client,
        true,
        createConsoleLogger('test'),
      ),
    ).toBe(false);
    expect(fakeBridge.admitCalls).toHaveLength(0);
    const row = await testDatabase.pool.query<{ state: string }>(
      `select state from task_links where id = $1`,
      [taskId],
    );
    expect(row.rows[0]?.state).toBe('pending_admission');
  }, 30_000);

  it('keeps a launch pending while another execution owns the repository lease', async () => {
    const holderConversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'lease holder') returning id`,
      [ownerId, repository.id],
    );
    const holderTask = await testDatabase.pool.query<{ id: string }>(
      `insert into task_links
         (conversation_id, repository_id, upstream_ticket_id, state, base_branch)
       values ($1, $2, 'holder-ticket', 'running', 'main') returning id`,
      [holderConversation.rows[0]!.id, repository.id],
    );
    const holderLease = await testDatabase.pool.query<{ id: string }>(
      `insert into runtime_leases (repository_id, holder_id, expires_at)
       values ($1, 'dispatcher:holder', now() + interval '10 minutes') returning id`,
      [repository.id],
    );
    await testDatabase.pool.query(`update task_links set runtime_lease_id = $2 where id = $1`, [
      holderTask.rows[0]!.id,
      holderLease.rows[0]!.id,
    ]);

    const conversation = await testDatabase.pool.query<{ id: string }>(
      `insert into conversations (owner_id, repository_id, mode, execution_mode, base_branch, title)
       values ($1, $2, 'work', 'execute', 'main', 'lease waiter') returning id`,
      [ownerId, repository.id],
    );
    const message = await testDatabase.pool.query<{ id: string }>(
      `insert into messages (conversation_id, direction, kind, client_message_id, body)
       values ($1, 'inbound', 'user', 'client-lease-wait', 'wait for the slot') returning id`,
      [conversation.rows[0]!.id],
    );
    const waitingMessageId = message.rows[0]!.id;
    await testDatabase.pool.query(
      `insert into deliveries (message_id, upstream_trigger_id) values ($1, $2)`,
      [waitingMessageId, `workspace:${waitingMessageId}`],
    );
    await testDatabase.pool.query(
      `insert into task_links (conversation_id, repository_id, state, base_branch)
       values ($1, $2, 'pending_admission', 'main')`,
      [conversation.rows[0]!.id, repository.id],
    );

    const triggerCalls = fakeBridge.triggerCalls.length;
    expect(
      await deliverOnce(testDatabase.pool, config, repository, client, createConsoleLogger('test')),
    ).toBe(true);
    expect(fakeBridge.triggerCalls).toHaveLength(triggerCalls);
    const delivery = await testDatabase.pool.query<{
      state: string;
      evidence: Record<string, unknown>;
    }>(`select state, evidence from deliveries where message_id = $1`, [waitingMessageId]);
    expect(delivery.rows[0]?.state).toBe('pending');
    expect(delivery.rows[0]?.evidence.leaseRefused).toBe('held');
  });
});

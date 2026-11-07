import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync, writeFileSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { eq } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import type { BridgeTriggerInput } from '@moyu/runtime-client';
import {
  conversations,
  deliveries,
  messages,
  owners,
  repositories,
  repositoryInstances,
  taskLinks,
  type Database,
} from '@moyu/db';

import { loadDispatcherConfig } from '../../src/config';
import { runDispatcher } from '../../src/dispatcher';
import { createTestDatabase, type TestDatabase } from './helpers';

interface FakeBridge {
  url: string;
  calls: Array<{ token: string | undefined; body: BridgeTriggerInput }>;
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

async function startBridge(): Promise<FakeBridge> {
  const calls: FakeBridge['calls'] = [];
  const server = createServer(async (request: IncomingMessage, response: ServerResponse) => {
    const send = (status: number, value: unknown) => {
      response.writeHead(status, { 'content-type': 'application/json' });
      response.end(JSON.stringify(value));
    };
    const url = new URL(request.url ?? '/', 'http://127.0.0.1');
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
        source: { kind: 'workspace', enabled: true },
      });
      return;
    }
    if (request.method === 'POST' && url.pathname === '/bridge/v1/triggers') {
      const body = (await readBody(request)) as BridgeTriggerInput;
      calls.push({ token: request.headers.authorization, body });
      send(200, {
        result: 'stored',
        triggerId: body.triggerId,
        baseCommit: 'a'.repeat(40),
        ticketId: null,
      });
      return;
    }
    if (url.pathname === '/bridge/v1/events') {
      send(200, { events: [], next: 0, hasMore: false });
      return;
    }
    send(404, { error: { code: 'not_found', message: 'route not found' } });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('fake bridge did not listen');
  return {
    url: `http://127.0.0.1:${address.port}`,
    calls,
    server,
    close: () => new Promise<void>((resolve) => server.close(() => resolve())),
  };
}

async function waitFor(predicate: () => boolean, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error('condition timed out');
}

describe('multi-repository dispatcher isolation on real PostgreSQL', () => {
  let testDatabase: TestDatabase;
  let database: Database;
  let bridgeA: FakeBridge;
  let bridgeB: FakeBridge;
  let tempDir: string;
  let ownerId: string;
  let repositoryAId: string;
  let repositoryBId: string;
  let config: ReturnType<typeof loadDispatcherConfig>;

  beforeAll(async () => {
    testDatabase = await createTestDatabase();
    database = testDatabase.db;
    const [owner] = await database
      .insert(owners)
      .values({ displayName: 'Multi-repo Owner' })
      .returning();
    if (!owner) throw new Error('owner insert failed');
    ownerId = owner.id;
    bridgeA = await startBridge();
    bridgeB = await startBridge();
    tempDir = mkdtempSync(join(tmpdir(), 'moyu-m5-dispatcher-'));
    const insertRepository = async (fullName: string, bridge: FakeBridge) => {
      const [row] = await database
        .insert(repositories)
        .values({
          ownerId,
          fullName,
          defaultBranch: 'main',
          bridgeUrl: bridge.url,
          bridgeCredentialRef: 'placeholder',
          status: 'active',
        })
        .returning();
      if (!row) throw new Error('repository insert failed');
      return row;
    };
    const repositoryA = await insertRepository('Bouyer12/m5a-alpha', bridgeA);
    const repositoryB = await insertRepository('Bouyer12/m5a-beta', bridgeB);
    repositoryAId = repositoryA.id;
    repositoryBId = repositoryB.id;
    const tokenA = 'bridge-token-a-0123456789abcdef';
    const tokenB = 'bridge-token-b-0123456789abcdef';
    for (const [index, [repository, bridge, token]] of (
      [
        [repositoryA, bridgeA, tokenA],
        [repositoryB, bridgeB, tokenB],
      ] as const
    ).entries()) {
      const name = `${repository.id}.json`;
      const path = join(tempDir, name);
      writeFileSync(path, JSON.stringify({ bridgeUrl: bridge.url, bridgeToken: token }), {
        mode: 0o600,
      });
      chmodSync(path, 0o600);
      await database
        .update(repositories)
        .set({ bridgeCredentialRef: `file:${name}` })
        .where(eq(repositories.id, repository.id));
      await database.insert(repositoryInstances).values({
        repositoryId: repository.id,
        state: 'online',
        desiredState: 'online',
        reasonCode: 'listening',
        port: 3901 + index * 2,
        bridgePort: 3902 + index * 2,
        pid: 1,
      });
    }
    config = loadDispatcherConfig({
      NODE_ENV: 'test',
      DATABASE_URL: testDatabase.url,
      RUNTIME_CREDENTIALS_DIR: tempDir,
      RECONCILE_ON_START: 'false',
      DELIVERY_INTERVAL_MS: '100',
      EVENT_SYNC_INTERVAL_MS: '100',
      OPERATION_INTERVAL_MS: '100',
      REPORT_CAPTURE_INTERVAL_MS: '500',
      DUPLICATE_INTERVAL_MS: '200',
    });
  }, 60_000);

  afterAll(async () => {
    await bridgeA?.close();
    await bridgeB?.close();
    await testDatabase?.drop();
    if (tempDir) rmSync(tempDir, { recursive: true, force: true });
  });

  async function seedMessage(
    repositoryId: string,
    text: string,
    clientMessageId: string,
  ): Promise<string> {
    const [conversation] = await database
      .insert(conversations)
      .values({
        ownerId,
        repositoryId,
        mode: 'work',
        executionMode: 'execute',
        baseBranch: 'main',
        clientRequestId: randomUUID(),
        source: 'web',
      })
      .returning();
    if (!conversation) throw new Error('conversation insert failed');
    await database.insert(taskLinks).values({
      conversationId: conversation.id,
      repositoryId,
      state: 'queued',
      baseBranch: 'main',
    });
    const messageId = randomUUID();
    await database.insert(messages).values({
      id: messageId,
      conversationId: conversation.id,
      direction: 'inbound',
      kind: 'user',
      clientMessageId,
      body: text,
      source: 'web',
    });
    await database
      .insert(deliveries)
      .values({ messageId, upstreamTriggerId: `workspace:${messageId}` });
    return messageId;
  }

  it('delivers each message only to its own repository Bridge with its own token', async () => {
    await seedMessage(repositoryAId, 'alpha-only-message', 'm5-a-msg');
    await seedMessage(repositoryBId, 'beta-only-message', 'm5-b-msg');
    const controller = new AbortController();
    const running = runDispatcher(config, { signal: controller.signal });
    try {
      await waitFor(() => bridgeA.calls.length >= 1 && bridgeB.calls.length >= 1, 15_000);
    } finally {
      controller.abort();
      await running;
    }
    expect(bridgeA.calls).toHaveLength(1);
    expect(bridgeB.calls).toHaveLength(1);
    expect(bridgeA.calls[0]!.body.text).toBe('alpha-only-message');
    expect(bridgeB.calls[0]!.body.text).toBe('beta-only-message');
    expect(bridgeA.calls[0]!.token).toContain('bridge-token-a-');
    expect(bridgeB.calls[0]!.token).toContain('bridge-token-b-');
    expect(bridgeA.calls[0]!.body.executionSpec.repository).toBe('Bouyer12/m5a-alpha');
    expect(bridgeB.calls[0]!.body.executionSpec.repository).toBe('Bouyer12/m5a-beta');
  }, 20_000);

  it('stops a repository loop once the supervisor marks it sleeping', async () => {
    await database
      .update(repositoryInstances)
      .set({ state: 'sleeping', desiredState: 'sleeping' })
      .where(eq(repositoryInstances.repositoryId, repositoryAId));
    const beforeA = bridgeA.calls.length;
    await seedMessage(repositoryAId, 'alpha-while-sleeping', 'm5-a-sleeping');
    const controller = new AbortController();
    const running = runDispatcher(config, { signal: controller.signal });
    await new Promise((resolve) => setTimeout(resolve, 1200));
    controller.abort();
    await running;
    expect(bridgeA.calls.length).toBe(beforeA);
  }, 20_000);
});

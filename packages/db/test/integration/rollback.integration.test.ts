import { spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import { sql } from 'drizzle-orm';

import type { Database } from '../../src/client';
import { migrateDatabase } from '../../src/migrate';
import { messages, owners } from '../../src/schema';
import { createTestDatabaseUpTo, type TestDatabase } from './helpers';

const REPOSITORY_ROOT = fileURLToPath(new URL('../../../../', import.meta.url));
const FORWARD_MIGRATION = path.join(
  REPOSITORY_ROOT,
  'packages/db/migrations/0004_nappy_yellowjacket.sql',
);
const ROLLBACK_SCRIPT = path.join(
  REPOSITORY_ROOT,
  'packages/db/migrations/rollback/0004_nappy_yellowjacket.down.sql',
);

let forwardMigrationHash = '';
const databases: TestDatabase[] = [];

async function createDatabase(): Promise<TestDatabase> {
  // The 0004 rollback only applies while 0004 is the latest migration, so the
  // suite builds a database at 0004 and lets 0005 run later through migrate.
  const database = await createTestDatabaseUpTo(4);
  databases.push(database);
  return database;
}

afterEach(async () => {
  while (databases.length > 0) {
    const database = databases.pop();
    if (database) {
      await database.drop();
    }
  }
});

function runRollback(database: TestDatabase, hash?: string) {
  const args = [database.url, '-v', 'ON_ERROR_STOP=1'];
  if (hash !== undefined) {
    args.push('-v', `hash=${hash}`);
  }
  args.push('-f', ROLLBACK_SCRIPT);
  const result = spawnSync('psql', args, { encoding: 'utf8' });
  if (result.error) {
    throw new Error(`psql is required for the rollback integration tests: ${result.error.message}`);
  }
  return {
    status: result.status,
    stdout: result.stdout ?? '',
    stderr: result.stderr ?? '',
  };
}

interface IntegrityState {
  constraints: number;
  indexes: number;
  migrations: number;
  messages: number;
  taskLinks: number;
}

async function readState(database: TestDatabase): Promise<IntegrityState> {
  const [constraints, indexes, migrations, messageCount, taskLinkCount] = await Promise.all([
    database.pool.query(
      `select count(*)::int as count from pg_constraint
        where conname in ('messages_reply_same_conversation_fk','task_links_conversation_repository_fk')`,
    ),
    database.pool.query(
      `select count(*)::int as count from pg_indexes
        where indexname in ('messages_id_conversation_uq','conversations_id_repository_uq')`,
    ),
    database.pool.query('select count(*)::int as count from drizzle.__drizzle_migrations'),
    database.pool.query('select count(*)::int as count from messages'),
    database.pool.query('select count(*)::int as count from task_links'),
  ]);
  return {
    constraints: constraints.rows[0]?.count ?? -1,
    indexes: indexes.rows[0]?.count ?? -1,
    migrations: migrations.rows[0]?.count ?? -1,
    messages: messageCount.rows[0]?.count ?? -1,
    taskLinks: taskLinkCount.rows[0]?.count ?? -1,
  };
}

async function seedBusinessData(database: Database): Promise<void> {
  const [owner] = await database
    .insert(owners)
    .values({ displayName: `Rollback Owner ${randomUUID()}` })
    .returning();
  if (!owner) {
    throw new Error('owner insert returned no row');
  }
  // This database intentionally predates later migrations; use the columns
  // that schema actually has instead of a current-schema insert.
  const repositoryResult = await database.execute(
    sql`insert into repositories (owner_id, full_name, status)
        values (${owner.id}, ${`sutrasky/rollback-${randomUUID()}`}, 'active') returning id`,
  );
  const repositoryId = repositoryResult.rows[0]?.id;
  if (typeof repositoryId !== 'string') {
    throw new Error('repository insert returned no row');
  }
  const conversationResult = await database.execute(
    sql`insert into conversations (owner_id, repository_id, base_branch)
        values (${owner.id}, ${repositoryId}, 'main') returning id`,
  );
  const conversationId = conversationResult.rows[0]?.id;
  if (typeof conversationId !== 'string') {
    throw new Error('conversation insert returned no row');
  }
  const [parent] = await database
    .insert(messages)
    .values({
      conversationId: conversationId,
      direction: 'inbound',
      body: 'parent',
      source: 'web',
    })
    .returning({ id: messages.id });
  if (!parent) {
    throw new Error('message insert returned no row');
  }
  await database.insert(messages).values({
    conversationId: conversationId,
    direction: 'inbound',
    body: 'reply',
    source: 'web',
    replyToMessageId: parent.id,
  });
  await database.execute(
    sql`insert into task_links (conversation_id, repository_id, state, base_branch)
        values (${conversationId}, ${repositoryId}, 'pending_admission', 'main')`,
  );
}

describe('0004 rollback script on real PostgreSQL', () => {
  beforeAll(async () => {
    forwardMigrationHash = createHash('sha256')
      .update(await readFile(FORWARD_MIGRATION))
      .digest('hex');
  });

  it('rolls back with the correct hash, keeps business data and re-applies the constraints', async () => {
    const database = await createDatabase();
    await seedBusinessData(database.db);
    expect(await readState(database)).toMatchObject({
      constraints: 2,
      indexes: 2,
      migrations: 5,
      messages: 2,
      taskLinks: 1,
    });

    const rollback = runRollback(database, forwardMigrationHash);
    expect(rollback.stderr).toBe('');
    expect(rollback.status).toBe(0);
    expect(await readState(database)).toMatchObject({
      constraints: 0,
      indexes: 0,
      migrations: 4,
      messages: 2,
      taskLinks: 1,
    });

    await migrateDatabase(database.db);
    expect(await readState(database)).toMatchObject({
      constraints: 2,
      indexes: 2,
      migrations: 15,
      messages: 2,
      taskLinks: 1,
    });
  }, 30_000);

  it('rejects a wrong hash and keeps the schema, migration history and data', async () => {
    const database = await createDatabase();
    await seedBusinessData(database.db);

    const rollback = runRollback(database, '0'.repeat(64));
    expect(rollback.status).not.toBe(0);
    expect(rollback.stderr).toContain('does not match 0004_nappy_yellowjacket.sql');
    expect(await readState(database)).toMatchObject({
      constraints: 2,
      indexes: 2,
      migrations: 5,
      messages: 2,
      taskLinks: 1,
    });
  }, 30_000);

  it('rejects a missing hash and keeps the schema and migration history', async () => {
    const database = await createDatabase();

    const rollback = runRollback(database);
    expect(rollback.status).not.toBe(0);
    expect(rollback.stderr).toContain('pass -v hash=');
    expect(await readState(database)).toMatchObject({
      constraints: 2,
      indexes: 2,
      migrations: 5,
    });
  }, 30_000);

  it('rejects a hash whose migration record is no longer the latest applied row', async () => {
    const database = await createDatabase();
    await database.pool.query(
      `insert into drizzle.__drizzle_migrations (hash, created_at) values ('a-newer-migration', 9999999999999)`,
    );

    const rollback = runRollback(database, forwardMigrationHash);
    expect(rollback.status).not.toBe(0);
    expect(rollback.stderr).toContain('is not the latest applied migration');
    expect(await readState(database)).toMatchObject({
      constraints: 2,
      indexes: 2,
      migrations: 6,
    });
  }, 30_000);

  it('rolls back all DDL when a later statement in the same transaction fails', async () => {
    const database = await createDatabase();
    await seedBusinessData(database.db);
    await database.pool.query(
      `create table rollback_dependency (
         message_id uuid,
         conversation_id uuid,
         constraint rollback_dependency_fk
           foreign key (message_id, conversation_id) references messages (id, conversation_id)
       )`,
    );

    const failed = runRollback(database, forwardMigrationHash);
    expect(failed.status).not.toBe(0);
    expect(failed.stderr).toContain('cannot drop index messages_id_conversation_uq');
    expect(await readState(database)).toMatchObject({
      constraints: 2,
      indexes: 2,
      migrations: 5,
      messages: 2,
      taskLinks: 1,
    });
    const [dependency] = await database.pool
      .query(
        `select count(*)::int as count from pg_constraint where conname = 'rollback_dependency_fk'`,
      )
      .then((result) => result.rows);
    expect(dependency?.count).toBe(1);

    await database.pool.query('drop table rollback_dependency');
    const recovered = runRollback(database, forwardMigrationHash);
    expect(recovered.status).toBe(0);
    expect(await readState(database)).toMatchObject({
      constraints: 0,
      indexes: 0,
      migrations: 4,
      messages: 2,
      taskLinks: 1,
    });
  }, 30_000);
});

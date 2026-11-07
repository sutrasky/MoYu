import { spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { sql } from 'drizzle-orm';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import { migrateDatabase } from '../../src/migrate';
import type { Database } from '../../src/client';
import { createTestDatabaseUpTo, type TestDatabase } from './helpers';

const REPOSITORY_ROOT = fileURLToPath(new URL('../../../../', import.meta.url));
const FORWARD_MIGRATION = path.join(
  REPOSITORY_ROOT,
  'packages/db/migrations/0005_colossal_brood.sql',
);
const ROLLBACK_SCRIPT = path.join(
  REPOSITORY_ROOT,
  'packages/db/migrations/rollback/0005_colossal_brood.down.sql',
);

let forwardMigrationHash = '';
const databases: TestDatabase[] = [];

async function createDatabase(): Promise<TestDatabase> {
  const database = await createTestDatabaseUpTo(5);
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

interface M4State {
  migrations: number;
  conversations: number;
  taskLinks: number;
  operationsTable: boolean;
  conversationSource: boolean;
  taskPlanRevision: boolean;
  taskOperationKindEnum: boolean;
}

async function readState(database: TestDatabase): Promise<M4State> {
  const count = (table: string) =>
    database.pool
      .query(`select count(*)::int as count from ${table}`)
      .then((result) => Number(result.rows[0]?.count ?? -1));
  const column = (table: string, name: string) =>
    database.pool
      .query(
        `select 1 from information_schema.columns
          where table_schema = 'public' and table_name = $1 and column_name = $2`,
        [table, name],
      )
      .then((result) => (result.rowCount ?? 0) > 0);
  const table = (name: string) =>
    database.pool
      .query(
        `select 1 from information_schema.tables where table_schema = 'public' and table_name = $1`,
        [name],
      )
      .then((result) => (result.rowCount ?? 0) > 0);
  const enumType = (name: string) =>
    database.pool
      .query(`select 1 from pg_type where typname = $1 and typtype = 'e'`, [name])
      .then((result) => (result.rowCount ?? 0) > 0);
  return {
    migrations: await count('drizzle.__drizzle_migrations'),
    conversations: await count('conversations'),
    taskLinks: await count('task_links'),
    operationsTable: await table('task_operations'),
    conversationSource: await column('conversations', 'source'),
    taskPlanRevision: await column('task_links', 'plan_revision'),
    taskOperationKindEnum: await enumType('task_operation_kind'),
  };
}

async function seed(database: Database): Promise<void> {
  const owner = await database.execute(
    sql`insert into owners (display_name) values (${`Rollback M4 ${randomUUID()}`}) returning id`,
  );
  const ownerId = String(owner.rows[0]?.id ?? '');
  const repository = await database.execute(
    sql`insert into repositories (owner_id, full_name, status)
        values (${ownerId}, ${`Bouyer12/m4-rollback-${randomUUID()}`}, 'active') returning id`,
  );
  const repositoryId = String(repository.rows[0]?.id ?? '');
  const conversation = await database.execute(
    sql`insert into conversations
          (owner_id, repository_id, mode, execution_mode, source, external_key, base_branch, plan_revision)
        values (${ownerId}, ${repositoryId}, 'work', 'execute', 'github', 'github:9', 'main', 2)
        returning id`,
  );
  const conversationId = String(conversation.rows[0]?.id ?? '');
  const task = await database.execute(
    sql`insert into task_links
          (conversation_id, repository_id, upstream_ticket_id, state, base_branch,
           plan_revision, stopped_at, stop_evidence)
        values (${conversationId}, ${repositoryId}, 'ticket-9', 'stopped', 'main',
                2, now(), '{"agentStatus":"idle"}'::jsonb)
        returning id`,
  );
  const taskId = String(task.rows[0]?.id ?? '');
  await database.execute(
    sql`insert into task_operations (task_id, repository_id, kind, client_command_id, request_fingerprint)
        values (${taskId}, ${repositoryId}, 'pause', 'cmd-9', 'fingerprint')`,
  );
}

describe('0005 rollback script on real PostgreSQL', () => {
  beforeAll(async () => {
    forwardMigrationHash = createHash('sha256')
      .update(await readFile(FORWARD_MIGRATION))
      .digest('hex');
  });

  it('rolls back M4 DDL with the correct hash, keeps business rows, and re-applies the migration', async () => {
    const database = await createDatabase();
    await seed(database.db);
    expect(await readState(database)).toMatchObject({
      migrations: 6,
      conversations: 1,
      taskLinks: 1,
      operationsTable: true,
      conversationSource: true,
      taskPlanRevision: true,
      taskOperationKindEnum: true,
    });

    const rollback = runRollback(database, forwardMigrationHash);
    expect(rollback.stderr).toBe('');
    expect(rollback.status).toBe(0);
    expect(await readState(database)).toMatchObject({
      migrations: 5,
      conversations: 1,
      taskLinks: 1,
      operationsTable: false,
      conversationSource: false,
      taskPlanRevision: false,
      taskOperationKindEnum: false,
    });

    await migrateDatabase(database.db);
    expect(await readState(database)).toMatchObject({
      migrations: 15,
      conversations: 1,
      taskLinks: 1,
      operationsTable: true,
      conversationSource: true,
      taskPlanRevision: true,
      taskOperationKindEnum: true,
    });
  }, 30_000);

  it('rejects a wrong hash without changing the schema or data', async () => {
    const database = await createDatabase();
    await seed(database.db);
    const before = await readState(database);

    const rollback = runRollback(database, '0'.repeat(64));
    expect(rollback.status).not.toBe(0);
    expect(rollback.stderr).toContain('does not match 0005_colossal_brood.sql');
    expect(await readState(database)).toEqual(before);
  }, 30_000);

  it('rejects a missing hash and keeps the M4 schema', async () => {
    const database = await createDatabase();
    const rollback = runRollback(database);
    expect(rollback.status).not.toBe(0);
    expect(rollback.stderr).toContain('pass -v hash=');
    expect(await readState(database)).toMatchObject({
      migrations: 6,
      operationsTable: true,
      conversationSource: true,
    });
  }, 30_000);
});

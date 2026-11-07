import { spawnSync } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { sql } from 'drizzle-orm';
import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import { migrateDatabase } from '../../src/migrate';
import { createTestDatabaseUpTo, type TestDatabase } from './helpers';

const ROOT = fileURLToPath(new URL('../../../../', import.meta.url));
const FORWARD = path.join(ROOT, 'packages/db/migrations/0006_aspiring_fenris.sql');
const ROLLBACK = path.join(ROOT, 'packages/db/migrations/rollback/0006_aspiring_fenris.down.sql');
let hash = '';
const databases: TestDatabase[] = [];

const createDatabase = async (): Promise<TestDatabase> => {
  const database = await createTestDatabaseUpTo(6);
  databases.push(database);
  return database;
};

const runRollback = (database: TestDatabase, supplied?: string) =>
  spawnSync(
    'psql',
    [
      database.url,
      '-v',
      'ON_ERROR_STOP=1',
      ...(supplied ? ['-v', `hash=${supplied}`] : []),
      '-f',
      ROLLBACK,
    ],
    { encoding: 'utf8' },
  );

const seed = async (database: TestDatabase): Promise<void> => {
  const owner = await database.db.execute(
    sql`insert into owners (display_name) values (${`Rollback 0006 ${randomUUID()}`}) returning id`,
  );
  const ownerId = String(owner.rows[0]?.id ?? '');
  const repository = await database.db.execute(
    sql`insert into repositories (owner_id, full_name, status) values (${ownerId}, ${`Bouyer12/rollback-6-${randomUUID()}`}, 'active') returning id`,
  );
  const repositoryId = String(repository.rows[0]?.id ?? '');
  const conversation = await database.db.execute(
    sql`insert into conversations (owner_id, repository_id, mode, base_branch) values (${ownerId}, ${repositoryId}, 'work', 'main') returning id`,
  );
  const task = await database.db.execute(
    sql`insert into task_links (conversation_id, repository_id, state, base_branch) values (${String(conversation.rows[0]?.id)}, ${repositoryId}, 'running', 'main') returning id`,
  );
  await database.db.execute(
    sql`insert into duplicate_tickets (repository_id, task_id, ticket_id) values (${repositoryId}, ${String(task.rows[0]?.id)}, 'ticket-duplicate')`,
  );
};

const tableExists = async (database: TestDatabase): Promise<boolean> => {
  const result = await database.db.execute(
    sql`select 1 from information_schema.tables where table_schema = 'public' and table_name = 'duplicate_tickets'`,
  );
  return (result.rows.length ?? 0) > 0;
};

const migrationCount = async (database: TestDatabase): Promise<number> => {
  const result = await database.db.execute(
    sql`select count(*)::int as count from drizzle.__drizzle_migrations`,
  );
  return Number(result.rows[0]?.count ?? -1);
};

afterEach(async () => {
  while (databases.length > 0) await databases.pop()?.drop();
});

describe('0006 rollback script on real PostgreSQL', () => {
  beforeAll(async () => {
    hash = createHash('sha256')
      .update(await readFile(FORWARD))
      .digest('hex');
  });

  it('drops the durable duplicate queue and re-applies it', async () => {
    const database = await createDatabase();
    await seed(database);
    expect(await tableExists(database)).toBe(true);
    expect(await migrationCount(database)).toBe(7);

    const rollback = runRollback(database, hash);
    expect(rollback.stderr).toBe('');
    expect(rollback.status).toBe(0);
    expect(await tableExists(database)).toBe(false);
    expect(await migrationCount(database)).toBe(6);

    await migrateDatabase(database.db);
    expect(await tableExists(database)).toBe(true);
    expect(await migrationCount(database)).toBe(15);
  }, 30_000);

  it('rejects a missing or wrong hash and keeps the table', async () => {
    const database = await createDatabase();
    expect(runRollback(database).status).not.toBe(0);
    expect(runRollback(database, '0'.repeat(64)).status).not.toBe(0);
    expect(await tableExists(database)).toBe(true);
  }, 30_000);
});

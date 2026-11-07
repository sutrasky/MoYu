import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import { migrateDatabase } from '../../src/migrate';
import { createTestDatabaseUpTo, type TestDatabase } from './helpers';

const ROOT = fileURLToPath(new URL('../../../../', import.meta.url));
const FORWARD = path.join(ROOT, 'packages/db/migrations/0009_far_typhoid_mary.sql');
const ROLLBACK = path.join(ROOT, 'packages/db/migrations/rollback/0009_far_typhoid_mary.down.sql');
let hash = '';
const databases: TestDatabase[] = [];

const createDatabase = async (): Promise<TestDatabase> => {
  // 0009's rollback is only legal while 0009 is the latest applied migration.
  const database = await createTestDatabaseUpTo(9);
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

const read = async (database: TestDatabase) => {
  const table = await database.pool.query(
    `select count(*)::int as count from information_schema.tables
       where table_schema = 'public' and table_name = 'repository_preferences'`,
  );
  const index = await database.pool.query(
    `select count(*)::int as count from pg_indexes
       where schemaname = 'public' and indexname = 'messages_conversation_created_id_idx'`,
  );
  const ledger = await database.pool.query(
    'select count(*)::int as count from drizzle.__drizzle_migrations where hash = $1',
    [hash],
  );
  const total = await database.pool.query(
    'select count(*)::int as count from drizzle.__drizzle_migrations',
  );
  return {
    table: Number(table.rows[0]?.count ?? -1),
    index: Number(index.rows[0]?.count ?? -1),
    count: Number(ledger.rows[0]?.count ?? -1),
    total: Number(total.rows[0]?.count ?? -1),
  };
};

afterEach(async () => {
  while (databases.length > 0) await databases.pop()?.drop();
});

describe('0009 rollback script on real PostgreSQL', () => {
  beforeAll(async () => {
    hash = createHash('sha256')
      .update(await readFile(FORWARD))
      .digest('hex');
  });

  it('drops repository_preferences and the keyset index, then re-applies both', async () => {
    const database = await createDatabase();
    expect(await read(database)).toMatchObject({
      table: 1,
      index: 1,
      count: 1,
      total: 10,
    });

    const rollback = runRollback(database, hash);
    expect(rollback.stderr).toBe('');
    expect(rollback.status).toBe(0);
    expect(await read(database)).toMatchObject({
      table: 0,
      index: 0,
      count: 0,
      total: 9,
    });

    await migrateDatabase(database.db);
    expect(await read(database)).toMatchObject({
      table: 1,
      index: 1,
      count: 1,
      total: 15,
    });
  }, 30_000);

  it('refuses wrong, missing and non-latest hashes without changing schema or ledger', async () => {
    const wrong = await createDatabase();
    expect(runRollback(wrong, '0'.repeat(64)).status).not.toBe(0);
    expect(await read(wrong)).toMatchObject({ table: 1, index: 1, count: 1, total: 10 });
    expect(runRollback(wrong).status).not.toBe(0);
    expect(await read(wrong)).toMatchObject({ table: 1, index: 1, count: 1, total: 10 });

    const newer = await createDatabase();
    await newer.pool.query(
      "insert into drizzle.__drizzle_migrations (hash, created_at) values ('newer-than-0009', 9999999999999)",
    );
    const rejected = runRollback(newer, hash);
    expect(rejected.status).not.toBe(0);
    expect(rejected.stderr).toContain('not the latest applied migration');
    expect(await read(newer)).toMatchObject({ table: 1, index: 1, count: 1, total: 11 });
  }, 30_000);
});

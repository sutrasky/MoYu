import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import { migrateDatabase } from '../../src/migrate';
import { createTestDatabaseUpTo, type TestDatabase } from './helpers';

const ROOT = fileURLToPath(new URL('../../../../', import.meta.url));
const ROLLBACK = path.join(ROOT, 'packages/db/migrations/rollback/0012_fast_marvel_apes.down.sql');
let hash = '';
const databases: TestDatabase[] = [];

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
  const indexes = await database.pool.query<{ count: number }>(
    `select count(*)::int as count from pg_indexes
      where tablename = 'repository_instances'
        and indexname in ('repository_instances_profile_uq','repository_instances_port_uq','repository_instances_bridge_port_uq')`,
  );
  const ledger = await database.pool.query<{ count: number }>(
    'select count(*)::int as count from drizzle.__drizzle_migrations where hash = $1',
    [hash],
  );
  return {
    indexes: Number(indexes.rows[0]?.count ?? -1),
    ledger: Number(ledger.rows[0]?.count ?? -1),
  };
};

afterEach(async () => {
  while (databases.length > 0) await databases.pop()?.drop();
});

describe('0012 rollback script on real PostgreSQL', () => {
  beforeAll(async () => {
    hash = createHash('sha256')
      .update(await readFile(path.join(ROOT, 'packages/db/migrations/0012_fast_marvel_apes.sql')))
      .digest('hex');
  });

  it('removes the allocation indexes and re-applies them', async () => {
    const database = await createTestDatabaseUpTo(12);
    databases.push(database);
    expect(await read(database)).toEqual({ indexes: 3, ledger: 1 });

    const wrong = runRollback(database, '0'.repeat(64));
    expect(wrong.status).not.toBe(0);
    expect(await read(database)).toEqual({ indexes: 3, ledger: 1 });

    const rollback = runRollback(database, hash);
    expect(rollback.stderr).toBe('');
    expect(rollback.status).toBe(0);
    expect(await read(database)).toEqual({ indexes: 0, ledger: 0 });

    await migrateDatabase(database.db);
    expect(await read(database)).toEqual({ indexes: 3, ledger: 1 });
  }, 30_000);
});

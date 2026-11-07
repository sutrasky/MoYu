import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { afterEach, beforeAll, describe, expect, it } from 'vitest';

import { migrateDatabase } from '../../src/migrate';
import { createTestDatabaseUpTo, type TestDatabase } from './helpers';

const ROOT = fileURLToPath(new URL('../../../../', import.meta.url));
const ROLLBACK = path.join(
  ROOT,
  'packages/db/migrations/rollback/0013_loving_quentin_quire.down.sql',
);
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
  const tables = await database.pool.query<{ count: number }>(
    `select count(*)::int as count from information_schema.tables
      where table_schema = 'public'
        and table_name in ('runtime_lease_events','runtime_resource_samples')`,
  );
  const columns = await database.pool.query<{ count: number }>(
    `with added(table_name, column_name) as (values
        ('runtime_leases','recovery_state'), ('runtime_leases','stop_proof'),
        ('task_operations','runtime_lease_id'), ('task_operations','fencing_token'),
        ('task_operations','stale_at'), ('deliveries','runtime_lease_id'),
        ('deliveries','fencing_token'), ('repository_instances','memory_current_bytes'),
        ('repository_instances','resource_state'))
     select count(*)::int as count from information_schema.columns c
       join added a on a.table_name = c.table_name and a.column_name = c.column_name`,
  );
  const ledger = await database.pool.query<{ count: number }>(
    'select count(*)::int as count from drizzle.__drizzle_migrations where hash = $1',
    [hash],
  );
  return {
    tables: Number(tables.rows[0]?.count ?? -1),
    columns: Number(columns.rows[0]?.count ?? -1),
    ledger: Number(ledger.rows[0]?.count ?? -1),
  };
};

afterEach(async () => {
  while (databases.length > 0) await databases.pop()?.drop();
});

describe('0013 rollback script on real PostgreSQL', () => {
  beforeAll(async () => {
    hash = createHash('sha256')
      .update(
        await readFile(path.join(ROOT, 'packages/db/migrations/0013_loving_quentin_quire.sql')),
      )
      .digest('hex');
  });

  it('removes the lease/resource schema and re-applies it', async () => {
    const database = await createTestDatabaseUpTo(13);
    databases.push(database);
    expect(await read(database)).toEqual({ tables: 2, columns: 9, ledger: 1 });

    const wrong = runRollback(database, '0'.repeat(64));
    expect(wrong.status).not.toBe(0);
    expect(await read(database)).toEqual({ tables: 2, columns: 9, ledger: 1 });

    const rollback = runRollback(database, hash);
    expect(rollback.stderr).toBe('');
    expect(rollback.status).toBe(0);
    expect(await read(database)).toEqual({ tables: 0, columns: 0, ledger: 0 });

    await migrateDatabase(database.db);
    expect(await read(database)).toEqual({ tables: 2, columns: 9, ledger: 1 });
  }, 30_000);
});

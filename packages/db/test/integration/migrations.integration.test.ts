import { sql } from 'drizzle-orm';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';

import { migrateDatabase } from '../../src/migrate';
import { createTestDatabase, type TestDatabase } from './helpers';

const EXPECTED_TABLES = [
  'artifacts',
  'auth_challenges',
  'bootstrap_codes',
  'conversations',
  'deferred_bridge_events',
  'deliveries',
  'duplicate_tickets',
  'events',
  'execution_reservations',
  'messages',
  'owners',
  'passkey_credentials',
  'repositories',
  'repository_instances',
  'repository_preferences',
  'runtime_cursors',
  'runtime_lease_events',
  'runtime_leases',
  'runtime_resource_samples',
  'sessions',
  'task_links',
  'task_operations',
];

const EXPECTED_ENUMS = [
  'artifact_type',
  'auth_challenge_kind',
  'bootstrap_code_purpose',
  'conversation_mode',
  'conversation_source',
  'delivery_state',
  'execution_mode',
  'message_direction',
  'repository_status',
  'runtime_desired_state',
  'runtime_instance_state',
  'runtime_lease_state',
  'task_operation_kind',
  'task_operation_state',
  'task_state',
];

const EXPECTED_INDEXES = [
  'bootstrap_codes_code_hash_uq',
  'bootstrap_codes_token_hash_uq',
  'deliveries_message_uq',
  'events_seq_uq',
  'conversations_id_repository_uq',
  'conversations_owner_client_request_uq',
  'conversations_merged_into_idx',
  'messages_conversation_client_message_uq',
  'messages_conversation_created_id_idx',
  'messages_id_conversation_uq',
  'messages_conversation_source_event_uq',
  'passkey_credentials_credential_id_uq',
  'runtime_leases_active_repository_uq',
  'runtime_leases_fencing_token_uq',
  'runtime_leases_recovery_idx',
  'runtime_lease_events_repository_idx',
  'runtime_lease_events_lease_idx',
  'runtime_resource_samples_scope_idx',
  'runtime_resource_samples_repository_idx',
  'sessions_token_hash_uq',
  'task_links_conversation_uq',
  'task_links_repository_upstream_ticket_uq',
  'conversations_repository_external_uq',
  'task_operations_task_client_command_uq',
  'task_operations_active_task_uq',
  'task_operations_state_next_attempt_idx',
  'task_operations_repository_idx',
  'duplicate_tickets_repository_ticket_uq',
  'duplicate_tickets_due_idx',
  'repository_preferences_owner_repository_uq',
  'repository_instances_state_idx',
  'repository_instances_wake_idx',
];

async function stringColumn(database: TestDatabase, query: ReturnType<typeof sql>, column: string) {
  const result = await database.db.execute(query);
  return result.rows.map((row) => String(row[column]));
}

describe('initial migration on an empty PostgreSQL database', () => {
  let testDatabase: TestDatabase;

  beforeAll(async () => {
    testDatabase = await createTestDatabase();
  }, 60_000);

  afterAll(async () => {
    if (testDatabase) {
      await testDatabase.drop();
    }
  });

  it('creates all M1, M2 and M3 tables', async () => {
    const tables = await stringColumn(
      testDatabase,
      sql`select table_name from information_schema.tables where table_schema = 'public' order by table_name`,
      'table_name',
    );

    expect(tables).toEqual(EXPECTED_TABLES);
  });

  it('creates the M1 and M2 enums', async () => {
    const enums = await stringColumn(
      testDatabase,
      sql`select typname from pg_type where typtype = 'e' order by typname`,
      'typname',
    );

    expect(enums).toEqual(EXPECTED_ENUMS);
  });

  it('creates the uniqueness and lease indexes used by the acceptance checks', async () => {
    const indexes = await stringColumn(
      testDatabase,
      sql`select indexname from pg_indexes where schemaname = 'public'`,
      'indexname',
    );

    for (const expected of EXPECTED_INDEXES) {
      expect(indexes).toContain(expected);
    }
  });

  it('keeps the clientMessageId index unique and partial', async () => {
    const result = await testDatabase.db.execute(
      sql`select indexdef from pg_indexes where schemaname = 'public' and indexname = 'messages_conversation_client_message_uq'`,
    );
    const definition = String(result.rows[0]?.indexdef ?? '');

    expect(definition).toContain('UNIQUE INDEX');
    expect(definition).toContain('(conversation_id, client_message_id)');
    expect(definition).toContain('WHERE (client_message_id IS NOT NULL)');
  });

  it('keeps the M3 idempotency indexes unique and partial', async () => {
    const rows = await testDatabase.db.execute(
      sql`select indexname, indexdef from pg_indexes where schemaname = 'public'
           and indexname in ('conversations_owner_client_request_uq', 'messages_conversation_source_event_uq')`,
    );
    const definitions = new Map(
      rows.rows.map((row) => [String(row.indexname), String(row.indexdef)]),
    );
    expect(definitions.get('conversations_owner_client_request_uq')).toContain('UNIQUE INDEX');
    expect(definitions.get('conversations_owner_client_request_uq')).toContain(
      '(owner_id, client_request_id)',
    );
    expect(definitions.get('conversations_owner_client_request_uq')).toContain(
      'WHERE (client_request_id IS NOT NULL)',
    );
    expect(definitions.get('messages_conversation_source_event_uq')).toContain('UNIQUE INDEX');
    expect(definitions.get('messages_conversation_source_event_uq')).toContain(
      '(conversation_id, source_event_id)',
    );
  });

  it('is idempotent when the migration folder is applied twice', async () => {
    await migrateDatabase(testDatabase.db);

    const tables = await stringColumn(
      testDatabase,
      sql`select table_name from information_schema.tables where table_schema = 'public' order by table_name`,
      'table_name',
    );

    expect(tables).toEqual(EXPECTED_TABLES);
  });
});

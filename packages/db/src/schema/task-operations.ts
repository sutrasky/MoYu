import {
  bigint,
  check,
  index,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from 'drizzle-orm/pg-core';
import { sql } from 'drizzle-orm';

import { taskOperationKind, taskOperationState } from './enums';
import { repositories } from './repositories';
import { runtimeLeases } from './runtime-leases';
import { taskLinks } from './task-links';

/**
 * One durable owner command for a task (start, pause, resume, retry). The API
 * records the intent and returns; the dispatcher calls the runtime Bridge and
 * stores the operation's evidence. `(task_id, client_command_id)` is the
 * idempotency boundary, and the client command fingerprint rejects a reused id
 * with a different intent.
 */
export const taskOperations = pgTable(
  'task_operations',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    taskId: uuid('task_id')
      .notNull()
      .references(() => taskLinks.id, { onDelete: 'cascade' }),
    repositoryId: uuid('repository_id')
      .notNull()
      .references(() => repositories.id, { onDelete: 'cascade' }),
    kind: taskOperationKind('kind').notNull(),
    state: taskOperationState('state').notNull().default('queued'),
    clientCommandId: text('client_command_id').notNull(),
    /** Hash of the command payload, so a reused id with a different payload is a conflict. */
    requestFingerprint: text('request_fingerprint').notNull(),
    /** The runtime operation id, which Bridge retries reuse for idempotency. */
    bridgeOperationId: text('bridge_operation_id'),
    evidence: jsonb('evidence').$type<Record<string, unknown>>().notNull().default({}),
    error: text('error'),
    attemptCount: integer('attempt_count').notNull().default(0),
    nextAttemptAt: timestamp('next_attempt_at', { withTimezone: true }).notNull().defaultNow(),
    leaseOwner: text('lease_owner'),
    leaseExpiresAt: timestamp('lease_expires_at', { withTimezone: true }),
    /** Execution lease the command was sent under. */
    runtimeLeaseId: uuid('runtime_lease_id').references(() => runtimeLeases.id, {
      onDelete: 'set null',
    }),
    /** Fencing token of that lease; a result carrying an older generation is rejected. */
    fencingToken: bigint('fencing_token', { mode: 'bigint' }),
    /** Set when a late runtime result was refused because its lease generation was stale. */
    staleAt: timestamp('stale_at', { withTimezone: true }),
    staleReason: text('stale_reason'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex('task_operations_task_client_command_uq').on(table.taskId, table.clientCommandId),
    // At most one owner command may be in flight per task; the database rejects
    // a second stop/start/resume/retry while the first is queued or running.
    uniqueIndex('task_operations_active_task_uq')
      .on(table.taskId)
      .where(sql`${table.state} in ('queued', 'running')`),
    index('task_operations_state_next_attempt_idx').on(table.state, table.nextAttemptAt),
    index('task_operations_repository_idx').on(table.repositoryId, table.state),
    check('task_operations_attempt_count_nonnegative', sql`${table.attemptCount} >= 0`),
  ],
);

export type TaskOperation = typeof taskOperations.$inferSelect;
export type NewTaskOperation = typeof taskOperations.$inferInsert;

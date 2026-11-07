import { sql } from 'drizzle-orm';
import {
  bigint,
  bigserial,
  index,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from 'drizzle-orm/pg-core';

import { runtimeLeaseState } from './enums';
import { repositories } from './repositories';

/**
 * Execution admission lease. At most one active lease may exist per repository;
 * `fencingToken` increases monotonically and is persisted on every command and
 * receipt sent under the lease. A stale token is rejected rather than applied.
 *
 * An expired lease is not reusable on its own: `recoveryState` tracks whether a
 * replacement has been proven safe. `stopProof` records the evidence that the
 * old execution and its managed descendants actually stopped (runtime stop
 * evidence, cgroup empty, ports closed); without it a new lease cannot start.
 */
export const runtimeLeases = pgTable(
  'runtime_leases',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    repositoryId: uuid('repository_id')
      .notNull()
      .references(() => repositories.id, { onDelete: 'cascade' }),
    holderId: text('holder_id').notNull(),
    /** Monotonic execution generation; the fencing token checked by the platform. */
    fencingToken: bigserial('fencing_token', { mode: 'bigint' }).notNull(),
    state: runtimeLeaseState('state').notNull().default('active'),
    /**
     * `none`: a normal live lease.
     * `required`: expired or orphaned; the old execution must be proven stopped.
     * `verifying`: recovery is in progress (a pause/stop proof was requested).
     * `blocked`: recovery failed or is unknown; no replacement may start.
     * `recovered`: the old execution is proven stopped; the row can expire/release.
     */
    recoveryState: text('recovery_state').notNull().default('none'),
    recoveryReason: text('recovery_reason'),
    /** What a blocked recovery is waiting for, for example `execution_stop_proof`. */
    waitingFor: text('waiting_for'),
    /** Sanitised evidence that the lease's execution stopped. Never raw credentials. */
    stopProof: jsonb('stop_proof').$type<Record<string, unknown>>().notNull().default({}),
    stopProvenAt: timestamp('stop_proven_at', { withTimezone: true }),
    /** Number of late receipts rejected for this lease generation. */
    staleReceiptCount: bigint('stale_receipt_count', { mode: 'number' }).notNull().default(0),
    lastError: text('last_error'),
    acquiredAt: timestamp('acquired_at', { withTimezone: true }).notNull().defaultNow(),
    heartbeatAt: timestamp('heartbeat_at', { withTimezone: true }).notNull().defaultNow(),
    expiresAt: timestamp('expires_at', { withTimezone: true }).notNull(),
    releasedAt: timestamp('released_at', { withTimezone: true }),
  },
  (table) => [
    uniqueIndex('runtime_leases_fencing_token_uq').on(table.fencingToken),
    uniqueIndex('runtime_leases_active_repository_uq')
      .on(table.repositoryId)
      .where(sql`${table.state} = 'active'`),
    index('runtime_leases_expires_at_idx').on(table.expiresAt),
    index('runtime_leases_recovery_idx').on(table.state, table.recoveryState),
  ],
);

export type RuntimeLease = typeof runtimeLeases.$inferSelect;
export type NewRuntimeLease = typeof runtimeLeases.$inferInsert;

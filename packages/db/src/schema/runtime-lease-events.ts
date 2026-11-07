import { bigint, index, jsonb, pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core';

import { repositories } from './repositories';
import { runtimeLeases } from './runtime-leases';

/**
 * Append-only audit of lease generation changes. A late receipt or a rejected
 * takeover is recorded here; the task/operation state is never rewritten from it.
 */
export const runtimeLeaseEvents = pgTable(
  'runtime_lease_events',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    leaseId: uuid('lease_id').references(() => runtimeLeases.id, { onDelete: 'set null' }),
    repositoryId: uuid('repository_id')
      .notNull()
      .references(() => repositories.id, { onDelete: 'cascade' }),
    fencingToken: bigint('fencing_token', { mode: 'bigint' }),
    type: text('type').notNull(),
    detail: jsonb('detail').$type<Record<string, unknown>>().notNull().default({}),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    index('runtime_lease_events_repository_idx').on(table.repositoryId, table.createdAt),
    index('runtime_lease_events_lease_idx').on(table.leaseId, table.createdAt),
  ],
);

export type RuntimeLeaseEvent = typeof runtimeLeaseEvents.$inferSelect;
export type NewRuntimeLeaseEvent = typeof runtimeLeaseEvents.$inferInsert;

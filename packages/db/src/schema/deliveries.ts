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

import { deliveryState } from './enums';
import { messages } from './messages';
import { runtimeLeases } from './runtime-leases';

/**
 * Durable outbox entry for a message. It is written in the same transaction as
 * the message so an accepted message cannot exist without a delivery record.
 */
export const deliveries = pgTable(
  'deliveries',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    messageId: uuid('message_id')
      .notNull()
      .references(() => messages.id, { onDelete: 'cascade' }),
    state: deliveryState('state').notNull().default('pending'),
    attemptCount: integer('attempt_count').notNull().default(0),
    nextAttemptAt: timestamp('next_attempt_at', { withTimezone: true }).notNull().defaultNow(),
    leaseOwner: text('lease_owner'),
    leaseExpiresAt: timestamp('lease_expires_at', { withTimezone: true }),
    /** The stable runtime Trigger id used for the delivery; retries reuse it. */
    upstreamTriggerId: text('upstream_trigger_id'),
    /** Execution lease that admitted the trigger launch/reply, and its fencing generation. */
    runtimeLeaseId: uuid('runtime_lease_id').references(() => runtimeLeases.id, {
      onDelete: 'set null',
    }),
    fencingToken: bigint('fencing_token', { mode: 'bigint' }),
    /** When the runtime recorded the Trigger (executor received it). Never set by an HTTP 202 alone. */
    deliveredAt: timestamp('delivered_at', { withTimezone: true }),
    /** When the runtime handed the message to the worker Session inbox. */
    handedToWorkerAt: timestamp('handed_to_worker_at', { withTimezone: true }),
    /** When the worker Session history proved the model could read the message. */
    readAt: timestamp('read_at', { withTimezone: true }),
    lastAttemptAt: timestamp('last_attempt_at', { withTimezone: true }),
    /** Runtime response evidence kept for audit (result, ticketId, http status). */
    evidence: jsonb('evidence').$type<Record<string, unknown>>().notNull().default({}),
    lastError: text('last_error'),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    uniqueIndex('deliveries_message_uq').on(table.messageId),
    index('deliveries_state_next_attempt_idx').on(table.state, table.nextAttemptAt),
    check('deliveries_attempt_count_nonnegative', sql`${table.attemptCount} >= 0`),
  ],
);

export type Delivery = typeof deliveries.$inferSelect;
export type NewDelivery = typeof deliveries.$inferInsert;

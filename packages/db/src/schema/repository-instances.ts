import {
  bigint,
  index,
  integer,
  jsonb,
  pgTable,
  text,
  timestamp,
  uniqueIndex,
  uuid,
} from 'drizzle-orm/pg-core';

import { runtimeDesiredState, runtimeInstanceState } from './enums';
import { repositories } from './repositories';

/**
 * Host-managed lifecycle for one repository's dedicated runtime instance.
 *
 * The supervisor is the only writer of process fields (pid/port/profile); the
 * platform API reads this projection to explain a repository's state. A row is
 * created when a repository is added, and the supervisor converges it to
 * `state`/`desiredState`; `reasonCode` and `waitingFor` are machine-readable so
 * the UI never has to guess why an instance is not usable.
 */
export const repositoryInstances = pgTable(
  'repository_instances',
  {
    repositoryId: uuid('repository_id')
      .primaryKey()
      .references(() => repositories.id, { onDelete: 'cascade' }),
    state: runtimeInstanceState('state').notNull().default('sleeping'),
    /** Target after a supervisor restart; an explicit sleep keeps it sleeping. */
    desiredState: runtimeDesiredState('desired_state').notNull().default('sleeping'),
    /** Stable machine code, for example `capacity_wait`, `start_failed`, `listening_paused`. */
    reasonCode: text('reason_code').notNull().default('never_started'),
    /** Sanitised owner-facing explanation. Never contains tokens or raw stderr. */
    reasonDetail: text('reason_detail'),
    /** What the instance is waiting for: capacity, execution_slot, background_service, owner, none. */
    waitingFor: text('waiting_for'),
    /** Lower numbers start first; a reply to waiting work is 10, a new task is 20. */
    queuePriority: integer('queue_priority').notNull().default(100),
    wakeRequestedAt: timestamp('wake_requested_at', { withTimezone: true }),
    lastActivityAt: timestamp('last_activity_at', { withTimezone: true }),
    lastProgressAt: timestamp('last_progress_at', { withTimezone: true }),
    lastStartedAt: timestamp('last_started_at', { withTimezone: true }),
    lastStoppedAt: timestamp('last_stopped_at', { withTimezone: true }),
    lastHealthAt: timestamp('last_health_at', { withTimezone: true }),
    pid: integer('pid'),
    /** DSH web/panel port (loopback only). */
    port: integer('port'),
    /** Machine Bridge port; a separate loopback listener from the panel. */
    bridgePort: integer('bridge_port'),
    profile: text('profile'),
    /**
     * Process ownership snapshot used to prove a stop. It records the root pid,
     * its process group and the descendants seen while the runtime was alive, so
     * a child that survives its parent can still be found and waited for.
     */
    ownership: jsonb('ownership').$type<Record<string, unknown>>().notNull().default({}),
    /** Bounded start attempts; an explicit retry resets it. */
    attemptCount: integer('attempt_count').notNull().default(0),
    errorCode: text('error_code'),
    errorDetail: text('error_detail'),
    /** Last measured cgroup memory (bytes); null until the first sample. */
    memoryCurrentBytes: bigint('memory_current_bytes', { mode: 'number' }),
    memoryPeakBytes: bigint('memory_peak_bytes', { mode: 'number' }),
    /** Hard per-instance cgroup limit, measured from the cgroup itself. */
    memoryLimitBytes: bigint('memory_limit_bytes', { mode: 'number' }),
    /** Raw cgroup `memory.events` counters (oom, oom_kill, high, max). */
    memoryEvents: jsonb('memory_events').$type<Record<string, number>>().notNull().default({}),
    /** `ok`, `resource_wait`, `oom`, `over_budget`, `unknown` — measured, never inferred. */
    resourceState: text('resource_state').notNull().default('unknown'),
    resourceDetail: text('resource_detail'),
    resourceCheckedAt: timestamp('resource_checked_at', { withTimezone: true }),
    createdAt: timestamp('created_at', { withTimezone: true }).notNull().defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    index('repository_instances_state_idx').on(table.state),
    index('repository_instances_wake_idx').on(table.wakeRequestedAt),
    uniqueIndex('repository_instances_profile_uq').on(table.profile),
    uniqueIndex('repository_instances_port_uq').on(table.port),
    uniqueIndex('repository_instances_bridge_port_uq').on(table.bridgePort),
  ],
);

export type RepositoryInstance = typeof repositoryInstances.$inferSelect;
export type NewRepositoryInstance = typeof repositoryInstances.$inferInsert;

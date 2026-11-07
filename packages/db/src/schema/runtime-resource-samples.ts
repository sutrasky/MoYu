import { bigint, index, integer, jsonb, pgTable, text, timestamp, uuid } from 'drizzle-orm/pg-core';

import { repositories } from './repositories';

/**
 * Real cgroup/host resource samples written by the supervisor. `scope` is
 * `host`, `aggregate`, `platform` or `instance`; an instance sample names its
 * repository. Only measured values are stored — no estimate is presented as a
 * measurement.
 */
export const runtimeResourceSamples = pgTable(
  'runtime_resource_samples',
  {
    id: uuid('id').primaryKey().defaultRandom(),
    scope: text('scope').notNull(),
    repositoryId: uuid('repository_id').references(() => repositories.id, { onDelete: 'cascade' }),
    memoryCurrentBytes: bigint('memory_current_bytes', { mode: 'number' }),
    memoryPeakBytes: bigint('memory_peak_bytes', { mode: 'number' }),
    memoryLimitBytes: bigint('memory_limit_bytes', { mode: 'number' }),
    memoryEvents: jsonb('memory_events').$type<Record<string, number>>().notNull().default({}),
    oomKillCount: integer('oom_kill_count').notNull().default(0),
    hostMemAvailableBytes: bigint('host_mem_available_bytes', { mode: 'number' }),
    pressure: jsonb('pressure').$type<Record<string, unknown>>().notNull().default({}),
    detail: jsonb('detail').$type<Record<string, unknown>>().notNull().default({}),
    sampledAt: timestamp('sampled_at', { withTimezone: true }).notNull().defaultNow(),
  },
  (table) => [
    index('runtime_resource_samples_scope_idx').on(table.scope, table.sampledAt),
    index('runtime_resource_samples_repository_idx').on(table.repositoryId, table.sampledAt),
  ],
);

export type RuntimeResourceSample = typeof runtimeResourceSamples.$inferSelect;
export type NewRuntimeResourceSample = typeof runtimeResourceSamples.$inferInsert;
